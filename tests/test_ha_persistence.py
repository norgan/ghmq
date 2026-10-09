"""Real Home Assistant, real transport, private disk, and zero external HTTP.

These regressions exercise the adapter's durable cooldown and pinned-identity
contracts together. Only the aiohttp session boundary is replaced; the journal,
config-entry lifecycle, config helper, and GitHubClient are production code.
"""

from __future__ import annotations

import asyncio
import copy
import importlib.util
import json
import tempfile
import threading
import time
import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import MappingProxyType
from unittest.mock import patch

try:
    import probatio
    from homeassistant import config_entries, loader
    from homeassistant.core import CoreState, HomeAssistant
    from homeassistant.exceptions import ConfigEntryNotReady, HomeAssistantError
except ModuleNotFoundError as error:
    if importlib.util.find_spec("homeassistant") is None:
        raise unittest.SkipTest(
            "Real Home Assistant dependencies are not installed"
        ) from error
    raise

from multidict import CIMultiDict

import custom_components.ghmq as adapter
from custom_components.ghmq import config_flow
from custom_components.ghmq import journal as journal_module
from custom_components.ghmq.const import DOMAIN
from custom_components.ghmq.core import (
    IdentityError,
    JournalError,
    RetryableError,
    Sender,
    build_event,
    digest,
    normalize_request,
)
from custom_components.ghmq.github import GitHubClient
from custom_components.ghmq.journal import Journal

DATA = {
    "owner": "example-owner",
    "repository": "private-relay",
    "branch": "relay/events",
    "pull_request_number": 7,
    "token": "inert-unit-test-placeholder",
    "repository_id": 101,
    "pull_request_id": 202,
}
PINS = {"repository_id": 101, "pull_request_id": 202}
FORM_DATA = {key: value for key, value in DATA.items() if key not in PINS}
REQUEST = {
    "event_id": "persistence-test",
    "event_type": "unit.test",
    "message": "Inert local regression",
    "synthetic": True,
    "ttl_seconds": 3600,
}
REPOSITORY = {"id": 101, "private": True, "default_branch": "main"}
PULL_REQUEST = {
    "id": 202,
    "state": "open",
    "head": {"ref": "relay/events", "repo": {"id": 101}},
}


class MockHTTPResponse:
    def __init__(self, body=None, *, status=200, headers=None):
        self.status = status
        self.headers = CIMultiDict(headers or {})
        self._body = json.dumps({} if body is None else body).encode()
        self.content = self

    async def read(self, count):
        result, self._body = self._body[:count], self._body[count:]
        return result

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return False


class BlockedHTTPResponse(MockHTTPResponse):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.started = asyncio.Event()
        self.release = asyncio.Event()

    async def __aenter__(self):
        self.started.set()
        await self.release.wait()
        return self


class MockHTTPSession:
    def __init__(self, *responses):
        self.responses = list(responses)
        self.calls = []

    def request(self, method, url, **kwargs):
        self.calls.append((method, url, kwargs))
        if not self.responses:
            raise AssertionError("Unexpected HTTP request at mocked boundary")
        if not url.startswith(
            "https://api.github.com/repos/example-owner/private-relay"
        ):
            raise AssertionError("Unexpected transport destination")
        return self.responses.pop(0)

    @property
    def puts(self):
        return [call for call in self.calls if call[0] == "PUT"]


def destination_responses(repository=None, pull_request=None):
    return [
        MockHTTPResponse(
            copy.deepcopy(REPOSITORY if repository is None else repository)
        ),
        MockHTTPResponse(
            copy.deepcopy(PULL_REQUEST if pull_request is None else pull_request)
        ),
    ]


def invalid_destinations():
    for field, value in (
        ("repository", None),
        ("repository", 999),
        ("pull_request", None),
        ("pull_request", 999),
        ("head_repository", None),
        ("head_repository", 999),
    ):
        repo, pr = copy.deepcopy(REPOSITORY), copy.deepcopy(PULL_REQUEST)
        target = (
            repo
            if field == "repository"
            else (pr if field == "pull_request" else pr["head"]["repo"])
        )
        if value is None:
            target.pop("id")
        else:
            target["id"] = value
        yield f"{field}-{value}", repo, pr


class RealHAPersistenceTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.hasses = []
        self.entries = []
        self.network = patch(
            "aiohttp.ClientSession._request",
            side_effect=AssertionError(
                "External HTTP is forbidden in persistence tests"
            ),
        )
        self.network_mock = self.network.start()
        self.hass = await self.new_hass()

    async def asyncTearDown(self):
        for hass, entry in self.entries:
            if getattr(entry, "runtime_data", None) is not None:
                await adapter.async_unload_entry(hass, entry)
            await entry._async_process_on_unload(hass)
        for hass in self.hasses:
            if hass.state is not CoreState.stopped:
                await hass.async_stop(force=True)
        self.network_mock.assert_not_called()
        self.network.stop()
        self.temp.cleanup()

    async def new_hass(self):
        hass = HomeAssistant(self.temp.name)
        hass.config.skip_pip = True
        hass.config_entries = config_entries.ConfigEntries(hass, {})
        loader.async_setup(hass)
        self.hasses.append(hass)
        await adapter.async_setup(hass, {})
        return hass

    def new_entry(self, *, data=None, hass=None, entry_id=None):
        hass = hass or self.hass
        entry = config_entries.ConfigEntry(
            domain=DOMAIN,
            title="GHMQ persistence regression",
            data=DATA.copy() if data is None else data,
            options={"enabled": False},
            version=1,
            minor_version=1,
            source=config_entries.SOURCE_USER,
            unique_id=f"persistence-{len(self.entries)}",
            discovery_keys=MappingProxyType({}),
            subentries_data=None,
            entry_id=entry_id,
        )
        hass.config_entries._entries[entry.entry_id] = entry
        self.entries.append((hass, entry))
        return entry

    async def setup_entry(self, entry=None, session=None):
        entry = entry or self.new_entry()
        session = session or MockHTTPSession(*destination_responses())
        with patch.object(adapter, "async_get_clientsession", return_value=session):
            self.assertTrue(await adapter.async_setup_entry(self.hass, entry))
        entry._async_set_state(self.hass, config_entries.ConfigEntryState.LOADED, None)
        return entry, session

    async def seed_pending(self, entry, *, count=3):
        request = normalize_request(REQUEST)
        state = {"pending": {}, "recent": {}}
        for number in range(count):
            event_id = f"pending-{number}"
            state["pending"][event_id] = {
                "event": build_event(event_id, request, datetime.now(timezone.utc)),
                "request": request,
                "request_hash": digest(request),
            }
        await Journal.for_hass(self.hass, entry.entry_id).async_save(state)
        return state

    async def test_make_client_copies_pins_and_validation_never_replaces_them(self):
        data = DATA.copy()
        session = MockHTTPSession(*destination_responses())
        with patch.object(adapter, "async_get_clientsession", return_value=session):
            client = adapter.make_client(self.hass, data)
        self.assertIsInstance(client, GitHubClient)
        data.update(repository_id=999, pull_request_id=888)
        self.assertEqual(await client.validate_destination(), PINS)
        self.assertEqual((client._repository_id, client._pull_request_id), (101, 202))
        self.assertEqual(session.puts, [])

    async def test_successful_setup_uses_real_client_and_entry_pins(self):
        entry, session = await self.setup_entry()
        client = entry.runtime_data.sender.client
        self.assertIsInstance(client, GitHubClient)
        self.assertEqual((client._repository_id, client._pull_request_id), (101, 202))
        self.assertEqual(dict(entry.data), DATA)
        self.assertEqual([call[0] for call in session.calls], ["GET", "GET"])
        self.assertEqual(session.puts, [])

    async def test_rate_limited_setup_persists_and_blocks_new_homeassistant_instance(
        self,
    ):
        entry = self.new_entry()
        session = MockHTTPSession(
            MockHTTPResponse(status=429, headers={"Retry-After": "120"})
        )
        before = time.time()
        with patch.object(adapter, "async_get_clientsession", return_value=session):
            with self.assertRaises(ConfigEntryNotReady):
                await adapter.async_setup_entry(self.hass, entry)
        persisted = json.loads(
            Journal.for_hass(self.hass, entry.entry_id).path.read_text()
        )
        self.assertEqual(persisted["version"], 2)
        self.assertGreaterEqual(persisted["not_before"], before + 120)
        self.assertEqual(persisted["state"], {"pending": {}, "recent": {}})
        await self.hass.async_stop(force=True)
        restarted = await self.new_hass()
        restarted_entry = self.new_entry(hass=restarted, entry_id=entry.entry_id)
        with patch.object(adapter, "async_get_clientsession", return_value=session):
            with self.assertRaises(ConfigEntryNotReady):
                await adapter.async_setup_entry(restarted, restarted_entry)
            with self.assertRaises(RetryableError):
                await adapter.async_validate_config(
                    restarted, DATA, entry=restarted_entry
                )
        self.assertEqual(len(session.calls), 1)
        self.assertEqual(session.puts, [])
        self.assertFalse(hasattr(restarted_entry, "runtime_data"))

    async def test_existing_future_cooldown_blocks_setup_without_one_http_request(self):
        entry = self.new_entry()
        journal = Journal.for_hass(self.hass, entry.entry_id)
        await journal.async_set_cooldown(time.time() + 120)
        session = MockHTTPSession()
        with patch.object(adapter, "async_get_clientsession", return_value=session):
            for _ in range(2):
                with self.assertRaises(ConfigEntryNotReady):
                    await adapter.async_setup_entry(self.hass, entry)
            with self.assertRaises(RetryableError):
                await adapter.async_validate_config(self.hass, DATA, entry=entry)
        self.assertEqual(session.calls, [])

    async def test_invalid_durable_cooldown_blocks_setup_and_helper_before_http(self):
        for deadline in (True, -1, "120", None):
            with self.subTest(deadline=deadline):
                entry = self.new_entry()
                path = Journal.for_hass(self.hass, entry.entry_id).path
                path.parent.mkdir(exist_ok=True)
                path.write_text(
                    json.dumps(
                        {
                            "version": 2,
                            "state": {"pending": {}, "recent": {}},
                            "not_before": deadline,
                        }
                    )
                )
                session = MockHTTPSession()
                with patch.object(
                    adapter, "async_get_clientsession", return_value=session
                ):
                    with self.assertRaises(ConfigEntryNotReady):
                        await adapter.async_setup_entry(self.hass, entry)
                    with self.assertRaises(JournalError):
                        await adapter.async_validate_config(
                            self.hass, DATA, entry=entry
                        )
                self.assertEqual(session.calls, [])
                self.assertFalse(hasattr(entry, "runtime_data"))

    async def test_expired_persisted_cooldown_allows_setup_validation(self):
        entry = self.new_entry()
        journal = Journal.for_hass(self.hass, entry.entry_id)
        await journal.async_set_cooldown(time.time() - 30)
        entry, session = await self.setup_entry(entry)
        self.assertEqual(len(session.calls), 2)
        self.assertEqual(
            entry.runtime_data.sender.store.get_cooldown_deadline(),
            journal.get_cooldown_deadline(),
        )

    async def test_separate_initial_config_flows_share_hashed_durable_cooldown(self):
        session = MockHTTPSession(
            MockHTTPResponse(status=429, headers={"Retry-After": "120"})
        )
        with patch.object(adapter, "async_get_clientsession", return_value=session):
            for token in ("inert-first-token", "inert-second-token"):
                flow = config_flow.ConfigFlow()
                flow.hass = self.hass
                flow.context = {}
                result = await flow.async_step_user({**FORM_DATA, "token": token})
                self.assertEqual(result["type"], "form")
                self.assertEqual(result["errors"], {"base": "rate_limited"})
                self.assertIn("UTC", result["description_placeholders"]["retry_at"])
        journals = list(Path(self.temp.name).glob(".storage/ghmq.setup_*.journal"))
        self.assertEqual(len(journals), 1)
        self.assertNotIn(DATA["owner"], journals[0].name)
        self.assertNotIn(DATA["repository"], journals[0].name)
        self.assertEqual(json.loads(journals[0].read_text())["version"], 2)
        self.assertEqual(len(session.calls), 1)
        self.assertEqual(session.puts, [])

    async def test_legacy_ambiguous_setup_deadline_survives_restart_and_new_token(self):
        # V2 has no deadline provenance. A previously inflated deadline must
        # remain intact rather than risk discarding a legitimate server limit.
        first = MockHTTPSession(
            MockHTTPResponse(status=429, headers={"Retry-After": "3600"})
        )
        with patch.object(adapter, "async_get_clientsession", return_value=first):
            with self.assertRaises(RetryableError):
                await adapter.async_validate_config(self.hass, FORM_DATA, discover=True)
        path = next(Path(self.temp.name).glob(".storage/ghmq.setup_*.journal"))
        original = path.read_bytes()
        await self.hass.async_stop(force=True)
        restarted = await self.new_hass()
        blocked = MockHTTPSession()
        flow = config_flow.ConfigFlow()
        flow.hass = restarted
        flow.context = {}
        with patch.object(adapter, "async_get_clientsession", return_value=blocked):
            result = await flow.async_step_user(
                {**FORM_DATA, "token": "inert-replacement-token"}
            )
        self.assertEqual(result["errors"], {"base": "rate_limited"})
        self.assertIn("UTC", result["description_placeholders"]["retry_at"])
        self.assertEqual(path.read_bytes(), original)
        self.assertEqual(blocked.calls, [])

    async def test_initial_discovery_returns_server_pins_without_mutating_form(self):
        session = MockHTTPSession(*destination_responses())
        original = FORM_DATA.copy()
        with patch.object(adapter, "async_get_clientsession", return_value=session):
            pins = await adapter.async_validate_config(
                self.hass, FORM_DATA, discover=True
            )
        self.assertEqual(pins, PINS)
        self.assertEqual(FORM_DATA, original)
        self.assertEqual(session.puts, [])

    async def test_missing_or_invalid_local_pins_block_setup_and_reauth_before_http(
        self,
    ):
        session = MockHTTPSession()
        for name in PINS:
            for bad_value in (None, 0, -1, True, "101"):
                with self.subTest(name=name, value=bad_value):
                    data = {**DATA, name: bad_value}
                    entry = self.new_entry(data=data)
                    with patch.object(
                        adapter, "async_get_clientsession", return_value=session
                    ):
                        with self.assertRaises(ConfigEntryNotReady):
                            await adapter.async_setup_entry(self.hass, entry)
                        with self.assertRaises(IdentityError):
                            await adapter.async_validate_config(
                                self.hass, data, entry=entry
                            )
                    self.assertFalse(hasattr(entry, "runtime_data"))
        self.assertEqual(session.calls, [])

    async def test_missing_local_pins_block_delivery_before_http(self):
        for name in PINS:
            with self.subTest(name=name):
                entry = self.new_entry(
                    data={key: value for key, value in DATA.items() if key != name}
                )
                session = MockHTTPSession()
                with patch.object(
                    adapter, "async_get_clientsession", return_value=session
                ):
                    client = adapter.make_client(self.hass, entry.data)
                journal = Journal.for_hass(self.hass, entry.entry_id)
                sender = Sender(client, journal)
                await sender.load()
                adapter.bind_journal(client, journal)
                with self.assertRaises(IdentityError):
                    await sender.send(REQUEST)
                self.assertEqual(session.calls, [])
                self.assertIn(REQUEST["event_id"], sender.state["pending"])

    async def test_missing_or_changed_server_ids_fail_setup_with_zero_put(self):
        for name, repo, pr in invalid_destinations():
            with self.subTest(destination=name):
                entry = self.new_entry()
                session = MockHTTPSession(*destination_responses(repo, pr))
                with patch.object(
                    adapter, "async_get_clientsession", return_value=session
                ):
                    with self.assertRaises(ConfigEntryNotReady):
                        await adapter.async_setup_entry(self.hass, entry)
                self.assertFalse(hasattr(entry, "runtime_data"))
                self.assertEqual(dict(entry.data), DATA)
                self.assertEqual(session.puts, [])

    async def test_missing_or_changed_server_ids_fail_reauth_helper_with_zero_put(self):
        for name, repo, pr in invalid_destinations():
            with self.subTest(destination=name):
                entry = self.new_entry()
                session = MockHTTPSession(*destination_responses(repo, pr))
                with patch.object(
                    adapter, "async_get_clientsession", return_value=session
                ):
                    with self.assertRaises(IdentityError):
                        await adapter.async_validate_config(
                            self.hass, {**DATA, "token": "inert-new-token"}, entry=entry
                        )
                self.assertEqual(dict(entry.data), DATA)
                self.assertEqual(session.puts, [])

    async def test_missing_or_changed_server_ids_fail_service_delivery_with_zero_put(
        self,
    ):
        for name, repo, pr in invalid_destinations():
            with self.subTest(destination=name):
                session = MockHTTPSession(
                    *destination_responses(), *destination_responses(repo, pr)
                )
                entry, _ = await self.setup_entry(session=session)
                entry.runtime_data.enabled = True
                with self.assertRaises(HomeAssistantError):
                    await self.hass.services.async_call(
                        DOMAIN,
                        "send_event",
                        {**REQUEST, "config_entry_id": entry.entry_id},
                        blocking=True,
                        return_response=True,
                    )
                sender = entry.runtime_data.sender
                self.assertIn(REQUEST["event_id"], sender.state["pending"])
                self.assertEqual(sender.state["recent"], {})
                self.assertEqual(
                    (sender.client._repository_id, sender.client._pull_request_id),
                    (101, 202),
                )
                self.assertEqual(session.puts, [])

    async def test_forged_service_pin_inputs_are_rejected_without_http(self):
        entry, session = await self.setup_entry()
        entry.runtime_data.enabled = True
        for name in PINS:
            with self.subTest(name=name), self.assertRaises(probatio.Invalid):
                await self.hass.services.async_call(
                    DOMAIN,
                    "send_event",
                    {**REQUEST, name: 999},
                    blocking=True,
                    return_response=True,
                )
        self.assertEqual(len(session.calls), 2)
        self.assertEqual(session.puts, [])

    async def test_headerless_secondary_limit_does_not_request_for_each_pending_event(
        self,
    ):
        entry = self.new_entry()
        initial = await self.seed_pending(entry)
        session = MockHTTPSession(
            *destination_responses(),
            MockHTTPResponse(
                {"message": "You have exceeded a secondary rate limit."}, status=403
            ),
        )
        entry, _ = await self.setup_entry(entry, session)
        await entry.runtime_data.sender.retry_pending()
        await entry.runtime_data.sender.retry_pending()
        self.assertEqual(len(session.calls), 3)
        self.assertEqual(session.puts, [])
        persisted = json.loads(entry.runtime_data.sender.store.path.read_text())
        self.assertEqual(persisted["state"], initial)
        self.assertGreater(persisted["not_before"], time.time() + 50)

    async def test_live_reauth_journal_preserves_queue_and_cools_existing_sender(self):
        entry = self.new_entry()
        initial = await self.seed_pending(entry)
        entry, delivery_session = await self.setup_entry(entry)
        sender = entry.runtime_data.sender
        journal = sender.store
        validation_session = MockHTTPSession(
            MockHTTPResponse(status=429, headers={"Retry-After": "120"})
        )
        with (
            patch.object(
                adapter, "async_get_clientsession", return_value=validation_session
            ),
            patch.object(
                Journal,
                "for_hass",
                side_effect=AssertionError("Live journal must be reused"),
            ),
            patch.object(
                journal,
                "async_load",
                side_effect=AssertionError("Live state must not be reloaded"),
            ),
        ):
            with self.assertRaises(RetryableError):
                await adapter.async_validate_config(self.hass, DATA, entry=entry)
        await sender.retry_pending()
        self.assertEqual(len(delivery_session.calls), 2)
        self.assertEqual(len(validation_session.calls), 1)
        self.assertEqual(json.loads(journal.path.read_text())["state"], initial)
        self.assertEqual(sender.state, initial)
        self.assertGreater(sender.client.not_before, time.time() + 100)
        self.assertEqual(delivery_session.puts + validation_session.puts, [])

    async def test_failed_live_cooldown_save_blocks_both_new_and_existing_clients(self):
        entry, session = await self.setup_entry()
        journal = entry.runtime_data.sender.store
        validation_session = MockHTTPSession(MockHTTPResponse(status=429))
        with (
            patch.object(
                adapter, "async_get_clientsession", return_value=validation_session
            ),
            patch.object(
                journal, "_write_atomic", side_effect=OSError("injected local failure")
            ),
        ):
            with self.assertRaises(JournalError):
                await adapter.async_validate_config(self.hass, DATA, entry=entry)
        with self.assertRaises(JournalError):
            await entry.runtime_data.sender.client.validate_destination()
        with patch.object(
            adapter, "async_get_clientsession", return_value=validation_session
        ):
            with self.assertRaises(JournalError):
                await adapter.async_validate_config(self.hass, DATA, entry=entry)
        self.assertEqual(len(session.calls), 2)
        self.assertEqual(len(validation_session.calls), 1)
        self.assertEqual(session.puts + validation_session.puts, [])

    async def test_factory_reuses_same_journal_for_same_hass_and_entry(self):
        first = Journal.for_hass(self.hass, "shared-entry")
        second = Journal.for_hass(self.hass, "shared-entry")
        other = Journal.for_hass(self.hass, "other-entry")
        self.assertIs(first, second)
        self.assertIsNot(first, other)
        await first.async_set_cooldown(time.time() + 120)
        self.assertAlmostEqual(
            second.get_cooldown_deadline(), first.get_cooldown_deadline(), delta=0.01
        )
        restarted_hass = await self.new_hass()
        restarted_journal = Journal.for_hass(restarted_hass, "shared-entry")
        self.assertIsNot(first, restarted_journal)
        await restarted_journal.async_load()
        self.assertAlmostEqual(
            restarted_journal.get_cooldown_deadline(),
            first.get_cooldown_deadline(),
            delta=0.01,
        )

    async def test_concurrent_setup_validators_cannot_shorten_shared_cooldown(self):
        long_response = BlockedHTTPResponse(status=429, headers={"Retry-After": "120"})
        short_response = BlockedHTTPResponse(status=429, headers={"Retry-After": "60"})
        session = MockHTTPSession(long_response, short_response)
        clients = []
        journal_init = Journal.__init__

        def journal_with_fixed_clock(store, *args, **kwargs):
            journal_init(
                store,
                *args,
                **kwargs,
                wall_clock=lambda: 1000.0,
                monotonic=lambda: 100.0,
            )

        def client_with_fixed_clock(*args, **kwargs):
            client = GitHubClient(
                *args, **kwargs, wall_clock=lambda: 1000.0, monotonic=lambda: 100.0
            )
            clients.append(client)
            return client

        tasks = []
        try:
            with (
                patch.object(adapter, "async_get_clientsession", return_value=session),
                patch.object(
                    adapter, "GitHubClient", side_effect=client_with_fixed_clock
                ),
                patch.object(Journal, "__init__", journal_with_fixed_clock),
            ):
                tasks.append(
                    asyncio.create_task(
                        adapter.async_validate_config(
                            self.hass, FORM_DATA, discover=True
                        )
                    )
                )
                await asyncio.wait_for(long_response.started.wait(), 2)
                tasks.append(
                    asyncio.create_task(
                        adapter.async_validate_config(
                            self.hass, FORM_DATA, discover=True
                        )
                    )
                )
                await asyncio.wait_for(short_response.started.wait(), 2)
                long_response.release.set()
                with self.assertRaises(RetryableError):
                    await tasks[0]
                short_response.release.set()
                with self.assertRaises(RetryableError):
                    await tasks[1]
            paths = list(Path(self.temp.name).glob(".storage/ghmq.setup_*.journal"))
            self.assertEqual(len(paths), 1)
            self.assertEqual(json.loads(paths[0].read_text())["not_before"], 1120.0)
            for client in clients:
                with self.assertRaises(RetryableError) as error:
                    await client.validate_destination(discover=True)
                self.assertGreaterEqual(error.exception.delay, 120)
            self.assertEqual(len(session.calls), 2)
            self.assertEqual(session.puts, [])
        finally:
            long_response.release.set()
            short_response.release.set()
            await asyncio.gather(*tasks, return_exceptions=True)

    async def test_shared_pending_commit_blocks_other_validator_after_deadline_passes(
        self,
    ):
        limited = BlockedHTTPResponse(status=429, headers={"Retry-After": "120"})
        repository = BlockedHTTPResponse(REPOSITORY)
        session = MockHTTPSession(limited, repository)
        clock = {"wall": 1000.0, "mono": 100.0}
        clients = []
        disk_started = threading.Event()
        disk_release = threading.Event()
        writer = journal_module.private_atomic_writer
        journal_init = Journal.__init__

        def journal_with_clock(store, *args, **kwargs):
            journal_init(
                store,
                *args,
                **kwargs,
                wall_clock=lambda: clock["wall"],
                monotonic=lambda: clock["mono"],
            )

        def client_with_clock(*args, **kwargs):
            client = GitHubClient(
                *args,
                **kwargs,
                wall_clock=lambda: clock["wall"],
                monotonic=lambda: clock["mono"],
            )
            clients.append(client)
            return client

        def blocked_writer(*args, **kwargs):
            disk_started.set()
            if not disk_release.wait(4):
                raise RuntimeError("Test disk barrier timed out")
            writer(*args, **kwargs)

        tasks = []
        try:
            with (
                patch.object(adapter, "async_get_clientsession", return_value=session),
                patch.object(adapter, "GitHubClient", side_effect=client_with_clock),
                patch.object(Journal, "__init__", journal_with_clock),
                patch.object(
                    journal_module, "private_atomic_writer", side_effect=blocked_writer
                ),
            ):
                tasks.append(
                    asyncio.create_task(
                        adapter.async_validate_config(
                            self.hass, FORM_DATA, discover=True
                        )
                    )
                )
                await asyncio.wait_for(limited.started.wait(), 2)
                tasks.append(
                    asyncio.create_task(
                        adapter.async_validate_config(
                            self.hass, FORM_DATA, discover=True
                        )
                    )
                )
                await asyncio.wait_for(repository.started.wait(), 2)
                limited.release.set()
                self.assertTrue(await asyncio.to_thread(disk_started.wait, 2))
                journal = clients[0]._current_deadline.__self__
                self.assertTrue(journal.is_cooldown_pending())
                self.assertEqual(journal.get_cooldown_deadline(), 1120.0)
                clock.update(wall=2000.0, mono=1100.0)
                repository.release.set()
                with self.assertRaises(RetryableError):
                    await asyncio.wait_for(tasks[1], 2)
                self.assertEqual(
                    len(session.calls),
                    2,
                    "PR GET must wait for durable cooldown, even past its deadline",
                )
                self.assertFalse(tasks[0].done())
                disk_release.set()
                with self.assertRaises(RetryableError):
                    await asyncio.wait_for(tasks[0], 2)
            self.assertFalse(journal.is_cooldown_pending())
            self.assertEqual(json.loads(journal.path.read_text())["not_before"], 1120.0)
            self.assertEqual(session.puts, [])
        finally:
            limited.release.set()
            repository.release.set()
            disk_release.set()
            await asyncio.gather(*tasks, return_exceptions=True)

    async def test_unload_drains_blocked_reauth_get_and_its_cooldown_commit(self):
        entry = self.new_entry()
        initial = await self.seed_pending(entry)
        entry, delivery_session = await self.setup_entry(entry)
        runtime = entry.runtime_data
        runtime.enabled = True
        journal = runtime.sender.store
        response = BlockedHTTPResponse(status=429, headers={"Retry-After": "120"})
        validation_session = MockHTTPSession(response)
        disk_started = threading.Event()
        disk_release = threading.Event()
        writer = journal._write_atomic

        def blocked_writer(*args, **kwargs):
            disk_started.set()
            if not disk_release.wait(4):
                raise RuntimeError("Test disk barrier timed out")
            writer(*args, **kwargs)

        validation = unloading = None
        try:
            with (
                patch.object(
                    adapter, "async_get_clientsession", return_value=validation_session
                ),
                patch.object(journal, "_write_atomic", side_effect=blocked_writer),
            ):
                validation = asyncio.create_task(
                    adapter.async_validate_config(self.hass, DATA, entry=entry)
                )
                await asyncio.wait_for(response.started.wait(), 2)
                unloading = asyncio.create_task(
                    adapter.async_unload_entry(self.hass, entry)
                )
                await asyncio.sleep(0.02)
                self.assertFalse(
                    unloading.done(), "Unload must drain an admitted reauth GET"
                )
                self.assertFalse(runtime.enabled)
                self.assertTrue(runtime.sender.lock.locked())
                response.release.set()
                self.assertTrue(await asyncio.to_thread(disk_started.wait, 2))
                await asyncio.sleep(0)
                self.assertFalse(
                    unloading.done(), "Unload must also drain the cooldown commit"
                )
                self.assertFalse(validation.done())
                disk_release.set()
                with self.assertRaises(RetryableError):
                    await asyncio.wait_for(validation, 2)
                self.assertTrue(await asyncio.wait_for(unloading, 2))
            persisted = json.loads(journal.path.read_text())
            self.assertEqual(persisted["state"], initial)
            self.assertGreater(persisted["not_before"], time.time() + 100)
            self.assertEqual(runtime.sender.state, initial)
            self.assertEqual(len(delivery_session.calls), 2)
            self.assertEqual(len(validation_session.calls), 1)
            self.assertEqual(delivery_session.puts + validation_session.puts, [])
        finally:
            response.release.set()
            disk_release.set()
            await asyncio.gather(
                *(task for task in (validation, unloading) if task is not None),
                return_exceptions=True,
            )

    async def test_ha_journal_factory_runs_read_and_write_outside_event_loop(self):
        journal = Journal.for_hass(self.hass, "thread-boundary-test")
        main_thread = threading.get_ident()
        threads = []
        read, write = journal._read, journal._write

        def tracked_read():
            threads.append(threading.get_ident())
            return read()

        def tracked_write(content):
            threads.append(threading.get_ident())
            return write(content)

        with (
            patch.object(journal, "_read", tracked_read),
            patch.object(journal, "_write", tracked_write),
        ):
            self.assertIsNone(await journal.async_load())
            await journal.async_set_cooldown(time.time() + 120)
        self.assertEqual(len(threads), 2)
        self.assertTrue(all(thread != main_thread for thread in threads))
        self.assertGreater(journal.get_cooldown_deadline(), time.time())


if __name__ == "__main__":
    unittest.main()
