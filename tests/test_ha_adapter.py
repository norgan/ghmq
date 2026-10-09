"""Contract tests against the real installed Home Assistant, never HA stubs.

Run with Python 3.14.2 + homeassistant==2026.10.0. The network boundary is
replaced with an inert in-memory client; no live HA instance or GitHub is used.
Absent Home Assistant makes this module an explicit skip, not a passing shim.
"""

from __future__ import annotations

import asyncio
import importlib.metadata
import importlib.util
import json
import stat
import tempfile
import threading
import unittest
from pathlib import Path
from types import MappingProxyType
from unittest.mock import AsyncMock, patch

try:
    import probatio
    from homeassistant import config_entries, loader, setup
    from homeassistant.const import EVENT_HOMEASSISTANT_STOP
    from homeassistant.core import CoreState, HomeAssistant, callback
    from homeassistant.exceptions import (
        ConfigEntryAuthFailed,
        ConfigEntryNotReady,
        HomeAssistantError,
        ServiceValidationError,
    )
except ModuleNotFoundError as error:
    if importlib.util.find_spec("homeassistant") is None:
        raise unittest.SkipTest(
            "Real Home Assistant dependencies are not installed"
        ) from error
    raise

import custom_components.ghmq as adapter
from custom_components.ghmq import config_flow
from custom_components.ghmq import journal as journal_module
from custom_components.ghmq.const import CONF_ENABLED, DOMAIN
from custom_components.ghmq.core import (
    AuthError,
    BridgeError,
    JournalError,
    digest,
    normalize_request,
)
from custom_components.ghmq.diagnostics import async_get_config_entry_diagnostics
from custom_components.ghmq.journal import Journal

EMPTY = {"pending": {}, "recent": {}}
DATA = {
    "owner": "example-owner",
    "repository": "ghmq",
    "branch": "relay/events",
    "pull_request_number": 1,
    "token": "inert-unit-test-placeholder",
    "repository_id": 101,
    "pull_request_id": 202,
}
PINS = {
    "repository_id": DATA["repository_id"],
    "pull_request_id": DATA["pull_request_id"],
}
FORM_DATA = {key: value for key, value in DATA.items() if key not in PINS}
REQUEST = {
    "event_id": "adapter-test",
    "event_type": "unit.test",
    "message": "Inert local test",
    "synthetic": True,
}


class InertGitHub:
    def __init__(self):
        self.validations = 0
        self.reads = 0
        self.prepares = 0
        self.writes: list[tuple[str, bytes]] = []
        self.remote: dict[str, bytes] = {}
        self.validate_error = None
        self.read_error = None
        self.read_started = None
        self.read_release = None

    def bind_cooldown(
        self,
        deadline,
        persist,
        *,
        current_deadline=None,
        announce_deadline=None,
        is_cooldown_pending=None,
    ):
        self.cooldown_deadline = deadline
        self.persist_cooldown = persist
        self.current_deadline = current_deadline
        self.announce_deadline = announce_deadline
        self.is_cooldown_pending = is_cooldown_pending

    async def validate_destination(self, *, discover=False):
        self.validations += 1
        if self.validate_error:
            raise self.validate_error
        return PINS.copy()

    async def read_event(self, path):
        self.reads += 1
        if self.read_started:
            self.read_started.set()
            await self.read_release.wait()
        if self.read_error:
            raise self.read_error
        return self.remote.get(path)

    async def prepare_create(self):
        self.prepares += 1

    async def create_event(self, path, payload, *, expires_at):
        assert expires_at.utcoffset() is not None
        self.writes.append((path, payload))
        self.remote[path] = payload


class RealHomeAssistantTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.hass = HomeAssistant(self.temp.name)
        self.hass.config.skip_pip = True
        self.hass.config_entries = config_entries.ConfigEntries(self.hass, {})
        loader.async_setup(self.hass)
        self.clients = []
        self.entries = []
        # A test accidentally crossing the external HTTP boundary must fail.
        self.network_patch = patch(
            "aiohttp.ClientSession._request",
            side_effect=AssertionError("External HTTP is forbidden in adapter tests"),
        )
        self.network_patch.start()
        await adapter.async_setup(self.hass, {})

    async def asyncTearDown(self):
        for client in self.clients:
            if client.read_release:
                client.read_release.set()
        for entry in self.entries:
            if getattr(entry, "runtime_data", None) is not None:
                await adapter.async_unload_entry(self.hass, entry)
            await entry._async_process_on_unload(self.hass)
        if self.hass.state is not CoreState.stopped:
            await self.hass.async_stop(force=True)
        self.network_patch.stop()
        self.temp.cleanup()

    def new_entry(
        self, *, enabled=False, state=config_entries.ConfigEntryState.NOT_LOADED
    ):
        entry = config_entries.ConfigEntry(
            domain=DOMAIN,
            title="GHMQ local test",
            data=DATA.copy(),
            options={CONF_ENABLED: enabled},
            version=1,
            minor_version=1,
            source=config_entries.SOURCE_USER,
            unique_id=f"test-{len(self.entries)}",
            discovery_keys=MappingProxyType({}),
            subentries_data=None,
            state=state,
        )
        self.hass.config_entries._entries[entry.entry_id] = entry
        self.entries.append(entry)
        return entry

    async def setup_entry(self, *, enabled=False, client=None):
        client = client or InertGitHub()
        self.clients.append(client)
        entry = self.new_entry(enabled=enabled)
        with patch.object(adapter, "make_client", return_value=client):
            self.assertTrue(await adapter.async_setup_entry(self.hass, entry))
        entry._async_set_state(self.hass, config_entries.ConfigEntryState.LOADED, None)
        return entry, client

    async def call(self, data=None):
        return await self.hass.services.async_call(
            DOMAIN,
            "send_event",
            REQUEST.copy() if data is None else data,
            blocking=True,
            return_response=True,
        )

    async def test_actual_homeassistant_loader_and_optional_response(self):
        self.assertTrue(importlib.metadata.version("homeassistant"))
        self.assertTrue(await setup.async_setup_component(self.hass, DOMAIN, {}))
        self.assertTrue(self.hass.services.has_service(DOMAIN, "send_event"))
        service = self.hass.services.async_services()[DOMAIN]["send_event"]
        self.assertEqual(service.supports_response.value, "optional")

    async def test_real_config_entry_manager_setup_and_unload(self):
        entry = self.new_entry()
        client = InertGitHub()
        with patch.object(adapter, "make_client", return_value=client):
            self.assertTrue(await self.hass.config_entries.async_setup(entry.entry_id))
        self.assertEqual(entry.state, config_entries.ConfigEntryState.LOADED)
        self.assertIsInstance(entry.runtime_data, adapter.Runtime)
        self.assertEqual(client.validations, 1)
        self.assertEqual(client.writes, [])
        self.assertTrue(await self.hass.config_entries.async_unload(entry.entry_id))
        self.assertEqual(entry.state, config_entries.ConfigEntryState.NOT_LOADED)
        self.assertFalse(hasattr(entry, "runtime_data"))

    async def test_service_schema_rejects_extra_destinations_and_credentials(self):
        for name in ("token", "repository", "owner", "image", "context", "url"):
            with self.subTest(name=name), self.assertRaises(probatio.Invalid):
                await self.call({**REQUEST, name: "forbidden"})

    async def test_service_schema_defaults_and_required_fields(self):
        schema = self.hass.services.async_services()[DOMAIN]["send_event"].schema
        result = schema({"event_type": "test", "message": "local"})
        self.assertEqual(result["ttl_seconds"], 300)
        self.assertEqual(result["severity"], "info")
        self.assertFalse(result["synthetic"])
        for data in ({}, {"message": "local"}, {"event_type": "test"}):
            with self.subTest(data=data), self.assertRaises(probatio.Invalid):
                schema(data)

    async def test_disabled_setup_only_validates_and_never_sends(self):
        entry, client = await self.setup_entry()
        self.assertFalse(entry.runtime_data.enabled)
        self.assertIs(
            entry.runtime_data.sender.store._write_atomic,
            journal_module.private_atomic_writer,
        )
        self.assertEqual(client.validations, 1)
        self.assertEqual(client.reads, 0)
        self.assertEqual(client.writes, [])
        self.assertEqual(entry._background_tasks, set())
        self.assertEqual(entry.update_listeners, [])
        with self.assertRaises(ServiceValidationError):
            await self.call()
        self.assertEqual(client.writes, [])

    async def test_no_loaded_entry_fails_before_network(self):
        with self.assertRaises(ServiceValidationError):
            await self.call()
        entry, client = await self.setup_entry(enabled=True)
        entry._async_set_state(
            self.hass, config_entries.ConfigEntryState.UNLOAD_IN_PROGRESS, None
        )
        with self.assertRaises(ServiceValidationError):
            await self.call()
        self.assertEqual(client.writes, [])

    async def test_enabled_service_persists_and_returns_duplicate_without_write(self):
        entry, client = await self.setup_entry(enabled=True)
        response = await self.call()
        self.assertEqual(
            response, {"event_id": REQUEST["event_id"], "status": "accepted"}
        )
        self.assertEqual(len(client.writes), 1)
        self.assertEqual(await self.call(), response)
        self.assertEqual(len(client.writes), 1)
        journal = json.loads(entry.runtime_data.sender.store.path.read_text())
        self.assertEqual(journal["version"], 2)
        self.assertEqual(journal["not_before"], 0.0)
        self.assertEqual(journal["state"]["pending"], {})
        self.assertEqual(
            journal["state"]["recent"][REQUEST["event_id"]]["status"], "accepted"
        )

    async def test_core_validation_rejects_bool_ttl_without_disk_or_network(self):
        entry, client = await self.setup_entry(enabled=True)
        with self.assertRaises(HomeAssistantError):
            await self.call({**REQUEST, "ttl_seconds": True})
        self.assertFalse(entry.runtime_data.sender.store.path.exists())
        self.assertEqual(client.reads, 0)
        self.assertEqual(client.writes, [])

    async def test_multiple_loaded_entries_require_explicit_selection(self):
        first, first_client = await self.setup_entry(enabled=True)
        second, second_client = await self.setup_entry(enabled=True)
        with self.assertRaises(ServiceValidationError):
            await self.call()
        with self.assertRaises(ServiceValidationError):
            await self.call({**REQUEST, "config_entry_id": "unknown"})
        response = await self.call({**REQUEST, "config_entry_id": second.entry_id})
        self.assertEqual(response["status"], "accepted")
        self.assertEqual(first_client.writes, [])
        self.assertEqual(len(second_client.writes), 1)

    async def test_setup_auth_failure_uses_ha_auth_exception(self):
        client = InertGitHub()
        client.validate_error = AuthError("Credentials rejected")
        entry = self.new_entry()
        with patch.object(adapter, "make_client", return_value=client):
            with self.assertRaises(ConfigEntryAuthFailed):
                await adapter.async_setup_entry(self.hass, entry)
        self.assertFalse(hasattr(entry, "runtime_data"))

    async def test_setup_corrupt_journal_uses_ha_retryable_exception(self):
        entry = self.new_entry()
        journal = Journal.for_hass(self.hass, entry.entry_id)
        journal.path.parent.mkdir()
        journal.path.write_text(
            '{"version":1,"state":{"pending":{},"recent":{},"unknown":true}}'
        )
        client = InertGitHub()
        with patch.object(adapter, "make_client", return_value=client):
            with self.assertRaises(ConfigEntryNotReady):
                await adapter.async_setup_entry(self.hass, entry)
        self.assertFalse(hasattr(entry, "runtime_data"))
        self.assertEqual(client.writes, [])

    async def test_service_auth_failure_triggers_reauth_and_keeps_pending(self):
        entry, client = await self.setup_entry(enabled=True)
        client.read_error = AuthError("Authentication failed")
        with patch.object(config_entries.ConfigEntry, "async_start_reauth") as reauth:
            with self.assertRaisesRegex(HomeAssistantError, "reconnect"):
                await self.call()
            reauth.assert_called_once_with(self.hass)
        self.assertIn(REQUEST["event_id"], entry.runtime_data.sender.state["pending"])
        self.assertEqual(client.writes, [])

    async def test_service_persistence_failure_freezes_without_network(self):
        entry, client = await self.setup_entry(enabled=True)
        store = entry.runtime_data.sender.store
        with patch.object(
            store, "_write_atomic", side_effect=OSError("sensitive error omitted")
        ):
            with self.assertRaises(HomeAssistantError) as error:
                await self.call()
        self.assertNotIn("sensitive", str(error.exception))
        self.assertFalse(entry.runtime_data.sender.diagnostics()["journal_healthy"])
        self.assertEqual(client.reads, 0)
        self.assertEqual(client.writes, [])
        with self.assertRaises(HomeAssistantError):
            await self.call()
        self.assertEqual(client.writes, [])

    async def test_unload_closes_sender_and_real_ha_cancels_retry_task(self):
        entry, client = await self.setup_entry(enabled=True)
        runtime = entry.runtime_data
        tasks = list(entry._background_tasks)
        self.assertEqual(len(tasks), 1)
        integration = loader.Integration.resolve_from_root(
            self.hass, __import__("custom_components"), DOMAIN
        )
        self.assertIsNotNone(integration)
        async with entry.setup_lock:
            self.assertTrue(
                await entry.async_unload(self.hass, integration=integration)
            )
        self.assertEqual(entry.state, config_entries.ConfigEntryState.NOT_LOADED)
        self.assertFalse(hasattr(entry, "runtime_data"))
        self.assertFalse(runtime.enabled)
        self.assertTrue(runtime.sender.diagnostics()["closing"])
        self.assertTrue(all(task.cancelled() for task in tasks))
        with self.assertRaises(ServiceValidationError):
            await self.call()
        self.assertEqual(client.writes, [])

    async def test_unload_drains_inflight_service_and_blocks_following_mutation(self):
        entry, client = await self.setup_entry(enabled=True)
        client.read_started = asyncio.Event()
        client.read_release = asyncio.Event()
        sending = asyncio.create_task(self.call())
        await asyncio.wait_for(client.read_started.wait(), 2)
        unloading = asyncio.create_task(adapter.async_unload_entry(self.hass, entry))
        await asyncio.sleep(0)
        self.assertFalse(unloading.done())
        self.assertFalse(entry.runtime_data.enabled)
        client.read_release.set()
        with self.assertRaises(HomeAssistantError):
            await sending
        self.assertTrue(await unloading)
        self.assertEqual(client.prepares, 0)
        self.assertEqual(client.writes, [])
        self.assertIn(REQUEST["event_id"], entry.runtime_data.sender.state["pending"])

    async def test_real_ha_shutdown_drains_blocked_disk_before_final_cleanup(self):
        entry, client = await self.setup_entry(enabled=True)
        runtime = entry.runtime_data
        store = runtime.sender.store
        started = threading.Event()
        release = threading.Event()
        stop_observed = asyncio.Event()
        writer_calls = []
        loop = asyncio.get_running_loop()
        old_handler = loop.get_exception_handler()
        reports = []
        loop.set_exception_handler(lambda _loop, context: reports.append(context))

        @callback
        def observe_stop(event):
            stop_observed.set()

        remove_listener = self.hass.bus.async_listen_once(
            EVENT_HOMEASSISTANT_STOP, observe_stop
        )

        def blocked_writer(*args, **kwargs):
            writer_calls.append(args)
            started.set()
            if not release.wait(4):
                raise RuntimeError("test synchronization timeout")
            journal_module.private_atomic_writer(*args, **kwargs)

        sending = stopping = None
        try:
            with (
                self.assertNoLogs(level="WARNING"),
                patch.object(store, "_write_atomic", side_effect=blocked_writer),
            ):
                sending = asyncio.create_task(self.call())
                self.assertTrue(await asyncio.to_thread(started.wait, 2))
                stopping = asyncio.create_task(self.hass.async_stop(force=True))
                await asyncio.wait_for(stop_observed.wait(), 2)
                await asyncio.sleep(0.02)
                self.assertFalse(
                    stopping.done(),
                    "HA shutdown returned before executor disk I/O completed",
                )
                self.assertFalse(
                    sending.done(),
                    "Sender released its serialization before disk I/O completed",
                )
                self.assertEqual(self.hass.state, CoreState.stopping)
                self.assertTrue(runtime.sender.lock.locked())
                self.assertTrue(runtime.sender.diagnostics()["closing"])
                self.assertFalse(runtime.enabled)
                self.assertEqual(client.writes, [])
                with self.assertRaises(ServiceValidationError):
                    await self.call({**REQUEST, "event_id": "blocked-after-stop"})
                self.assertEqual(len(writer_calls), 1)
                release.set()
                with self.assertRaises(HomeAssistantError):
                    await asyncio.wait_for(sending, 2)
                await asyncio.wait_for(stopping, 2)
                # A later entry unload must remove its already-fired STOP
                # listener cleanly, without double-unsubscribe warnings.
                self.assertTrue(await adapter.async_unload_entry(self.hass, entry))
                await entry._async_process_on_unload(self.hass)
                self.assertFalse(entry._on_unload)
            self.assertEqual(self.hass.state, CoreState.stopped)
            self.assertEqual(client.writes, [])
            self.assertEqual(client.reads, 0)
            self.assertEqual(len(writer_calls), 1)
            persisted = json.loads(store.path.read_text())["state"]
            self.assertEqual(runtime.sender.state, persisted)
            self.assertIn(REQUEST["event_id"], persisted["pending"])
            self.assertEqual(persisted["recent"], {})
            self.assertTrue(runtime.sender.diagnostics()["journal_healthy"])
            self.assertEqual(
                reports, [], "HA shutdown must not report raw executor errors"
            )
        finally:
            release.set()
            for task in (sending, stopping):
                if task is not None:
                    await asyncio.gather(task, return_exceptions=True)
            if not stop_observed.is_set():
                remove_listener()
            loop.set_exception_handler(old_handler)

    async def test_diagnostics_excludes_credentials_and_payload(self):
        entry, _ = await self.setup_entry(enabled=True)
        await self.call()
        result = await async_get_config_entry_diagnostics(self.hass, entry)
        encoded = json.dumps(result)
        self.assertNotIn(DATA["token"], encoded)
        self.assertNotIn(REQUEST["message"], encoded)
        self.assertNotIn(DATA["owner"], encoded)
        self.assertNotIn(REQUEST["event_id"], encoded)

    async def test_config_flow_schema_and_creation_default_disabled(self):
        flow = config_flow.ConfigFlow()
        flow.hass = self.hass
        flow.context = {}
        form = await flow.async_step_user()
        self.assertEqual(form["type"], "form")
        self.assertEqual(form["step_id"], "user")
        self.assertEqual(form["data_schema"](FORM_DATA), FORM_DATA)
        with self.assertRaises(probatio.Invalid):
            form["data_schema"](DATA)
        validator = AsyncMock(return_value=PINS.copy())
        with patch.object(config_flow, "async_validate_config", validator):
            result = await flow.async_step_user(FORM_DATA)
        self.assertEqual(result["type"], "create_entry")
        self.assertEqual(result["options"], {CONF_ENABLED: False})
        self.assertEqual(result["data"], DATA)
        validator.assert_awaited_once_with(self.hass, FORM_DATA, discover=True)

    async def test_config_flow_auth_and_connection_errors_remain_forms(self):
        for error, expected in (
            (AuthError("denied"), "invalid_auth"),
            (BridgeError("invalid destination"), "cannot_connect"),
        ):
            with self.subTest(expected=expected):
                flow = config_flow.ConfigFlow()
                flow.hass = self.hass
                flow.context = {}
                with patch.object(
                    config_flow, "async_validate_config", AsyncMock(side_effect=error)
                ):
                    result = await flow.async_step_user(FORM_DATA)
                self.assertEqual(result["type"], "form")
                self.assertEqual(result["errors"], {"base": expected})

    async def test_options_flow_uses_ha_reload_contract(self):
        self.assertTrue(
            issubclass(config_flow.OptionsFlow, config_entries.OptionsFlowWithReload)
        )
        entry, _ = await self.setup_entry()
        flow = config_flow.ConfigFlow.async_get_options_flow(entry)
        flow.hass = self.hass
        flow.handler = entry.entry_id
        form = await flow.async_step_init()
        self.assertFalse(form["data_schema"]({})[CONF_ENABLED])
        result = await flow.async_step_init({CONF_ENABLED: True})
        self.assertEqual(result["data"], {CONF_ENABLED: True})
        self.assertEqual(entry.update_listeners, [])


class RealAtomicJournalTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.path = Path(self.temp.name) / ".storage" / "ghmq.test.journal"
        self.journal = Journal(
            self.path, asyncio.to_thread, journal_module.private_atomic_writer
        )

    async def asyncTearDown(self):
        self.temp.cleanup()

    async def test_production_atomic_writer_roundtrip_private_mode_and_envelope(self):
        self.assertIsNone(await self.journal.async_load())
        await self.journal.async_save(EMPTY)
        self.assertEqual(await self.journal.async_load(), EMPTY)
        self.assertEqual(
            json.loads(self.path.read_text()),
            {"version": 2, "state": EMPTY, "not_before": 0.0},
        )
        self.assertEqual(stat.S_IMODE(self.path.stat().st_mode), 0o600)
        self.assertEqual(stat.S_IMODE(self.path.parent.stat().st_mode), 0o700)

    async def test_legacy_v1_load_preserves_state_and_next_write_migrates_to_v2(self):
        request = normalize_request(REQUEST)
        state = {
            "pending": {},
            "recent": {
                REQUEST["event_id"]: {
                    "request": request,
                    "request_hash": digest(request),
                    "status": "accepted",
                }
            },
        }
        self.path.parent.mkdir()
        legacy = json.dumps({"version": 1, "state": state})
        self.path.write_text(legacy)
        self.assertEqual(await self.journal.async_load(), state)
        self.assertEqual(self.journal.get_cooldown_deadline(), 0.0)
        self.assertEqual(self.path.read_text(), legacy)
        await self.journal.async_save(state)
        self.assertEqual(
            json.loads(self.path.read_text()),
            {"version": 2, "state": state, "not_before": 0.0},
        )

    async def test_corrupt_version_and_duplicate_json_fail_closed(self):
        self.path.parent.mkdir()
        for raw in (
            "{",
            '{"version":2,"state":{"pending":{},"recent":{}}}',
            '{"version":1,"version":1,"state":{"pending":{},"recent":{}}}',
            '{"version":true,"state":{"pending":{},"recent":{}}}',
        ):
            with self.subTest(raw=raw):
                self.path.write_text(raw)
                with self.assertRaises(JournalError):
                    await self.journal.async_load()

    async def test_atomic_replace_failure_propagates_without_logging_and_keeps_previous_file(
        self,
    ):
        await self.journal.async_save(EMPTY)
        original = self.path.read_bytes()
        with (
            self.assertNoLogs(level="ERROR"),
            patch("os.rename", side_effect=OSError("injected replace failure")),
        ):
            with self.assertRaises(JournalError):
                await self.journal.async_save(EMPTY)
        self.assertEqual(self.path.read_bytes(), original)
        self.assertEqual(list(self.path.parent.iterdir()), [self.path])

    async def test_atomic_fsync_failure_propagates_without_logging_and_preserves_previous_file(
        self,
    ):
        await self.journal.async_save(EMPTY)
        original = self.path.read_bytes()
        with (
            self.assertNoLogs(level="ERROR"),
            patch(
                "atomicwrites._proper_fsync",
                side_effect=OSError("injected fsync failure"),
            ),
        ):
            with self.assertRaises(JournalError):
                await self.journal.async_save(EMPTY)
        self.assertEqual(self.path.read_bytes(), original)

    async def test_directory_fsync_failure_reports_uncertainty_after_replace(self):
        await self.journal.async_save(EMPTY)
        request = normalize_request(REQUEST)
        candidate = {
            "pending": {},
            "recent": {
                REQUEST["event_id"]: {
                    "request": request,
                    "request_hash": digest(request),
                    "status": "accepted",
                }
            },
        }
        # atomicwrites syncs file data before rename and its directory afterward.
        # An error at the latter stage must be reported even if new bytes exist.
        with self.assertNoLogs(level="ERROR"):
            with patch(
                "atomicwrites._proper_fsync",
                side_effect=[None, OSError("directory fsync failed")],
            ) as sync:
                with self.assertRaises(JournalError):
                    await self.journal.async_save(candidate)
                self.assertEqual(sync.call_count, 2)
        self.assertEqual(await self.journal.async_load(), candidate)

    async def test_cancelled_failed_write_never_reports_raw_executor_exception(self):
        started = threading.Event()
        release = threading.Event()
        loop = asyncio.get_running_loop()
        old_handler = loop.get_exception_handler()
        reports = []
        loop.set_exception_handler(lambda _loop, context: reports.append(context))

        def writer(*args, **kwargs):
            started.set()
            if not release.wait(3):
                raise RuntimeError("test synchronization timeout")
            raise OSError("private-path-or-token-marker")

        journal = Journal(self.path, asyncio.to_thread, writer)
        saving = asyncio.create_task(journal.async_save(EMPTY))
        try:
            self.assertTrue(await asyncio.to_thread(started.wait, 2))
            saving.cancel()
            await asyncio.sleep(0)
            release.set()
            with self.assertRaises(asyncio.CancelledError):
                await saving
            await asyncio.sleep(0)
            self.assertEqual(
                reports,
                [],
                "Canceled disk errors must not leak through asyncio callbacks",
            )
        finally:
            release.set()
            loop.set_exception_handler(old_handler)

    async def test_cancellation_drains_executor_before_releasing_journal_lock(self):
        started = threading.Event()
        release = threading.Event()
        second_started = threading.Event()
        calls = []

        def writer(*args, **kwargs):
            calls.append(args)
            if len(calls) == 1:
                started.set()
                if not release.wait(3):
                    raise RuntimeError("test synchronization timeout")
            else:
                second_started.set()
            journal_module.private_atomic_writer(*args, **kwargs)

        journal = Journal(self.path, asyncio.to_thread, writer)
        first = asyncio.create_task(journal.async_save(EMPTY))
        self.assertTrue(await asyncio.to_thread(started.wait, 2))
        first.cancel()
        second = asyncio.create_task(journal.async_save(EMPTY))
        try:
            await asyncio.sleep(0.02)
            first.cancel()
            await asyncio.sleep(0)
            self.assertFalse(first.done())
            self.assertFalse(second_started.is_set())
        finally:
            release.set()
        with self.assertRaises(asyncio.CancelledError):
            await first
        await second
        self.assertTrue(second_started.is_set())
        self.assertEqual(await journal.async_load(), EMPTY)


if __name__ == "__main__":
    unittest.main()
