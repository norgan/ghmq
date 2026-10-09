"""Fault-injected delivery tests. No real network or credentials are used."""

import asyncio
import unittest
from copy import deepcopy
from datetime import datetime, timedelta, timezone

from ghmq_test_subject.const import MAX_PENDING, MAX_RECENT
from ghmq_test_subject.core import (
    BridgeError,
    EventConflict,
    JournalError,
    RetryableError,
    Sender,
    SenderClosed,
    build_event,
    canonical,
    digest,
    normalize_request,
    strict_json,
    validate_state,
)


class Clock:
    def __init__(self):
        self.now = datetime(2026, 10, 9, tzinfo=timezone.utc)

    def __call__(self):
        return self.now

    def advance(self, seconds=60):
        self.now += timedelta(seconds=seconds)

    async def sleep(self, seconds):
        self.advance(seconds)


class Store:
    def __init__(self, state=None):
        self.state = deepcopy(state)
        self.calls = 0
        self.fail_on = set()
        self.block_on = set()
        self.entered = asyncio.Event()
        self.release = asyncio.Event()
        self.active = 0
        self.max_active = 0

    async def async_load(self):
        return deepcopy(self.state)

    async def async_save(self, state):
        self.calls += 1
        call = self.calls
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        try:
            if call in self.block_on:
                self.entered.set()
                await self.release.wait()
            if call in self.fail_on:
                raise OSError("sensitive path or token must never escape")
            self.state = deepcopy(state)
        finally:
            self.active -= 1


class Client:
    def __init__(self, store, clock):
        self.store, self.clock = store, clock
        self.remote = {}
        self.puts = 0
        self.calls = []
        self.hooks = {}
        self.commit_then_timeout = False
        self.put_entered = asyncio.Event()
        self.put_release = asyncio.Event()
        self.block_put = False

    async def hook(self, point):
        self.calls.append(point)
        if point in self.hooks:
            await self.hooks[point]()

    async def validate_destination(self):
        await self.hook("validate")

    async def read_event(self, path):
        await self.hook("read")
        return self.remote.get(path)

    async def prepare_create(self):
        await self.hook("pace")

    async def create_event(self, path, payload, *, expires_at=None):
        await self.hook("create")
        if self.block_put:
            self.put_entered.set()
            await self.put_release.wait()
        assert self.store.state is not None
        event_id = path.removeprefix("events/").removesuffix(".json")
        assert event_id in self.store.state["pending"], "PUT without durable admission"
        assert canonical(self.store.state["pending"][event_id]["event"]) == payload
        assert expires_at > self.clock()
        self.puts += 1
        self.remote[path] = payload
        if self.commit_then_timeout:
            self.commit_then_timeout = False
            raise RetryableError()


class SenderTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.clock = Clock()
        self.store = Store()
        self.client = Client(self.store, self.clock)
        self.sender = Sender(
            self.client, self.store, clock=self.clock, sleep=self.clock.sleep
        )
        self.request = {
            "event_id": "event-001",
            "event_type": "test.ping",
            "message": "synthetic",
            "ttl_seconds": 60,
        }

    async def test_failed_initial_save_freezes_later_send_and_retry(self):
        self.store.fail_on = {1}
        with self.assertRaisesRegex(JournalError, "Could not persist") as err:
            await self.sender.send(self.request)
        self.assertNotIn("sensitive", str(err.exception))
        self.assertEqual(self.sender.state["pending"], {})
        with self.assertRaises(JournalError):
            await self.sender.send(self.request)
        with self.assertRaises(JournalError):
            await self.sender.retry_pending()
        self.assertEqual(self.client.puts, 0)
        self.assertEqual(self.client.calls, [])
        self.assertFalse(self.sender.diagnostics()["journal_healthy"])

    async def test_success_then_identical_duplicate_has_one_put(self):
        first = await self.sender.send(self.request)
        second = await self.sender.send(self.request)
        self.assertEqual(first, {"event_id": "event-001", "status": "accepted"})
        self.assertEqual(first, second)
        self.assertEqual(self.client.puts, 1)
        validate_state(self.store.state)

    async def test_changed_id_content_rejected_locally(self):
        await self.sender.send(self.request)
        with self.assertRaises(EventConflict):
            await self.sender.send({**self.request, "message": "changed"})
        self.assertEqual(self.client.puts, 1)

    async def test_remote_conflict_never_overwritten(self):
        self.client.remote["events/event-001.json"] = b"other"
        with self.assertRaises(EventConflict):
            await self.sender.send(self.request)
        self.assertEqual(self.client.puts, 0)
        self.assertIn("event-001", self.store.state["pending"])

    async def test_timeout_after_commit_reconciles_exact_bytes(self):
        self.client.commit_then_timeout = True
        self.assertEqual((await self.sender.send(self.request))["status"], "accepted")
        self.assertEqual(self.client.puts, 1)
        self.assertEqual(self.client.calls.count("read"), 2)

    async def test_finalize_failure_freezes_then_restart_reconciles(self):
        self.store.fail_on = {2}
        with self.assertRaises(JournalError):
            await self.sender.send(self.request)
        self.assertEqual(self.client.puts, 1)
        self.assertIn("event-001", self.sender.state["pending"])
        with self.assertRaises(JournalError):
            await self.sender.retry_pending()
        restarted = Sender(
            self.client, self.store, clock=self.clock, sleep=self.clock.sleep
        )
        await restarted.load()
        await restarted.retry_pending()
        self.assertEqual(self.client.puts, 1)
        self.assertEqual(restarted.state["recent"]["event-001"]["status"], "accepted")

    async def test_cancellation_commits_admission_without_immediate_send(self):
        self.store.block_on = {1}
        task = asyncio.create_task(self.sender.send(self.request))
        await self.store.entered.wait()
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        await asyncio.sleep(0)
        self.assertFalse(task.done())
        self.assertTrue(self.sender.lock.locked())
        self.assertEqual(self.client.calls, [])
        self.store.release.set()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertIn("event-001", self.sender.state["pending"])
        self.assertEqual(self.sender.state, self.store.state)
        self.assertEqual(self.client.puts, 0)
        await self.sender.retry_pending()
        self.assertEqual(self.client.puts, 1)

    async def test_repeated_cancel_cannot_release_competing_write(self):
        self.store.block_on = {1}
        first = asyncio.create_task(self.sender.send(self.request))
        await self.store.entered.wait()
        first.cancel()
        second = asyncio.create_task(
            self.sender.send({**self.request, "event_id": "event-002"})
        )
        for _ in range(4):
            first.cancel()
            await asyncio.sleep(0)
        self.assertEqual(self.store.calls, 1)
        self.assertFalse(second.done())
        self.store.release.set()
        with self.assertRaises(asyncio.CancelledError):
            await first
        await second
        self.assertEqual(self.store.max_active, 1)
        self.assertIn("event-001", self.store.state["pending"])
        self.assertIn("event-002", self.store.state["recent"])

    async def test_finalization_cancellation_keeps_committed_result(self):
        self.store.block_on = {2}
        task = asyncio.create_task(self.sender.send(self.request))
        await self.store.entered.wait()
        self.assertEqual(self.client.puts, 1)
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        self.store.release.set()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertEqual(self.sender.state, self.store.state)
        self.assertEqual(self.sender.state["pending"], {})
        self.assertEqual(self.sender.state["recent"]["event-001"]["status"], "accepted")
        await self.sender.send(self.request)
        self.assertEqual(self.client.puts, 1)

    async def test_cancelled_failed_save_freezes(self):
        loop = asyncio.get_running_loop()
        reported = []
        old_handler = loop.get_exception_handler()
        loop.set_exception_handler(lambda _loop, context: reported.append(context))
        self.addCleanup(loop.set_exception_handler, old_handler)
        self.store.block_on = {1}
        self.store.fail_on = {1}
        task = asyncio.create_task(self.sender.send(self.request))
        await self.store.entered.wait()
        task.cancel()
        self.store.release.set()
        with self.assertRaises(asyncio.CancelledError):
            await task
        with self.assertRaises(JournalError):
            await self.sender.send(self.request)
        self.assertEqual(self.client.puts, 0)
        await asyncio.sleep(0)
        self.assertEqual(
            reported, [], "Raw storage errors must not leak via loop logging"
        )

    async def test_expiry_after_each_pre_put_await(self):
        for point in ("validate", "read", "pace"):
            with self.subTest(point=point):
                self.setUp()

                async def expire():
                    self.clock.advance(60)

                self.client.hooks[point] = expire
                result = await self.sender.send(self.request)
                self.assertEqual(result["status"], "expired")
                self.assertEqual(self.client.puts, 0)

    async def test_expiry_during_retry_sleep_prevents_another_request(self):
        async def fail():
            self.clock.advance(59)
            raise RetryableError(1)

        self.client.hooks["validate"] = fail
        self.assertEqual((await self.sender.send(self.request))["status"], "expired")
        self.assertEqual(self.client.calls, ["validate"])
        self.assertEqual(self.client.puts, 0)

    async def test_expired_pending_after_restart_never_contacts_remote(self):
        self.store.block_on = {1}
        task = asyncio.create_task(self.sender.send(self.request))
        await self.store.entered.wait()
        task.cancel()
        self.store.release.set()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.clock.advance(61)
        restart = Sender(self.client, self.store, clock=self.clock)
        await restart.load()
        await restart.retry_pending()
        self.assertEqual(self.client.calls, [])
        self.assertEqual(restart.state["recent"]["event-001"]["status"], "expired")

    async def test_unload_drains_active_put_and_closes_admission(self):
        self.client.block_put = True
        send = asyncio.create_task(self.sender.send(self.request))
        await self.client.put_entered.wait()
        close = asyncio.create_task(self.sender.async_close())
        await asyncio.sleep(0)
        self.assertFalse(close.done())
        later = asyncio.create_task(
            self.sender.send({**self.request, "event_id": "later"})
        )
        self.client.put_release.set()
        await send
        await close
        with self.assertRaises(SenderClosed):
            await later
        self.assertEqual(self.client.puts, 1)
        self.assertEqual(self.sender.state, self.store.state)

    async def test_unload_during_admission_drains_disk_without_put(self):
        self.store.block_on = {1}
        send = asyncio.create_task(self.sender.send(self.request))
        await self.store.entered.wait()
        close = asyncio.create_task(self.sender.async_close())
        await asyncio.sleep(0)
        close.cancel()
        await asyncio.sleep(0)
        close.cancel()
        self.assertFalse(close.done())
        self.store.release.set()
        with self.assertRaises(SenderClosed):
            await send
        with self.assertRaises(asyncio.CancelledError):
            await close
        self.assertEqual(self.client.puts, 0)
        self.assertEqual(self.sender.state, self.store.state)

    async def test_parallel_duplicate_calls_serialize(self):
        results = await asyncio.gather(
            *(self.sender.send(self.request) for _ in range(20))
        )
        self.assertTrue(all(result["status"] == "accepted" for result in results))
        self.assertEqual(self.client.puts, 1)
        self.assertEqual(self.store.max_active, 1)

    async def test_queue_capacity_rejects_without_writes(self):
        request = normalize_request(self.request)
        self.sender.state["pending"] = {
            f"e{i}": {
                "request": request,
                "request_hash": digest(request),
                "event": build_event(f"e{i}", request, self.clock()),
            }
            for i in range(MAX_PENDING)
        }
        with self.assertRaisesRegex(BridgeError, "queue is full"):
            await self.sender.send(self.request)
        self.assertEqual(self.store.calls, 0)
        self.assertEqual(self.client.puts, 0)

    async def test_invalid_restore_freezes_and_does_not_clear(self):
        self.store.state = {"pending": {"broken": {}}, "recent": {}}
        before = deepcopy(self.store.state)
        with self.assertRaises(JournalError):
            await self.sender.load()
        with self.assertRaises(JournalError):
            await self.sender.send(self.request)
        self.assertEqual(self.store.state, before)
        self.assertEqual(self.client.puts, 0)

    async def test_diagnostics_are_allowlisted_and_contain_no_content(self):
        await self.sender.send(self.request)
        output = self.sender.diagnostics()
        self.assertEqual(
            set(output),
            {
                "pending_count",
                "recent_count",
                "last_status",
                "journal_healthy",
                "closing",
            },
        )
        self.assertNotIn("synthetic", str(output))
        self.assertNotIn("event-001", str(output))


class ValidationTests(unittest.TestCase):
    def setUp(self):
        self.clock = Clock()
        self.request = normalize_request(
            {"event_type": "test.ping", "message": "synthetic", "ttl_seconds": 60}
        )
        self.record = {
            "request": self.request,
            "request_hash": digest(self.request),
            "event": build_event("one", self.request, self.clock()),
        }
        self.state = {"pending": {"one": self.record}, "recent": {}}

    def test_valid_pending_and_recent(self):
        validate_state(self.state)
        validate_state(
            {
                "pending": {},
                "recent": {
                    "one": {
                        "request": self.request,
                        "request_hash": digest(self.request),
                        "status": "accepted",
                    }
                },
            }
        )

    def test_rejects_corrupt_exact_fields_hashes_times_and_types(self):
        mutations = [
            lambda s: s.update(extra=True),
            lambda s: s["recent"].update(
                one={
                    "request": self.request,
                    "request_hash": digest(self.request),
                    "status": "accepted",
                }
            ),
            lambda s: s["pending"]["one"].update(extra=True),
            lambda s: s["pending"]["one"].update(request_hash="0" * 64),
            lambda s: s["pending"]["one"]["request"].update(ttl_seconds=True),
            lambda s: s["pending"]["one"]["request"].update(extra="value"),
            lambda s: s["pending"]["one"]["event"].update(event_id="different"),
            lambda s: s["pending"]["one"]["event"].update(schema_version=True),
            lambda s: s["pending"]["one"]["event"].update(source="other"),
            lambda s: s["pending"]["one"]["event"].update(extra="value"),
            lambda s: s["pending"]["one"]["event"].update(
                expires_at="2026-10-09T00:02:00Z"
            ),
            lambda s: s["pending"]["one"]["event"].update(occurred_at="2026-10-09"),
            lambda s: s["pending"].update({"../escape": s["pending"].pop("one")}),
        ]
        for mutate in mutations:
            with self.subTest(mutate=mutate):
                state = deepcopy(self.state)
                mutate(state)
                with self.assertRaises(JournalError):
                    validate_state(state)

    def test_recent_status_and_size_bound(self):
        for bad in ("queued", None, [], True):
            state = {
                "pending": {},
                "recent": {
                    "one": {
                        "request": self.request,
                        "request_hash": digest(self.request),
                        "status": bad,
                    }
                },
            }
            with self.assertRaises(JournalError):
                validate_state(state)
        state = {
            "pending": {},
            "recent": {
                str(i): {
                    "request": self.request,
                    "request_hash": digest(self.request),
                    "status": "accepted",
                }
                for i in range(MAX_RECENT + 1)
            },
        }
        with self.assertRaises(JournalError):
            validate_state(state)

    def test_strict_json_rejects_duplicates_and_nonfinite(self):
        for raw in (
            b'{"x":1,"x":2}',
            b'{"x":NaN}',
            b'{"x":Infinity}',
            b'{"x":1e999}',
            b'{"x":"\\ud800"}',
            b"\xff",
            b"{",
        ):
            with self.subTest(raw=raw), self.assertRaises(BridgeError):
                strict_json(raw)

    def test_request_rejects_unsupported_and_invalid_utf8(self):
        for update in (
            {"token": "secret"},
            {"ttl_seconds": True},
            {"message": "\ud800"},
            {"synthetic": 1},
            {"severity": []},
            {"occurred_at": "yesterday"},
        ):
            with self.subTest(update=update), self.assertRaises(BridgeError):
                normalize_request(
                    {"event_type": "test.ping", "message": "synthetic", **update}
                )

    def test_recent_request_must_fit_an_admissible_envelope(self):
        request = normalize_request(
            {
                "event_type": "test.ping",
                "message": "synthetic",
                "entity_id": "x" * 5000 + ".entity",
            }
        )
        state = {
            "pending": {},
            "recent": {
                "one": {
                    "request": request,
                    "request_hash": digest(request),
                    "status": "accepted",
                }
            },
        }
        with self.assertRaises(JournalError):
            validate_state(state)
