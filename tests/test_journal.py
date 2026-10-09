"""Actual filesystem/executor tests for the strict journal adapter."""

import asyncio
import os
import tempfile
import threading
import unittest
from datetime import datetime, timezone
from pathlib import Path

from ghmq_test_subject.const import MAX_JOURNAL_BYTES
from ghmq_test_subject.core import (
    JournalError,
    Sender,
    build_event,
    digest,
    normalize_request,
)
from ghmq_test_subject.journal import Journal

from tests.test_core import Clock


async def executor(fn):
    return await asyncio.to_thread(fn)


def atomic_writer(path, content, *, private):
    assert private is True
    temporary = str(path) + ".tmp"
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
        stream.write(content)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)


def sample_state(event_id="one"):
    request = normalize_request({"event_type": "test.ping", "message": "synthetic"})
    return {
        "pending": {
            event_id: {
                "request": request,
                "request_hash": digest(request),
                "event": build_event(
                    event_id, request, datetime(2026, 10, 9, tzinfo=timezone.utc)
                ),
            }
        },
        "recent": {},
    }


class JournalTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.path = Path(self.temporary.name) / "private" / "journal.json"
        self.journal = Journal(self.path, executor, atomic_writer)

    async def test_round_trip_version_and_permissions(self):
        self.assertIsNone(await self.journal.async_load())
        state = sample_state()
        await self.journal.async_save(state)
        self.assertEqual(await self.journal.async_load(), state)
        self.assertEqual(self.path.stat().st_mode & 0o777, 0o600)
        self.assertEqual(self.path.parent.stat().st_mode & 0o777, 0o700)
        self.assertIn(b'"version":2', self.path.read_bytes())

    async def test_missing_is_distinct_from_empty_or_invalid(self):
        self.path.parent.mkdir()
        for content in (
            b"",
            b"null",
            b"{}",
            b'{"version":2,"state":{"pending":{},"recent":{}}}',
            b'{"version":true,"state":{"pending":{},"recent":{}}}',
            b'{"version":1,"version":1,"state":{"pending":{},"recent":{}}}',
            b'{"version":1,"state":{"pending":{},"recent":{},"bad":NaN}}',
            b'{"version":1,"state":{"pending":{},"recent":{}},"extra":1}',
        ):
            with self.subTest(content=content):
                self.path.write_bytes(content)
                with self.assertRaises(JournalError):
                    await self.journal.async_load()
                self.assertEqual(self.path.read_bytes(), content)

    async def test_oversized_input_rejected_without_rewrite(self):
        self.path.parent.mkdir()
        original = b" " * (MAX_JOURNAL_BYTES + 1)
        self.path.write_bytes(original)
        with self.assertRaises(JournalError):
            await self.journal.async_load()
        self.assertEqual(self.path.read_bytes(), original)

    async def test_invalid_save_preserves_existing_bytes(self):
        await self.journal.async_save(sample_state())
        before = self.path.read_bytes()
        with self.assertRaises(JournalError):
            await self.journal.async_save(
                {"pending": {}, "recent": {}, "extra": "sensitive"}
            )
        self.assertEqual(self.path.read_bytes(), before)

    async def test_atomic_writer_failure_is_sanitized(self):
        def broken(*args, **kwargs):
            raise OSError("SECRET_AND_PRIVATE_PATH")

        journal = Journal(self.path, executor, broken)
        with self.assertRaises(JournalError) as caught:
            await journal.async_save(sample_state())
        self.assertNotIn("SECRET", str(caught.exception))
        self.assertFalse(self.path.exists())

    async def test_repeated_cancellation_drains_executor_before_competing_save(self):
        entered, release = threading.Event(), threading.Event()
        counters = {"active": 0, "maximum": 0, "writes": 0}

        def writer(path, content, *, private):
            counters["active"] += 1
            counters["maximum"] = max(counters["maximum"], counters["active"])
            try:
                counters["writes"] += 1
                if counters["writes"] == 1:
                    entered.set()
                    if not release.wait(5):
                        raise RuntimeError("test did not release write")
                atomic_writer(path, content, private=private)
            finally:
                counters["active"] -= 1

        journal = Journal(self.path, executor, writer)
        first = asyncio.create_task(journal.async_save(sample_state("first")))
        self.assertTrue(await asyncio.to_thread(entered.wait, 5))
        first.cancel()
        second = asyncio.create_task(journal.async_save(sample_state("second")))
        for _ in range(5):
            first.cancel()
            await asyncio.sleep(0)
        self.assertFalse(first.done())
        self.assertFalse(second.done())
        self.assertEqual(counters["writes"], 1)
        release.set()
        with self.assertRaises(asyncio.CancelledError):
            await first
        await second
        self.assertEqual(counters["maximum"], 1)
        self.assertEqual(await journal.async_load(), sample_state("second"))

    async def test_failure_after_atomic_replace_freezes_sender_until_reload(self):
        writes = 0

        def uncertain(path, content, *, private):
            nonlocal writes
            atomic_writer(path, content, private=private)
            writes += 1
            if writes == 1:
                raise OSError("simulated failure after replace")

        journal = Journal(self.path, executor, uncertain)
        clock = Clock()

        class NoNetwork:
            puts = 0

            async def validate_destination(self):
                raise AssertionError("uncertain admission must not transmit")

        client = NoNetwork()
        sender = Sender(client, journal, clock=clock)
        request = {"event_id": "one", "event_type": "test.ping", "message": "synthetic"}
        with self.assertRaises(JournalError):
            await sender.send(request)
        self.assertEqual(sender.state["pending"], {})
        self.assertIn("one", (await journal.async_load())["pending"])
        with self.assertRaises(JournalError):
            await sender.send(request)
        with self.assertRaises(JournalError):
            await sender.retry_pending()
        await sender.load()
        self.assertIn("one", sender.state["pending"])
        self.assertTrue(sender.diagnostics()["journal_healthy"])

    async def test_truncated_uncertain_write_cannot_recover_as_empty(self):
        def truncated(path, content, *, private):
            Path(path).write_bytes(b'{"version":')
            raise OSError("simulated partial write")

        journal = Journal(self.path, executor, truncated)
        sender = Sender(None, journal)
        with self.assertRaises(JournalError):
            await sender.send({"event_type": "test.ping", "message": "synthetic"})
        with self.assertRaises(JournalError):
            await sender.load()
        self.assertFalse(sender.diagnostics()["journal_healthy"])
        self.assertEqual(self.path.read_bytes(), b'{"version":')

    async def test_direct_save_task_cancel_commits_admission_memory_without_put(self):
        entered, release = threading.Event(), threading.Event()

        def writer(path, content, *, private):
            entered.set()
            if not release.wait(5):
                raise RuntimeError("test timeout")
            atomic_writer(path, content, private=private)

        class TrackedJournal(Journal):
            async def async_save(self, state):
                self.save_task = asyncio.current_task()
                await super().async_save(state)

        journal = TrackedJournal(self.path, executor, writer)
        sender = Sender(None, journal, clock=Clock())
        request = {"event_id": "one", "event_type": "test.ping", "message": "synthetic"}
        send = asyncio.create_task(sender.send(request))
        self.assertTrue(await asyncio.to_thread(entered.wait, 5))
        journal.save_task.cancel()
        await asyncio.sleep(0)
        journal.save_task.cancel()
        self.assertFalse(send.done())
        release.set()
        with self.assertRaises(asyncio.CancelledError):
            await send
        self.assertEqual(sender.state, await journal.async_load())
        self.assertIn("one", sender.state["pending"])
        self.assertTrue(sender.diagnostics()["journal_healthy"])

    async def test_direct_finalize_task_cancel_commits_memory(self):
        entered, release = threading.Event(), threading.Event()
        writes = 0

        def writer(path, content, *, private):
            nonlocal writes
            writes += 1
            if writes == 2:
                entered.set()
                if not release.wait(5):
                    raise RuntimeError("test timeout")
            atomic_writer(path, content, private=private)

        class TrackedJournal(Journal):
            async def async_save(self, state):
                self.save_task = asyncio.current_task()
                await super().async_save(state)

        journal = TrackedJournal(self.path, executor, writer)

        class Remote:
            puts = 0

            async def validate_destination(self):
                pass

            async def read_event(self, _path):
                return None

            async def prepare_create(self):
                pass

            async def create_event(self, _path, _payload, *, expires_at):
                self.puts += 1

        client = Remote()
        sender = Sender(client, journal, clock=Clock())
        send = asyncio.create_task(
            sender.send(
                {"event_id": "one", "event_type": "test.ping", "message": "synthetic"}
            )
        )
        self.assertTrue(await asyncio.to_thread(entered.wait, 5))
        journal.save_task.cancel()
        await asyncio.sleep(0)
        journal.save_task.cancel()
        release.set()
        with self.assertRaises(asyncio.CancelledError):
            await send
        self.assertEqual(sender.state, await journal.async_load())
        self.assertEqual(sender.state["pending"], {})
        self.assertEqual(sender.state["recent"]["one"]["status"], "accepted")
        self.assertEqual(client.puts, 1)

    async def test_cooldown_survives_restart_and_never_shortens(self):
        await self.journal.async_save(sample_state())
        await self.journal.async_set_cooldown(2000.0)
        await self.journal.async_set_cooldown(1000.0)
        restarted = Journal(self.path, executor, atomic_writer)
        self.assertEqual(await restarted.async_load(), sample_state())
        self.assertEqual(restarted.get_cooldown_deadline(), 2000.0)
        await restarted.async_save(sample_state("two"))
        again = Journal(self.path, executor, atomic_writer)
        self.assertEqual(await again.async_load(), sample_state("two"))
        self.assertEqual(again.get_cooldown_deadline(), 2000.0)

    async def test_strict_v1_upgrade_preserves_pending_records(self):
        self.path.parent.mkdir()
        from ghmq_test_subject.core import canonical

        self.path.write_bytes(canonical({"version": 1, "state": sample_state()}))
        self.assertEqual(await self.journal.async_load(), sample_state())
        self.assertEqual(self.journal.get_cooldown_deadline(), 0)
        await self.journal.async_set_cooldown(2000)
        restarted = Journal(self.path, executor, atomic_writer)
        self.assertEqual(await restarted.async_load(), sample_state())
        self.assertEqual(restarted.get_cooldown_deadline(), 2000)
        self.assertIn(b'"version":2', self.path.read_bytes())

    async def test_cooldown_metadata_rejects_invalid_values_and_preserves_bytes(self):
        self.path.parent.mkdir()
        for value in ("true", "-1", '"later"', "NaN", "Infinity", "1e999", "null"):
            raw = (
                '{"version":2,"not_before":'
                + value
                + ',"state":{"pending":{},"recent":{}}}'
            ).encode()
            self.path.write_bytes(raw)
            with self.subTest(value=value), self.assertRaises(JournalError):
                await self.journal.async_load()
            self.assertEqual(self.path.read_bytes(), raw)

    async def test_provisional_cooldown_visible_before_disk_completes(self):
        entered, release = threading.Event(), threading.Event()

        def blocked(path, content, *, private):
            entered.set()
            if not release.wait(5):
                raise RuntimeError("test timeout")
            atomic_writer(path, content, private=private)

        journal = Journal(self.path, executor, blocked)
        task = asyncio.create_task(journal.async_set_cooldown(2000))
        self.assertTrue(await asyncio.to_thread(entered.wait, 5))
        self.assertEqual(journal.cooldown_deadline, 0)
        self.assertEqual(journal.get_cooldown_deadline(), 2000)
        release.set()
        await task
        self.assertEqual(journal.cooldown_deadline, 2000)

    async def test_cancelled_cooldown_lock_wait_still_persists_later_deadline(self):
        entered, release = threading.Event(), threading.Event()
        writes = 0

        def blocked(path, content, *, private):
            nonlocal writes
            writes += 1
            if writes == 1:
                entered.set()
                if not release.wait(5):
                    raise RuntimeError("test timeout")
            atomic_writer(path, content, private=private)

        journal = Journal(self.path, executor, blocked)
        first = asyncio.create_task(journal.async_save(sample_state()))
        self.assertTrue(await asyncio.to_thread(entered.wait, 5))
        later = asyncio.create_task(journal.async_set_cooldown(2000))
        await asyncio.sleep(0)
        self.assertEqual(journal.get_cooldown_deadline(), 2000)
        for _ in range(3):
            later.cancel()
            await asyncio.sleep(0)
        self.assertFalse(later.done())
        release.set()
        await first
        with self.assertRaises(asyncio.CancelledError):
            await later
        restarted = Journal(self.path, executor, atomic_writer)
        self.assertEqual(await restarted.async_load(), sample_state())
        self.assertEqual(restarted.get_cooldown_deadline(), 2000)

    async def test_uncertain_cooldown_failure_blocks_shared_getter_and_writes(self):
        def broken(*args, **kwargs):
            raise OSError("private detail")

        journal = Journal(self.path, executor, broken)
        with self.assertRaises(JournalError):
            await journal.async_set_cooldown(2000)
        with self.assertRaises(JournalError):
            journal.get_cooldown_deadline()
        with self.assertRaises(JournalError):
            await journal.async_save(sample_state())
        with self.assertRaises(JournalError):
            await journal.async_set_cooldown(1000)

    async def test_shared_monotonic_guard_survives_forward_wall_clock_jump(self):
        clock = {"wall": 1000.0, "mono": 100.0}
        journal = Journal(
            self.path,
            executor,
            atomic_writer,
            wall_clock=lambda: clock["wall"],
            monotonic=lambda: clock["mono"],
        )
        await journal.async_set_cooldown(1120.0)
        clock["wall"] = 5000.0
        self.assertEqual(journal.get_cooldown_deadline(), 5120.0)
        clock["mono"] += 120
        self.assertEqual(journal.get_cooldown_deadline(), 1120.0)
