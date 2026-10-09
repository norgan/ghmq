"""Strict versioned journal; persistence errors must reach the sender."""

from __future__ import annotations

import asyncio
import math
import os
import time
from copy import deepcopy
from functools import partial
from pathlib import Path
from typing import Any

from .const import DOMAIN, JOURNAL_VERSION, MAX_JOURNAL_BYTES
from .core import (
    JournalCommitCancelled,
    JournalError,
    canonical,
    strict_json,
    validate_state,
)


def private_atomic_writer(path: str, content: str, *, private: bool = True) -> None:
    """Use HA's atomicwrites backend without its raw-path exception logger."""
    from atomicwrites import AtomicWriter

    if private is not True:
        raise JournalError("Relay journal must remain private")
    with AtomicWriter(
        path, mode="w", overwrite=True, encoding="utf-8"
    ).open() as stream:
        os.fchmod(stream.fileno(), 0o600)
        stream.write(content)


class Journal:
    """Size-bounded disk storage with injectable executor and atomic writer.

    The HA factory uses its installed atomicwrites backend (fsync, replace and
    directory sync), without the HA helper's raw-error logging wrapper. All I/O
    runs in HA's executor; failures propagate rather than being logged/swallowed.
    """

    def __init__(
        self,
        path: Path,
        run_in_executor,
        write_atomic,
        *,
        wall_clock=time.time,
        monotonic=time.monotonic,
    ) -> None:
        self.path = path
        self._run = run_in_executor
        self._write_atomic = write_atomic
        self._lock = asyncio.Lock()
        self._state = None
        self.cooldown_deadline = 0.0
        self._provisional_deadline = 0.0
        self._failed = False
        self._wall_clock = wall_clock
        self._monotonic = monotonic
        self._monotonic_deadline = 0.0

    @classmethod
    def for_hass(cls, hass, entry_id: str):
        path = Path(hass.config.path(".storage", f"ghmq.{entry_id}.journal"))

        # Use HA's configured default executor, but do not expose its raw
        # completion Future to HA's cancellable background-task bucket. Cancelling
        # an executor Future does not stop its thread and would defeat draining.
        # The config entry tracks the enclosing save task and drains it on stop.
        def execute(target):
            return hass.loop.run_in_executor(None, target)

        registry = hass.data.setdefault(DOMAIN, {}).setdefault("journals", {})
        key = str(path)
        if key not in registry:
            registry[key] = cls(path, execute, private_atomic_writer)
        return registry[key]

    def _read(self):
        try:
            with self.path.open("rb") as stream:
                raw = stream.read(MAX_JOURNAL_BYTES + 1)
        except FileNotFoundError:
            return None, 0.0
        if len(raw) > MAX_JOURNAL_BYTES:
            raise JournalError("Relay journal exceeds its size limit")
        document = strict_json(raw)
        if type(document) is not dict or type(document.get("version")) is not int:
            raise JournalError("Relay journal version or structure is unsupported")
        if document["version"] == 1 and set(document) == {"version", "state"}:
            # The reviewed pre-deployment journal had no cooldown metadata.
            # Preserve and validate its entire state; the next write upgrades it.
            deadline = 0.0
        elif document["version"] == JOURNAL_VERSION and set(document) == {
            "version",
            "state",
            "not_before",
        }:
            deadline = self._validate_deadline(document["not_before"])
        else:
            raise JournalError("Relay journal version or structure is unsupported")
        validate_state(document["state"])
        return document["state"], deadline

    @staticmethod
    def _validate_deadline(value):
        if type(value) not in (int, float) or not math.isfinite(value) or value < 0:
            raise JournalError("Relay cooldown metadata is invalid")
        return float(value)

    async def async_load(self):
        async with self._lock:
            try:
                state, deadline = await self._run(self._read)
                self._state = deepcopy(state)
                self.cooldown_deadline = deadline
                self._provisional_deadline = max(self._provisional_deadline, deadline)
                self._monotonic_deadline = max(
                    self._monotonic_deadline,
                    self._monotonic() + max(0.0, deadline - self._wall_clock()),
                )
                self._failed = False
                if self._provisional_deadline > self.cooldown_deadline:
                    await self._persist(
                        state if state is not None else {"pending": {}, "recent": {}},
                        self._provisional_deadline,
                    )
                return state
            except asyncio.CancelledError:
                raise
            except Exception:
                self._failed = True
                raise JournalError(
                    "Cannot read relay journal; inspect or restore it before sending"
                ) from None

    def _write(self, content: str) -> None:
        self.path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        self._write_atomic(str(self.path), content, private=True)

    async def async_save(self, state: dict[str, Any]) -> None:
        validate_state(state)
        async with self._lock:
            await self._persist(state, self.get_cooldown_deadline())

    def get_cooldown_deadline(self) -> float:
        if self._failed:
            raise JournalError(
                "Relay journal persistence is uncertain; reload before sending"
            )
        remaining = self._monotonic_deadline - self._monotonic()
        mapped = self._wall_clock() + remaining if remaining > 0 else 0.0
        return max(self.cooldown_deadline, self._provisional_deadline, mapped)

    def note_cooldown(self, deadline: float) -> None:
        """Publish a shared request barrier synchronously, before yielding."""
        deadline = self._validate_deadline(deadline)
        if self._failed:
            raise JournalError(
                "Relay journal persistence is uncertain; reload before sending"
            )
        self._provisional_deadline = max(self._provisional_deadline, deadline)
        self._monotonic_deadline = max(
            self._monotonic_deadline,
            self._monotonic() + max(0.0, deadline - self._wall_clock()),
        )

    def is_cooldown_pending(self) -> bool:
        return self._provisional_deadline > self.cooldown_deadline

    async def async_set_cooldown(self, deadline: float) -> None:
        """Durably record server limits, including cancellation while lock-waiting."""
        self.note_cooldown(deadline)
        acquire = asyncio.create_task(self._lock.acquire())
        cancelled = False
        while not acquire.done():
            try:
                await asyncio.wait({acquire})
            except asyncio.CancelledError:
                cancelled = True
        acquire.result()
        try:
            if self._failed:
                raise JournalError(
                    "Relay journal persistence is uncertain; reload before sending"
                )
            deadline = self.get_cooldown_deadline()
            if deadline > self.cooldown_deadline:
                state = (
                    self._state
                    if self._state is not None
                    else {"pending": {}, "recent": {}}
                )
                try:
                    await self._persist(state, deadline)
                except JournalCommitCancelled:
                    cancelled = True
        finally:
            self._lock.release()
        if cancelled:
            raise JournalCommitCancelled

    async def _persist(self, state: dict[str, Any], deadline: float) -> None:
        """Called only while holding the journal lock; commit state and metadata together."""
        if self._failed:
            raise JournalError(
                "Relay journal persistence is uncertain; reload before sending"
            )
        try:
            validate_state(state)
            snapshot = deepcopy(state)
            raw = canonical(
                {"version": JOURNAL_VERSION, "state": snapshot, "not_before": deadline}
            )
            if len(raw) > MAX_JOURNAL_BYTES:
                raise JournalError("Relay journal exceeds its size limit")
            task = asyncio.ensure_future(
                self._run(partial(self._write, raw.decode("utf-8")))
            )
        except Exception:
            self._failed = True
            raise JournalError("Could not start relay journal persistence") from None
        # asyncio.wait does not cancel the actual disk completion Future. Keep
        # draining repeated cancellation; never release the serialization early.
        cancelled = False
        while not task.done():
            try:
                await asyncio.wait({task})
            except asyncio.CancelledError:
                cancelled = True
        try:
            task.result()
        except BaseException:
            self._failed = True
            if cancelled:
                raise asyncio.CancelledError from None
            raise JournalError("Could not persist relay journal") from None
        self._state = snapshot
        self.cooldown_deadline = deadline
        if cancelled:
            raise JournalCommitCancelled
