"""Bounded immutable events and serialized, durably admitted delivery."""

from __future__ import annotations

import asyncio
import hashlib
import json
import math
import re
import uuid
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from typing import Any

from .const import (
    DEFAULT_TTL,
    MAX_JOURNAL_BYTES,
    MAX_PAYLOAD_BYTES,
    MAX_PENDING,
    MAX_RECENT,
)


class BridgeError(Exception):
    """Public errors contain fixed messages, never credentials or payloads."""


class AuthError(BridgeError):
    """The configured credential needs replacement."""


class IdentityError(BridgeError):
    """The configured immutable GitHub destination identity is missing or changed."""


class JournalError(BridgeError):
    """Persistence is uncertain; sending is frozen until a successful reload."""


class JournalCommitCancelled(asyncio.CancelledError):
    """A journal write completed successfully before its task was cancelled."""


class EventExpired(BridgeError):
    """The immutable event's original lifetime has elapsed."""


class SenderClosed(BridgeError):
    """No new work is admitted while the integration unloads."""


class RetryableError(BridgeError):
    def __init__(self, delay: float = 1) -> None:
        super().__init__("Temporary GitHub delivery failure; event remains queued")
        self.delay = max(1.0, delay) if math.isfinite(delay) else 60.0


class RateLimitError(RetryableError):
    """Requests are paused until a durable, non-secret wall-clock deadline."""

    def __init__(self, delay: float, retry_at: float) -> None:
        super().__init__(delay)
        self.retry_at = retry_at
        self.args = ("GitHub requests are paused until the retry deadline",)


class EventConflict(BridgeError):
    """An event ID cannot be reused with different content."""


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def parse_time(value: str) -> datetime:
    try:
        if not isinstance(value, str):
            raise ValueError
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if result.tzinfo is None:
            raise ValueError
        return result.astimezone(timezone.utc)
    except (ValueError, AttributeError, TypeError, OverflowError):
        raise BridgeError("Timestamp must be an ISO-8601 time with timezone") from None


def canonical(value: Any) -> bytes:
    try:
        return (
            json.dumps(
                value,
                sort_keys=True,
                ensure_ascii=False,
                allow_nan=False,
                separators=(",", ":"),
            )
            + "\n"
        ).encode("utf-8")
    except (ValueError, TypeError, UnicodeError, RecursionError):
        raise BridgeError("Value must be finite, valid UTF-8 JSON") from None


def strict_json(raw: bytes) -> Any:
    """Reject duplicate keys, non-finite numbers and malformed UTF-8."""

    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError
            result[key] = value
        return result

    def invalid_constant(_value):
        raise ValueError

    try:
        value = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=pairs,
            parse_constant=invalid_constant,
        )
        # Also catches numeric overflow (e.g. 1e999) and lone surrogate escapes.
        canonical(value)
        return value
    except (ValueError, TypeError, UnicodeError, RecursionError, BridgeError):
        raise BridgeError("Invalid JSON document") from None


def digest(value: Any) -> str:
    return hashlib.sha256(canonical(value)).hexdigest()


def validate_destination(
    owner: str, repository: str, branch: str, pr_number: int
) -> None:
    if not isinstance(owner, str) or not re.fullmatch(
        r"[A-Za-z0-9][A-Za-z0-9-]{0,38}", owner
    ):
        raise BridgeError("Invalid GitHub owner")
    if (
        not isinstance(repository, str)
        or not re.fullmatch(r"[A-Za-z0-9_.-]{1,100}", repository)
        or repository in (".", "..")
    ):
        raise BridgeError("Invalid GitHub repository")
    if (
        not isinstance(branch, str)
        or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_/-]{0,99}", branch)
        or "//" in branch
        or branch.endswith("/")
    ):
        raise BridgeError("Invalid relay branch")
    if type(pr_number) is not int or pr_number < 1:
        raise BridgeError("Invalid pull request number")


def normalize_request(data: dict[str, Any]) -> dict[str, Any]:
    allowed = {
        "event_id",
        "event_type",
        "message",
        "severity",
        "synthetic",
        "entity_id",
        "occurred_at",
        "ttl_seconds",
    }
    if type(data) is not dict or set(data) - allowed:
        raise BridgeError(
            "Unsupported event field; destinations, images and arbitrary context are not accepted"
        )
    result = {
        "event_type": data.get("event_type"),
        "message": data.get("message"),
        "severity": data.get("severity", "info"),
        "synthetic": data.get("synthetic", False),
        "ttl_seconds": data.get("ttl_seconds", DEFAULT_TTL),
    }
    if not isinstance(result["event_type"], str) or not re.fullmatch(
        r"[a-z][a-z0-9_.-]{0,63}", result["event_type"]
    ):
        raise BridgeError("Invalid event type")
    if (
        not isinstance(result["message"], str)
        or not 1 <= len(result["message"]) <= 512
        or any(ord(c) < 32 and c not in "\n\t" for c in result["message"])
    ):
        raise BridgeError("Message must contain 1 to 512 plain-text characters")
    if (
        result["severity"] not in ("info", "warning", "urgent")
        or type(result["synthetic"]) is not bool
    ):
        raise BridgeError("Invalid severity or synthetic flag")
    if (
        type(result["ttl_seconds"]) is not int
        or not 60 <= result["ttl_seconds"] <= 3600
    ):
        raise BridgeError("Event lifetime must be 60 to 3600 seconds")
    if "entity_id" in data:
        if not isinstance(data["entity_id"], str) or not re.fullmatch(
            r"[a-z0-9_]+\.[a-z0-9_]{1,100}", data["entity_id"]
        ):
            raise BridgeError("Invalid entity ID")
        result["entity_id"] = data["entity_id"]
    if "occurred_at" in data:
        result["occurred_at"] = iso(parse_time(data["occurred_at"]))
    canonical(result)
    return result


def event_id_for(data: dict[str, Any]) -> str:
    event_id = data.get("event_id", uuid.uuid4().hex)
    if not isinstance(event_id, str) or not re.fullmatch(
        r"[A-Za-z0-9_-]{1,96}", event_id
    ):
        raise BridgeError(
            "Event ID must contain 1 to 96 letters, numbers, underscores or hyphens"
        )
    return event_id


def build_event(
    event_id: str, request: dict[str, Any], now: datetime
) -> dict[str, Any]:
    occurred = parse_time(request["occurred_at"]) if "occurred_at" in request else now
    try:
        expires = occurred + timedelta(seconds=request["ttl_seconds"])
    except OverflowError:
        raise BridgeError("Event timestamp is out of range") from None
    if occurred > now + timedelta(seconds=30):
        raise BridgeError("Event timestamp is too far in the future")
    if expires <= now:
        raise EventExpired("Event has expired")
    event = {
        "schema_version": 1,
        "event_id": event_id,
        "source": "home_assistant",
        "event_type": request["event_type"],
        "message": request["message"],
        "severity": request["severity"],
        "synthetic": request["synthetic"],
        "occurred_at": iso(occurred),
        "expires_at": iso(expires),
    }
    if "entity_id" in request:
        event["entity_id"] = request["entity_id"]
    if len(canonical(event)) > MAX_PAYLOAD_BYTES:
        raise BridgeError("Event exceeds the payload size limit")
    return event


def validate_state(value: Any) -> None:
    """Validate every stored field without interpreting stale events as fresh."""
    try:
        if type(value) is not dict or set(value) != {"pending", "recent"}:
            raise ValueError
        if any(type(value[k]) is not dict for k in ("pending", "recent")):
            raise ValueError
        if (
            len(value["pending"]) > MAX_PENDING
            or len(value["recent"]) > MAX_RECENT
            or set(value["pending"]) & set(value["recent"])
        ):
            raise ValueError
        for group in ("pending", "recent"):
            for event_id, record in value[group].items():
                event_id_for({"event_id": event_id})
                fields = {
                    "request",
                    "request_hash",
                    "event" if group == "pending" else "status",
                }
                if type(record) is not dict or set(record) != fields:
                    raise ValueError
                request = record["request"]
                if (
                    type(request) is not dict
                    or normalize_request(request) != request
                    or "event_id" in request
                ):
                    raise ValueError
                if (
                    not isinstance(record["request_hash"], str)
                    or not re.fullmatch(r"[0-9a-f]{64}", record["request_hash"])
                    or record["request_hash"] != digest(request)
                ):
                    raise ValueError
                if group == "recent":
                    if record["status"] not in ("accepted", "expired"):
                        raise ValueError
                    # Recent records must also describe an admissible envelope.
                    # A synthetic full-precision UTC time bounds generated times
                    # without treating a previously accepted event as fresh.
                    occurred = (
                        parse_time(request["occurred_at"])
                        if "occurred_at" in request
                        else datetime(
                            2000, 1, 1, microsecond=123456, tzinfo=timezone.utc
                        )
                    )
                    build_event(event_id, request, occurred)
                    continue
                event = record["event"]
                if (
                    type(event) is not dict
                    or type(event.get("schema_version")) is not int
                ):
                    raise ValueError
                occurred = parse_time(event.get("occurred_at"))
                expected = build_event(event_id, request, occurred)
                if canonical(event) != canonical(expected):
                    raise ValueError
        if len(canonical(value)) > MAX_JOURNAL_BYTES - 128:
            raise ValueError
    except (ValueError, TypeError, KeyError, OverflowError, BridgeError):
        raise JournalError(
            "Relay journal is invalid; inspect or restore it before sending"
        ) from None


class Sender:
    """One serializer covers admission, disk transitions and remote writes.

    Cancellation never rolls back a completed disk commit. Uncertain disk errors
    freeze this instance: reload and validate the on-disk journal to recover.
    """

    def __init__(
        self,
        client: Any,
        store: Any,
        *,
        clock=utcnow,
        sleep=asyncio.sleep,
        create_task=asyncio.create_task,
    ) -> None:
        self.client, self.store, self.clock, self.sleep = client, store, clock, sleep
        self._create_task = create_task
        self.lock = asyncio.Lock()
        self.state: dict[str, Any] = {"pending": {}, "recent": {}}
        self.last_status = "idle"
        self._frozen = False
        self._closing = False

    async def load(self) -> None:
        async with self.lock:
            try:
                value = await self.store.async_load()
                if value is None:
                    value = {"pending": {}, "recent": {}}
                validate_state(value)
            except asyncio.CancelledError:
                raise
            except Exception:
                self._frozen = True
                self.last_status = "journal_error"
                raise JournalError(
                    "Cannot load relay journal; inspect or restore it before sending"
                ) from None
            self.state = deepcopy(value)
            self._frozen = False

    def _check_open(self) -> None:
        if self._closing:
            raise SenderClosed("GHMQ is unloading; event delivery is not confirmed")
        if self._frozen:
            raise JournalError(
                "Relay journal persistence is uncertain; reload before sending"
            )

    async def _commit(self, candidate: dict[str, Any]) -> None:
        """Do not release serialization until I/O is done, even on repeated cancel."""
        validate_state(candidate)
        task = self._create_task(self._save_snapshot(deepcopy(candidate)))
        # asyncio.wait shields its input from caller cancellation without the
        # cancelled-shield exception logging behavior introduced in Python 3.14.
        cancelled = False
        while not task.done():
            try:
                await asyncio.wait({task})
            except asyncio.CancelledError:
                cancelled = True
            except Exception:
                break
        try:
            saved, save_cancelled = task.result()
        except asyncio.CancelledError:
            saved, save_cancelled = False, True
        cancelled = cancelled or save_cancelled
        if not saved:
            self._frozen = True
            self.last_status = "journal_error"
            if cancelled:
                raise asyncio.CancelledError from None
            raise JournalError(
                "Could not persist relay journal; reload before sending"
            ) from None
        # The actual disk save finished. Memory must match it before propagating
        # cancellation, otherwise a later save could discard committed admission.
        self.state = candidate
        if cancelled:
            raise asyncio.CancelledError

    async def _save_snapshot(self, candidate: dict[str, Any]) -> tuple[bool, bool]:
        """Return a receipt, even if HA observes cancellation of the save task.

        A CancelledError subclass can be consumed by another task observer. Turn
        the journal's completed-write signal into a normal result so the receipt
        remains reliable for both HA lifecycle tracking and this serializer.
        """
        try:
            await self.store.async_save(candidate)
        except JournalCommitCancelled:
            return True, True
        except asyncio.CancelledError:
            return False, True
        except Exception:
            return False, False
        return True, False

    async def send(self, data: dict[str, Any]) -> dict[str, Any]:
        request = normalize_request(data)
        event_id = event_id_for(data)
        fingerprint = digest(request)
        async with self.lock:
            self._check_open()
            recent = self.state["recent"].get(event_id)
            pending = self.state["pending"].get(event_id)
            existing = recent or pending
            if existing:
                if existing["request_hash"] != fingerprint:
                    raise EventConflict(
                        "Event ID was already used for different content"
                    )
                if recent:
                    return {"event_id": event_id, "status": recent["status"]}
            else:
                if len(self.state["pending"]) >= MAX_PENDING:
                    raise BridgeError(
                        "Relay queue is full; resolve delivery before adding events"
                    )
                candidate = deepcopy(self.state)
                candidate["pending"][event_id] = {
                    "event": build_event(event_id, request, self.clock()),
                    "request": request,
                    "request_hash": fingerprint,
                }
                await self._commit(candidate)
            return await self._deliver(event_id)

    async def _finish(self, event_id: str, status: str) -> dict[str, Any]:
        candidate = deepcopy(self.state)
        record = candidate["pending"].pop(event_id)
        candidate["recent"][event_id] = {
            "request": record["request"],
            "request_hash": record["request_hash"],
            "status": status,
        }
        while len(candidate["recent"]) > MAX_RECENT:
            del candidate["recent"][next(iter(candidate["recent"]))]
        await self._commit(candidate)
        self.last_status = status
        return {"event_id": event_id, "status": status}

    def _check_fresh(self, event: dict[str, Any]) -> None:
        self._check_open()
        if parse_time(event["expires_at"]) <= self.clock():
            raise EventExpired("Event has expired")

    async def _deliver(self, event_id: str) -> dict[str, Any]:
        event = self.state["pending"][event_id]["event"]
        payload = canonical(event)
        path = f"events/{event_id}.json"
        for attempt in range(3):
            try:
                self._check_fresh(event)
                await self.client.validate_destination()
                self._check_fresh(event)
                existing = await self.client.read_event(path)
                self._check_fresh(event)
                if existing is not None:
                    if existing != payload:
                        raise EventConflict(
                            "Remote event ID already exists with different content"
                        )
                    return await self._finish(event_id, "accepted")
                await self.client.prepare_create()
                self._check_fresh(event)
                await self.client.create_event(
                    path, payload, expires_at=parse_time(event["expires_at"])
                )
                return await self._finish(event_id, "accepted")
            except EventExpired:
                return await self._finish(event_id, "expired")
            except RetryableError as err:
                self.last_status = "retry_pending"
                if err.delay > 8 or attempt == 2:
                    raise
                await self.sleep(max(err.delay, 2**attempt))
        raise BridgeError("Delivery did not complete")

    async def retry_pending(self) -> None:
        async with self.lock:
            self._check_open()
            for event_id in list(self.state["pending"]):
                self._check_open()
                try:
                    await self._deliver(event_id)
                except AuthError:
                    self.last_status = "authentication_required"
                    raise
                except (JournalError, SenderClosed):
                    raise
                except BridgeError:
                    self.last_status = "retry_pending"

    async def async_close(self) -> None:
        """Close admission synchronously, then drain the current serialized operation."""
        self._closing = True
        cancelled = False
        task = asyncio.create_task(self._drain())
        while not task.done():
            try:
                await asyncio.wait({task})
            except asyncio.CancelledError:
                cancelled = True
        task.result()
        if cancelled:
            raise asyncio.CancelledError

    async def _drain(self) -> None:
        async with self.lock:
            pass

    def diagnostics(self) -> dict[str, Any]:
        return {
            "pending_count": len(self.state["pending"]),
            "recent_count": len(self.state["recent"]),
            "last_status": self.last_status,
            "journal_healthy": not self._frozen,
            "closing": self._closing,
        }
