"""Small fixed-origin GitHub transport. No redirects or arbitrary endpoints."""

from __future__ import annotations

import asyncio
import base64
import binascii
import logging
import math
import time
from datetime import datetime
from email.utils import parsedate_to_datetime
from urllib.parse import quote

import aiohttp

from .const import API_ROOT, MAX_PAYLOAD_BYTES, VERSION
from .core import (
    AuthError,
    BridgeError,
    EventExpired,
    IdentityError,
    JournalError,
    RateLimitError,
    RetryableError,
    strict_json,
    validate_destination,
)

_LOGGER = logging.getLogger(__name__)

MAX_RESPONSE_BYTES = 65536
READ_CHUNK_BYTES = 8192


def _finite_float(value):
    result = float(value)
    if not math.isfinite(result):
        raise ValueError
    return result


def _nonnegative_number(value):
    try:
        result = _finite_float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return result if result >= 0 else None


def _positive_id(value):
    # bool is an int subclass, and must never stand in for a GitHub ID.
    return type(value) is int and value > 0


class GitHubClient:
    def __init__(
        self,
        session,
        owner: str,
        repository: str,
        branch: str,
        pr_number: int,
        token: str,
        *,
        repository_id: int | None = None,
        pull_request_id: int | None = None,
        wall_clock=time.time,
        monotonic=time.monotonic,
        sleep=asyncio.sleep,
    ):
        validate_destination(owner, repository, branch, pr_number)
        if (
            not isinstance(token, str)
            or not token
            or len(token) > 512
            or any(not 33 <= ord(c) <= 126 for c in token)
        ):
            raise AuthError("A GitHub credential must be entered in Home Assistant")
        self.session, self.owner, self.repository = session, owner, repository
        self.branch, self.pr_number, self._token = branch, pr_number, token
        self._repository_id, self._pull_request_id = repository_id, pull_request_id
        self._prefix = f"/repos/{owner}/{repository}"
        self._wall_clock, self._monotonic, self._sleep = wall_clock, monotonic, sleep
        self.not_before = 0.0
        self._rate_limit_until = 0.0
        self._persist_cooldown = None
        self._current_deadline = None
        self._announce_deadline = None
        self._is_cooldown_pending = None
        self._cooldown_failed = False
        self._cooldown_saving = 0
        self._cooldown_lock = asyncio.Lock()
        self._last_mutation = None
        self._mutation_lock = asyncio.Lock()

    def _check_identity(self, *, discover=False):
        for identity in (self._repository_id, self._pull_request_id):
            if identity is None and discover:
                continue
            if not _positive_id(identity):
                raise IdentityError("GitHub destination identity is missing or invalid")

    def bind_cooldown(
        self,
        deadline: float,
        persist,
        *,
        current_deadline=None,
        announce_deadline=None,
        is_cooldown_pending=None,
    ):
        """Restore a durable wall-clock deadline and bind its async writer.

        A second binding cannot shorten a running monotonic delay or recover a
        failed writer. Only constructing a new client after reload can do that.
        """
        if self._cooldown_failed:
            raise JournalError("Rate-limit persistence failed; reload is required")
        if (
            type(deadline) not in (int, float)
            or (restored := _nonnegative_number(deadline)) is None
            or not callable(persist)
            or (current_deadline is not None and not callable(current_deadline))
            or (announce_deadline is not None and not callable(announce_deadline))
            or (is_cooldown_pending is not None and not callable(is_cooldown_pending))
        ):
            self._cooldown_failed = True
            raise JournalError("Invalid persisted rate-limit state; reload is required")
        self._persist_cooldown = persist
        self._current_deadline = current_deadline
        self._announce_deadline = announce_deadline
        self._is_cooldown_pending = is_cooldown_pending
        self.not_before = max(self.not_before, restored)
        self._rate_limit_until = max(
            self._rate_limit_until,
            self._monotonic() + max(0.0, self.not_before - self._wall_clock()),
        )

    def _refresh_cooldown(self):
        if self._current_deadline is None:
            return
        try:
            value = self._current_deadline()
            if (
                type(value) not in (int, float)
                or (deadline := _nonnegative_number(value)) is None
            ):
                raise ValueError
        except Exception:
            self._cooldown_failed = True
            raise JournalError(
                "Invalid persisted rate-limit state; reload is required"
            ) from None
        if deadline > self.not_before:
            self.not_before = deadline
            self._rate_limit_until = max(
                self._rate_limit_until,
                self._monotonic() + max(0.0, deadline - self._wall_clock()),
            )

    def _check_rate_limit(self):
        if self._cooldown_failed:
            raise JournalError("Rate-limit persistence failed; reload is required")
        self._refresh_cooldown()
        shared_pending = False
        if self._is_cooldown_pending is not None:
            try:
                shared_pending = self._is_cooldown_pending()
                if type(shared_pending) is not bool:
                    raise ValueError
            except Exception:
                self._cooldown_failed = True
                raise JournalError(
                    "Invalid persisted rate-limit state; reload is required"
                ) from None
        # A wall-clock adjustment must not shorten a delay already received.
        delay = max(
            self.not_before - self._wall_clock(),
            self._rate_limit_until - self._monotonic(),
        )
        if delay > 0 or self._cooldown_saving or shared_pending:
            raise RateLimitError(delay, self._wall_clock() + max(0.0, delay))

    async def _rate_limited(self, headers, *, secondary=False, use_reset=True):
        self._refresh_cooldown()
        now = self._wall_clock()
        deadlines = [
            now + (60 if secondary else 1),
            # Translate the still-active monotonic guard into the current
            # wall clock before persisting a late in-flight response.
            now + max(0.0, self._rate_limit_until - self._monotonic()),
        ]
        retry_header = headers.get("Retry-After")
        # A quota reset is not a retry instruction when permission is denied
        # and quota remains. Only genuine rate-limit evidence may use it.
        reset_header = headers.get("X-RateLimit-Reset") if use_reset else None
        if retry_header is not None:
            seconds = _nonnegative_number(retry_header)
            deadline = None if seconds is None else now + seconds
            if deadline is None:
                # Retry-After may also be an HTTP date. A malformed header
                # never invalidates a separately valid reset deadline.
                try:
                    date = parsedate_to_datetime(retry_header)
                    if date.tzinfo is not None:
                        deadline = date.timestamp()
                except (TypeError, ValueError, OverflowError):
                    pass
            deadlines.append(
                deadline
                if deadline is not None and math.isfinite(deadline)
                else now + 60
            )
        if reset_header is not None:
            reset = _nonnegative_number(reset_header)
            deadlines.append(reset if reset is not None else now + 60)
        if retry_header is None and reset_header is None:
            deadlines.append(now + 60)
        self.not_before = max(self.not_before, *deadlines)
        delay = max(1, self.not_before - now)
        self._rate_limit_until = max(self._rate_limit_until, self._monotonic() + delay)
        # Block every other request immediately, including if a slow save
        # outlasts this deadline. Persist before telling the caller to retry.
        self._cooldown_saving += 1
        try:
            # Publish the provisional shared deadline synchronously: sibling
            # clients may already be ahead of our save task in the ready queue.
            if self._announce_deadline is not None:
                self._announce_deadline(self.not_before)
            # Own the entire serialization + save task through completion.
            # Cancellation while waiting for an earlier save cannot discard
            # the newer server deadline or release its persistence obligation.
            task = asyncio.create_task(self._save_cooldown())
            cancelled = False
            while not task.done():
                try:
                    await asyncio.wait({task})
                except asyncio.CancelledError:
                    cancelled = True
            try:
                task.result()
            except BaseException as err:
                self._cooldown_failed = True
                if cancelled or isinstance(err, asyncio.CancelledError):
                    raise asyncio.CancelledError from None
                raise JournalError(
                    "Rate-limit persistence failed; reload is required"
                ) from None
            if cancelled:
                self._cooldown_failed = True
                raise asyncio.CancelledError
            if self._cooldown_failed:
                raise JournalError("Rate-limit persistence failed; reload is required")
        except asyncio.CancelledError:
            self._cooldown_failed = True
            raise
        except Exception:
            self._cooldown_failed = True
            raise JournalError(
                "Rate-limit persistence failed; reload is required"
            ) from None
        finally:
            self._cooldown_saving -= 1
        delay = max(delay, self._rate_limit_until - self._monotonic())
        retry_at = max(
            self.not_before,
            self._wall_clock() + max(0.0, self._rate_limit_until - self._monotonic()),
        )
        return RateLimitError(delay, retry_at)

    async def _save_cooldown(self):
        async with self._cooldown_lock:
            # A separate save's caller may have been cancelled after commit.
            # Still complete every already-observed deadline's persistence.
            if self._persist_cooldown is not None:
                await self._persist_cooldown(self.not_before)

    async def _read_response(self, response):
        # read(n) can return a short chunk before EOF. Read through EOF,
        # bounded to one byte beyond the maximum accepted body.
        raw = bytearray()
        while True:
            chunk = await response.content.read(
                min(READ_CHUNK_BYTES, MAX_RESPONSE_BYTES + 1 - len(raw))
            )
            if not chunk:
                break
            raw.extend(chunk)
            if len(raw) > MAX_RESPONSE_BYTES:
                raise BridgeError("GitHub response exceeds size limit")
        try:
            result = strict_json(raw)
        except BridgeError:
            raise BridgeError("GitHub returned an invalid response") from None
        if not isinstance(result, dict):
            raise BridgeError("GitHub returned an invalid response")
        return result

    async def prepare_create(self):
        """Wait for mutation spacing; callers must check expiry after waiting."""
        self._check_rate_limit()
        while self._last_mutation is not None:
            delay = self._last_mutation + 1 - self._monotonic()
            if delay <= 0:
                break
            await self._sleep(delay)
            self._check_rate_limit()

    async def _request(
        self,
        method,
        suffix,
        *,
        body=None,
        params=None,
        missing_ok=False,
        _discover=False,
    ):
        self._check_identity(discover=_discover)
        self._check_rate_limit()
        endpoint = (
            "repository"
            if not suffix
            else "pull_request"
            if suffix == f"/pulls/{self.pr_number}"
            else "contents"
            if suffix.startswith("/contents/")
            else "other"
        )
        status = None
        try:
            async with self.session.request(
                method,
                API_ROOT + self._prefix + suffix,
                headers={
                    "Authorization": f"Bearer {self._token}",
                    "Accept": "application/vnd.github+json",
                    "X-GitHub-Api-Version": "2022-11-28",
                    "User-Agent": f"ghmq/{VERSION}",
                },
                json=body,
                params=params,
                allow_redirects=False,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as response:
                status = response.status
                if not 200 <= status < 300 and not (status == 404 and missing_ok):
                    _LOGGER.warning(
                        "GHMQ request failed: category=http_status status=%s endpoint=%s",
                        status,
                        endpoint,
                    )
                if status == 401:
                    raise AuthError(
                        "GitHub authentication failed; reconnect in Home Assistant"
                    )
                if status == 429 or (
                    status == 403
                    and (
                        response.headers.get("X-RateLimit-Remaining") == "0"
                        or "Retry-After" in response.headers
                    )
                ):
                    raise await self._rate_limited(
                        response.headers,
                        secondary=status == 403
                        and response.headers.get("X-RateLimit-Remaining") != "0",
                    )
                if status == 404 and missing_ok:
                    return None
                if status >= 500 or status in (409, 422):
                    raise RetryableError()
                if status == 403:
                    # Secondary limits may omit every rate-limit header.
                    # Persist a conservative cooldown even when the bounded
                    # error body is absent, malformed, or a permission denial.
                    rate_error = await self._rate_limited(
                        response.headers, secondary=True, use_reset=False
                    )
                    try:
                        result = await self._read_response(response)
                    except (BridgeError, aiohttp.ClientError, asyncio.TimeoutError):
                        raise rate_error from None
                    message = result.get("message")
                    if isinstance(message, str) and any(
                        marker in message.lower()
                        for marker in (
                            "secondary rate limit",
                            "api rate limit exceeded",
                            "abuse detection mechanism",
                            "abuse rate limit",
                        )
                    ):
                        # The bounded body now supplies genuine rate-limit
                        # evidence. Keep the provisional cooldown while
                        # persisting any longer server reset deadline.
                        if "X-RateLimit-Reset" in response.headers:
                            raise await self._rate_limited(
                                response.headers, secondary=True
                            )
                        raise rate_error
                    raise BridgeError(
                        "GitHub denied access; check the repository-scoped credential"
                    )
                if 300 <= status < 400:
                    raise BridgeError(
                        "GitHub redirected the request; verify the configured destination"
                    )
                if not 200 <= status < 300:
                    raise BridgeError(
                        "GitHub request rejected; check the configured private repository and PR"
                    )
                return await self._read_response(response)
        except (aiohttp.ClientError, asyncio.TimeoutError) as err:
            category = (
                "timeout"
                if isinstance(err, asyncio.TimeoutError)
                else "tls"
                if isinstance(err, aiohttp.ClientSSLError)
                else "dns"
                if isinstance(err, aiohttp.ClientConnectorDNSError)
                else "connection"
                if isinstance(err, aiohttp.ClientConnectionError)
                else "http_client"
            )
            _LOGGER.warning(
                "GHMQ request failed: category=%s status=%s endpoint=%s",
                category,
                status,
                endpoint,
            )
            if self._cooldown_failed:
                raise JournalError(
                    "Rate-limit persistence failed; reload is required"
                ) from None
            raise RetryableError() from None

    async def validate_destination(self, *, discover=False):
        """Verify immutable IDs; only initial configuration may discover them."""
        self._check_identity(discover=discover)
        repo = await self._request("GET", "", _discover=discover)
        repository_id = repo.get("id")
        if not _positive_id(repository_id) or (
            self._repository_id is not None and repository_id != self._repository_id
        ):
            raise IdentityError("GitHub repository identity changed or is invalid")
        if (
            repo.get("private") is not True
            or repo.get("archived")
            or repo.get("disabled")
        ):
            raise BridgeError("Bridge requires an active private repository")
        if self.branch == repo.get("default_branch"):
            raise BridgeError("Relay branch must be separate from the default branch")
        pr = await self._request("GET", f"/pulls/{self.pr_number}", _discover=discover)
        pull_request_id = pr.get("id")
        if not _positive_id(pull_request_id) or (
            self._pull_request_id is not None
            and pull_request_id != self._pull_request_id
        ):
            raise IdentityError("GitHub pull request identity changed or is invalid")
        head = pr.get("head")
        head_repo = head.get("repo") if isinstance(head, dict) else None
        if (
            not isinstance(head_repo, dict)
            or not _positive_id(head_repo.get("id"))
            or head_repo.get("id") != repository_id
        ):
            raise IdentityError("GitHub pull request repository identity is invalid")
        if (
            pr.get("state") != "open"
            or not isinstance(head, dict)
            or head.get("ref") != self.branch
        ):
            raise BridgeError(
                "Relay requires an open PR from the configured branch in the same repository"
            )
        return {"repository_id": repository_id, "pull_request_id": pull_request_id}

    async def read_event(self, path: str):
        result = await self._request(
            "GET",
            "/contents/" + quote(path, safe="/"),
            params={"ref": self.branch},
            missing_ok=True,
        )
        if result is None:
            return None
        if result.get("type") != "file" or result.get("encoding") != "base64":
            raise BridgeError("Remote event is not a regular JSON file")
        try:
            content = result.get("content")
            if not isinstance(content, str):
                raise ValueError
            encoded = content.replace("\n", "")
            data = base64.b64decode(encoded, validate=True)
            # validate=True rejects non-alphabet characters, but not all
            # noncanonical padding or nonzero pad bits on supported Python.
            if base64.b64encode(data).decode("ascii") != encoded:
                raise ValueError
        except (binascii.Error, ValueError):
            raise BridgeError("Remote event content is invalid") from None
        if len(data) > MAX_PAYLOAD_BYTES:
            raise BridgeError("Remote event exceeds size limit")
        return data

    async def create_event(
        self, path: str, payload: bytes, *, expires_at: datetime | None = None
    ):
        if len(payload) > MAX_PAYLOAD_BYTES:
            raise BridgeError("Event exceeds payload limit")
        async with self._mutation_lock:
            await self.prepare_create()
            if expires_at is not None:
                if (
                    not isinstance(expires_at, datetime)
                    or expires_at.utcoffset() is None
                ):
                    raise BridgeError("Invalid event expiry")
                if expires_at.timestamp() <= self._wall_clock():
                    raise EventExpired("Event has expired")
            # Reserve every attempted mutation, including ambiguous timeouts
            # and conflicts, before any network await can hand off execution.
            self._last_mutation = self._monotonic()
            await self._request(
                "PUT",
                "/contents/" + quote(path, safe="/"),
                body={
                    "message": "Add Home Assistant bridge event",
                    "branch": self.branch,
                    "content": base64.b64encode(payload).decode("ascii"),
                },
            )
