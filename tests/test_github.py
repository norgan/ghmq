"""Mocked HTTP transport tests. These tests never make network requests."""

import asyncio
import base64
import json
import math
import traceback
import unittest
from datetime import datetime, timezone
from email.utils import format_datetime
from unittest.mock import patch

import aiohttp
from ghmq_test_subject.const import API_ROOT, MAX_PAYLOAD_BYTES, VERSION
from ghmq_test_subject.core import (
    AuthError,
    BridgeError,
    EventExpired,
    IdentityError,
    JournalCommitCancelled,
    JournalError,
    RateLimitError,
    RetryableError,
)
from ghmq_test_subject.github import MAX_RESPONSE_BYTES, READ_CHUNK_BYTES, GitHubClient
from multidict import CIMultiDict


class FakeClock:
    def __init__(self):
        self.wall = 1000.0
        self.mono = 100.0
        self.sleeps = []

    def advance(self, seconds):
        self.wall += seconds
        self.mono += seconds

    async def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.advance(seconds)
        await asyncio.sleep(0)


class FakeStream:
    def __init__(self, chunks):
        self.chunks = list(chunks)
        self.requests = []
        self.returned = 0
        self.eof_read = False

    async def read(self, count):
        self.requests.append(count)
        if not self.chunks:
            self.eof_read = True
            return b""
        chunk = self.chunks.pop(0)
        if isinstance(chunk, BaseException):
            raise chunk
        if len(chunk) > count:
            self.chunks.insert(0, chunk[count:])
            chunk = chunk[:count]
        self.returned += len(chunk)
        return chunk


class FakeResponse:
    def __init__(self, value=None, *, status=200, headers=None, chunks=None):
        self.status = status
        self.headers = CIMultiDict(headers or {})
        self.content = FakeStream(
            chunks
            if chunks is not None
            else [json.dumps({} if value is None else value).encode()]
        )
        self.closed = False

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        self.closed = True


class FakeSession:
    def __init__(self, responses, clock):
        self.responses = list(responses)
        self.clock = clock
        self.calls = []

    def request(self, method, url, **kwargs):
        self.calls.append((method, url, kwargs, self.clock.mono))
        if not self.responses:
            raise AssertionError("Unexpected HTTP request")
        item = self.responses.pop(0)
        if isinstance(item, BaseException):
            raise item
        return item


class GitHubTransportTests(unittest.IsolatedAsyncioTestCase):
    def make_client(self, *responses, repository_id=1, pull_request_id=77):
        clock = FakeClock()
        session = FakeSession(responses, clock)
        client = GitHubClient(
            session,
            "example",
            "private-relay",
            "relay",
            7,
            "github_test_secret",
            repository_id=repository_id,
            pull_request_id=pull_request_id,
            wall_clock=lambda: clock.wall,
            monotonic=lambda: clock.mono,
            sleep=clock.sleep,
        )
        session.saved_cooldowns = []

        async def persist(deadline):
            session.saved_cooldowns.append(deadline)

        client.bind_cooldown(0, persist)
        return client, session, clock

    async def test_http_failure_logs_only_numeric_status_and_endpoint_kind(self):
        for status in (301, 401, 403, 404, 422, 429, 500):
            for suffix, endpoint in (
                ("", "repository"),
                ("/pulls/7", "pull_request"),
                ("/contents/private-event-secret.json", "contents"),
            ):
                with self.subTest(status=status, endpoint=endpoint):
                    client, _, _ = self.make_client(
                        FakeResponse(
                            {"message": "private-response-secret"},
                            status=status,
                            headers={
                                "Location": "https://private-url-secret",
                                "X-Private": "private-header-secret",
                            },
                        )
                    )
                    with (
                        self.assertLogs(
                            "ghmq_test_subject.github", level="WARNING"
                        ) as logs,
                        self.assertRaises(BridgeError),
                    ):
                        await client._request("GET", suffix)
                    self.assertEqual(
                        [record.getMessage() for record in logs.records],
                        [
                            f"GHMQ request failed: category=http_status status={status} endpoint={endpoint}"
                        ],
                    )
                    self.assertTrue(
                        all(record.exc_info is None for record in logs.records)
                    )
                    for secret in (
                        "github_test_secret",
                        "private-response-secret",
                        "private-url-secret",
                        "private-header-secret",
                        "private-event-secret",
                        "private-relay",
                    ):
                        self.assertNotIn(secret, " ".join(logs.output))

    async def test_network_failure_logs_only_fixed_categories(self):
        import ssl
        from types import SimpleNamespace

        key = SimpleNamespace(host="private-host-secret", port=443, ssl=True)
        for error, category in (
            (asyncio.TimeoutError("private-exception-secret"), "timeout"),
            (
                aiohttp.ClientConnectorDNSError(
                    key, OSError("private-exception-secret")
                ),
                "dns",
            ),
            (
                aiohttp.ClientConnectorSSLError(
                    key, ssl.SSLError("private-exception-secret")
                ),
                "tls",
            ),
            (aiohttp.ClientConnectionError("private-exception-secret"), "connection"),
            (aiohttp.ClientError("private-exception-secret"), "http_client"),
        ):
            with self.subTest(category=category):
                client, _, _ = self.make_client(error)
                with (
                    self.assertLogs(
                        "ghmq_test_subject.github", level="WARNING"
                    ) as logs,
                    self.assertRaises(RetryableError),
                ):
                    await client._request("GET", "/pulls/7")
                self.assertEqual(
                    [record.getMessage() for record in logs.records],
                    [
                        f"GHMQ request failed: category={category} status=None endpoint=pull_request"
                    ],
                )
                self.assertTrue(all(record.exc_info is None for record in logs.records))
                self.assertNotIn("private-", " ".join(logs.output))

    async def test_expected_missing_event_and_success_do_not_log_warnings(self):
        client, _, _ = self.make_client(FakeResponse(status=404), FakeResponse())
        with self.assertNoLogs("ghmq_test_subject.github", level="WARNING"):
            self.assertIsNone(
                await client._request("GET", "/contents/test.json", missing_ok=True)
            )
            self.assertEqual(await client._request("GET", ""), {})

    async def test_request_uses_fixed_origin_no_redirects_and_timeout(self):
        client, session, _ = self.make_client(FakeResponse())
        await client._request("GET", "/pulls/7")
        method, url, options, _ = session.calls[0]
        self.assertEqual(method, "GET")
        self.assertEqual(url, API_ROOT + "/repos/example/private-relay/pulls/7")
        self.assertIs(options["allow_redirects"], False)
        self.assertEqual(options["timeout"].total, 10)
        self.assertEqual(options["headers"]["User-Agent"], f"ghmq/{VERSION}")
        self.assertEqual(
            options["headers"]["Authorization"], "Bearer github_test_secret"
        )

    async def test_split_body_is_read_until_eof(self):
        response = FakeResponse(chunks=[b'{"pr', b'ivate":', b"true", b"}"])
        client, _, _ = self.make_client(response)
        self.assertEqual(await client._request("GET", ""), {"private": True})
        self.assertTrue(response.content.eof_read)
        self.assertTrue(response.closed)

    async def test_complete_json_first_chunk_does_not_hide_trailing_data(self):
        client, _, _ = self.make_client(
            FakeResponse(chunks=[b"{}", b'{"hidden":true}'])
        )
        with self.assertRaisesRegex(BridgeError, "invalid response"):
            await client._request("GET", "")

    async def test_exact_response_size_limit_is_accepted_after_eof(self):
        response = FakeResponse(chunks=[b"{}" + b" " * (MAX_RESPONSE_BYTES - 2)])
        client, _, _ = self.make_client(response)
        self.assertEqual(await client._request("GET", ""), {})
        self.assertTrue(response.content.eof_read)
        self.assertLessEqual(max(response.content.requests), READ_CHUNK_BYTES)

    async def test_oversized_response_is_bounded_even_when_chunked(self):
        response = FakeResponse(chunks=[b"{}", b" " * (MAX_RESPONSE_BYTES * 2)])
        client, _, _ = self.make_client(response)
        with self.assertRaisesRegex(BridgeError, "size limit"):
            await client._request("GET", "")
        self.assertEqual(response.content.returned, MAX_RESPONSE_BYTES + 1)
        self.assertTrue(response.closed)

    async def test_duplicate_json_keys_are_rejected_at_every_depth(self):
        for body in (
            b'{"private":false,"private":true}',
            b'{"head":{"ref":"bad","ref":"relay"}}',
        ):
            with self.subTest(body=body):
                client, _, _ = self.make_client(FakeResponse(chunks=[body]))
                with self.assertRaisesRegex(BridgeError, "invalid response"):
                    await client._request("GET", "")

    async def test_nonfinite_json_numbers_are_rejected(self):
        for number in ("NaN", "Infinity", "-Infinity", "1e999", "-1e999"):
            with self.subTest(number=number):
                client, _, _ = self.make_client(
                    FakeResponse(chunks=[('{"value":' + number + "}").encode()])
                )
                with self.assertRaisesRegex(BridgeError, "invalid response"):
                    await client._request("GET", "")

    async def test_malformed_json_and_wrong_top_level_fail_closed(self):
        for body in (
            b"",
            b"{",
            b"[]",
            b"null",
            b'"github_test_secret"',
            b'{"x":"\xff"}',
        ):
            with self.subTest(body_prefix=body[:20]):
                client, _, _ = self.make_client(FakeResponse(chunks=[body]))
                with self.assertRaisesRegex(BridgeError, "invalid response"):
                    await client._request("GET", "")

    async def test_json_recursion_failure_is_sanitized(self):
        client, _, _ = self.make_client(FakeResponse())
        with patch(
            "ghmq_test_subject.core.json.loads",
            side_effect=RecursionError("private-detail"),
        ):
            with self.assertRaisesRegex(BridgeError, "invalid response") as context:
                await client._request("GET", "")
        self.assertNotIn("private-detail", str(context.exception))

    async def test_finite_json_number_is_accepted(self):
        client, _, _ = self.make_client(FakeResponse(chunks=[b'{"value":1.25}']))
        self.assertEqual(await client._request("GET", ""), {"value": 1.25})

    async def test_redirects_never_follow_or_disclose_location(self):
        for status in (301, 302, 303, 304, 307, 308):
            with self.subTest(status=status):
                response = FakeResponse(
                    status=status,
                    headers={
                        "Location": "https://untrusted.invalid/github_test_secret"
                    },
                )
                client, session, _ = self.make_client(response)
                with self.assertRaisesRegex(BridgeError, "redirected") as context:
                    await client._request("GET", "")
                self.assertNotIn("untrusted", str(context.exception))
                self.assertNotIn("github_test_secret", str(context.exception))
                self.assertEqual(len(session.calls), 1)
                self.assertFalse(response.content.requests)

    async def test_http_errors_never_disclose_remote_body_or_credentials(self):
        for status, error in (
            (401, AuthError),
            (403, BridgeError),
            (404, BridgeError),
            (400, BridgeError),
            (409, RetryableError),
            (422, RetryableError),
            (500, RetryableError),
        ):
            with self.subTest(status=status):
                response = FakeResponse(
                    {"message": "github_test_secret remote-private-detail"},
                    status=status,
                )
                client, _, _ = self.make_client(response)
                with self.assertRaises(error) as context:
                    await client._request("GET", "")
                self.assertNotIn("github_test_secret", str(context.exception))
                self.assertNotIn("remote-private-detail", str(context.exception))
                self.assertEqual(bool(response.content.requests), status == 403)

    async def test_missing_event_returns_none(self):
        client, _, _ = self.make_client(FakeResponse(status=404))
        self.assertIsNone(await client.read_event("events/a.json"))

    async def test_request_and_body_timeouts_are_sanitized(self):
        for response in (
            asyncio.TimeoutError("github_test_secret"),
            aiohttp.ClientConnectionError("github_test_secret"),
            FakeResponse(chunks=[b"{", asyncio.TimeoutError("github_test_secret")]),
            FakeResponse(chunks=[aiohttp.ClientPayloadError("github_test_secret")]),
        ):
            with self.subTest(response=type(response).__name__):
                client, _, _ = self.make_client(response)
                try:
                    await client._request("GET", "")
                except RetryableError as err:
                    rendered = "".join(traceback.format_exception(err))
                    self.assertNotIn("github_test_secret", str(err))
                    self.assertNotIn("TimeoutError: github_test_secret", rendered)
                    self.assertNotIn(
                        "ClientConnectionError: github_test_secret", rendered
                    )
                    self.assertNotIn("ClientPayloadError: github_test_secret", rendered)
                else:
                    self.fail("Transport failure was not translated")

    async def test_cancellation_is_not_swallowed(self):
        client, _, _ = self.make_client(asyncio.CancelledError())
        with self.assertRaises(asyncio.CancelledError):
            await client._request("GET", "")

    async def test_token_header_control_characters_are_rejected(self):
        for token in (
            "",
            "has space",
            "has\nnewline",
            "has\x00null",
            "nonasciié",
            "x" * 513,
        ):
            with self.subTest(token_length=len(token)):
                with self.assertRaises(AuthError):
                    GitHubClient(None, "example", "repo", "relay", 7, token)

    async def test_base64_file_round_trip_allows_github_line_wrapping(self):
        data = b'{"message":"hello"}\n'
        encoded = base64.b64encode(data).decode()
        client, session, _ = self.make_client(
            FakeResponse(
                {
                    "type": "file",
                    "encoding": "base64",
                    "content": encoded[:8] + "\n" + encoded[8:] + "\n",
                }
            )
        )
        self.assertEqual(await client.read_event("events/a.json"), data)
        self.assertEqual(session.calls[0][2]["params"], {"ref": "relay"})

    async def test_malformed_base64_is_rejected(self):
        for content in (
            None,
            17,
            [],
            {},
            "???",
            "Zg=",
            "Zg===",
            "Zh==",
            "Zm9=",
            "Zg==AAAA",
            "Zg== ",
            "Zg==\r\n",
            "é",
        ):
            with self.subTest(content=content):
                client, _, _ = self.make_client(
                    FakeResponse(
                        {"type": "file", "encoding": "base64", "content": content}
                    )
                )
                with self.assertRaisesRegex(BridgeError, "content is invalid"):
                    await client.read_event("events/a.json")

    async def test_missing_base64_content_is_rejected(self):
        client, _, _ = self.make_client(
            FakeResponse({"type": "file", "encoding": "base64"})
        )
        with self.assertRaisesRegex(BridgeError, "content is invalid"):
            await client.read_event("events/a.json")

    async def test_event_payload_size_is_enforced_after_decode(self):
        for length, valid in (
            (MAX_PAYLOAD_BYTES, True),
            (MAX_PAYLOAD_BYTES + 1, False),
        ):
            with self.subTest(length=length):
                client, _, _ = self.make_client(
                    FakeResponse(
                        {
                            "type": "file",
                            "encoding": "base64",
                            "content": base64.b64encode(b"x" * length).decode(),
                        }
                    )
                )
                if valid:
                    self.assertEqual(
                        len(await client.read_event("events/a.json")), length
                    )
                else:
                    with self.assertRaisesRegex(BridgeError, "size limit"):
                        await client.read_event("events/a.json")

    async def test_non_file_or_unsupported_encoding_is_rejected(self):
        for value in (
            {"type": "dir", "encoding": "base64"},
            {"type": "file", "encoding": "utf-8"},
        ):
            client, _, _ = self.make_client(FakeResponse(value))
            with self.assertRaisesRegex(BridgeError, "regular JSON file"):
                await client.read_event("events/a.json")

    async def test_malformed_pull_request_shape_has_fixed_error(self):
        repo = {"private": True, "default_branch": "main", "id": 1}
        for head in (
            None,
            "private-detail",
            [],
            {"ref": "relay", "repo": None},
            {"ref": "relay", "repo": "private-detail"},
        ):
            client, _, _ = self.make_client(
                FakeResponse(repo),
                FakeResponse({"id": 77, "state": "open", "head": head}),
            )
            with self.assertRaisesRegex(IdentityError, "repository identity"):
                await client.validate_destination()

    async def test_valid_destination_can_be_checked(self):
        client, _, _ = self.make_client(
            FakeResponse({"private": True, "default_branch": "main", "id": 1}),
            FakeResponse(
                {"id": 77, "state": "open", "head": {"ref": "relay", "repo": {"id": 1}}}
            ),
        )
        self.assertEqual(
            await client.validate_destination(),
            {"repository_id": 1, "pull_request_id": 77},
        )

    async def test_discovery_returns_valid_ids_without_repinning(self):
        client, session, _ = self.make_client(
            FakeResponse({"private": True, "default_branch": "main", "id": 123}),
            FakeResponse(
                {
                    "id": 456,
                    "state": "open",
                    "head": {"ref": "relay", "repo": {"id": 123}},
                }
            ),
            repository_id=None,
            pull_request_id=None,
        )
        self.assertEqual(
            await client.validate_destination(discover=True),
            {"repository_id": 123, "pull_request_id": 456},
        )
        with self.assertRaises(IdentityError):
            await client.create_event("events/a.json", b"{}")
        self.assertEqual([call[0] for call in session.calls], ["GET", "GET"])

    async def test_missing_or_invalid_pins_fail_before_every_http_path(self):
        for pin in ("repository_id", "pull_request_id"):
            for invalid in (None, True, False, 0, -1, 1.0, "1", [], {}):
                for operation in ("validate", "read", "create"):
                    with self.subTest(pin=pin, invalid=invalid, operation=operation):
                        client, session, _ = self.make_client(**{pin: invalid})
                        with self.assertRaises(IdentityError):
                            if operation == "validate":
                                await client.validate_destination()
                            elif operation == "read":
                                await client.read_event("events/a.json")
                            else:
                                await client.create_event("events/a.json", b"{}")
                        self.assertFalse(session.calls)

    async def test_changed_or_invalid_server_ids_never_reach_put(self):
        for destination in ("repository", "pull_request", "head_repository"):
            for invalid in (None, True, False, 0, -1, 1.0, "1", [], {}, 999):
                with self.subTest(destination=destination, invalid=invalid):
                    repository = {"private": True, "default_branch": "main", "id": 1}
                    pull_request = {
                        "id": 77,
                        "state": "open",
                        "head": {"ref": "relay", "repo": {"id": 1}},
                    }
                    target = {
                        "repository": repository,
                        "pull_request": pull_request,
                        "head_repository": pull_request["head"]["repo"],
                    }[destination]
                    if invalid is None:
                        target.pop("id")
                    else:
                        target["id"] = invalid
                    client, session, _ = self.make_client(
                        FakeResponse(repository), FakeResponse(pull_request)
                    )
                    with self.assertRaises(IdentityError):
                        await client.validate_destination()
                        await client.create_event("events/a.json", b"{}")
                    self.assertTrue(all(call[0] == "GET" for call in session.calls))

    async def test_discovery_rejects_missing_and_noninteger_server_ids(self):
        for destination in ("repository", "pull_request", "head_repository"):
            for invalid in (None, True, False, 0, -1, 1.0, "1"):
                with self.subTest(destination=destination, invalid=invalid):
                    repository = {"private": True, "default_branch": "main", "id": 1}
                    pull_request = {
                        "id": 77,
                        "state": "open",
                        "head": {"ref": "relay", "repo": {"id": 1}},
                    }
                    target = {
                        "repository": repository,
                        "pull_request": pull_request,
                        "head_repository": pull_request["head"]["repo"],
                    }[destination]
                    if invalid is None:
                        target.pop("id")
                    else:
                        target["id"] = invalid
                    client, session, _ = self.make_client(
                        FakeResponse(repository),
                        FakeResponse(pull_request),
                        repository_id=None,
                        pull_request_id=None,
                    )
                    with self.assertRaises(IdentityError):
                        await client.validate_destination(discover=True)
                    self.assertTrue(all(call[0] == "GET" for call in session.calls))

    async def test_discovery_cannot_override_an_existing_pin(self):
        client, session, _ = self.make_client(
            FakeResponse({"private": True, "default_branch": "main", "id": 999})
        )
        with self.assertRaises(IdentityError):
            await client.validate_destination(discover=True)
        self.assertEqual(len(session.calls), 1)

    async def test_permission_denied_403_ignores_unexhausted_quota_reset(self):
        for remaining in ("4998", "1", None):
            headers = {"X-RateLimit-Reset": "4600"}
            if remaining is not None:
                headers["X-RateLimit-Remaining"] = remaining
            with self.subTest(remaining=remaining):
                client, session, clock = self.make_client(
                    FakeResponse(
                        {"message": "Resource not accessible by personal access token"},
                        status=403,
                        headers=headers,
                    ),
                    FakeResponse(),
                )
                with self.assertRaisesRegex(BridgeError, "denied access"):
                    await client._request("GET", "/pulls/7")
                self.assertEqual(session.saved_cooldowns, [1060])
                self.assertEqual(client.not_before, 1060)
                clock.advance(59)
                with self.assertRaises(RateLimitError) as error:
                    await client._request("GET", "")
                self.assertEqual(error.exception.retry_at, 1060)
                self.assertEqual(len(session.calls), 1)
                clock.advance(1)
                await client._request("GET", "")
                self.assertEqual(len(session.calls), 2)

    async def test_genuine_rate_limit_evidence_preserves_later_reset(self):
        cases = (
            (429, {"X-RateLimit-Remaining": "4998"}, {}),
            (403, {"X-RateLimit-Remaining": "0"}, {}),
            (403, {"Retry-After": "120", "X-RateLimit-Remaining": "4998"}, {}),
            (
                403,
                {"X-RateLimit-Remaining": "4998"},
                {"message": "You have exceeded a secondary rate limit"},
            ),
            (403, {}, {"message": "API rate limit exceeded"}),
        )
        for status, headers, body in cases:
            with self.subTest(status=status, headers=headers, body=body):
                client, session, _ = self.make_client(
                    FakeResponse(
                        body,
                        status=status,
                        headers={**headers, "X-RateLimit-Reset": "4600"},
                    )
                )
                with self.assertRaises(RateLimitError) as error:
                    await client._request("GET", "")
                self.assertEqual(error.exception.delay, 3600)
                self.assertEqual(error.exception.retry_at, 4600)
                self.assertEqual(client.not_before, 4600)
                self.assertEqual(session.saved_cooldowns[-1], 4600)

    async def test_unlabelled_or_malformed_403_only_gets_conservative_cooldown(self):
        for chunks in ([b"{}"], [b"{"], [b""], [b'{"message":"unclassified"}']):
            with self.subTest(chunks=chunks):
                client, session, _ = self.make_client(
                    FakeResponse(
                        status=403,
                        chunks=chunks,
                        headers={
                            "X-RateLimit-Remaining": "4998",
                            "X-RateLimit-Reset": "4600",
                        },
                    )
                )
                with self.assertRaises(BridgeError):
                    await client._request("GET", "")
                self.assertEqual(client.not_before, 1060)
                self.assertEqual(session.saved_cooldowns, [1060])

    async def test_slow_cooldown_save_does_not_inflate_displayed_deadline(self):
        client, _, clock = self.make_client(
            FakeResponse(status=429, headers={"Retry-After": "120"})
        )

        async def slow_save(_deadline):
            clock.advance(30)

        client.bind_cooldown(0, slow_save)
        with self.assertRaises(RateLimitError) as error:
            await client._request("GET", "")
        self.assertEqual(error.exception.retry_at, 1120)
        self.assertEqual(client.not_before, 1120)

    async def test_existing_ambiguous_deadline_is_not_migrated_or_shortened(self):
        client, session, clock = self.make_client(FakeResponse())
        client.bind_cooldown(4600, client._persist_cooldown)
        clock.advance(397)
        with self.assertRaises(RateLimitError) as error:
            await client._request("GET", "")
        self.assertEqual(error.exception.retry_at, 4600)
        self.assertEqual(error.exception.delay, 3203)
        self.assertEqual(client.not_before, 4600)
        self.assertFalse(session.calls)
        self.assertFalse(session.saved_cooldowns)

    async def test_retry_after_and_reset_use_later_deadline(self):
        client, session, clock = self.make_client(
            FakeResponse(
                status=429, headers={"Retry-After": "10", "X-RateLimit-Reset": "1400"}
            ),
            FakeResponse(),
        )
        with self.assertRaises(RetryableError) as context:
            await client._request("GET", "")
        self.assertEqual(context.exception.delay, 400)
        self.assertEqual(client.not_before, 1400)
        clock.advance(399)
        with self.assertRaises(RetryableError):
            await client._request("GET", "")
        self.assertEqual(len(session.calls), 1)
        clock.advance(1)
        await client._request("GET", "")
        self.assertEqual(len(session.calls), 2)

    async def test_rate_limited_403_uses_case_insensitive_headers(self):
        client, _, _ = self.make_client(
            FakeResponse(
                status=403,
                headers={"x-ratelimit-remaining": "0", "x-ratelimit-reset": "1400"},
            )
        )
        with self.assertRaises(RetryableError) as context:
            await client._request("GET", "")
        self.assertEqual(context.exception.delay, 400)

    async def test_retry_after_alone_identifies_secondary_403(self):
        client, _, _ = self.make_client(
            FakeResponse(status=403, headers={"Retry-After": "120"})
        )
        with self.assertRaises(RetryableError) as context:
            await client._request("GET", "")
        self.assertEqual(context.exception.delay, 120)

    async def test_headerless_secondary_403_blocks_all_events_and_reads(self):
        for message in (
            "You have exceeded a secondary rate limit. Please wait.",
            "You have triggered an abuse detection mechanism.",
            "API rate limit exceeded for the private account.",
        ):
            with self.subTest(message=message):
                client, session, clock = self.make_client(
                    FakeResponse({"message": message}, status=403),
                    FakeResponse(),
                )
                with self.assertRaises(RetryableError) as context:
                    await client.create_event("events/a.json", b"{}")
                self.assertEqual(context.exception.delay, 60)
                self.assertEqual(session.saved_cooldowns, [1060])
                for number in range(3):
                    with self.assertRaises(RetryableError):
                        await client.create_event(f"events/{number}.json", b"{}")
                    with self.assertRaises(RetryableError):
                        await client.read_event(f"events/{number}.json")
                    with self.assertRaises(RetryableError):
                        await client.validate_destination()
                self.assertEqual(len(session.calls), 1)
                clock.advance(60)
                await client.create_event("events/b.json", b"{}")
                self.assertEqual(len(session.calls), 2)

    async def test_short_secondary_header_has_one_minute_minimum(self):
        client, session, _ = self.make_client(
            FakeResponse(status=403, headers={"Retry-After": "1"})
        )
        with self.assertRaises(RetryableError) as context:
            await client._request("GET", "")
        self.assertEqual(context.exception.delay, 60)
        self.assertEqual(session.saved_cooldowns, [1060])

    async def test_ordinary_403_stays_permission_error_with_bounded_body(self):
        response = FakeResponse(
            {"message": "Resource not accessible by personal access token"}, status=403
        )
        client, session, _ = self.make_client(response)
        with self.assertRaisesRegex(BridgeError, "denied access"):
            await client._request("GET", "")
        self.assertEqual(session.saved_cooldowns, [1060])
        self.assertLessEqual(max(response.content.requests), READ_CHUNK_BYTES)

    async def test_403_error_bodies_are_bounded_and_sanitized(self):
        response = FakeResponse(status=403, chunks=[b"x" * (MAX_RESPONSE_BYTES * 2)])
        client, session, _ = self.make_client(response)
        with self.assertRaises(RetryableError) as context:
            await client._request("GET", "")
        self.assertEqual(context.exception.delay, 60)
        self.assertEqual(session.saved_cooldowns, [1060])
        self.assertEqual(response.content.returned, MAX_RESPONSE_BYTES + 1)

    async def test_malformed_or_missing_403_body_keeps_shared_cooldown(self):
        for chunks in (
            [],
            [b"{"],
            [b"[]"],
            [b"null"],
            [b'{"message":true}'],
            [asyncio.TimeoutError("private-detail")],
        ):
            with self.subTest(chunks=chunks):
                client, session, _ = self.make_client(
                    FakeResponse(status=403, chunks=chunks)
                )
                with self.assertRaises(BridgeError) as context:
                    await client._request("GET", "")
                self.assertNotIn("private-detail", str(context.exception))
                self.assertEqual(session.saved_cooldowns, [1060])
                with self.assertRaises(RetryableError):
                    await client.create_event("events/a.json", b"{}")
                self.assertEqual(len(session.calls), 1)

    async def test_cooldown_is_durable_before_retry_is_returned(self):
        client, session, clock = self.make_client(
            FakeResponse(status=429, headers={"Retry-After": "120"})
        )
        started = asyncio.Event()
        release = asyncio.Event()
        saved = []

        async def persist(deadline):
            started.set()
            await release.wait()
            saved.append(deadline)

        client.bind_cooldown(0, persist)
        request = asyncio.create_task(client._request("GET", ""))
        await started.wait()
        self.assertFalse(request.done())
        self.assertEqual(client.not_before, 1120)
        # A slow durable save must block requests even after clock expiry.
        clock.advance(300)
        with self.assertRaises(RetryableError):
            await client._request("GET", "")
        self.assertEqual(len(session.calls), 1)
        release.set()
        with self.assertRaises(RetryableError):
            await request
        self.assertEqual(saved, [1120])

    async def test_cooldown_save_failure_freezes_all_http_until_reload(self):
        for failure in (
            OSError("github_test_secret private-path"),
            asyncio.TimeoutError("github_test_secret private-path"),
        ):
            with self.subTest(failure=type(failure).__name__):
                client, session, clock = self.make_client(FakeResponse(status=429))

                async def persist(_deadline):
                    raise failure

                client.bind_cooldown(0, persist)
                with self.assertRaises(JournalError) as context:
                    await client._request("GET", "")
                rendered = "".join(traceback.format_exception(context.exception))
                self.assertNotIn("github_test_secret", rendered)
                self.assertNotIn("private-path", rendered)
                clock.advance(10000)
                for operation in (
                    client.validate_destination,
                    lambda: client.read_event("events/a.json"),
                    lambda: client.create_event("events/a.json", b"{}"),
                ):
                    with self.assertRaises(JournalError):
                        await operation()
                with self.assertRaises(JournalError):
                    client.bind_cooldown(0, persist)
                self.assertEqual(len(session.calls), 1)

    async def test_cancelled_durable_cooldown_save_cannot_resume_http(self):
        client, session, clock = self.make_client(FakeResponse(status=429))
        started = asyncio.Event()
        release = asyncio.Event()
        saved = []

        async def persist(deadline):
            async def commit():
                started.set()
                await release.wait()
                saved.append(deadline)

            writing = asyncio.create_task(commit())
            try:
                await asyncio.shield(writing)
            except asyncio.CancelledError:
                await asyncio.shield(writing)
                raise JournalCommitCancelled() from None

        client.bind_cooldown(0, persist)
        request = asyncio.create_task(client._request("GET", ""))
        await started.wait()
        request.cancel()
        await asyncio.sleep(0)
        self.assertFalse(request.done())
        clock.advance(10000)
        with self.assertRaises(RetryableError):
            await client._request("GET", "")
        release.set()
        with self.assertRaises(asyncio.CancelledError):
            await request
        self.assertEqual(saved, [1060])
        with self.assertRaises(JournalError):
            await client._request("GET", "")
        self.assertEqual(len(session.calls), 1)

    async def test_restored_deadline_blocks_all_http_and_wall_clock_jumps(self):
        client, session, clock = self.make_client(FakeResponse())

        async def persist(_deadline):
            self.fail("Restoring a deadline should not rewrite storage")

        client.bind_cooldown(1600, persist)
        clock.wall += 3600
        for operation in (
            client.validate_destination,
            lambda: client.read_event("events/a.json"),
            lambda: client.create_event("events/a.json", b"{}"),
        ):
            with self.assertRaises(RetryableError) as context:
                await operation()
            self.assertEqual(context.exception.delay, 600)
        self.assertFalse(session.calls)
        clock.mono += 600
        await client._request("GET", "")
        self.assertEqual(len(session.calls), 1)

    async def test_cancellation_waiting_for_cooldown_lock_keeps_latest_deadline(self):
        client, session, _ = self.make_client()
        first_started = asyncio.Event()
        release = asyncio.Event()
        saved = []

        async def persist(deadline):
            if not saved:
                first_started.set()
                await release.wait()
            saved.append(deadline)

        client.bind_cooldown(0, persist)
        first = asyncio.create_task(client._rate_limited({"Retry-After": "600"}))
        await first_started.wait()
        second = asyncio.create_task(client._rate_limited({"Retry-After": "1200"}))
        await asyncio.sleep(0)
        self.assertEqual(client.not_before, 2200)
        second.cancel()
        await asyncio.sleep(0)
        second.cancel()
        await asyncio.sleep(0)
        self.assertFalse(second.done())
        release.set()
        await first
        with self.assertRaises(asyncio.CancelledError):
            await second
        self.assertEqual(saved, [1600, 2200])
        with self.assertRaises(JournalError):
            await client._request("GET", "")
        self.assertFalse(session.calls)

    async def test_cancelled_earlier_saver_cannot_discard_later_deadline(self):
        client, _, _ = self.make_client()
        first_started = asyncio.Event()
        release_first = asyncio.Event()
        second_started = asyncio.Event()
        release_second = asyncio.Event()
        saved = []

        async def persist(deadline):
            if not saved:
                first_started.set()
                await release_first.wait()
            else:
                second_started.set()
                await release_second.wait()
            saved.append(deadline)

        client.bind_cooldown(0, persist)
        first = asyncio.create_task(client._rate_limited({"Retry-After": "600"}))
        await first_started.wait()
        second = asyncio.create_task(client._rate_limited({"Retry-After": "1200"}))
        await asyncio.sleep(0)
        first.cancel()
        release_first.set()
        with self.assertRaises(asyncio.CancelledError):
            await first
        await second_started.wait()
        release_second.set()
        with self.assertRaises(JournalError):
            await second
        self.assertEqual(saved, [1600, 2200])

    async def test_earlier_rebind_cannot_shorten_restored_cooldown(self):
        client, session, clock = self.make_client()

        async def persist(_deadline):
            pass

        client.bind_cooldown(1600, persist)
        clock.advance(10)
        client.bind_cooldown(1100, persist)
        self.assertEqual(client.not_before, 1600)
        with self.assertRaises(RetryableError) as context:
            await client._request("GET", "")
        self.assertEqual(context.exception.delay, 590)
        self.assertFalse(session.calls)

    async def test_live_journal_cooldown_blocks_sibling_client_without_http(self):
        deadline = 0
        client, session, clock = self.make_client()

        async def persist(_deadline):
            pass

        client.bind_cooldown(0, persist, current_deadline=lambda: deadline)
        deadline = 1600
        with self.assertRaises(RetryableError) as context:
            await client.validate_destination()
        self.assertEqual(context.exception.delay, 600)
        clock.wall += 3600
        with self.assertRaises(RetryableError) as context:
            await client.read_event("events/a.json")
        self.assertEqual(context.exception.delay, 600)
        deadline = 1100
        with self.assertRaises(RetryableError) as context:
            await client.create_event("events/a.json", b"{}")
        self.assertEqual(context.exception.delay, 600)
        self.assertFalse(session.calls)

    async def test_failed_or_invalid_shared_deadline_freezes_transport(self):
        for value in (float("nan"), "1600", True, -1, None):
            client, session, _ = self.make_client()

            async def persist(_deadline):
                pass

            client.bind_cooldown(0, persist, current_deadline=lambda: value)
            with self.assertRaises(JournalError):
                await client._request("GET", "")
            self.assertFalse(session.calls)
        client, session, _ = self.make_client()

        def failed_getter():
            raise JournalError("private-detail")

        client.bind_cooldown(0, persist, current_deadline=failed_getter)
        with self.assertRaises(JournalError) as context:
            await client._request("GET", "")
        self.assertNotIn(
            "private-detail", "".join(traceback.format_exception(context.exception))
        )
        self.assertFalse(session.calls)

    async def test_announced_cooldown_precedes_already_queued_sibling_request(self):
        client, session, _ = self.make_client(FakeResponse(status=429))
        sibling, sibling_session, _ = self.make_client()
        shared_deadline = 0
        order = []
        saved = []

        def announce(deadline):
            nonlocal shared_deadline
            order.append("announce")
            shared_deadline = max(shared_deadline, deadline)

        async def persist(deadline):
            order.append("persist")
            saved.append(deadline)

        for transport in (client, sibling):
            transport.bind_cooldown(
                0,
                persist,
                current_deadline=lambda: shared_deadline,
                announce_deadline=announce,
            )

        async def sibling_request():
            order.append("sibling")
            await sibling._request("GET", "")

        # The sibling is ready before the first response can schedule its
        # asynchronous save. It must see the announcement at the first yield.
        first = asyncio.create_task(client._request("GET", ""))
        second = asyncio.create_task(sibling_request())
        results = await asyncio.gather(first, second, return_exceptions=True)
        self.assertTrue(all(isinstance(result, RetryableError) for result in results))
        self.assertEqual(order, ["announce", "sibling", "persist"])
        self.assertEqual(saved, [1060])
        self.assertEqual(len(session.calls), 1)
        self.assertFalse(sibling_session.calls)

    async def test_announcement_failure_is_sanitized_and_freezes_transport(self):
        client, session, clock = self.make_client(FakeResponse(status=429))
        saved = []

        def announce(_deadline):
            raise OSError("github_test_secret private-path")

        async def persist(deadline):
            saved.append(deadline)

        client.bind_cooldown(0, persist, announce_deadline=announce)
        with self.assertRaises(JournalError) as context:
            await client._request("GET", "")
        rendered = "".join(traceback.format_exception(context.exception))
        self.assertNotIn("github_test_secret", rendered)
        self.assertNotIn("private-path", rendered)
        clock.advance(10000)
        with self.assertRaises(JournalError):
            await client._request("GET", "")
        self.assertEqual(len(session.calls), 1)
        self.assertFalse(saved)

    async def test_deadline_announcement_never_shortens_existing_maximum(self):
        client, _, clock = self.make_client()
        announced = []

        async def persist(_deadline):
            pass

        client.bind_cooldown(0, persist, announce_deadline=announced.append)
        await client._rate_limited({"Retry-After": "600"})
        clock.advance(10)
        await client._rate_limited({"Retry-After": "1"})
        self.assertEqual(announced, [1600, 1600])

    async def test_sibling_stays_blocked_until_slow_cooldown_save_finishes(self):
        client, _, _ = self.make_client(FakeResponse(status=429))
        sibling, sibling_session, sibling_clock = self.make_client(FakeResponse())
        deadline = 0
        committed = 0
        started = asyncio.Event()
        release = asyncio.Event()

        def announce(value):
            nonlocal deadline
            deadline = max(deadline, value)

        async def persist(value):
            nonlocal committed
            started.set()
            await release.wait()
            committed = max(committed, value)

        for transport in (client, sibling):
            transport.bind_cooldown(
                0,
                persist,
                current_deadline=lambda: deadline,
                announce_deadline=announce,
                is_cooldown_pending=lambda: deadline > committed,
            )

        request = asyncio.create_task(client._request("GET", ""))
        await started.wait()
        sibling_clock.advance(10000)
        with self.assertRaises(RetryableError) as context:
            await sibling._request("GET", "")
        self.assertGreaterEqual(context.exception.delay, 1)
        self.assertFalse(sibling_session.calls)
        release.set()
        with self.assertRaises(RetryableError):
            await request
        self.assertEqual(committed, 1060)
        await sibling._request("GET", "")
        self.assertEqual(len(sibling_session.calls), 1)

    async def test_invalid_or_failed_shared_pending_getter_freezes_transport(self):
        for value in (None, 0, 1, "true", [], {}):
            with self.subTest(value=value):
                client, session, _ = self.make_client()

                async def persist(_deadline):
                    pass

                client.bind_cooldown(0, persist, is_cooldown_pending=lambda: value)
                with self.assertRaises(JournalError):
                    await client._request("GET", "")
                self.assertFalse(session.calls)
        client, session, _ = self.make_client()

        def failed_getter():
            raise OSError("github_test_secret private-path")

        client.bind_cooldown(0, persist, is_cooldown_pending=failed_getter)
        with self.assertRaises(JournalError) as context:
            await client._request("GET", "")
        rendered = "".join(traceback.format_exception(context.exception))
        self.assertNotIn("github_test_secret", rendered)
        self.assertNotIn("private-path", rendered)
        with self.assertRaises(JournalError):
            client.bind_cooldown(0, persist)
        self.assertFalse(session.calls)

    async def test_invalid_restored_deadline_freezes_without_http(self):
        for deadline in (None, True, False, -1, "1600", float("nan"), float("inf")):
            with self.subTest(deadline=deadline):
                client, session, _ = self.make_client()

                async def persist(_deadline):
                    pass

                with self.assertRaises(JournalError):
                    client.bind_cooldown(deadline, persist)
                with self.assertRaises(JournalError):
                    await client._request("GET", "")
                self.assertFalse(session.calls)

    async def test_missing_rate_headers_default_to_one_minute(self):
        client, _, _ = self.make_client(FakeResponse(status=429))
        with self.assertRaises(RetryableError) as context:
            await client._request("GET", "")
        self.assertEqual(context.exception.delay, 60)

    async def test_bad_retry_header_cannot_shorten_valid_reset(self):
        for malformed in ("NaN", "inf", "-Infinity", "1e999", "-12", "nonsense", ""):
            with self.subTest(header=malformed):
                client, _, _ = self.make_client(
                    FakeResponse(
                        status=429,
                        headers={"Retry-After": malformed, "X-RateLimit-Reset": "2000"},
                    )
                )
                with self.assertRaises(RetryableError) as context:
                    await client._request("GET", "")
                self.assertEqual(context.exception.delay, 1000)
                self.assertTrue(math.isfinite(client.not_before))

    async def test_bad_reset_cannot_shorten_valid_retry_header(self):
        for malformed in ("NaN", "Infinity", "1e999", "-12", "nonsense", ""):
            with self.subTest(header=malformed):
                client, _, _ = self.make_client(
                    FakeResponse(
                        status=429,
                        headers={"Retry-After": "3600", "X-RateLimit-Reset": malformed},
                    )
                )
                with self.assertRaises(RetryableError) as context:
                    await client._request("GET", "")
                self.assertEqual(context.exception.delay, 3600)
                self.assertTrue(math.isfinite(client.not_before))

    async def test_both_bad_rate_headers_have_finite_safe_fallback(self):
        client, _, _ = self.make_client(
            FakeResponse(
                status=429,
                headers={"Retry-After": "NaN", "X-RateLimit-Reset": "Infinity"},
            )
        )
        with self.assertRaises(RetryableError) as context:
            await client._request("GET", "")
        self.assertEqual(context.exception.delay, 60)
        self.assertTrue(math.isfinite(client.not_before))

    async def test_http_date_retry_after_is_honored(self):
        deadline = format_datetime(
            datetime.fromtimestamp(1600, timezone.utc), usegmt=True
        )
        client, _, _ = self.make_client(
            FakeResponse(status=429, headers={"Retry-After": deadline})
        )
        with self.assertRaises(RetryableError) as context:
            await client._request("GET", "")
        self.assertEqual(context.exception.delay, 600)

    async def test_late_in_flight_rate_response_never_shortens_deadline(self):
        client, session, clock = self.make_client()
        await client._rate_limited({"Retry-After": "600"})
        clock.advance(10)
        error = await client._rate_limited({"Retry-After": "1"})
        self.assertEqual(client.not_before, 1600)
        self.assertEqual(error.delay, 590)
        self.assertEqual(session.saved_cooldowns, [1600, 1600])

    async def test_wall_clock_jump_does_not_shorten_retry_after(self):
        client, session, clock = self.make_client(
            FakeResponse(status=429, headers={"Retry-After": "600"})
        )
        with self.assertRaises(RetryableError):
            await client._request("GET", "")
        clock.wall += 3600
        with self.assertRaises(RetryableError) as context:
            await client._request("GET", "")
        self.assertEqual(context.exception.delay, 600)
        self.assertEqual(len(session.calls), 1)

    async def test_late_response_persists_remaining_delay_after_wall_clock_jump(self):
        client, session, clock = self.make_client()
        await client._rate_limited({"Retry-After": "600"})
        clock.advance(10)
        clock.wall += 3600
        error = await client._rate_limited({"Retry-After": "1"})
        self.assertEqual(error.delay, 590)
        self.assertEqual(client.not_before, 5200)
        self.assertEqual(session.saved_cooldowns, [1600, 5200])

    async def test_late_response_observes_longer_shared_journal_cooldown(self):
        client, session, _ = self.make_client()
        current = 1800

        async def persist(deadline):
            session.saved_cooldowns.append(deadline)

        client.bind_cooldown(0, persist, current_deadline=lambda: current)
        error = await client._rate_limited({"Retry-After": "1"})
        self.assertEqual(error.delay, 800)
        self.assertEqual(session.saved_cooldowns, [1800])

    async def test_large_finite_server_delay_is_not_capped(self):
        client, _, _ = self.make_client(
            FakeResponse(status=429, headers={"Retry-After": "1000000000"})
        )
        with self.assertRaises(RetryableError) as context:
            await client._request("GET", "")
        self.assertEqual(context.exception.delay, 1000000000)

    async def test_creates_are_spaced_across_successful_calls(self):
        client, session, clock = self.make_client(
            FakeResponse(), FakeResponse(), FakeResponse()
        )
        for _ in range(3):
            await client.create_event("events/a.json", b"{}")
        self.assertEqual([call[3] for call in session.calls], [100, 101, 102])
        self.assertEqual(clock.sleeps, [1, 1])

    async def test_preparation_does_not_double_delay_create(self):
        client, session, clock = self.make_client(FakeResponse(), FakeResponse())
        await client.create_event("events/a.json", b"{}")
        await client.prepare_create()
        await client.create_event("events/b.json", b"{}")
        self.assertEqual([call[3] for call in session.calls], [100, 101])
        self.assertEqual(clock.sleeps, [1])

    async def test_failed_creates_reserve_the_same_spacing(self):
        for failure in (
            FakeResponse(status=409),
            FakeResponse(status=422),
            FakeResponse(status=500),
            asyncio.TimeoutError("secret"),
        ):
            with self.subTest(failure=type(failure).__name__):
                client, session, _ = self.make_client(failure, FakeResponse())
                with self.assertRaises(RetryableError):
                    await client.create_event("events/a.json", b"{}")
                await client.create_event("events/a.json", b"{}")
                self.assertGreaterEqual(session.calls[1][3] - session.calls[0][3], 1)

    async def test_concurrent_creates_are_serialized_and_spaced(self):
        client, session, _ = self.make_client(*(FakeResponse() for _ in range(5)))
        await asyncio.gather(
            *(client.create_event(f"events/{n}.json", b"{}") for n in range(5))
        )
        self.assertEqual([call[3] for call in session.calls], [100, 101, 102, 103, 104])

    async def test_early_sleep_wakeup_rechecks_spacing(self):
        client, session, clock = self.make_client(FakeResponse(), FakeResponse())

        async def early_sleep(seconds):
            clock.advance(min(seconds, 0.25))

        client._sleep = early_sleep
        await client.create_event("events/a.json", b"{}")
        await client.create_event("events/b.json", b"{}")
        self.assertEqual(session.calls[1][3] - session.calls[0][3], 1)

    async def test_expiry_is_rechecked_after_pacing_before_any_write(self):
        client, session, clock = self.make_client(FakeResponse())
        await client.create_event("events/a.json", b"{}")
        expiry = datetime.fromtimestamp(clock.wall + 0.5, timezone.utc)
        with self.assertRaises(EventExpired):
            await client.create_event("events/b.json", b"{}", expires_at=expiry)
        self.assertEqual(len(session.calls), 1)

    async def test_already_expired_event_never_writes(self):
        client, session, clock = self.make_client()
        with self.assertRaises(EventExpired):
            await client.create_event(
                "events/a.json",
                b"{}",
                expires_at=datetime.fromtimestamp(clock.wall, timezone.utc),
            )
        self.assertFalse(session.calls)

    async def test_future_expiry_allows_write_and_keeps_immutable_body(self):
        client, session, clock = self.make_client(FakeResponse())
        await client.create_event(
            "events/a.json",
            b"{}",
            expires_at=datetime.fromtimestamp(clock.wall + 60, timezone.utc),
        )
        body = session.calls[0][2]["json"]
        self.assertEqual(body["branch"], "relay")
        self.assertEqual(base64.b64decode(body["content"]), b"{}")
        self.assertNotIn("sha", body)

    async def test_invalid_expiry_has_fixed_error_and_never_writes(self):
        for expiry in ("secret", datetime(2026, 1, 1)):
            client, session, _ = self.make_client()
            with self.assertRaisesRegex(BridgeError, "Invalid event expiry"):
                await client.create_event("events/a.json", b"{}", expires_at=expiry)
            self.assertFalse(session.calls)

    async def test_oversized_outbound_payload_never_writes(self):
        client, session, _ = self.make_client()
        with self.assertRaisesRegex(BridgeError, "payload limit"):
            await client.create_event("events/a.json", b"x" * (MAX_PAYLOAD_BYTES + 1))
        self.assertFalse(session.calls)

    async def test_lone_surrogate_json_is_rejected(self):
        client, session, _ = self.make_client(
            FakeResponse(chunks=[b'{"unused":"\\ud800"}'])
        )
        with self.assertRaisesRegex(BridgeError, "invalid response"):
            await client._request("GET", "")


if __name__ == "__main__":
    unittest.main()
