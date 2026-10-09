# GHMQ receiver contract, schema version 1

GHMQ supplies the **sender only**. This document describes the files it writes and the minimum validation and handling expected of a separate receiver. It is not a claim that a receiver, webhook subscription, or end-to-end notification path is installed or working.

## Transport boundary

The sender creates `events/<event_id>.json` on its configured non-default branch in an active private GitHub repository. The branch must be the head of an open pull request in the same repository. Repository and pull-request numeric IDs are pinned at setup and rechecked before delivery attempts.

An event is written through the GitHub Contents API without a replacement SHA. GHMQ does not intentionally overwrite an existing event. If the path already exists, its decoded bytes must exactly match the original serialized envelope; different bytes cause a conflict. A same-ID file with different whitespace therefore conflicts too.

The wire representation is UTF-8 JSON, with object keys sorted, no insignificant spaces, literal Unicode, and one trailing newline. The whole decoded file is at most **4,096 bytes**. Receivers should parse strictly and validate the schema; canonical byte identity is primarily the sender's conflict check, not proof of authenticity.

A GitHub push or pull-request notification can serve as a wake-up signal for a receiver, but GHMQ does not configure one. A webhook payload is not the event envelope. Authenticate the wake-up, then retrieve bounded files from the trusted repository and a verified commit/ref. Do not follow attacker-supplied URLs or treat an arbitrary PR's content as trusted.

## Exact envelope

There are **nine required fields** and **one optional field**. No other fields are defined for schema version 1.

| Field | Type | Constraint |
| --- | --- | --- |
| `schema_version` | Integer | Exactly `1`; a boolean is not an integer |
| `event_id` | String | `[A-Za-z0-9_-]{1,96}`; must equal the filename stem |
| `source` | String | Exactly `home_assistant` |
| `event_type` | String | `[a-z][a-z0-9_.-]{0,63}` |
| `message` | String | 1–512 characters; no code point below U+0020 except newline and tab |
| `severity` | String | `info`, `warning`, or `urgent` |
| `synthetic` | Boolean | Exactly a JSON boolean |
| `occurred_at` | String | ISO-8601 timestamp with timezone; sender emits UTC with `Z` |
| `expires_at` | String | ISO-8601 timestamp with timezone; sender emits UTC with `Z` |
| `entity_id` | String, optional | `[a-z0-9_]+\.[a-z0-9_]{1,100}`; identifier only |

`expires_at - occurred_at` must be an integer number of seconds from **60 through 3,600**. The sender defaults to 300 seconds. `ttl_seconds` and `config_entry_id` are action parameters and are **not wire fields**. There is no signature, token, repository, PR number, arbitrary context, image, or entity state in the envelope.

This illustrative event is synthetic. Its fixed timestamps are for documentation and will be expired when replayed later:

```json
{
  "schema_version": 1,
  "event_id": "example-event-001",
  "source": "home_assistant",
  "event_type": "demo.check",
  "message": "Synthetic GHMQ connectivity check",
  "severity": "info",
  "synthetic": true,
  "occurred_at": "2026-01-01T12:00:00Z",
  "expires_at": "2026-01-01T12:05:00Z"
}
```

The example is pretty-printed for reading. [The file example](../examples/event-v1.json) uses the sender's canonical serialization. Generate current timestamps through the Home Assistant action for a live check; do not edit and resend an old event as if it were fresh.

## Receiver acceptance procedure

1. **Authenticate the transport.** Validate GitHub webhook signatures against a securely stored webhook secret when using webhooks. Verify repository identity, event/action type, branch/ref, and the expected PR identity if using PR events. A valid webhook signature proves the configured delivery source, not that every repository writer's content is safe. Follow [GitHub's webhook validation guidance](https://docs.github.com/en/webhooks/using-webhooks/validating-webhook-deliveries).
2. **Resolve a trusted file.** Restrict reads to the configured private repository and `events/` namespace, and to the expected branch or a verified commit belonging to that delivery. Reject traversal, symlinks, submodules, unexpected nested paths, oversized files, and arbitrary download destinations. A commit-pinned read avoids silently replacing a notified event with later branch content.
3. **Parse strictly and bound resources.** Decode UTF-8 and JSON with size, time, and nesting limits. Reject duplicate keys, non-finite numbers, invalid Unicode, non-object envelopes, unknown fields, missing fields, wrong types, unsupported schema versions, and violations of the table above. Validate the path/ID match and the complete envelope before handing content to a downstream system.
4. **Check freshness independently.** Reject at or after `expires_at`, impossible lifetimes, and occurrence times more than 30 seconds in the future. Keep clocks synchronized. Recheck expiry before a delayed downstream action. The sender checks freshness before attempts, but requests and webhook processing can cross the deadline.
5. **Deduplicate durably.** Use a key including the trusted repository identity and event ID; never rely on `event_id` alone across unrelated senders. Retain a content fingerprint and reject conflicting reuse. GitHub delivery IDs can additionally identify repeated webhook deliveries but do not replace application event IDs. Persist deduplication and recovery state across receiver restarts.
6. **Authorize the effect.** Allow-list event types and destinations and handle `synthetic` events through an intentional test path. Treat message and entity text as data, not code, markup, commands, or instructions to an AI. An `urgent` label does not grant extra permissions.
7. **Record the real outcome.** Track received, rejected, expired, deduplicated, and processed outcomes separately. Design crash recovery around the downstream action; recording a deduplication key alone cannot make an external side effect exactly once. Use downstream idempotency where available.

These are receiver requirements and recommendations; GHMQ does not enforce them in another service.

## Acknowledgement and failure semantics

The sender's `accepted` result means GitHub accepted the file, the identical file was found, or a retained local result records earlier acceptance. It is **not** an acknowledgement from this receiver. The integration has no callback, response event, receipt file, or consumer-processed status.

The sender journals before remote mutation and retries eligible pending events while enabled. A service error or cancellation can leave work durably pending, and a network timeout can occur after GitHub commits the file. In-flight requests can finish after the deadline. A receiver must expect duplicate wake-ups, late delivery, missing wake-ups, uncertain sender outcomes, and process restarts.

An `expired` sender result means further local delivery attempts stop for that retained event. It does not prove that no prior ambiguous request reached GitHub. Receiver-side expiry enforcement remains necessary.

There is no end-to-end ordering or exactly-once guarantee. Multiple independent senders, receivers, repository edits, and retry timing can change arrival order. Use event IDs as identifiers, not an ordering sequence.

## Missed wake-ups and recovery

GitHub does not automatically redeliver failed webhook deliveries. The receiver operator must decide whether to monitor and redeliver failed deliveries, reconcile branch contents, or accept missed events. Reconciliation must preserve original expiry and use bounded, paginated reads; expired history must never become a fresh notification. See [GitHub's failed-delivery guidance](https://docs.github.com/en/webhooks/using-webhooks/handling-failed-webhook-deliveries).

Reconciliation only when another webhook arrives is insufficient: if that later wake-up never happens, a committed event can remain unnoticed. Stronger reliability needs an independently scheduled polling/backfill path, durable receiver state, and explicit consumer acknowledgements or downstream receipts. GHMQ provides none of those mechanisms, and its short event lifetimes still bound what a delayed receiver may process. It must not be treated as an alarm-grade channel.

Do not assume one Contents API directory listing is a complete backlog: GitHub documents a 1,000-file directory limit. A long-lived relay needs a considered tree/enumeration and retention strategy, with GitHub API limits in mind. See [repository contents limits](https://docs.github.com/en/rest/repos/contents#get-repository-content).

Webhook availability, event subscriptions, repository access, downstream delivery, and recovery are deployment-specific. A successful synthetic file write verifies none of them by itself.

## Retention and permissions

Expiry is a processing deadline only. GHMQ does not delete expired files or prune Git history, and recent local request records persist until bounded eviction. Receivers and their operators must account for their own logs, stores, webhook records, clones, and backups.

Give a read-only receiver only the repository permissions it actually needs. Do not reuse the sender's write token just for convenience. If a receiver needs to administer webhooks or redeliver failures, separately review those additional permissions. Never store either access credentials or the webhook secret in an event.
