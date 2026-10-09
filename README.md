# GHMQ: GitHub Message Queuing for Home Assistant

GHMQ is an outbound-only Home Assistant event bridge for agents and other receivers connected through GitHub. It is designed for occasional, non-sensitive notification events. An explicit `ghmq.send_event` action saves a small JSON envelope locally, then creates `events/<event_id>.json` on a private GitHub relay branch with an open pull request.

A separate receiver can observe that branch and handle the event. **No receiver, webhook endpoint, GitHub App, or notification service is included.**

> Version **0.1.1** is published in [norgan/ghmq](https://github.com/norgan/ghmq) under the [MIT License](LICENSE). GHMQ is not included in the HACS default catalog. Check [GitHub Actions](https://github.com/norgan/ghmq/actions) for hosted validation status.

## What this is for

- Low-volume, asynchronous notifications where delayed or missed delivery is acceptable
- A reviewable event trail in a dedicated private repository
- Explicit, narrow automations that choose exactly what text leaves Home Assistant

This is not a general message broker. There are no consumer acknowledgements, subscriptions, visibility timeouts, delivery guarantees, or automatic retention cleanup. There is no inbound command channel, Home Assistant state export, device discovery, or automatic state listener. The integration does not read the state or attributes of an optional `entity_id`.

For continuous telemetry, high throughput, low latency, or safety-critical alerts, use infrastructure designed for those requirements. GitHub availability, API limits, repository rules, and the receiver all affect delivery. An `urgent` severity is just a label.

## Compatibility

- Targeted and tested runtime: Home Assistant **2026.10.0**, Python **3.14.2**
- GitHub.com only, using its REST API; GitHub Enterprise Server and arbitrary API hosts are not supported
- A private, active repository, an existing non-default relay branch, and an open pull request whose head is that branch in the same repository

Older and future Home Assistant versions require their own verification. HACS metadata specifies Home Assistant 2026.10.0 as the minimum.

## Install

### Manual installation

1. Download the source from this repository. Copy only `custom_components/ghmq` into your Home Assistant configuration directory's `custom_components` directory.
2. Restart Home Assistant.
3. Open **Settings → Devices & services → Add integration**, then search for **GHMQ**.

Do not copy a development environment, test evidence, credentials, or another installation's configuration into Home Assistant.

### HACS custom repository

This public repository uses the HACS custom integration layout and is not included in the HACS default catalog. Follow [HACS custom repository instructions](https://www.hacs.xyz/docs/faq/custom_repositories/), add `https://github.com/norgan/ghmq` with type **Integration**, download GHMQ, and restart Home Assistant. Adding a custom repository does not mean HACS has reviewed or endorsed it.

## Prepare a private relay

The public integration source and your private event relay are different repositories. Never use a public source repository as the event destination.

1. Create or choose a dedicated private repository containing no application secrets or unrelated personal data. Review its collaborators, installed apps, Actions workflows, and webhooks.
2. Create a branch such as `relay/events`, separate from the default branch. Add a harmless initial change if needed to open a pull request.
3. Open a pull request from that branch to another branch in the same repository. Keep it open while using the relay. GHMQ does not create, merge, close, or maintain this pull request.
4. Configure your separate receiver for the chosen repository and branch. See the [receiver contract](docs/receiver-contract.md).
5. Create a fine-grained personal access token limited to **only this relay repository**, with:
   - **Contents: Read and write**
   - **Metadata: Read**
   - **Pull requests: Read**, recommended for reliable pull-request validation

The file-create endpoint requires Contents write; repository lookup uses Metadata read. GitHub's current Get a pull request documentation lists Contents read or Pull requests read as alternatives. GHMQ nevertheless recommends explicit Pull requests read because permission behavior must be verified for the actual repository and token. Do not assume Contents alone will validate a private pull request. These permissions do not require Workflows, Administration, or Pull requests write. See [file creation](https://docs.github.com/en/rest/repos/contents#create-or-update-file-contents), [repository lookup](https://docs.github.com/en/rest/repos/repos#get-a-repository), [pull-request lookup](https://docs.github.com/en/rest/pulls/pulls#get-a-pull-request), and [fine-grained token setup](https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/managing-your-personal-access-tokens).

Set an appropriate token expiry and satisfy any organization approval requirements. The token's permissions remain repository-wide: GHMQ's branch/path restrictions are application safeguards, not a branch-scoped GitHub credential. Protect the token accordingly.

## Configure

Enter these fields in the integration's setup form:

| Field | Meaning |
| --- | --- |
| GitHub owner | Exact account or organization owning the private relay |
| Repository | Repository name only, without an owner or URL |
| Relay branch | Existing non-default branch; default suggestion is `relay/events` |
| Pull request number | Positive integer identifying its existing open pull request |
| GitHub access token | The repository-scoped token, entered in the password-style field |

Setup reads GitHub to validate the destination and saves its numeric repository and pull-request IDs. It does **not** write a test event. Sending starts **disabled**.

After reviewing the destination, access permissions, receiver, and privacy implications, open GHMQ's options and turn on **Enable sending**. This also resumes queued events that have not expired. Disabling stops new admissions and automatic retries after active work drains; it cannot recall an in-flight request or a file already accepted by GitHub.

A destination identity mismatch fails closed. Reauthentication changes only the token and preserves the saved destination IDs and sending option; it cannot approve a different repository or pull request. A new destination needs a separately reviewed configuration.

## Send a synthetic event

In **Developer tools → Actions**, select `ghmq.send_event`, or use this YAML:

```yaml
action: ghmq.send_event
data:
  event_type: demo.check
  message: Synthetic GHMQ connectivity check
  severity: info
  synthetic: true
  ttl_seconds: 300
```

This writes real data to the configured private repository once sending is enabled. `synthetic: true` marks a test; it does not anonymize text or automatically prevent receiver actions. Start with harmless text and inspect both the GitHub file and the receiver separately.

If multiple GHMQ entries are loaded, select the intended integration using `config_entry_id`. This selector cannot override a destination or credential.

### Action fields

| Field | Required | Contract |
| --- | --- | --- |
| `event_type` | Yes | 1–64 characters matching `[a-z][a-z0-9_.-]{0,63}` |
| `message` | Yes | 1–512 plain-text characters; control characters below U+0020 are rejected except newline and tab |
| `event_id` | No | 1–96 characters matching `[A-Za-z0-9_-]{1,96}`; generated if omitted |
| `occurred_at` | No | ISO-8601 timestamp with timezone; defaults to first admission time |
| `ttl_seconds` | No | Integer 60–3600, default 300; measured from `occurred_at` |
| `severity` | No | `info`, `warning`, or `urgent`; default `info` |
| `synthetic` | No | Boolean, default `false` |
| `entity_id` | No | Identifier text matching `[a-z0-9_]+\.[a-z0-9_]{1,100}`; no state or attributes are fetched |
| `config_entry_id` | Sometimes | Required when the loaded destination cannot otherwise be selected unambiguously |

Expired events and occurrence times more than 30 seconds in the future are rejected on first admission. The encoded event must fit within 4,096 bytes, so character limits alone do not guarantee acceptance. Arbitrary context, images, attachments, destination overrides, and credentials are not accepted fields.

The action supports an optional response:

```json
{"event_id": "example-event-001", "status": "accepted"}
```

`accepted` means GitHub accepted the file, the same bytes were found there, or GHMQ has a local receipt of that earlier acceptance. It does **not** confirm receiver processing or notification delivery. `expired` means GHMQ will not attempt further delivery for that retained event; an earlier ambiguous request might already have reached GitHub.

### Retry the same occurrence safely

Provide a stable `event_id` when your automation may repeat a call for the same occurrence. Retry with the identical action content, including the same explicit occurrence time if supplied. Defaults are normalized. If you originally omitted `occurred_at`, continue omitting it: the retained event keeps its original timestamps and expiry.

Reusing an ID with different content is an error. Omitting `event_id` generates a new ID for every call and cannot deduplicate a caller's repeated attempts. Use a new ID for a genuinely new occurrence. Local recent-history retention is bounded; it is not a permanent deduplication database.

See [generic examples](examples/) and the exact [wire contract](docs/receiver-contract.md).

## Delivery, limits, and recovery

- **Durable admission first:** an event is written to the local journal before a network send. Atomic replacement and filesystem synchronization reduce crash-loss risk; they are not a promise against failed disks, lost storage, restored backups, or administrator edits.
- **Ambiguous outcomes:** an action error, timeout, or cancellation can still leave a queued event, and GitHub may have accepted a timed-out request. Do not infer “nothing was sent” from an error.
- **Bounded queue:** at most 100 pending events, 1,000 recent results, and a 4 MiB journal. A full queue rejects new admissions. Recent records retain normalized request content until evicted.
- **Retries:** while sending is enabled, the background loop revisits pending work approximately every 30 seconds, subject to processing time and cooldowns. Individual attempts have bounded short retries. Authentication, identity, persistence, and permission problems require attention.
- **Immutable expiry:** retries never refresh the original expiry. A request already in flight can complete after the deadline; receivers must check expiry before acting.
- **Rate limits:** mutation attempts are serialized and spaced at least one second apart within a running client. GitHub cooldown deadlines are persisted and honored across reloads, restarts, and reauthentication. These safeguards do not provide a quota reservation or coordinate independent installations. GitHub can impose additional or changing primary and secondary limits; plan for low volume and see its [REST API limits](https://docs.github.com/en/rest/using-the-rest-api/rate-limits-for-the-rest-api).
- **Receiver delivery:** GHMQ does not configure or check webhooks. GitHub does not automatically redeliver failed webhook deliveries; a receiver needs its own detection and recovery strategy. Reconciliation only on a later webhook can leave an event unnoticed if no later wake-up occurs. Stronger reliability requires independent polling/backfill and consumer acknowledgements outside GHMQ. See [GitHub's failed-delivery guidance](https://docs.github.com/en/webhooks/using-webhooks/handling-failed-webhook-deliveries).
- **No retention deletion:** expiry stops processing attempts; it does not delete files, Git history, webhook records, clones, backups, or local recent records. GHMQ never removes remote event files.

### Troubleshooting

| Symptom | Check |
| --- | --- |
| Sending disabled | Review the destination and enable sending in the integration options |
| Setup cannot validate | Private/active repository, exact owner/name, non-default branch, open same-repository PR, connectivity, and token access |
| HTTP 403 / access denied | Repository selection, Contents and Pull requests permissions, organization approval/policies, and GitHub cooldown; do not blindly broaden the token |
| Authentication required | Replace the expired/revoked token through Home Assistant's reauthentication flow |
| Rate-limited | Wait until the shown retry deadline; repeated setup, permission changes, or restart do not bypass it |
| Identity changed | Keep sending disabled and review the destination; do not manually remove saved identity checks |
| Event ID conflict | Preserve the original event; use identical content for a retry or a fresh ID for a new occurrence |
| Journal/persistence error | Keep sending disabled, preserve the journal, investigate storage, then reload after repair or a reviewed restore |
| Accepted but no notification | Inspect receiver health, webhook delivery, receiver deduplication and expiry; accepted is not a consumer acknowledgement |

A 403 permission denial can also establish a conservative cooldown. If permissions are corrected, wait for that deadline before retrying. Reloading after a journal fault revalidates durable state; deleting the journal can lose pending work, deduplication history, and cooldowns.

GHMQ diagnostics expose version, loaded/enabled flags, counts, and fixed status categories. They deliberately omit configuration, tokens, event IDs, messages, and entity IDs. Review any surrounding Home Assistant logs before sharing them.

## Privacy and security

Only put text into events that is appropriate for everyone and every app with access to the private repository. Do not send credentials, camera images, sensitive personal details, location histories, or security-critical commands. Event text is untrusted data, including if a receiver passes it to an AI system.

The token is stored in Home Assistant's configuration storage. Event requests are stored in its local journal and may enter backups. The masked input field is not an encryption guarantee. See [SECURITY.md](SECURITY.md) for the trust model, incident handling, and disclosure guidance.

## Related work

Git-backed messaging is an existing idea. [GitMQ](https://github.com/emad-elsaid/gitmq) uses Git commits as messages, while [git-queue](https://github.com/nautilus-cyberneering/git-queue) implements a GitHub Actions job queue. Adjacent Home Assistant projects include [git-ha-ppens](https://github.com/manuveli/git-ha-ppens) for configuration versioning and [hermes-homeassistant](https://github.com/NousResearch/hermes-homeassistant) for an agent gateway. GHMQ focuses on explicit Home Assistant event publishing with a bounded durable sender; these links do not imply compatibility, endorsement, or shared code.

## Development and release status

See [CONTRIBUTING.md](CONTRIBUTING.md) for reproducible offline tests and review requirements, [CHANGELOG.md](CHANGELOG.md) for version history, and the [receiver contract](docs/receiver-contract.md) for interoperability. HACS default-catalog inclusion is a separate review process. GHMQ is licensed under the [MIT License](LICENSE).
