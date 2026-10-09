# Changelog

## 0.1.1 — 2026-10-09

Initial public source release. HACS default-catalog inclusion is separate; see GitHub Actions for hosted validation results.

### Integration

- Explicit `ghmq.send_event` action with sending disabled by default and no automatic Home Assistant state export.
- UI configuration and token reauthentication for a reviewed private GitHub repository, non-default relay branch, and existing open same-repository pull request.
- Pinned numeric repository and pull-request identities, preserved during reauthentication.
- Strict version-1 JSON envelopes with nine required fields, optional `entity_id`, a 4,096-byte payload limit, and a 60–3,600-second event lifetime.
- Stable-ID deduplication, create-only GitHub event files, and separate `accepted` and `expired` sender results.
- Bounded durable journal, atomic persistence, serialized admission and delivery, retry handling, and lifecycle/cancellation protection.
- Persistent GitHub cooldowns, conservative handling of ambiguous 403 responses, and mutation spacing.
- Fixed-category logging, allow-listed diagnostics, English configuration/action text, and synthetic offline tests including real Home Assistant API contracts.

### Documentation

- Installation and configuration guidance, token-permission recommendations, and generic synthetic examples.
- Receiver contract covering validation, replay protection, independent expiry checks, and untrusted event content.
- Explicit limitations: no included receiver, no end-to-end acknowledgement or delivery guarantee, and no automatic removal of expired event data or Git history.
- Security guidance, contribution checks, issue and pull-request templates, and publication release gates.
- MIT License with a general GHMQ contributors notice.

### Compatibility target

Home Assistant 2026.10.0 on Python 3.14.2. See the validation workflow for results on each commit.
