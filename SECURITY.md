# Security

GHMQ sends explicitly selected notification text from Home Assistant to a private GitHub repository. It is not a security alarm transport, a secrets store, or a remote command system.

## Release and reporting status

GHMQ's public source repository offers GitHub private vulnerability reporting. No response-time commitment is established.

Do not put vulnerabilities involving real credentials, personal data, or private repository details in a public issue. Use **Report a vulnerability** on [the security advisories page](https://github.com/norgan/ghmq/security/advisories) to contact maintainers privately. If that option is unavailable, request a private reporting channel without posting exploit details or sensitive evidence.

A useful report includes the affected version, runtime version, expected and observed behavior, and a minimal synthetic reproduction. Never include an access token, a Home Assistant configuration export, a raw journal, or real event history.

## Trust boundaries

1. **Home Assistant administrators and host access.** The integration's token resides in Home Assistant configuration storage. The password-style field hides it on screen; GHMQ does not provide a separate encrypted credential vault. Host administrators, sufficiently privileged add-ons, and backups may expose configuration or event data.
2. **GitHub repository access.** GitHub receives event text and optional entity IDs. Collaborators, installed apps, workflows, and webhooks can receive or read that content according to their permissions. A private repository is access-controlled, not end-to-end encrypted.
3. **The GitHub credential.** Use a fine-grained token limited to one dedicated relay repository. Contents write is broader than “create files in this branch”; the application's fixed destination and create-only behavior do not narrow a stolen token's GitHub permissions.
4. **The receiver.** A separately operated receiver is responsible for authenticating its inputs, enforcing the [receiver contract](docs/receiver-contract.md), preventing replay, enforcing expiry, and authorizing downstream effects. GHMQ neither supplies nor audits that receiver.
5. **Message content.** Treat every string as untrusted text. Never execute it, interpolate it into a shell, treat it as HTML, follow URLs automatically, or let it override an AI receiver's instructions or permissions. `synthetic` and `severity` are data labels, not security controls.

## Safeguards in this integration

- Outbound-only calls to `api.github.com`, without HTTP redirect following or user-selectable API hosts
- An explicit action and a sending option that is off by default; no state-change listener or inbound command endpoint
- Validation of an active private repository, a separate relay branch, an open same-repository pull request, and pinned numeric repository and pull-request identities
- A strict, size-bounded event envelope; no arbitrary metadata, images, attachments, or per-call destination/credential overrides
- Locally journaled admission, serialized state transitions, immutable event content, bounded retries, persistent cooldowns, and fail-closed behavior on uncertain persistence
- Atomic journal writes with private file permissions; a strict parser rejects malformed JSON and duplicate keys
- Allow-listed diagnostics and fixed-category logs that omit tokens, destinations, event content, and raw remote error bodies

These safeguards are not a full independent security audit or a guarantee against compromised hosts, credentials, repository administrators, malicious dependencies, or receiver bugs. Identity and visibility checks happen before requests; they cannot make separate GitHub API calls atomic or prevent later access-policy changes.

## Data retention

`expires_at` is a processing deadline. It is not a deletion request or a retention policy.

GHMQ retains pending requests and up to 1,000 recent request records locally. Accepted files remain on the relay branch and in Git history. GitHub notifications, webhook deliveries, clones, receiver stores, and backups may retain additional copies. Deleting a working-tree file does not remove all historical copies. Changing a repository to public can expose existing event history; the sender rejecting future public-repository writes cannot undo that exposure.

Keep the relay private, minimize its membership and installed integrations, avoid unnecessary event content, and plan retention outside GHMQ. Do not collect data that your retention requirements cannot tolerate being stored in Git.

## If something goes wrong

- **Suspected token exposure:** disable sending and revoke the token through GitHub. Review repository activity and access, then create an appropriately scoped replacement and reauthenticate. Do not paste either token into a report.
- **Sensitive event sent:** disable further sending, restrict access as appropriate, and assess copies in Git history, webhook systems, receivers, clones, and backups. Expiry or deleting the latest file does not recall those copies. Consult GitHub's official sensitive-data removal guidance for repository cleanup.
- **Unexpected destination or identity change:** keep sending disabled and investigate. Do not bypass saved identity pins to make the error disappear.
- **Persistence failure:** preserve the journal and diagnose storage before a controlled reload or restore. Do not clear the journal as a routine fix; doing so loses delivery and cooldown information.
- **Action timeout or cancellation:** assume the event might still be queued or remotely accepted. Use the same event ID and identical content when retrying, and let the receiver deduplicate independently.

The sending option stops new admissions and background retries after active work drains. It does not cancel effects already accepted by GitHub or a receiver.

## Official guidance

- [Keeping personal access tokens secure](https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/managing-your-personal-access-tokens#keeping-your-personal-access-tokens-secure)
- [Validating webhook deliveries](https://docs.github.com/en/webhooks/using-webhooks/validating-webhook-deliveries)
- [Removing sensitive data from a repository](https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/removing-sensitive-data-from-a-repository)
