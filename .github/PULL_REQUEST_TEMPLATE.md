## Change

Explain the user-visible change and its scope. Link the related issue if one exists.

## Verification

State the exact Home Assistant and Python versions, commands run, and results. List skipped, blocked, and unrun checks separately. Offline tests do not prove webhook or receiver delivery.

## Review checklist

- [ ] Tests cover successful, repeated, and interrupted execution where relevant.
- [ ] The receiver contract and immutable expiry semantics remain accurate.
- [ ] Sending remains explicitly enabled, with no unexpected data export or new permissions.
- [ ] Errors, diagnostics, fixtures, examples, and screenshots contain no secrets or private deployment data.
- [ ] UI text, translations, documentation, examples, and changelog were updated where needed.
- [ ] Applicable license notices are preserved and contributions are compatible with the MIT License.

## Risks and migration

Describe any changed configuration, journal, protocol, compatibility, or recovery behavior. State whether manual action is needed.
