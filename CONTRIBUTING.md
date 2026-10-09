# Contributing

GHMQ's public source repository is [norgan/ghmq](https://github.com/norgan/ghmq). GHMQ uses the [MIT License](LICENSE). Keep contributions compatible with that license and retain applicable notices; no copyright assignment is requested here.

## Scope

The integration is deliberately narrow: explicit outbound notification events, one reviewed private GitHub destination per entry, a bounded durable queue, and a strict receiver contract. Proposals that add inbound commands, arbitrary metadata, automatic Home Assistant state export, broader credentials, or changed delivery semantics need explicit design and security review.

Keep a pull request focused. Explain the user-facing change, describe its failure modes, and include tests for both success and interrupted/repeated execution. Never develop against someone else's live Home Assistant instance or relay without their explicit permission.

## Development environment

Use **Python 3.14.2** and **Home Assistant 2026.10.0** for the runtime contract tests. From the repository root, a disposable environment can be prepared with:

```sh
python3.14 -m venv .venv
. .venv/bin/activate
python -m pip install -r tests/requirements-ci.txt
python -m pip check
python -m unittest discover -s tests -t . -v
python -m ruff check .
python -m ruff format --check .
python -m compileall -q custom_components tests scripts
python scripts/build_release.py
python -I scripts/check_install.py dist/ghmq-0.1.1-install.zip
```

Confirm that `python --version` reports the intended patch version. The pinned development dependencies are in `tests/requirements-ci.txt`; keep local checks aligned with the workflow in `.github/workflows/validate.yml`. No separate runtime library is bundled by this integration; dependencies come from Home Assistant.

The test suite uses synthetic fixtures and replaces network boundaries. It must not require a token, real repository, live Home Assistant instance, or external receiver. The Home Assistant adapter, persistence, and UI tests use the real installed Home Assistant APIs; they are explicitly skipped if Home Assistant is absent. **A run with skipped runtime tests is not a full compatibility pass.** Report passed, failed, and skipped checks separately.

## Review checklist

- Preserve the nine required wire fields and the optional `entity_id`, with exact types and limits.
- Preserve original timestamps and expiry during retries, restart, and reauthentication.
- Save admission before outbound mutation; do not silently swallow uncertain disk writes.
- Cover cancellation during lock acquisition, network I/O, journal writes, shutdown, and reload.
- Preserve cooldowns across setup, restart, token replacement, and competing calls.
- Keep repository and pull-request identity pins intact; a credential update must not approve a new destination.
- Reject unknown request fields and destination overrides.
- Keep errors and diagnostics free of credentials, request URLs, local paths, raw responses, and event content.
- Do not label GitHub acceptance as receiver processing or promise exactly-once delivery.
- Use only invented, non-sensitive names, IDs, messages, and destinations in tests, documentation, and screenshots.

When changing the action UI, update `services.yaml`, `strings.json`, English translations, examples, and tests together. When changing protocol behavior, update the [receiver contract](docs/receiver-contract.md) and [changelog](CHANGELOG.md) before suggesting a release.

## Safe issue reports

Include GHMQ, Home Assistant, and Python versions; the fixed error category or status; whether sending is enabled; and a small synthetic reproduction. Review diagnostics before sharing, even though GHMQ intentionally omits configuration and payloads.

Do not upload Home Assistant's configuration storage, journals, backups, actual event history, private deployment notes, or credentials. Follow [SECURITY.md](SECURITY.md) for security-sensitive reports.

## Release gates

Before publication, maintainers must:

1. Retain the MIT license and applicable notices, and establish an actual private vulnerability-reporting route.
2. Review the exact publication file list and scan it for secrets and personal/deployment data.
3. Run the complete suite without runtime skips against the stated Home Assistant and Python versions, plus lint and formatting checks.
4. Check the manifest, HACS metadata, version strings, translations, examples, and installable package layout.
5. Distinguish locally verified checks from remote CI or HACS validation that has not run.
6. Obtain explicit approval for the destination, visibility, and material being published.

A source package or draft pull request does not authorize publishing a repository, release, relay history, or live configuration. An integration-only install archive should contain the runtime component, not tests, deployment evidence, credentials, or developer environments.
