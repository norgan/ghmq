"""Build deterministic install/source ZIPs from an explicit public-file allowlist."""

from __future__ import annotations

import argparse
import hashlib
import json
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PUBLIC_FILES = (
    ".github/ISSUE_TEMPLATE/bug_report.md",
    ".github/ISSUE_TEMPLATE/feature_request.md",
    ".github/PULL_REQUEST_TEMPLATE.md",
    ".github/workflows/validate.yml",
    ".gitignore",
    "CHANGELOG.md",
    "CONTRIBUTING.md",
    "LICENSE",
    "README.md",
    "SECURITY.md",
    "custom_components/ghmq/LICENSE",
    "custom_components/ghmq/__init__.py",
    "custom_components/ghmq/brand/icon.png",
    "custom_components/ghmq/brand/icon.svg",
    "custom_components/ghmq/config_flow.py",
    "custom_components/ghmq/const.py",
    "custom_components/ghmq/core.py",
    "custom_components/ghmq/diagnostics.py",
    "custom_components/ghmq/github.py",
    "custom_components/ghmq/journal.py",
    "custom_components/ghmq/manifest.json",
    "custom_components/ghmq/services.yaml",
    "custom_components/ghmq/strings.json",
    "custom_components/ghmq/translations/en.json",
    "docs/receiver-contract.md",
    "examples/README.md",
    "examples/event-v1.json",
    "examples/synthetic-action.yaml",
    "examples/synthetic-automation.yaml",
    "hacs.json",
    "pyproject.toml",
    "scripts/build_release.py",
    "scripts/check_install.py",
    "tests/__init__.py",
    "tests/requirements-ci.txt",
    "tests/test_core.py",
    "tests/test_github.py",
    "tests/test_ha_adapter.py",
    "tests/test_ha_persistence.py",
    "tests/test_ha_ui.py",
    "tests/test_journal.py",
    "tests/test_release.py",
)


def public_files() -> list[Path]:
    """Only exact reviewed paths enter either archive; unknown files stay out."""
    files = []
    for name in PUBLIC_FILES:
        path = ROOT / name
        if any(parent.is_symlink() for parent in (path, *path.parents)):
            raise ValueError("Release paths must not contain symlinks")
        if not path.is_file():
            raise ValueError(f"Missing reviewed release file: {name}")
        files.append(path)
    return files


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def write_zip(path: Path, files: list[Path]) -> dict:
    with zipfile.ZipFile(
        path, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9
    ) as archive:
        for source in files:
            info = zipfile.ZipInfo(
                source.relative_to(ROOT).as_posix(), (2020, 1, 1, 0, 0, 0)
            )
            info.create_system = 3
            info.external_attr = 0o100644 << 16
            info.compress_type = zipfile.ZIP_DEFLATED
            archive.writestr(info, source.read_bytes())
    data = path.read_bytes()
    return {"name": path.name, "bytes": len(data), "sha256": sha256(data)}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, default=ROOT / "dist")
    args = parser.parse_args()
    version = json.loads((ROOT / "custom_components/ghmq/manifest.json").read_text())[
        "version"
    ]
    source = public_files()
    install = [p for p in source if p.is_relative_to(ROOT / "custom_components/ghmq")]
    args.output_dir.mkdir(parents=True, exist_ok=True)
    archives = [
        write_zip(args.output_dir / f"ghmq-{version}-install.zip", install),
        write_zip(args.output_dir / f"ghmq-{version}-source.zip", source),
    ]
    manifest = {
        "version": version,
        "archives": archives,
        "files": [
            {
                "path": p.relative_to(ROOT).as_posix(),
                "bytes": p.stat().st_size,
                "sha256": sha256(p.read_bytes()),
                "install": p in install,
            }
            for p in source
        ],
    }
    (args.output_dir / f"ghmq-{version}-manifest.json").write_text(
        json.dumps(manifest, indent=2) + "\n"
    )
    print(
        json.dumps(
            {
                "version": version,
                "source_files": len(source),
                "install_files": len(install),
                "archives": archives,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
