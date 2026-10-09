"""Public packaging contracts; no external service access or Home Assistant needed."""

from __future__ import annotations

import importlib.util
import json
import struct
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest.mock import patch

from ghmq_test_subject.const import VERSION

ROOT = Path(__file__).resolve().parents[1]
INTEGRATION = ROOT / "custom_components/ghmq"
spec = importlib.util.spec_from_file_location(
    "ghmq_release_builder", ROOT / "scripts/build_release.py"
)
assert spec and spec.loader
builder = importlib.util.module_from_spec(spec)
spec.loader.exec_module(builder)


class ReleaseContractTests(unittest.TestCase):
    def test_manifest_identity_version_and_public_links(self):
        manifest = json.loads((INTEGRATION / "manifest.json").read_text())
        self.assertEqual(
            manifest,
            {
                "domain": "ghmq",
                "name": "GHMQ",
                "codeowners": ["@norgan"],
                "config_flow": True,
                "documentation": "https://github.com/norgan/ghmq",
                "integration_type": "service",
                "iot_class": "cloud_push",
                "issue_tracker": "https://github.com/norgan/ghmq/issues",
                "requirements": [],
                "version": VERSION,
            },
        )
        self.assertEqual(VERSION, "0.1.1")

    def test_hacs_uses_standard_layout_and_target_core_version(self):
        self.assertEqual(
            json.loads((ROOT / "hacs.json").read_text()),
            {"name": "GHMQ", "homeassistant": "2026.10.0"},
        )
        self.assertEqual(
            [
                p.name
                for p in (ROOT / "custom_components").iterdir()
                if p.is_dir() and p.name != "__pycache__"
            ],
            ["ghmq"],
        )

    def test_brand_png_is_square_rgba_and_editable_source_is_present(self):
        png = (INTEGRATION / "brand/icon.png").read_bytes()
        self.assertEqual(png[:8], b"\x89PNG\r\n\x1a\n")
        self.assertEqual(png[12:16], b"IHDR")
        width, height, depth, color = struct.unpack(">IIBB", png[16:26])
        self.assertEqual((width, height, depth, color), (256, 256, 8, 6))
        self.assertIn("<svg", (INTEGRATION / "brand/icon.svg").read_text())

    def test_approved_mit_license_and_public_docs_are_present(self):
        license_text = (ROOT / "LICENSE").read_text()
        self.assertIn("MIT License", license_text)
        self.assertIn("Permission is hereby granted, free of charge", license_text)
        self.assertEqual((INTEGRATION / "LICENSE").read_text(), license_text)
        for name in (
            "README.md",
            "SECURITY.md",
            "CONTRIBUTING.md",
            "CHANGELOG.md",
            "docs/receiver-contract.md",
        ):
            self.assertTrue((ROOT / name).read_text().strip(), name)

    def test_public_inventory_excludes_development_and_runtime_state(self):
        paths = [p.relative_to(ROOT).as_posix() for p in builder.public_files()]
        self.assertEqual(paths, sorted(set(paths)))
        self.assertIn("custom_components/ghmq/brand/icon.png", paths)
        self.assertIn("LICENSE", paths)
        for path in paths:
            self.assertFalse(
                set(Path(path).parts)
                & {
                    ".git",
                    ".storage",
                    "__pycache__",
                    ".venv",
                    "evidence",
                    "deployment",
                    "dist",
                }
            )
            self.assertNotIn(Path(path).suffix, {".pyc", ".journal", ".log", ".bak"})

    def test_unknown_files_are_not_added_to_exact_reviewed_inventory(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            for relative in builder.PUBLIC_FILES:
                path = root / relative
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text("inert fixture")
            (root / "docs/unreviewed.json").write_text("inert extra")
            (root / "custom_components/ghmq/unreviewed.txt").write_text("inert extra")
            with patch.object(builder, "ROOT", root):
                paths = [p.relative_to(root).as_posix() for p in builder.public_files()]
            self.assertEqual(paths, list(builder.PUBLIC_FILES))
            self.assertNotIn("docs/unreviewed.json", paths)
            self.assertNotIn("custom_components/ghmq/unreviewed.txt", paths)

    def test_missing_or_symlinked_release_files_fail_closed(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            with patch.object(builder, "ROOT", root):
                with self.assertRaisesRegex(ValueError, "Missing reviewed"):
                    builder.public_files()
            for relative in builder.PUBLIC_FILES:
                path = root / relative
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text("inert fixture")
            path = root / builder.PUBLIC_FILES[0]
            path.unlink()
            path.symlink_to(root / "LICENSE")
            with patch.object(builder, "ROOT", root):
                with self.assertRaisesRegex(ValueError, "symlinks"):
                    builder.public_files()

    def test_install_zip_is_deterministic_and_contains_only_integration_files(self):
        files = [p for p in builder.public_files() if p.is_relative_to(INTEGRATION)]
        with tempfile.TemporaryDirectory() as temporary:
            first = Path(temporary) / "first.zip"
            second = Path(temporary) / "second.zip"
            a = builder.write_zip(first, files)
            b = builder.write_zip(second, files)
            self.assertEqual(a["sha256"], b["sha256"])
            self.assertEqual(first.read_bytes(), second.read_bytes())
            with zipfile.ZipFile(first) as archive:
                self.assertEqual(len(archive.namelist()), 14)
                for name in archive.namelist():
                    self.assertTrue(name.startswith("custom_components/ghmq/"))
                    self.assertEqual(archive.read(name), (ROOT / name).read_bytes())


if __name__ == "__main__":
    unittest.main()
