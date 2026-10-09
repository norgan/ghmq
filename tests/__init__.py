"""Tests load the pure modules without importing Home Assistant's adapter."""

import sys
import types
from pathlib import Path

PACKAGE = "ghmq_test_subject"
if PACKAGE not in sys.modules:
    module = types.ModuleType(PACKAGE)
    module.__path__ = [
        str(Path(__file__).resolve().parents[1] / "custom_components" / "ghmq")
    ]
    sys.modules[PACKAGE] = module
