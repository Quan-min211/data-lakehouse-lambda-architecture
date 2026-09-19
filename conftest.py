"""
conftest.py
===========
pytest configuration file:
- Thiet lap PYTHONPATH de co the import src.*
- Dinh nghia fixture chung
- Skip test co nhan requires_docker neu Docker service khong san sang
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

# ── PYTHONPATH: dam bao 'src.*' importable ───────────────────────────────────
ROOT_DIR = Path(__file__).parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


def pytest_configure(config):
    """Dang ky custom markers de pytest biet."""
    config.addinivalue_line(
        "markers",
        "requires_docker: mark test as needing Docker services (skip if unavailable)",
    )
    config.addinivalue_line(
        "markers",
        "requires_spark: mark test as needing PySpark / Java (skip if unavailable)",
    )
