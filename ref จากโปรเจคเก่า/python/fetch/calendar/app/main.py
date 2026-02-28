"""Entry point for running the calendar fetch pipeline."""

from __future__ import annotations

import sys
from pathlib import Path

CALENDAR_DIR = Path(__file__).resolve().parents[1]
if str(CALENDAR_DIR) not in sys.path:
    sys.path.insert(0, str(CALENDAR_DIR))

from pipeline import main as run_pipeline  # noqa: E402


def main() -> None:
    run_pipeline()


if __name__ == "__main__":
    main()
