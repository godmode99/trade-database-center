from __future__ import annotations

import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from run_fred import run_with_config


def main() -> None:
    run_with_config("daily_config.yaml", "Daily")


if __name__ == "__main__":
    main()
