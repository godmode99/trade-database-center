from __future__ import annotations

from pathlib import Path
import sys

sys.path.append(str(Path(__file__).resolve().parents[1]))

from run_fetch import run_with_config


def main() -> None:
    run_with_config("weekly_config.yaml")


if __name__ == "__main__":
    main()
