"""Logging setup utilities."""

from __future__ import annotations

import logging


def setup_logging(log_file: str) -> None:
    """Configure root logging for file and console output."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.FileHandler(log_file), logging.StreamHandler()],
    )
