from __future__ import annotations

import logging
from pathlib import Path


def get_logger(log_path: Path) -> logging.Logger:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("compliance_reporting")
    expected_path = str(log_path.resolve())
    current_path = getattr(logger, "_configured_log_path", None)
    if logger.handlers and current_path == expected_path:
        return logger
    if logger.handlers:
        for handler in list(logger.handlers):
            handler.close()
            logger.removeHandler(handler)

    logger.setLevel(logging.INFO)
    logger.propagate = False

    formatter = logging.Formatter(
        fmt="%(asctime)s %(levelname)s %(name)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%SZ",
    )

    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)
    logger._configured_log_path = expected_path  # type: ignore[attr-defined]

    return logger
