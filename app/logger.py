import logging
import os
from logging.handlers import RotatingFileHandler
from datetime import datetime


def setup_logging() -> None:
    """
    Configure application-wide logging.

    - Writes structured logs to logs/owner_intel.log (rotating)
    - Includes timestamp, level, logger name, and message
    """
    log_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "logs")
    os.makedirs(log_dir, exist_ok=True)

    log_file = os.path.join(log_dir, "owner_intel.log")

    # Basic format: 2026-03-19 18:45:12 [INFO] app.scraper.orchestrator: message
    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    file_handler = RotatingFileHandler(
        log_file,
        maxBytes=5 * 1024 * 1024,  # 5 MB
        backupCount=5,
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    root = logging.getLogger()
    # Avoid adding handlers multiple times if uvicorn reloads
    if not any(isinstance(h, RotatingFileHandler) for h in root.handlers):
        root.setLevel(logging.INFO)
        root.addHandler(file_handler)
        root.addHandler(console_handler)

