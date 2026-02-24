import logging
import os
from typing import Optional


APP_LOGGER_NAME = "health_analysis_summary"


def _resolve_log_level(level_name: Optional[str]) -> int:
    value = (level_name or "INFO").strip().upper()
    return getattr(logging, value, logging.INFO)


def configure_logging() -> logging.Logger:
    """
    Configure and return the application root logger.
    Safe to call multiple times; handlers are only added once.
    """
    logger = logging.getLogger(APP_LOGGER_NAME)
    logger.setLevel(_resolve_log_level(os.getenv("LOG_LEVEL")))
    logger.propagate = False

    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger


def get_logger(name: str) -> logging.Logger:
    """
    Return a child logger under the app logger namespace.
    Example: get_logger("notion_client")
    """
    configure_logging()
    return logging.getLogger(f"{APP_LOGGER_NAME}.{name}")


logger = configure_logging()

