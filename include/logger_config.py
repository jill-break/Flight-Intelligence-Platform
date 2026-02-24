"""
Centralized Logging Configuration
====================================
Provides consistent logging across all project components.
Logs are written to both console and the logs/ directory.
"""

import logging
import os
from datetime import datetime


def setup_logger(name, log_file=None, level=logging.INFO):
    """
    Create a logger that writes to both console and a log file.

    Args:
        name: Logger name (usually __name__ or module name)
        log_file: Optional filename for the log file. If None, derives from name.
        level: Logging level (default: INFO)

    Returns:
        Configured logging.Logger instance
    """
    logger = logging.getLogger(name)

    # Avoid adding duplicate handlers if logger already exists
    if logger.handlers:
        return logger

    logger.setLevel(level)

    # Formatter with timestamp, level, and module name
    formatter = logging.Formatter(
        '%(asctime)s | %(levelname)-8s | %(name)s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # Console handler — always active
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # File handler — writes to logs/ directory
    log_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'logs')
    os.makedirs(log_dir, exist_ok=True)

    if log_file is None:
        log_file = f"{name.replace('.', '_')}.log"

    file_handler = logging.FileHandler(
        os.path.join(log_dir, log_file),
        encoding='utf-8'
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger
