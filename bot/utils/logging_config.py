import logging
import os
from logging.handlers import RotatingFileHandler

def setup_logging():
    log_formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # Handler for console output
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_formatter)

    # Handler for file output with rotation
    log_file = "bot.log"
    file_handler = RotatingFileHandler(
        log_file, maxBytes=10_000_000, backupCount=5, encoding='utf-8'
    )
    file_handler.setFormatter(log_formatter)

    # Configure root logger
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger