"""Centralized logging configuration"""

import logging
import sys


def setup_logging():
    """Configure logging to output to stdout
    Only configure logging once
    """
    if not logging.getLogger().handlers:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            handlers=[logging.StreamHandler(sys.stdout)],
        )
        logging.getLogger().setLevel(logging.INFO)


def get_logger(name):
    return logging.getLogger(name)
