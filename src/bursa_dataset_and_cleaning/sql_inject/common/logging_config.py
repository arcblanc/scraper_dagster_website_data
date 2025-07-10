"""Common logging configuration for SQL injection scripts.
Ensures logs appear both in Dagster and terminal output.
"""

import logging
import sys


def setup_logging(name: str = None) -> logging.Logger:
    """Setup logging that works both in Dagster and terminal.

    Args:
        name: Logger name (usually __name__)

    Returns:
        Configured logger instance
    """
    # Create logger
    logger = logging.getLogger(name or __name__)

    # Only configure if not already configured
    if not logger.handlers:
        logger.setLevel(logging.INFO)

        # Create formatter
        formatter = logging.Formatter(
            "%(asctime)s | %(levelname)s | %(name)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

        # Create console handler (for terminal output)
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(formatter)

        # Force immediate flushing
        console_handler.flush = sys.stdout.flush

        # Add handler to logger
        logger.addHandler(console_handler)

        # Prevent propagation to avoid duplicate logs
        logger.propagate = False

    return logger


def get_logger(name: str = None) -> logging.Logger:
    """Get a configured logger instance.

    Args:
        name: Logger name (usually __name__)

    Returns:
        Configured logger instance
    """
    return setup_logging(name)


def force_flush_logs():
    """Force flush all log handlers to ensure output is displayed immediately."""
    for handler in logging.root.handlers:
        if hasattr(handler, "flush"):
            handler.flush()
    sys.stdout.flush()
    sys.stderr.flush()
