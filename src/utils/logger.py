"""Centralized logging configuration."""
import os
import datetime
import logging
from PyQt5.QtCore import QStandardPaths, QCoreApplication


_log_filename = None


def setup_logging(enabled=True):
    """
    Configure application logging.

    Args:
        enabled: Whether to enable logging

    Returns:
        Path to the log file
    """
    global _log_filename

    # Set application metadata for proper AppData path
    QCoreApplication.setApplicationName("Magic Boar Kafka Connector")
    QCoreApplication.setOrganizationName("MagicBoar")

    # Get AppData directory
    app_data_dir = QStandardPaths.writableLocation(QStandardPaths.AppDataLocation)
    if not os.path.exists(app_data_dir):
        os.makedirs(app_data_dir, exist_ok=True)

    # Create log filename
    _log_filename = os.path.join(
        app_data_dir,
        f"log_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    )

    if enabled:
        logging.basicConfig(
            filename=_log_filename,
            level=logging.DEBUG,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        logging.disable(logging.NOTSET)
    else:
        logging.basicConfig(level=logging.CRITICAL)
        logging.disable(logging.CRITICAL)

    return _log_filename


def get_log_filename():
    """Get the current log filename."""
    return _log_filename


def get_log_directory():
    """Get the directory where logs are stored."""
    QCoreApplication.setApplicationName("Magic Boar Kafka Connector")
    QCoreApplication.setOrganizationName("MagicBoar")
    return QStandardPaths.writableLocation(QStandardPaths.AppDataLocation)


def enable_logging():
    """Enable logging."""
    logging.disable(logging.NOTSET)


def disable_logging():
    """Disable logging."""
    logging.disable(logging.CRITICAL)
