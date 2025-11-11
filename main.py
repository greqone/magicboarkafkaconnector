#!/usr/bin/env python3
"""
Magic Boar Kafka Connector - Main entry point.

This is the refactored application with proper architecture:
- Separated concerns (UI, business logic, utilities)
- Service layer for Kafka operations
- Proper thread safety
- Input validation
- Qt6-compatible code
- Configurable settings
"""
import sys
from PyQt5 import QtWidgets, QtCore
from src.ui.main_window import KafkaApp
from src.utils.logger import setup_logging
from src.constants import APP_NAME, APP_ORG


def main():
    """Main entry point for the application."""
    # Initialize Qt application
    app = QtWidgets.QApplication(sys.argv)

    # Set application metadata
    QtCore.QCoreApplication.setApplicationName(APP_NAME)
    QtCore.QCoreApplication.setOrganizationName(APP_ORG)

    # Setup logging (will be configured by settings later)
    setup_logging(enabled=True)

    # Create and show main window
    kafka_app = KafkaApp()
    kafka_app.show()

    # Run application
    sys.exit(app.exec_())


if __name__ == '__main__':
    main()
