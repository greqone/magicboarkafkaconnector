"""Configuration management with standardized storage."""
import os
import json
from PyQt5.QtCore import QStandardPaths, QCoreApplication


class ConfigManager:
    """Manages application configuration with standardized storage in AppData."""

    def __init__(self):
        """Initialize config manager."""
        # Set application metadata
        QCoreApplication.setApplicationName("Magic Boar Kafka Connector")
        QCoreApplication.setOrganizationName("MagicBoar")

        # Get AppData directory
        self.app_data_dir = QStandardPaths.writableLocation(QStandardPaths.AppDataLocation)
        if not os.path.exists(self.app_data_dir):
            os.makedirs(self.app_data_dir, exist_ok=True)

        self.servers_file = os.path.join(self.app_data_dir, 'servers.conf')
        self.settings_file = os.path.join(self.app_data_dir, 'settings.conf')

    def load_servers(self):
        """Load server configurations."""
        if os.path.exists(self.servers_file):
            with open(self.servers_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        else:
            return {
                'Default Server': {
                    'bootstrap_servers': 'localhost:9092',
                    'security_protocol': 'PLAINTEXT',
                    'sasl_mechanism': '',
                    'sasl_username': '',
                    'sasl_password': '',
                    'ssl_cafile': '',
                    'ssl_certfile': '',
                    'ssl_keyfile': '',
                }
            }

    def save_servers(self, servers):
        """Save server configurations."""
        with open(self.servers_file, 'w', encoding='utf-8') as f:
            json.dump(servers, f, indent=4)

    def load_settings(self):
        """Load application settings."""
        if os.path.exists(self.settings_file):
            with open(self.settings_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        else:
            return {
                'theme': 'Light',
                'font_family': 'Segoe UI',
                'font_size': 12,
                'logging_enabled': True,
                'overview_message_limit': 100,
                'connection_retry_enabled': True,
                'connection_retry_attempts': 3,
                'connection_timeout': 10,
                'send_timeout': 10
            }

    def save_settings(self, settings):
        """Save application settings."""
        with open(self.settings_file, 'w', encoding='utf-8') as f:
            json.dump(settings, f, indent=4)

    def get_config_directory(self):
        """Get the configuration directory path."""
        return self.app_data_dir
