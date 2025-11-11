"""Main application window with proper architecture."""
import logging
from PyQt5 import QtWidgets, QtGui, QtCore
from ..kafka.kafka_service import KafkaService
from ..utils.config_manager import ConfigManager
from ..utils.resource_loader import get_resource_path, load_stylesheet, ResourceNotFoundError
from ..utils.validators import validate_topic_name
from ..threads.consume_thread import ConsumeThread
from .dialogs.server_dialog import ServerDialog
from .dialogs.settings_dialog import SettingsDialog
from .dialogs.messages_dialog import MessagesDialog


class KafkaApp(QtWidgets.QMainWindow):
    """Main application window."""

    def __init__(self):
        """Initialize main window."""
        super().__init__()

        # Initialize config manager and load configurations first
        self.config_manager = ConfigManager()
        self.servers = self.config_manager.load_servers()
        self.settings = self.config_manager.load_settings()

        # Initialize Kafka service with settings
        self.kafka_service = KafkaService(self.settings)

        # UI state
        self.consume_thread = None

        # Set window properties
        self.setWindowTitle("Magic Boar Kafka Connector")
        self.setGeometry(100, 100, 1200, 600)

        # Set icon
        try:
            icon_file = get_resource_path("logo.ico", required=False)
            self.setWindowIcon(QtGui.QIcon(icon_file))
        except ResourceNotFoundError:
            pass  # Icon is optional

        # Initialize UI
        self.init_ui()

        # Apply settings
        self.apply_settings()

        # Connect to first server if available
        if self.server_combo.count() > 0:
            self.server_combo.setCurrentIndex(0)
            self.server_selected()

    def init_ui(self):
        """Initialize UI components."""
        self.central_widget = QtWidgets.QWidget()
        self.setCentralWidget(self.central_widget)

        self.horizontal_layout = QtWidgets.QHBoxLayout(self.central_widget)

        # Background image label
        self.image_label = QtWidgets.QLabel()
        self.image_label.setScaledContents(False)
        self.image_label.setAlignment(QtCore.Qt.AlignCenter)
        self.horizontal_layout.addWidget(self.image_label)
        self.load_decor_image()

        # Main vertical layout
        self.main_vertical_layout = QtWidgets.QVBoxLayout()

        # Menu bar
        self.create_menu_bar()

        # Server selection layout
        self.create_server_layout()

        # Action buttons layout
        self.create_button_layout()

        # Output text area
        self.output_text = QtWidgets.QTextEdit()
        self.output_text.setReadOnly(True)
        self.main_vertical_layout.addWidget(self.output_text)

        # Consume control button (initially hidden)
        self.stop_consume_btn = QtWidgets.QPushButton("Stop Consuming")
        self.stop_consume_btn.clicked.connect(self.stop_consuming)
        self.stop_consume_btn.setVisible(False)
        self.main_vertical_layout.addWidget(self.stop_consume_btn)

        self.horizontal_layout.addLayout(self.main_vertical_layout)

    def create_menu_bar(self):
        """Create menu bar."""
        self.menu_bar = self.menuBar()

        # Settings menu
        self.settings_menu = self.menu_bar.addMenu('Settings')
        self.settings_action = QtWidgets.QAction('Preferences', self)
        self.settings_action.triggered.connect(self.open_settings)
        self.settings_menu.addAction(self.settings_action)

        # Admin menu
        self.admin_menu = self.menu_bar.addMenu('Admin')

        self.create_topic_action = QtWidgets.QAction('Create Topic', self)
        self.create_topic_action.triggered.connect(self.create_topic)
        self.admin_menu.addAction(self.create_topic_action)

        self.delete_topic_action = QtWidgets.QAction('Delete Topic', self)
        self.delete_topic_action.triggered.connect(self.delete_topic)
        self.admin_menu.addAction(self.delete_topic_action)

        self.describe_cluster_action = QtWidgets.QAction('Describe Cluster', self)
        self.describe_cluster_action.triggered.connect(self.describe_cluster)
        self.admin_menu.addAction(self.describe_cluster_action)

    def create_server_layout(self):
        """Create server selection layout."""
        self.server_layout = QtWidgets.QHBoxLayout()

        self.server_label = QtWidgets.QLabel("Server:")
        self.server_combo = QtWidgets.QComboBox()
        self.server_combo.addItems(self.servers.keys())
        self.server_combo.currentIndexChanged.connect(self.server_selected)

        self.add_server_btn = QtWidgets.QPushButton("Add")
        self.add_server_btn.clicked.connect(self.add_server)

        self.edit_server_btn = QtWidgets.QPushButton("Edit")
        self.edit_server_btn.clicked.connect(self.edit_server)

        self.remove_server_btn = QtWidgets.QPushButton("Remove")
        self.remove_server_btn.clicked.connect(self.remove_server)

        self.server_layout.addWidget(self.server_label)
        self.server_layout.addWidget(self.server_combo)
        self.server_layout.addWidget(self.add_server_btn)
        self.server_layout.addWidget(self.edit_server_btn)
        self.server_layout.addWidget(self.remove_server_btn)

        self.main_vertical_layout.addLayout(self.server_layout)

    def create_button_layout(self):
        """Create action buttons layout."""
        self.button_layout = QtWidgets.QHBoxLayout()

        self.send_payload_btn = QtWidgets.QPushButton("Send")
        self.send_payload_btn.clicked.connect(self.send_payload)

        self.overview_messages_btn = QtWidgets.QPushButton("Overview")
        self.overview_messages_btn.clicked.connect(self.overview_messages)

        self.list_topics_btn = QtWidgets.QPushButton("List")
        self.list_topics_btn.clicked.connect(self.list_topics)

        self.consume_messages_btn = QtWidgets.QPushButton("Consume")
        self.consume_messages_btn.clicked.connect(self.consume_messages)

        self.button_layout.addWidget(self.send_payload_btn)
        self.button_layout.addWidget(self.overview_messages_btn)
        self.button_layout.addWidget(self.list_topics_btn)
        self.button_layout.addWidget(self.consume_messages_btn)

        self.main_vertical_layout.addLayout(self.button_layout)

    def load_decor_image(self):
        """Load background decoration image."""
        try:
            bg_file = get_resource_path("background.png", required=False)
            pix = QtGui.QPixmap(bg_file)
            if not pix.isNull():
                self.image_pixmap_original = pix
                self.update_image_label()
        except (ResourceNotFoundError, Exception):
            pass  # Background image is optional

    def update_image_label(self):
        """Update image label with scaled pixmap."""
        if hasattr(self, 'image_pixmap_original'):
            scaled = self.image_pixmap_original.scaled(
                self.image_label.size(),
                QtCore.Qt.KeepAspectRatio,
                QtCore.Qt.SmoothTransformation
            )
            self.image_label.setPixmap(scaled)

    def resizeEvent(self, event):
        """Handle window resize."""
        super().resizeEvent(event)
        self.update_image_label()

    # Server management methods

    def server_selected(self):
        """Handle server selection change."""
        if self.server_combo.count() == 0:
            return

        server_name = self.server_combo.currentText()
        server_config = self.servers.get(server_name)

        if server_config:
            try:
                self.kafka_service.connect(server_config)
                self.output_text.append(f"Connected to {server_config['bootstrap_servers']}")
                logging.info(f"Connected to {server_config['bootstrap_servers']}")
                self.print_happy_emoticon()
            except Exception as e:
                error_msg = f"Error connecting to server: {e}"
                self.output_text.append(error_msg)
                logging.error(error_msg)
                self.print_sad_emoticon()

    def add_server(self):
        """Open dialog to add new server."""
        server_dialog = ServerDialog(self)
        if server_dialog.exec_():
            server_name, server_config = server_dialog.get_server_info()
            self.servers[server_name] = server_config
            self.server_combo.addItem(server_name)
            self.config_manager.save_servers(self.servers)
            self.output_text.append(f"Added server: {server_name}")

    def edit_server(self):
        """Open dialog to edit current server."""
        if self.server_combo.count() == 0:
            QtWidgets.QMessageBox.warning(self, "No Server", "No server to edit")
            return

        server_name = self.server_combo.currentText()
        server_config = self.servers.get(server_name)

        server_dialog = ServerDialog(self, server_name, server_config)
        if server_dialog.exec_():
            new_server_name, new_server_config = server_dialog.get_server_info()

            # Remove old entry if name changed
            if new_server_name != server_name:
                del self.servers[server_name]

            self.servers[new_server_name] = new_server_config
            self.server_combo.setItemText(self.server_combo.currentIndex(), new_server_name)
            self.config_manager.save_servers(self.servers)
            self.server_selected()
            self.output_text.append(f"Updated server: {new_server_name}")

    def remove_server(self):
        """Remove current server."""
        if self.server_combo.count() == 0:
            QtWidgets.QMessageBox.warning(self, "No Server", "No server to remove")
            return

        server_name = self.server_combo.currentText()
        reply = QtWidgets.QMessageBox.question(
            self,
            'Remove Server',
            f"Remove '{server_name}'?",
            QtWidgets.QMessageBox.Yes | QtWidgets.QMessageBox.No,
            QtWidgets.QMessageBox.No
        )

        if reply == QtWidgets.QMessageBox.Yes:
            del self.servers[server_name]
            self.server_combo.removeItem(self.server_combo.currentIndex())
            self.config_manager.save_servers(self.servers)
            self.output_text.append(f"Removed server: {server_name}")

            if self.server_combo.count() > 0:
                self.server_selected()
            else:
                self.kafka_service.disconnect()

    # Topic operations

    def select_topic(self):
        """
        Show dialog to select a topic.

        Returns:
            str or None: Selected topic name
        """
        if not self.kafka_service.is_connected():
            QtWidgets.QMessageBox.warning(self, "No Connection", "Not connected to any server")
            return None

        try:
            topics = self.kafka_service.list_topics()
            if not topics:
                QtWidgets.QMessageBox.warning(self, "No Topics", "No topics available")
                return None

            topic, ok = QtWidgets.QInputDialog.getItem(
                self,
                "Select Topic",
                "Topic:",
                sorted(topics),
                0,
                False
            )

            if ok:
                return topic
            return None

        except Exception as e:
            self.output_text.append(f"Error listing topics: {e}")
            logging.error(f"Error listing topics: {e}")
            self.print_sad_emoticon()
            return None

    def list_topics(self):
        """List all available topics."""
        if not self.kafka_service.is_connected():
            QtWidgets.QMessageBox.warning(self, "No Connection", "Not connected to any server")
            return

        try:
            topics = self.kafka_service.list_topics()
            if topics:
                self.output_text.append(f"Topics ({len(topics)}):")
                for topic in sorted(topics):
                    self.output_text.append(f"  - {topic}")
                self.print_happy_emoticon()
            else:
                self.output_text.append("No topics available")
                self.print_sad_emoticon()

        except Exception as e:
            self.output_text.append(f"Error: {e}")
            logging.error(f"Error listing topics: {e}")
            self.print_sad_emoticon()

    def send_payload(self):
        """Send a message to a topic."""
        if not self.kafka_service.is_connected():
            QtWidgets.QMessageBox.warning(self, "No Connection", "Not connected to any server")
            return

        topic = self.select_topic()
        if not topic:
            return

        payload, ok = QtWidgets.QInputDialog.getMultiLineText(
            self,
            "Send Message",
            "Enter message payload:"
        )

        if ok and payload:
            confirm = QtWidgets.QMessageBox.question(
                self,
                'Confirm Send',
                f"Send message to '{topic}'?",
                QtWidgets.QMessageBox.Yes | QtWidgets.QMessageBox.No,
                QtWidgets.QMessageBox.No
            )

            if confirm == QtWidgets.QMessageBox.Yes:
                try:
                    result = self.kafka_service.send_message(topic, payload)
                    self.output_text.append(
                        f"Sent to '{topic}' (Offset: {result.offset}, Partition: {result.partition})"
                    )
                    self.print_happy_emoticon()
                except Exception as e:
                    self.output_text.append(f"Error: {e}")
                    logging.error(f"Error sending message: {e}")
                    self.print_sad_emoticon()

    def overview_messages(self):
        """Open dialog to overview messages from a topic."""
        if not self.kafka_service.is_connected():
            QtWidgets.QMessageBox.warning(self, "No Connection", "Not connected to any server")
            return

        topic = self.select_topic()
        if not topic:
            return

        confirm = QtWidgets.QMessageBox.question(
            self,
            'Overview Messages',
            f"View messages from '{topic}'?",
            QtWidgets.QMessageBox.Yes | QtWidgets.QMessageBox.No,
            QtWidgets.QMessageBox.No
        )

        if confirm == QtWidgets.QMessageBox.Yes:
            try:
                messages_dialog = MessagesDialog(self.kafka_service, topic, self)
                messages_dialog.exec_()
                self.print_happy_emoticon()
            except Exception as e:
                self.output_text.append(f"Error: {e}")
                logging.error(f"Error opening messages dialog: {e}")
                self.print_sad_emoticon()

    def consume_messages(self):
        """Start consuming messages from a topic."""
        if not self.kafka_service.is_connected():
            QtWidgets.QMessageBox.warning(self, "No Connection", "Not connected to any server")
            return

        # Don't allow multiple simultaneous consumption
        if self.consume_thread and self.consume_thread.is_running():
            QtWidgets.QMessageBox.warning(
                self,
                "Already Consuming",
                "Already consuming messages. Stop current consumption first."
            )
            return

        topic = self.select_topic()
        if not topic:
            return

        confirm = QtWidgets.QMessageBox.question(
            self,
            'Consume Messages',
            f"Start consuming from '{topic}'?\n(This will run until stopped)",
            QtWidgets.QMessageBox.Yes | QtWidgets.QMessageBox.No,
            QtWidgets.QMessageBox.No
        )

        if confirm == QtWidgets.QMessageBox.Yes:
            try:
                # Create consumer for the topic
                temp_consumer = self.kafka_service.create_consumer_for_topic(
                    topic,
                    auto_offset_reset='earliest',
                    enable_auto_commit=True
                )

                # Start consume thread
                self.consume_thread = ConsumeThread(temp_consumer)
                self.consume_thread.message_signal.connect(self.display_message)
                self.consume_thread.start()

                # Show stop button, hide consume button
                self.stop_consume_btn.setVisible(True)
                self.consume_messages_btn.setEnabled(False)

                self.output_text.append(f"Consuming from '{topic}' (Click 'Stop Consuming' to stop)")
                self.print_happy_emoticon()

            except Exception as e:
                self.output_text.append(f"Error: {e}")
                logging.error(f"Error starting consumer: {e}")
                self.print_sad_emoticon()

    def stop_consuming(self):
        """Stop consuming messages."""
        if self.consume_thread and self.consume_thread.is_running():
            self.consume_thread.stop()
            self.consume_thread.wait()
            self.consume_thread = None

            # Hide stop button, show consume button
            self.stop_consume_btn.setVisible(False)
            self.consume_messages_btn.setEnabled(True)

            self.output_text.append("Stopped consuming")
            logging.info("Stopped consuming")
            self.print_happy_emoticon()

    def display_message(self, message):
        """
        Display a message in the output.

        Args:
            message: Message string to display
        """
        self.output_text.append(message)

    # Admin operations

    def create_topic(self):
        """Create a new topic."""
        if not self.kafka_service.is_connected():
            QtWidgets.QMessageBox.warning(self, "No Connection", "Not connected to any server")
            return

        # Get topic name
        topic_name, ok = QtWidgets.QInputDialog.getText(
            self,
            "Create Topic",
            "Topic name:"
        )

        if not ok or not topic_name:
            return

        # Validate topic name
        is_valid, error_msg = validate_topic_name(topic_name)
        if not is_valid:
            QtWidgets.QMessageBox.warning(self, "Invalid Topic Name", error_msg)
            return

        # Get number of partitions
        num_partitions, ok = QtWidgets.QInputDialog.getInt(
            self,
            "Partitions",
            "Number of partitions:",
            1, 1, 1000
        )

        if not ok:
            return

        # Get replication factor
        replication_factor, ok = QtWidgets.QInputDialog.getInt(
            self,
            "Replication Factor",
            "Replication factor:",
            1, 1, 10
        )

        if not ok:
            return

        try:
            self.kafka_service.create_topic(topic_name, num_partitions, replication_factor)
            self.output_text.append(
                f"Created topic '{topic_name}' "
                f"(Partitions: {num_partitions}, Replication: {replication_factor})"
            )
            self.print_happy_emoticon()
        except Exception as e:
            self.output_text.append(f"Error: {e}")
            logging.error(f"Error creating topic: {e}")
            self.print_sad_emoticon()

    def delete_topic(self):
        """Delete a topic."""
        if not self.kafka_service.is_connected():
            QtWidgets.QMessageBox.warning(self, "No Connection", "Not connected to any server")
            return

        try:
            topics = self.kafka_service.list_topics()
            if not topics:
                QtWidgets.QMessageBox.warning(self, "No Topics", "No topics available")
                return

            topic, ok = QtWidgets.QInputDialog.getItem(
                self,
                "Delete Topic",
                "Select topic to delete:",
                sorted(topics),
                0,
                False
            )

            if ok and topic:
                confirm = QtWidgets.QMessageBox.question(
                    self,
                    'Delete Topic',
                    f"Delete topic '{topic}'?\nThis action cannot be undone!",
                    QtWidgets.QMessageBox.Yes | QtWidgets.QMessageBox.No,
                    QtWidgets.QMessageBox.No
                )

                if confirm == QtWidgets.QMessageBox.Yes:
                    self.kafka_service.delete_topic(topic)
                    self.output_text.append(f"Deleted topic '{topic}'")
                    self.print_happy_emoticon()

        except Exception as e:
            self.output_text.append(f"Error: {e}")
            logging.error(f"Error deleting topic: {e}")
            self.print_sad_emoticon()

    def describe_cluster(self):
        """Describe the Kafka cluster."""
        if not self.kafka_service.is_connected():
            QtWidgets.QMessageBox.warning(self, "No Connection", "Not connected to any server")
            return

        try:
            cluster_info = self.kafka_service.describe_cluster()
            self.output_text.append("Cluster Information:")
            for key, value in cluster_info.items():
                self.output_text.append(f"  {key}: {value}")
            self.print_happy_emoticon()
        except Exception as e:
            self.output_text.append(f"Error: {e}")
            logging.error(f"Error describing cluster: {e}")
            self.print_sad_emoticon()

    # Settings

    def open_settings(self):
        """Open settings dialog."""
        settings_dialog = SettingsDialog(self)
        if settings_dialog.exec_():
            self.apply_settings()

    def apply_settings(self):
        """Apply current settings."""
        # Apply theme
        theme = self.settings.get('theme', 'Light').lower()
        try:
            stylesheet = load_stylesheet(theme)
            self.setStyleSheet(stylesheet)
        except Exception as e:
            logging.error(f"Error loading stylesheet: {e}")

        # Apply font
        font_family = self.settings.get('font_family', 'Segoe UI')
        font_size = self.settings.get('font_size', 12)
        font = QtGui.QFont(font_family, font_size)
        self.output_text.setFont(font)

        # Update Kafka service settings
        self.kafka_service.update_settings(self.settings)

    # UI helpers

    def print_happy_emoticon(self):
        """Print happy emoticon."""
        self.output_text.append("\n(•‿•)\n")

    def print_sad_emoticon(self):
        """Print sad emoticon."""
        self.output_text.append("\n(︶︹︺)\n")

    # Cleanup

    def closeEvent(self, event):
        """Handle application close."""
        # Stop consuming if active
        if self.consume_thread and self.consume_thread.is_running():
            self.consume_thread.stop()
            self.consume_thread.wait(1000)

        # Disconnect from Kafka
        self.kafka_service.disconnect()

        event.accept()
