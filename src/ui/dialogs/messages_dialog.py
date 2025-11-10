"""Messages overview dialog with proper separation of concerns."""
import json
from PyQt5 import QtWidgets, QtCore
from ...threads.overview_thread import OverviewThread
from ...highlighters.json_highlighter import JsonHighlighter
from ...kafka.client_factory import create_kafka_consumer


class MessagesDialog(QtWidgets.QDialog):
    """Dialog for viewing messages from a topic."""

    def __init__(self, kafka_service, topic, parent=None):
        """
        Initialize messages dialog.

        Args:
            kafka_service: KafkaService instance
            topic: Topic name
            parent: Parent widget
        """
        super().__init__(parent)
        self.setWindowTitle(f"Messages: {topic}")
        self.setGeometry(300, 200, 700, 500)
        self.kafka_service = kafka_service
        self.topic = topic
        self.highlighter = None
        self.overview_thread = None
        self.init_ui()
        self.fetch_messages()

    def init_ui(self):
        """Initialize UI components."""
        self.layout = QtWidgets.QVBoxLayout(self)

        # Search/Filter box
        search_layout = QtWidgets.QHBoxLayout()
        search_label = QtWidgets.QLabel("Filter:")
        self.search_input = QtWidgets.QLineEdit()
        self.search_input.setPlaceholderText("Search in messages...")
        self.search_input.textChanged.connect(self.filter_messages)
        search_layout.addWidget(search_label)
        search_layout.addWidget(self.search_input)
        self.layout.addLayout(search_layout)

        # Message list
        self.message_list = QtWidgets.QListWidget()
        self.message_list.currentItemChanged.connect(self.display_message)
        self.layout.addWidget(self.message_list)

        # Store all messages for filtering
        self.all_messages = []

        # Message text display
        self.message_text = QtWidgets.QTextEdit()
        self.message_text.setReadOnly(True)
        self.layout.addWidget(self.message_text)

        # Button layout
        button_layout = QtWidgets.QHBoxLayout()

        # Export button
        self.export_button = QtWidgets.QPushButton("Export Selected")
        self.export_button.clicked.connect(self.export_message)
        self.export_button.setEnabled(False)
        button_layout.addWidget(self.export_button)

        # Close button
        self.close_button = QtWidgets.QPushButton("Close")
        self.close_button.clicked.connect(self.accept)
        button_layout.addWidget(self.close_button)

        self.layout.addLayout(button_layout)

    def fetch_messages(self):
        """Fetch messages from topic using overview thread."""
        try:
            # Get config and limit from service
            config = self.kafka_service.get_current_config()
            if not config:
                raise Exception("No configuration available")

            # Get message limit from parent settings
            limit = 100
            if self.parent() and hasattr(self.parent(), 'settings'):
                limit = self.parent().settings.get('overview_message_limit', 100)

            # Create consumer for overview
            temp_consumer = create_kafka_consumer(
                config,
                self.topic,
                auto_offset_reset='earliest',
                enable_auto_commit=False
            )

            # Start overview thread
            self.overview_thread = OverviewThread(temp_consumer, self.topic, limit)
            self.overview_thread.message_signal.connect(self.add_message)
            self.overview_thread.finished.connect(self.fetch_finished)
            self.overview_thread.start()

        except Exception as e:
            QtWidgets.QMessageBox.critical(
                self,
                "Error",
                f"Failed to fetch messages: {e}"
            )
            self.reject()

    def add_message(self, message):
        """
        Add a message to the list.

        Args:
            message: Message object or error string
        """
        if isinstance(message, str):
            self.message_list.addItem("Error: " + message)
        else:
            offset = str(message.offset)
            key = str(message.key) if message.key else ''
            timestamp = message.timestamp if hasattr(message, 'timestamp') else ''

            item_text = f"Offset: {offset}, Key: {key}"
            if timestamp:
                item_text += f", Time: {timestamp}"

            # Decode and store message value
            value = message.value
            if value is not None:
                value = value.decode('utf-8', errors='replace')
            else:
                value = ""

            # Store message data for filtering
            message_data = {
                'text': item_text,
                'value': value,
                'offset': offset,
                'key': key,
                'timestamp': timestamp
            }
            self.all_messages.append(message_data)

            # Create list item
            item = QtWidgets.QListWidgetItem(item_text)
            item.setData(QtCore.Qt.UserRole, value)
            self.message_list.addItem(item)

    def display_message(self, current, previous):
        """
        Display selected message.

        Args:
            current: Current list item
            previous: Previous list item
        """
        if current:
            self.export_button.setEnabled(True)
            value = current.data(QtCore.Qt.UserRole)

            if value is None:
                value = ""

            # Try to format as JSON
            try:
                json_value = json.loads(value)
                pretty_json = json.dumps(json_value, indent=4)
                self.message_text.setPlainText(pretty_json)
                self.highlighter = JsonHighlighter(self.message_text.document())
            except (json.JSONDecodeError, TypeError):
                self.message_text.setPlainText(value)
                self.highlighter = None
        else:
            self.message_text.clear()
            self.highlighter = None
            self.export_button.setEnabled(False)

    def export_message(self):
        """Export selected message to file."""
        current_item = self.message_list.currentItem()
        if not current_item:
            return

        value = current_item.data(QtCore.Qt.UserRole)
        if not value:
            QtWidgets.QMessageBox.warning(self, "No Data", "No message data to export")
            return

        # Ask for file location
        file_path, _ = QtWidgets.QFileDialog.getSaveFileName(
            self,
            "Export Message",
            f"message_{self.topic}.txt",
            "Text Files (*.txt);;JSON Files (*.json);;All Files (*)"
        )

        if file_path:
            try:
                with open(file_path, 'w', encoding='utf-8') as f:
                    f.write(value)
                QtWidgets.QMessageBox.information(
                    self,
                    "Success",
                    f"Message exported to {file_path}"
                )
            except Exception as e:
                QtWidgets.QMessageBox.critical(
                    self,
                    "Error",
                    f"Failed to export message: {e}"
                )

    def fetch_finished(self):
        """Called when message fetching is complete."""
        if self.message_list.count() == 0:
            self.message_list.addItem("No messages found in topic")

    def filter_messages(self, filter_text):
        """
        Filter messages based on search text.

        Args:
            filter_text: Text to filter by (case-insensitive)
        """
        # Clear current list
        self.message_list.clear()

        # If no filter, show all messages
        if not filter_text:
            for msg_data in self.all_messages:
                item = QtWidgets.QListWidgetItem(msg_data['text'])
                item.setData(QtCore.Qt.UserRole, msg_data['value'])
                self.message_list.addItem(item)
            return

        # Filter messages
        filter_lower = filter_text.lower()
        for msg_data in self.all_messages:
            # Search in display text, value content, key, and offset
            if (filter_lower in msg_data['text'].lower() or
                filter_lower in msg_data['value'].lower() or
                filter_lower in msg_data['key'].lower() or
                filter_lower in msg_data['offset']):

                item = QtWidgets.QListWidgetItem(msg_data['text'])
                item.setData(QtCore.Qt.UserRole, msg_data['value'])
                self.message_list.addItem(item)

        # Show count
        if self.message_list.count() == 0:
            self.message_list.addItem(f"No messages matching '{filter_text}'")

    def closeEvent(self, event):
        """Handle dialog close event."""
        if self.overview_thread and self.overview_thread.isRunning():
            self.overview_thread.stop()
            self.overview_thread.wait(1000)
        event.accept()
