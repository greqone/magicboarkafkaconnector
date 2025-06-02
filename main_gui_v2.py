import sys
import os
import datetime
import logging
import json
from PyQt5 import QtWidgets, QtGui, QtCore
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError
from kafka.admin import KafkaAdminClient, NewTopic

minimal_light_style = """
QWidget {
    background-color: #f0f0f0;
    color: #333333;
    font-family: 'Segoe UI', sans-serif;
    font-size: 12pt;
}
QLineEdit, QPlainTextEdit, QTextEdit {
    background-color: #ffffff;
    color: #CCE8FF;
    border: 1px solid #cccccc;
    border-radius: 4px;
    padding: 6px;
    font-weight: bold;
}
QPushButton {
    background-color: #ffffff;
    color: #333333;
    border: 1px solid #cccccc;
    border-radius: 4px;
    padding: 8px;
    font-size: 11pt;
}
QPushButton:hover {
    background-color: #e6e6e6;
}
QComboBox, QSpinBox, QInputDialog {
    background-color: #ffffff;
    color: #333333;
    border: 1px solid #cccccc;
    border-radius: 4px;
    padding: 4px;
}
QListWidget {
    background-color: #ffffff;
    color: #333333;
    border: 1px solid #cccccc;
    border-radius: 4px;
    padding: 4px;
}
QMenuBar {
    background-color: #f0f0f0;
    color: #333333;
}
QMenu {
    background-color: #ffffff;
    color: #333333;
    border: 1px solid #cccccc;
}
QMenu::item:selected {
    background-color: #e6e6e6;
}
QMessageBox {
    background-color: #f0f0f0;
}
"""

minimal_dark_style = """
QWidget {
    background-color: #2b2b2b;
    color: #f0f0f0;
    font-family: 'Segoe UI', sans-serif;
    font-size: 12pt;
}
QLineEdit, QPlainTextEdit, QTextEdit {
    background-color: #3c3c3c;
    color: #CCE8FF;
    border: 1px solid #555555;
    border-radius: 4px;
    padding: 6px;
    font-weight: bold;
}
QPushButton {
    background-color: #3c3c3c;
    color: #f0f0f0;
    border: 1px solid #555555;
    border-radius: 4px;
    padding: 8px;
    font-size: 11pt;
}
QPushButton:hover {
    background-color: #555555;
}
QComboBox, QSpinBox, QInputDialog {
    background-color: #3c3c3c;
    color: #f0f0f0;
    border: 1px solid #555555;
    border-radius: 4px;
    padding: 4px;
}
QListWidget {
    background-color: #3c3c3c;
    color: #f0f0f0;
    border: 1px solid #555555;
    border-radius: 4px;
    padding: 4px;
}
QMenuBar {
    background-color: #2b2b2b;
    color: #f0f0f0;
}
QMenu {
    background-color: #3c3c3c;
    color: #f0f0f0;
    border: 1px solid #555555;
}
QMenu::item:selected {
    background-color: #555555;
}
QMessageBox {
    background-color: #2b2b2b;
}
"""

def resource_path(filename):
    """
    Attempt to load from _MEIPASS if running from a PyInstaller bundle.
    Fallback to a local 'resources' folder if not found in _MEIPASS or if _MEIPASS doesn't exist.
    """
    # 1. If we're in a PyInstaller context, try _MEIPASS
    base_path = getattr(sys, '_MEIPASS', None)
    if base_path:
        # Construct path inside _MEIPASS
        path_in_meipass = os.path.join(base_path, filename)
        if os.path.exists(path_in_meipass):
            print(f"[DEBUG] Found {filename} in _MEIPASS: {path_in_meipass}")
            return path_in_meipass
        else:
            print(f"[DEBUG] {filename} not in _MEIPASS, falling back to local resources folder.")

    # 2. Otherwise, or if not found above, fallback to a local "resources" folder
    fallback_dir = os.path.join(os.path.dirname(__file__), "resources")
    path_local = os.path.join(fallback_dir, filename)
    print(f"[DEBUG] Fallback path for {filename}: {path_local}, exists={os.path.exists(path_local)}")
    return path_local

def setup_logging(enabled=True):
    from PyQt5.QtCore import QStandardPaths, QCoreApplication
    QCoreApplication.setApplicationName("Magic Boar Kafka Connector")
    QCoreApplication.setOrganizationName("MagicBoar")
    app_data_dir = QStandardPaths.writableLocation(QStandardPaths.AppDataLocation)
    if not os.path.exists(app_data_dir):
        os.makedirs(app_data_dir, exist_ok=True)
    log_filename = os.path.join(
        app_data_dir,
        f"log_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    )
    if enabled:
        logging.basicConfig(
            filename=log_filename,
            level=logging.DEBUG,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        logging.disable(logging.NOTSET)
    else:
        logging.basicConfig(level=logging.CRITICAL)
        logging.disable(logging.CRITICAL)
    return log_filename

class KafkaApp(QtWidgets.QMainWindow):
    def __init__(self):
        super().__init__()
        # Set app icon from resource_path
        icon_file = resource_path("logo.ico")
        print(f"[DEBUG] Icon path={icon_file}, exists={os.path.exists(icon_file)}")
        self.setWindowIcon(QtGui.QIcon(icon_file))

        self.setWindowTitle("Magic Boar Kafka Connector")
        self.setGeometry(100, 100, 1200, 600)
        self.servers = {}
        self.producer = None
        self.consumer = None
        self.admin_client = None
        self.consume_thread = None
        self.current_config = None

        self.init_ui()
        self.load_servers()
        self.load_settings()

        if self.server_combo.count() > 0:
            self.server_combo.setCurrentIndex(0)

    def init_ui(self):
        self.central_widget = QtWidgets.QWidget()
        self.setCentralWidget(self.central_widget)

        self.horizontal_layout = QtWidgets.QHBoxLayout(self.central_widget)

        self.image_label = QtWidgets.QLabel()
        self.image_label.setScaledContents(False)
        self.horizontal_layout.addWidget(self.image_label)

        self.main_vertical_layout = QtWidgets.QVBoxLayout()

        self.menu_bar = self.menuBar()
        self.settings_menu = self.menu_bar.addMenu('Settings')
        self.settings_action = QtWidgets.QAction('Preferences', self)
        self.settings_action.triggered.connect(self.open_settings)
        self.settings_menu.addAction(self.settings_action)

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

        self.server_layout = QtWidgets.QHBoxLayout()
        self.server_label = QtWidgets.QLabel("Server:")
        self.server_combo = QtWidgets.QComboBox()
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

        self.output_text = QtWidgets.QTextEdit()
        self.output_text.setReadOnly(True)

        self.main_vertical_layout.addLayout(self.server_layout)
        self.main_vertical_layout.addLayout(self.button_layout)
        self.main_vertical_layout.addWidget(self.output_text)

        self.horizontal_layout.addLayout(self.main_vertical_layout)

        self.load_decor_image()

    def load_decor_image(self):
        bg_file = resource_path("background.png")
        print(f"[DEBUG] background path={bg_file}, exists={os.path.exists(bg_file)}")
        if os.path.exists(bg_file):
            pix = QtGui.QPixmap(bg_file)
            if pix.isNull():
                print("[DEBUG] QPixmap is null, image is invalid or corrupted.")
            else:
                print("[DEBUG] background.png loaded successfully.")
                self.image_pixmap_original = pix
                self.update_image_label()
        else:
            print("[DEBUG] background.png not found, no background image loaded.")

    def update_image_label(self):
        if hasattr(self, 'image_pixmap_original'):
            scaled = self.image_pixmap_original.scaled(
                self.image_label.size(),
                QtCore.Qt.KeepAspectRatio,
                QtCore.Qt.SmoothTransformation
            )
            self.image_label.setPixmap(scaled)

    def load_servers(self):
        self.servers_file = os.path.join(os.path.dirname(__file__), 'servers.conf')
        if os.path.exists(self.servers_file):
            with open(self.servers_file, 'r') as f:
                self.servers = json.load(f)
        else:
            self.servers = {
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
        self.server_combo.clear()
        self.server_combo.addItems(self.servers.keys())

    def save_servers(self):
        with open(self.servers_file, 'w') as f:
            json.dump(self.servers, f, indent=4)

    def server_selected(self):
        if self.server_combo.count() == 0:
            return
        server_name = self.server_combo.currentText()
        server_config = self.servers.get(server_name)
        if server_config:
            self.init_kafka_clients(server_config)

    def init_kafka_clients(self, config):
        try:
            if self.producer:
                self.producer.close()
            if self.consumer:
                self.consumer.close()
            if self.admin_client:
                self.admin_client.close()

            self.current_config = config

            self.producer = KafkaProducer(
                bootstrap_servers=config['bootstrap_servers'],
                security_protocol=config['security_protocol'],
                sasl_mechanism=config['sasl_mechanism'] or None,
                sasl_plain_username=config['sasl_username'] or None,
                sasl_plain_password=config['sasl_password'] or None,
                ssl_cafile=config['ssl_cafile'] or None,
                ssl_certfile=config['ssl_certfile'] or None,
                ssl_keyfile=config['ssl_keyfile'] or None,
            )
            self.consumer = KafkaConsumer(
                bootstrap_servers=config['bootstrap_servers'],
                security_protocol=config['security_protocol'],
                sasl_mechanism=config['sasl_mechanism'] or None,
                sasl_plain_username=config['sasl_username'] or None,
                sasl_plain_password=config['sasl_password'] or None,
                ssl_cafile=config['ssl_cafile'] or None,
                ssl_certfile=config['ssl_certfile'] or None,
                ssl_keyfile=config['ssl_keyfile'] or None,
            )
            self.admin_client = KafkaAdminClient(
                bootstrap_servers=config['bootstrap_servers'],
                security_protocol=config['security_protocol'],
                sasl_mechanism=config['sasl_mechanism'] or None,
                sasl_plain_username=config['sasl_username'] or None,
                sasl_plain_password=config['sasl_password'] or None,
                ssl_cafile=config['ssl_cafile'] or None,
                ssl_certfile=config['ssl_certfile'] or None,
                ssl_keyfile=config['ssl_keyfile'] or None,
            )
            self.output_text.append(f"Connected to {config['bootstrap_servers']}")
            logging.info(f"Connected to {config['bootstrap_servers']}")
            self.print_happy_emoticon()
        except Exception as e:
            error_msg = f"Error connecting to server: {e}"
            self.output_text.append(error_msg)
            logging.error(error_msg)
            self.print_sad_emoticon()

    def add_server(self):
        server_dialog = ServerDialog(self)
        if server_dialog.exec_():
            server_name, server_config = server_dialog.get_server_info()
            self.servers[server_name] = server_config
            self.server_combo.addItem(server_name)
            self.save_servers()

    def edit_server(self):
        server_name = self.server_combo.currentText()
        server_config = self.servers.get(server_name)
        server_dialog = ServerDialog(self, server_name, server_config)
        if server_dialog.exec_():
            new_server_name, new_server_config = server_dialog.get_server_info()
            del self.servers[server_name]
            self.servers[new_server_name] = new_server_config
            self.server_combo.setItemText(self.server_combo.currentIndex(), new_server_name)
            self.save_servers()
            self.server_selected()

    def remove_server(self):
        server_name = self.server_combo.currentText()
        reply = QtWidgets.QMessageBox.question(
            self,
            'Remove',
            f"Remove '{server_name}'?",
            QtWidgets.QMessageBox.Yes | QtWidgets.QMessageBox.No,
            QtWidgets.QMessageBox.No
        )
        if reply == QtWidgets.QMessageBox.Yes:
            del self.servers[server_name]
            self.server_combo.removeItem(self.server_combo.currentIndex())
            self.save_servers()
            if self.server_combo.count() > 0:
                self.server_selected()
            else:
                self.producer = None
                self.consumer = None
                self.admin_client = None

    def select_topic(self):
        if not self.consumer:
            QtWidgets.QMessageBox.warning(self, "No Connection", "Not connected to any server.")
            return None
        try:
            topics = list(self.consumer.topics())
            if not topics:
                QtWidgets.QMessageBox.warning(self, "No Topics", "No topics available.")
                return None
            topic, ok = QtWidgets.QInputDialog.getItem(self, "Topic", "Select:", topics, 0, False)
            if ok:
                return topic
            return None
        except Exception as e:
            self.output_text.append(f"Error: {e}")
            logging.error(f"Error: {e}")
            self.print_sad_emoticon()
            return None

    def send_payload(self):
        if not self.producer:
            QtWidgets.QMessageBox.warning(self, "No Connection", "Not connected to any server.")
            return
        topic = self.select_topic()
        if topic:
            payload, ok = QtWidgets.QInputDialog.getMultiLineText(self, "Payload", "Enter:")
            if ok:
                confirm = QtWidgets.QMessageBox.question(
                    self,
                    'Send',
                    f"Send to '{topic}'?",
                    QtWidgets.QMessageBox.Yes | QtWidgets.QMessageBox.No,
                    QtWidgets.QMessageBox.No
                )
                if confirm == QtWidgets.QMessageBox.Yes:
                    try:
                        future = self.producer.send(topic, value=payload.encode('utf-8'))
                        result = future.get(timeout=10)
                        self.output_text.append(f"Sent to '{topic}'")
                        logging.info(f"Sent to '{topic}'. {result}")
                        self.print_happy_emoticon()
                    except KafkaError as e:
                        self.output_text.append(f"Error: {e}")
                        logging.error(f"Error sending payload: {e}")
                        self.print_sad_emoticon()
                    except Exception as e:
                        self.output_text.append(f"Error: {e}")
                        logging.error(f"Error sending payload: {e}")
                        self.print_sad_emoticon()

    def overview_messages(self):
        if not self.consumer:
            QtWidgets.QMessageBox.warning(self, "No Connection", "Not connected to any server.")
            return
        if not self.current_config:
            QtWidgets.QMessageBox.warning(self, "No Config", "No current config available.")
            return
        topic = self.select_topic()
        if topic:
            confirm = QtWidgets.QMessageBox.question(
                self,
                'Overview',
                f"Overview from '{topic}'?",
                QtWidgets.QMessageBox.Yes | QtWidgets.QMessageBox.No,
                QtWidgets.QMessageBox.No
            )
            if confirm == QtWidgets.QMessageBox.Yes:
                try:
                    messages_dialog = MessagesDialog(self.current_config, topic, self)
                    messages_dialog.exec_()
                except Exception as e:
                    self.output_text.append(f"Error: {e}")
                    logging.error(f"Error overviewing messages: {e}")
                    self.print_sad_emoticon()

    def display_message(self, message):
        self.output_text.append(message)

    def list_topics(self):
        if not self.consumer:
            QtWidgets.QMessageBox.warning(self, "No Connection", "Not connected to any server.")
            return
        try:
            topics = list(self.consumer.topics())
            if topics:
                self.output_text.append("Topics:")
                for topic in topics:
                    self.output_text.append(f"- {topic}")
                logging.info("Listed topics.")
                self.print_happy_emoticon()
            else:
                self.output_text.append("No topics.")
                logging.info("No topics.")
                self.print_sad_emoticon()
        except Exception as e:
            self.output_text.append(f"Error: {e}")
            logging.error(f"Error listing topics: {e}")
            self.print_sad_emoticon()

    def consume_messages(self):
        if not self.consumer:
            QtWidgets.QMessageBox.warning(self, "No Connection", "Not connected to any server.")
            return
        if not self.current_config:
            QtWidgets.QMessageBox.warning(self, "No Config", "No current config available.")
            return
        topic = self.select_topic()
        if topic:
            confirm = QtWidgets.QMessageBox.question(
                self,
                'Consume',
                f"Consume from '{topic}'?",
                QtWidgets.QMessageBox.Yes | QtWidgets.QMessageBox.No,
                QtWidgets.QMessageBox.No
            )
            if confirm == QtWidgets.QMessageBox.Yes:
                try:
                    from kafka import KafkaConsumer
                    temp_consumer = KafkaConsumer(
                        topic,
                        bootstrap_servers=self.current_config['bootstrap_servers'],
                        security_protocol=self.current_config['security_protocol'],
                        sasl_mechanism=self.current_config['sasl_mechanism'] or None,
                        sasl_plain_username=self.current_config['sasl_username'] or None,
                        sasl_plain_password=self.current_config['sasl_password'] or None,
                        ssl_cafile=self.current_config['ssl_cafile'] or None,
                        ssl_certfile=self.current_config['ssl_certfile'] or None,
                        ssl_keyfile=self.current_config['ssl_keyfile'] or None,
                        auto_offset_reset='earliest',
                        enable_auto_commit=True
                    )
                    self.output_text.append(f"Consuming '{topic}' (Stop below)")
                    self.consume_thread = ConsumeThread(temp_consumer)
                    self.consume_thread.message_signal.connect(self.display_message)
                    self.consume_thread.start()
                    self.stop_consume_btn = QtWidgets.QPushButton("Stop")
                    self.stop_consume_btn.clicked.connect(self.stop_consuming)
                    self.main_vertical_layout.addWidget(self.stop_consume_btn)
                    self.print_happy_emoticon()
                except Exception as e:
                    self.output_text.append(f"Error: {e}")
                    logging.error(f"Error consuming messages: {e}")
                    self.print_sad_emoticon()

    def stop_consuming(self):
        if hasattr(self, 'consume_thread') and self.consume_thread:
            self.consume_thread.stop()
            self.consume_thread.wait()
            del self.consume_thread
            if hasattr(self, 'stop_consume_btn'):
                self.stop_consume_btn.deleteLater()
                del self.stop_consume_btn
            self.output_text.append("Stopped.")
            logging.info("Stopped consuming.")
            self.print_happy_emoticon()

    def open_settings(self):
        settings_dialog = SettingsDialog(self)
        if settings_dialog.exec_():
            self.apply_settings()

    def load_settings(self):
        self.settings_file = os.path.join(os.path.dirname(__file__), 'settings.conf')
        if os.path.exists(self.settings_file):
            with open(self.settings_file, 'r') as f:
                self.settings = json.load(f)
        else:
            self.settings = {
                'theme': 'Light',
                'font_family': 'Segoe UI',
                'font_size': 12,
                'logging_enabled': True
            }
        setup_logging(enabled=self.settings.get('logging_enabled', True))
        self.apply_settings()

    def save_settings(self):
        with open(self.settings_file, 'w') as f:
            json.dump(self.settings, f, indent=4)

    def apply_settings(self):
        theme = self.settings.get('theme', 'Light')
        if theme == 'Dark':
            self.setStyleSheet(minimal_dark_style)
        else:
            self.setStyleSheet(minimal_light_style)

        font_family = self.settings.get('font_family', 'Segoe UI')
        font_size = self.settings.get('font_size', 12)
        font = QtGui.QFont(font_family, font_size)
        self.output_text.setFont(font)

        if self.settings.get('logging_enabled', True):
            logging.disable(logging.NOTSET)
        else:
            logging.disable(logging.CRITICAL)

    def print_happy_emoticon(self):
        happy_emoticon = "\n(•‿•)\n"
        self.output_text.append(happy_emoticon)

    def print_sad_emoticon(self):
        sad_emoticon = "\n(︶︹︺)\n"
        self.output_text.append(sad_emoticon)

    def create_topic(self):
        if not self.admin_client:
            QtWidgets.QMessageBox.warning(self, "No Server", "No server selected.")
            return
        topic_name, ok = QtWidgets.QInputDialog.getText(self, "Create Topic", "Name:")
        if not ok or not topic_name.strip():
            return
        num_partitions, ok = QtWidgets.QInputDialog.getInt(self, "Partitions", "Count:", 1, 1)
        if not ok:
            return
        replication_factor, ok = QtWidgets.QInputDialog.getInt(self, "Replicas", "Count:", 1, 1)
        if not ok:
            return
        try:
            new_topic = NewTopic(name=topic_name.strip(), num_partitions=num_partitions, replication_factor=replication_factor)
            self.admin_client.create_topics(new_topics=[new_topic], validate_only=False)
            self.output_text.append(f"Created '{topic_name}'.")
            logging.info(f"Created '{topic_name}'.")
            self.print_happy_emoticon()
        except Exception as e:
            self.output_text.append(f"Error: {e}")
            logging.error(f"Error creating topic: {e}")
            self.print_sad_emoticon()

    def delete_topic(self):
        if not self.admin_client:
            QtWidgets.QMessageBox.warning(self, "No Server", "No server selected.")
            return
        try:
            topics = list(self.consumer.topics())
            if not topics:
                QtWidgets.QMessageBox.warning(self, "No Topics", "None.")
                return
            topic, ok = QtWidgets.QInputDialog.getItem(self, "Delete Topic", "Select:", topics, 0, False)
            if ok and topic:
                confirm = QtWidgets.QMessageBox.question(
                    self,
                    'Delete',
                    f"Delete '{topic}'?",
                    QtWidgets.QMessageBox.Yes | QtWidgets.QMessageBox.No,
                    QtWidgets.QMessageBox.No
                )
                if confirm == QtWidgets.QMessageBox.Yes:
                    self.admin_client.delete_topics([topic])
                    self.output_text.append(f"Deleted '{topic}'.")
                    logging.info(f"Deleted '{topic}'.")
                    self.print_happy_emoticon()
        except Exception as e:
            self.output_text.append(f"Error: {e}")
            logging.error(f"Error deleting topic: {e}")
            self.print_sad_emoticon()

    def describe_cluster(self):
        if not self.admin_client:
            QtWidgets.QMessageBox.warning(self, "No Server", "No server selected.")
            return
        try:
            cluster_info = self.admin_client.describe_cluster()
            self.output_text.append("Cluster Info:")
            for key, value in cluster_info.items():
                self.output_text.append(f"{key}: {value}")
            logging.info("Described cluster.")
            self.print_happy_emoticon()
        except Exception as e:
            self.output_text.append(f"Error: {e}")
            logging.error(f"Error describing cluster: {e}")
            self.print_sad_emoticon()

class ServerDialog(QtWidgets.QDialog):
    def __init__(self, parent=None, server_name='', server_config=None):
        super().__init__(parent)
        self.setWindowTitle("Server Configuration")
        self.setGeometry(200, 200, 400, 400)
        self.server_name = server_name
        self.server_config = server_config or {}
        self.init_ui()

    def init_ui(self):
        self.layout = QtWidgets.QFormLayout(self)
        self.server_name_edit = QtWidgets.QLineEdit(self.server_name)
        self.layout.addRow("Name:", self.server_name_edit)
        self.bootstrap_servers_edit = QtWidgets.QLineEdit(self.server_config.get('bootstrap_servers', ''))
        self.layout.addRow("Bootstrap:", self.bootstrap_servers_edit)
        self.security_protocol_combo = QtWidgets.QComboBox()
        self.security_protocol_combo.addItems(['PLAINTEXT', 'SASL_PLAINTEXT', 'SASL_SSL', 'SSL'])
        self.security_protocol_combo.setCurrentText(self.server_config.get('security_protocol', 'PLAINTEXT'))
        self.layout.addRow("Protocol:", self.security_protocol_combo)
        self.sasl_mechanism_edit = QtWidgets.QLineEdit(self.server_config.get('sasl_mechanism', ''))
        self.layout.addRow("SASL Mech:", self.sasl_mechanism_edit)
        self.sasl_username_edit = QtWidgets.QLineEdit(self.server_config.get('sasl_username', ''))
        self.layout.addRow("SASL User:", self.sasl_username_edit)
        self.sasl_password_edit = QtWidgets.QLineEdit(self.server_config.get('sasl_password', ''))
        self.sasl_password_edit.setEchoMode(QtWidgets.QLineEdit.Password)
        self.layout.addRow("SASL Pass:", self.sasl_password_edit)
        self.ssl_cafile_edit = QtWidgets.QLineEdit(self.server_config.get('ssl_cafile', ''))
        self.layout.addRow("SSL CA:", self.ssl_cafile_edit)
        self.ssl_certfile_edit = QtWidgets.QLineEdit(self.server_config.get('ssl_certfile', ''))
        self.layout.addRow("SSL Cert:", self.ssl_certfile_edit)
        self.ssl_keyfile_edit = QtWidgets.QLineEdit(self.server_config.get('ssl_keyfile', ''))
        self.layout.addRow("SSL Key:", self.ssl_keyfile_edit)
        self.button_box = QtWidgets.QDialogButtonBox(
            QtWidgets.QDialogButtonBox.Ok | QtWidgets.QDialogButtonBox.Cancel,
            QtCore.Qt.Horizontal, self)
        self.button_box.accepted.connect(self.accept)
        self.button_box.rejected.connect(self.reject)
        self.layout.addRow(self.button_box)

    def get_server_info(self):
        server_name = self.server_name_edit.text()
        server_config = {
            'bootstrap_servers': self.bootstrap_servers_edit.text(),
            'security_protocol': self.security_protocol_combo.currentText(),
            'sasl_mechanism': self.sasl_mechanism_edit.text(),
            'sasl_username': self.sasl_username_edit.text(),
            'sasl_password': self.sasl_password_edit.text(),
            'ssl_cafile': self.ssl_cafile_edit.text(),
            'ssl_certfile': self.ssl_certfile_edit.text(),
            'ssl_keyfile': self.ssl_keyfile_edit.text(),
        }
        return server_name, server_config

class MessagesDialog(QtWidgets.QDialog):
    def __init__(self, consumer_config, topic, parent=None):
        super().__init__(parent)
        self.setWindowTitle(f"Messages: {topic}")
        self.setGeometry(300, 200, 600, 400)
        self.consumer_config = consumer_config
        self.topic = topic
        self.highlighter = None
        self.init_ui()
        self.fetch_messages()

    def init_ui(self):
        self.layout = QtWidgets.QVBoxLayout(self)
        self.message_list = QtWidgets.QListWidget()
        self.message_list.currentItemChanged.connect(self.display_message)
        self.layout.addWidget(self.message_list)
        self.message_text = QtWidgets.QTextEdit()
        self.message_text.setReadOnly(True)
        self.message_text.setStyleSheet("QTextEdit {color: #CCE8FF; font-weight: bold;}")
        self.layout.addWidget(self.message_text)
        self.close_button = QtWidgets.QPushButton("Close")
        self.close_button.clicked.connect(self.accept)
        self.layout.addWidget(self.close_button)

    def fetch_messages(self):
        self.overview_thread = OverviewThread(self.consumer_config, self.topic)
        self.overview_thread.message_signal.connect(self.add_message)
        self.overview_thread.finished.connect(self.fetch_finished)
        self.overview_thread.start()

    def add_message(self, message):
        if isinstance(message, str):
            self.message_list.addItem("Error: " + message)
        else:
            offset = str(message.offset)
            key = str(message.key) if message.key else ''
            item_text = f"Offset: {offset}, Key: {key}"
            item = QtWidgets.QListWidgetItem(item_text)
            value = message.value
            if value is not None:
                value = value.decode('utf-8', errors='replace')
            else:
                value = ""
            item.setData(QtCore.Qt.UserRole, value)
            self.message_list.addItem(item)

    def display_message(self, current, previous):
        if current:
            value = current.data(QtCore.Qt.UserRole)
            if value is None:
                value = ""
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

    def fetch_finished(self):
        pass

class OverviewThread(QtCore.QThread):
    message_signal = QtCore.pyqtSignal(object)
    finished = QtCore.pyqtSignal()
    def __init__(self, consumer_config, topic):
        super().__init__()
        self.consumer_config = consumer_config
        self.topic = topic
    def run(self):
        try:
            from kafka import KafkaConsumer
            temp_consumer = KafkaConsumer(
                self.topic,
                bootstrap_servers=self.consumer_config['bootstrap_servers'],
                security_protocol=self.consumer_config['security_protocol'],
                sasl_mechanism=self.consumer_config['sasl_mechanism'] or None,
                sasl_plain_username=self.consumer_config['sasl_username'] or None,
                sasl_plain_password=self.consumer_config['sasl_password'] or None,
                ssl_cafile=self.consumer_config['ssl_cafile'] or None,
                ssl_certfile=self.consumer_config['ssl_certfile'] or None,
                ssl_keyfile=self.consumer_config['ssl_keyfile'] or None,
                auto_offset_reset='earliest',
                enable_auto_commit=False
            )
            count = 0
            for message in temp_consumer:
                self.message_signal.emit(message)
                logging.debug(f"Message: {message}")
                count += 1
                if count >= 10:
                    break
            temp_consumer.close()
            logging.info(f"Overviewed {count} messages from '{self.topic}'.")
        except Exception as e:
            error_msg = f"Overview error: {e}"
            self.message_signal.emit(error_msg)
            logging.error(error_msg)
        finally:
            self.finished.emit()

class JsonHighlighter(QtGui.QSyntaxHighlighter):
    def __init__(self, document):
        super().__init__(document)
        self.rules = []
        string_format = QtGui.QTextCharFormat()
        string_format.setForeground(QtGui.QColor('green'))
        self.rules.append((QtCore.QRegExp(r'\".*?\"(?=:)'), string_format))
        self.rules.append((QtCore.QRegExp(r'(?<=:\s)\".*?\"'), string_format))
        number_format = QtGui.QTextCharFormat()
        number_format.setForeground(QtGui.QColor('magenta'))
        self.rules.append((QtCore.QRegExp(r'\b[0-9]+\b'), number_format))
        bool_format = QtGui.QTextCharFormat()
        bool_format.setForeground(QtGui.QColor('red'))
        self.rules.append((QtCore.QRegExp(r'\b(true|false|null)\b'), bool_format))
        brace_format = QtGui.QTextCharFormat()
        brace_format.setForeground(QtGui.QColor('blue'))
        self.rules.append((QtCore.QRegExp(r'[\{\}\[\]]'), brace_format))
        colon_format = QtGui.QTextCharFormat()
        colon_format.setForeground(QtGui.QColor('black'))
        self.rules.append((QtCore.QRegExp(r':'), colon_format))
        comma_format = QtGui.QTextCharFormat()
        comma_format.setForeground(QtGui.QColor('black'))
        self.rules.append((QtCore.QRegExp(r','), comma_format))

    def highlightBlock(self, text):
        for pattern, fmt in self.rules:
            expression = QtCore.QRegExp(pattern)
            index = expression.indexIn(text)
            while index >= 0:
                length = expression.matchedLength()
                self.setFormat(index, length, fmt)
                index = expression.indexIn(text, index + length)

class ConsumeThread(QtCore.QThread):
    message_signal = QtCore.pyqtSignal(str)
    def __init__(self, consumer):
        super().__init__()
        self.consumer = consumer
        self._is_running = True
    def run(self):
        try:
            for message in self.consumer:
                if not self._is_running:
                    break
                value = message.value
                if value is not None:
                    try:
                        decoded = value.decode('utf-8', errors='replace')
                    except Exception:
                        decoded = str(value)
                else:
                    decoded = ""
                msg = f"Offset: {message.offset}, Key: {message.key}, Value: {decoded}"
                self.message_signal.emit(msg)
                logging.debug(f"Message: {message}")
        except Exception as e:
            self.message_signal.emit(f"Error: {e}")
            logging.error(f"Error consuming messages: {e}")
    def stop(self):
        self._is_running = False
        self.consumer.close()

class SettingsDialog(QtWidgets.QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Settings")
        self.setGeometry(200, 200, 300, 200)
        self.parent = parent
        self.selected_font = None
        self.init_ui()

    def init_ui(self):
        self.layout = QtWidgets.QFormLayout(self)
        self.theme_combo = QtWidgets.QComboBox()
        self.theme_combo.addItems(['Light', 'Dark'])
        self.theme_combo.setCurrentText(self.parent.settings.get('theme', 'Light'))
        self.layout.addRow("Theme:", self.theme_combo)

        self.font_btn = QtWidgets.QPushButton("Font")
        self.font_btn.clicked.connect(self.select_font)
        self.font_label = QtWidgets.QLabel(f"{self.parent.settings.get('font_family', 'Segoe UI')}, {self.parent.settings.get('font_size', 12)}")
        font_layout = QtWidgets.QHBoxLayout()
        font_layout.addWidget(self.font_btn)
        font_layout.addWidget(self.font_label)
        self.layout.addRow("Output Font:", font_layout)

        self.logging_checkbox = QtWidgets.QCheckBox("Enable Logging")
        self.logging_checkbox.setChecked(self.parent.settings.get('logging_enabled', True))
        self.layout.addRow(self.logging_checkbox)
        info_label = QtWidgets.QLabel("Logs stored in %APPDATA%/MagicBoar/MagicBoarKafkaConnector")
        info_label.setWordWrap(True)
        self.layout.addRow(info_label)

        self.button_box = QtWidgets.QDialogButtonBox(
            QtWidgets.QDialogButtonBox.Ok | QtWidgets.QDialogButtonBox.Cancel,
            QtCore.Qt.Horizontal, self)
        self.button_box.accepted.connect(self.save_settings)
        self.button_box.rejected.connect(self.reject)
        self.layout.addRow(self.button_box)

    def select_font(self):
        current_font = QtGui.QFont(
            self.parent.settings.get('font_family', 'Segoe UI'),
            self.parent.settings.get('font_size', 12)
        )
        font, ok = QtWidgets.QFontDialog.getFont(current_font, self, "Select Font")
        if ok:
            self.font_label.setText(f"{font.family()}, {font.pointSize()}")
            self.selected_font = font

    def save_settings(self):
        self.parent.settings['theme'] = self.theme_combo.currentText()
        if self.selected_font:
            self.parent.settings['font_family'] = self.selected_font.family()
            self.parent.settings['font_size'] = self.selected_font.pointSize()
        self.parent.settings['logging_enabled'] = self.logging_checkbox.isChecked()
        self.parent.save_settings()
        setup_logging(enabled=self.parent.settings.get('logging_enabled', True))
        self.accept()

def main():
    setup_logging(enabled=True)
    app = QtWidgets.QApplication(sys.argv)
    QtCore.QCoreApplication.setApplicationName("Magic Boar Kafka Connector")
    QtCore.QCoreApplication.setOrganizationName("MagicBoar")
    kafka_app = KafkaApp()
    kafka_app.show()
    sys.exit(app.exec_())

if __name__ == '__main__':
    main()
