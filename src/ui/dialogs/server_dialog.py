"""Server configuration dialog with validation."""
from PyQt5 import QtWidgets, QtCore
from ...utils.validators import validate_bootstrap_servers, validate_server_name, validate_file_path


class ServerDialog(QtWidgets.QDialog):
    """Dialog for configuring server connections."""

    def __init__(self, parent=None, server_name='', server_config=None):
        """
        Initialize server dialog.

        Args:
            parent: Parent widget
            server_name: Name of the server configuration
            server_config: Server configuration dictionary
        """
        super().__init__(parent)
        self.setWindowTitle("Server Configuration")
        self.setGeometry(200, 200, 500, 450)
        self.server_name = server_name
        self.server_config = server_config or {}
        self.init_ui()

    def init_ui(self):
        """Initialize UI components."""
        self.layout = QtWidgets.QFormLayout(self)

        # Server name
        self.server_name_edit = QtWidgets.QLineEdit(self.server_name)
        self.layout.addRow("Name:", self.server_name_edit)

        # Bootstrap servers
        self.bootstrap_servers_edit = QtWidgets.QLineEdit(
            self.server_config.get('bootstrap_servers', '')
        )
        self.layout.addRow("Bootstrap Servers:", self.bootstrap_servers_edit)

        # Help text for bootstrap servers
        help_label = QtWidgets.QLabel("(e.g., localhost:9092 or host1:9092,host2:9092)")
        help_label.setStyleSheet("font-size: 9pt; color: gray;")
        self.layout.addRow("", help_label)

        # Security protocol
        self.security_protocol_combo = QtWidgets.QComboBox()
        self.security_protocol_combo.addItems(['PLAINTEXT', 'SASL_PLAINTEXT', 'SASL_SSL', 'SSL'])
        self.security_protocol_combo.setCurrentText(
            self.server_config.get('security_protocol', 'PLAINTEXT')
        )
        self.layout.addRow("Security Protocol:", self.security_protocol_combo)

        # SASL mechanism
        self.sasl_mechanism_edit = QtWidgets.QLineEdit(
            self.server_config.get('sasl_mechanism', '')
        )
        self.layout.addRow("SASL Mechanism:", self.sasl_mechanism_edit)

        # SASL username
        self.sasl_username_edit = QtWidgets.QLineEdit(
            self.server_config.get('sasl_username', '')
        )
        self.layout.addRow("SASL Username:", self.sasl_username_edit)

        # SASL password
        self.sasl_password_edit = QtWidgets.QLineEdit(
            self.server_config.get('sasl_password', '')
        )
        self.sasl_password_edit.setEchoMode(QtWidgets.QLineEdit.Password)
        self.layout.addRow("SASL Password:", self.sasl_password_edit)

        # SSL CA file
        ssl_ca_layout = QtWidgets.QHBoxLayout()
        self.ssl_cafile_edit = QtWidgets.QLineEdit(
            self.server_config.get('ssl_cafile', '')
        )
        ssl_ca_browse = QtWidgets.QPushButton("Browse...")
        ssl_ca_browse.clicked.connect(lambda: self._browse_file(self.ssl_cafile_edit))
        ssl_ca_layout.addWidget(self.ssl_cafile_edit)
        ssl_ca_layout.addWidget(ssl_ca_browse)
        self.layout.addRow("SSL CA File:", ssl_ca_layout)

        # SSL cert file
        ssl_cert_layout = QtWidgets.QHBoxLayout()
        self.ssl_certfile_edit = QtWidgets.QLineEdit(
            self.server_config.get('ssl_certfile', '')
        )
        ssl_cert_browse = QtWidgets.QPushButton("Browse...")
        ssl_cert_browse.clicked.connect(lambda: self._browse_file(self.ssl_certfile_edit))
        ssl_cert_layout.addWidget(self.ssl_certfile_edit)
        ssl_cert_layout.addWidget(ssl_cert_browse)
        self.layout.addRow("SSL Cert File:", ssl_cert_layout)

        # SSL key file
        ssl_key_layout = QtWidgets.QHBoxLayout()
        self.ssl_keyfile_edit = QtWidgets.QLineEdit(
            self.server_config.get('ssl_keyfile', '')
        )
        ssl_key_browse = QtWidgets.QPushButton("Browse...")
        ssl_key_browse.clicked.connect(lambda: self._browse_file(self.ssl_keyfile_edit))
        ssl_key_layout.addWidget(self.ssl_keyfile_edit)
        ssl_key_layout.addWidget(ssl_key_browse)
        self.layout.addRow("SSL Key File:", ssl_key_layout)

        # Button box
        self.button_box = QtWidgets.QDialogButtonBox(
            QtWidgets.QDialogButtonBox.Ok | QtWidgets.QDialogButtonBox.Cancel,
            QtCore.Qt.Horizontal, self
        )
        self.button_box.accepted.connect(self.validate_and_accept)
        self.button_box.rejected.connect(self.reject)
        self.layout.addRow(self.button_box)

    def _browse_file(self, line_edit):
        """
        Open file browser and set path.

        Args:
            line_edit: QLineEdit to set the path in
        """
        file_path, _ = QtWidgets.QFileDialog.getOpenFileName(
            self,
            "Select File",
            "",
            "All Files (*)"
        )
        if file_path:
            line_edit.setText(file_path)

    def validate_and_accept(self):
        """Validate inputs before accepting."""
        # Validate server name
        server_name = self.server_name_edit.text().strip()
        is_valid, error_msg = validate_server_name(server_name)
        if not is_valid:
            QtWidgets.QMessageBox.warning(self, "Invalid Input", error_msg)
            return

        # Validate bootstrap servers
        bootstrap_servers = self.bootstrap_servers_edit.text().strip()
        is_valid, error_msg = validate_bootstrap_servers(bootstrap_servers)
        if not is_valid:
            QtWidgets.QMessageBox.warning(self, "Invalid Input", error_msg)
            return

        # Validate SSL files if provided
        ssl_cafile = self.ssl_cafile_edit.text().strip()
        if ssl_cafile:
            is_valid, error_msg = validate_file_path(ssl_cafile, must_exist=True)
            if not is_valid:
                QtWidgets.QMessageBox.warning(self, "Invalid Input", f"SSL CA File: {error_msg}")
                return

        ssl_certfile = self.ssl_certfile_edit.text().strip()
        if ssl_certfile:
            is_valid, error_msg = validate_file_path(ssl_certfile, must_exist=True)
            if not is_valid:
                QtWidgets.QMessageBox.warning(self, "Invalid Input", f"SSL Cert File: {error_msg}")
                return

        ssl_keyfile = self.ssl_keyfile_edit.text().strip()
        if ssl_keyfile:
            is_valid, error_msg = validate_file_path(ssl_keyfile, must_exist=True)
            if not is_valid:
                QtWidgets.QMessageBox.warning(self, "Invalid Input", f"SSL Key File: {error_msg}")
                return

        self.accept()

    def get_server_info(self):
        """
        Get server configuration from dialog.

        Returns:
            tuple: (server_name, server_config)
        """
        server_name = self.server_name_edit.text().strip()
        server_config = {
            'bootstrap_servers': self.bootstrap_servers_edit.text().strip(),
            'security_protocol': self.security_protocol_combo.currentText(),
            'sasl_mechanism': self.sasl_mechanism_edit.text().strip(),
            'sasl_username': self.sasl_username_edit.text().strip(),
            'sasl_password': self.sasl_password_edit.text(),
            'ssl_cafile': self.ssl_cafile_edit.text().strip(),
            'ssl_certfile': self.ssl_certfile_edit.text().strip(),
            'ssl_keyfile': self.ssl_keyfile_edit.text().strip(),
        }
        return server_name, server_config
