"""Settings dialog with dynamic path display."""
from PyQt5 import QtWidgets, QtGui, QtCore
from ...utils.logger import get_log_directory


class SettingsDialog(QtWidgets.QDialog):
    """Dialog for application settings."""

    def __init__(self, parent=None):
        """
        Initialize settings dialog.

        Args:
            parent: Parent widget (KafkaApp instance)
        """
        super().__init__(parent)
        self.setWindowTitle("Settings")
        self.setGeometry(200, 200, 400, 300)
        self.parent = parent
        self.selected_font = None
        self.init_ui()

    def init_ui(self):
        """Initialize UI components."""
        self.layout = QtWidgets.QFormLayout(self)

        # Theme selection
        self.theme_combo = QtWidgets.QComboBox()
        self.theme_combo.addItems(['Light', 'Dark'])
        self.theme_combo.setCurrentText(self.parent.settings.get('theme', 'Light'))
        self.layout.addRow("Theme:", self.theme_combo)

        # Font selection
        self.font_btn = QtWidgets.QPushButton("Select Font...")
        self.font_btn.clicked.connect(self.select_font)
        self.font_label = QtWidgets.QLabel(
            f"{self.parent.settings.get('font_family', 'Segoe UI')}, "
            f"{self.parent.settings.get('font_size', 12)}pt"
        )
        font_layout = QtWidgets.QHBoxLayout()
        font_layout.addWidget(self.font_btn)
        font_layout.addWidget(self.font_label)
        self.layout.addRow("Output Font:", font_layout)

        # Overview message limit
        self.limit_spinbox = QtWidgets.QSpinBox()
        self.limit_spinbox.setMinimum(1)
        self.limit_spinbox.setMaximum(10000)
        self.limit_spinbox.setValue(self.parent.settings.get('overview_message_limit', 100))
        self.layout.addRow("Message Overview Limit:", self.limit_spinbox)

        # Add separator
        separator1 = QtWidgets.QFrame()
        separator1.setFrameShape(QtWidgets.QFrame.HLine)
        separator1.setFrameShadow(QtWidgets.QFrame.Sunken)
        self.layout.addRow(separator1)

        # Connection retry enabled
        self.retry_checkbox = QtWidgets.QCheckBox("Enable Connection Retry")
        self.retry_checkbox.setChecked(self.parent.settings.get('connection_retry_enabled', True))
        self.layout.addRow(self.retry_checkbox)

        # Connection retry attempts
        self.retry_attempts_spinbox = QtWidgets.QSpinBox()
        self.retry_attempts_spinbox.setMinimum(1)
        self.retry_attempts_spinbox.setMaximum(10)
        self.retry_attempts_spinbox.setValue(self.parent.settings.get('connection_retry_attempts', 3))
        self.layout.addRow("Retry Attempts:", self.retry_attempts_spinbox)

        # Connection timeout
        self.connection_timeout_spinbox = QtWidgets.QSpinBox()
        self.connection_timeout_spinbox.setMinimum(1)
        self.connection_timeout_spinbox.setMaximum(300)
        self.connection_timeout_spinbox.setSuffix(" seconds")
        self.connection_timeout_spinbox.setValue(self.parent.settings.get('connection_timeout', 10))
        self.layout.addRow("Connection Timeout:", self.connection_timeout_spinbox)

        # Send timeout
        self.send_timeout_spinbox = QtWidgets.QSpinBox()
        self.send_timeout_spinbox.setMinimum(1)
        self.send_timeout_spinbox.setMaximum(300)
        self.send_timeout_spinbox.setSuffix(" seconds")
        self.send_timeout_spinbox.setValue(self.parent.settings.get('send_timeout', 10))
        self.layout.addRow("Send Timeout:", self.send_timeout_spinbox)

        # Add separator
        separator2 = QtWidgets.QFrame()
        separator2.setFrameShape(QtWidgets.QFrame.HLine)
        separator2.setFrameShadow(QtWidgets.QFrame.Sunken)
        self.layout.addRow(separator2)

        # Logging enabled
        self.logging_checkbox = QtWidgets.QCheckBox("Enable Logging")
        self.logging_checkbox.setChecked(self.parent.settings.get('logging_enabled', True))
        self.layout.addRow(self.logging_checkbox)

        # Log directory info (dynamic)
        log_dir = get_log_directory()
        info_label = QtWidgets.QLabel(f"Logs stored in:\n{log_dir}")
        info_label.setWordWrap(True)
        info_label.setStyleSheet("font-size: 9pt; color: gray;")
        self.layout.addRow(info_label)

        # Open logs folder button
        open_logs_btn = QtWidgets.QPushButton("Open Logs Folder")
        open_logs_btn.clicked.connect(self.open_logs_folder)
        self.layout.addRow(open_logs_btn)

        # Button box
        self.button_box = QtWidgets.QDialogButtonBox(
            QtWidgets.QDialogButtonBox.Ok | QtWidgets.QDialogButtonBox.Cancel,
            QtCore.Qt.Horizontal, self
        )
        self.button_box.accepted.connect(self.save_settings)
        self.button_box.rejected.connect(self.reject)
        self.layout.addRow(self.button_box)

    def select_font(self):
        """Open font selection dialog."""
        current_font = QtGui.QFont(
            self.parent.settings.get('font_family', 'Segoe UI'),
            self.parent.settings.get('font_size', 12)
        )
        font, ok = QtWidgets.QFontDialog.getFont(current_font, self, "Select Font")
        if ok:
            self.font_label.setText(f"{font.family()}, {font.pointSize()}pt")
            self.selected_font = font

    def open_logs_folder(self):
        """Open the logs folder in file explorer."""
        import subprocess
        import sys
        import os

        log_dir = get_log_directory()

        try:
            if sys.platform == 'win32':
                os.startfile(log_dir)
            elif sys.platform == 'darwin':
                subprocess.Popen(['open', log_dir])
            else:
                subprocess.Popen(['xdg-open', log_dir])
        except Exception as e:
            QtWidgets.QMessageBox.warning(
                self,
                "Error",
                f"Could not open logs folder: {e}"
            )

    def save_settings(self):
        """Save settings and apply."""
        from ...utils.logger import enable_logging, disable_logging

        self.parent.settings['theme'] = self.theme_combo.currentText()

        if self.selected_font:
            self.parent.settings['font_family'] = self.selected_font.family()
            self.parent.settings['font_size'] = self.selected_font.pointSize()

        self.parent.settings['overview_message_limit'] = self.limit_spinbox.value()
        self.parent.settings['connection_retry_enabled'] = self.retry_checkbox.isChecked()
        self.parent.settings['connection_retry_attempts'] = self.retry_attempts_spinbox.value()
        self.parent.settings['connection_timeout'] = self.connection_timeout_spinbox.value()
        self.parent.settings['send_timeout'] = self.send_timeout_spinbox.value()
        self.parent.settings['logging_enabled'] = self.logging_checkbox.isChecked()

        self.parent.config_manager.save_settings(self.parent.settings)

        # Apply logging setting
        if self.logging_checkbox.isChecked():
            enable_logging()
        else:
            disable_logging()

        self.accept()
