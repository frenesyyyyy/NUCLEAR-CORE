from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QLabel, QLineEdit, QComboBox, 
    QPushButton, QFormLayout, QSpacerItem, QSizePolicy
)
from PySide6.QtCore import Signal

class ControlPanel(QWidget):
    # Signals for main window to react to inputs
    run_requested = Signal(str, str, str)  # url, locale, typo
    stop_requested = Signal()

    def __init__(self):
        super().__init__()
        self.setObjectName("LeftPanel")
        self.setup_ui()

    def setup_ui(self):
        layout = QVBoxLayout(self)
        layout.setContentsMargins(15, 15, 15, 15)
        layout.setSpacing(10)

        title = QLabel("Optimizer Setup")
        title.setStyleSheet("font-size: 15px; font-weight: bold; color: #FFF; padding-bottom: 10px;")
        layout.addWidget(title)
        
        form = QFormLayout()
        form.setFieldGrowthPolicy(QFormLayout.AllNonFixedFieldsGrow)
        form.setVerticalSpacing(15)

        # Target Domain / URL
        self.url_input = QLineEdit()
        self.url_input.setPlaceholderText("https://example.com")
        form.addRow("Target URL:", self.url_input)

        # Location Settings
        self.locale_combo = QComboBox()
        self.locale_combo.addItems(["en", "it"])
        form.addRow("Locale:", self.locale_combo)

        # Niche / Schema Typo
        self.typo_combo = QComboBox()
        self.typo_combo.addItems(["tech", "food", "freelancer", "dentist"])
        form.addRow("Business Type:", self.typo_combo)
        
        layout.addLayout(form)

        # Push to bottom
        layout.addItem(QSpacerItem(20, 20, QSizePolicy.Minimum, QSizePolicy.Expanding))
        
        # Action Buttons
        self.run_btn = QPushButton("Run Audit")
        self.run_btn.setObjectName("RunButton")
        self.run_btn.clicked.connect(self.on_run)
        
        self.stop_btn = QPushButton("Stop Execution")
        self.stop_btn.setObjectName("StopButton")
        self.stop_btn.setEnabled(False)
        self.stop_btn.clicked.connect(self.on_stop)
        
        layout.addWidget(self.run_btn)
        layout.addWidget(self.stop_btn)

    def on_run(self):
        url = self.url_input.text().strip()
        if not url:
            self.url_input.setStyleSheet("border: 1px solid red;")
            return
        self.url_input.setStyleSheet("")
        self.run_requested.emit(url, self.locale_combo.currentText(), self.typo_combo.currentText())

    def on_stop(self):
        self.stop_requested.emit()
            
    def set_running_state(self, is_running: bool):
        """Toggle UI elements during execution."""
        self.run_btn.setEnabled(not is_running)
        self.stop_btn.setEnabled(is_running)
        self.url_input.setEnabled(not is_running)
        self.locale_combo.setEnabled(not is_running)
        self.typo_combo.setEnabled(not is_running)
