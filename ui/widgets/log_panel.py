from PySide6.QtWidgets import QWidget, QVBoxLayout, QLabel, QTextEdit, QProgressBar

class LogPanel(QWidget):
    def __init__(self):
        super().__init__()
        self.setObjectName("CenterPanel")
        self.setup_ui()

    def setup_ui(self):
        layout = QVBoxLayout(self)
        layout.setContentsMargins(15, 15, 15, 15)
        layout.setSpacing(10)

        # Status & Progress Visualizer
        self.status_label = QLabel("Status: Idle")
        self.status_label.setStyleSheet("font-size: 14px; font-weight: bold;")
        
        self.progress_bar = QProgressBar()
        self.progress_bar.setRange(0, 100)
        self.progress_bar.setValue(0)
        self.progress_bar.setTextVisible(False)
        self.progress_bar.setFixedHeight(8)
        
        layout.addWidget(self.status_label)
        layout.addWidget(self.progress_bar)

        # Scrolling Console Logs
        log_title = QLabel("Execution Logs")
        log_title.setStyleSheet("margin-top: 10px; color: #AAA;")
        layout.addWidget(log_title)

        self.log_text = QTextEdit()
        self.log_text.setReadOnly(True)
        self.log_text.setPlaceholderText("Execution logs will appear here...\n\nFill out the URL and setup parameters on the left, then click [Run Audit].")
        layout.addWidget(self.log_text)

    def append_log(self, text: str):
        self.log_text.append(text)
        
    def clear_logs(self):
        self.log_text.clear()
        
    def set_status(self, text: str):
        self.status_label.setText(f"Status: {text}")
