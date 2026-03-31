import os
from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QLabel, QListWidget, QPushButton, 
    QHBoxLayout, QSpacerItem, QSizePolicy
)
from PySide6.QtGui import QDesktopServices
from PySide6.QtCore import QUrl
from ui.utils import get_exports_dir

class ExportPanel(QWidget):
    def __init__(self):
        super().__init__()
        self.setObjectName("RightPanel")
        
        # Uses standard resolution avoiding CWD traps in production
        self.export_dir = get_exports_dir()
        
        self.setup_ui()
        self.refresh_exports()

    def setup_ui(self):
        layout = QVBoxLayout(self)
        layout.setContentsMargins(15, 15, 15, 15)
        layout.setSpacing(10)

        title = QLabel("Local Exports")
        title.setStyleSheet("font-size: 15px; font-weight: bold; color: #FFF;")
        layout.addWidget(title)
        
        self.exports_list = QListWidget()
        layout.addWidget(self.exports_list)
        
        btn_layout = QHBoxLayout()
        self.refresh_btn = QPushButton("Refresh")
        self.refresh_btn.clicked.connect(self.refresh_exports)
        btn_layout.addWidget(self.refresh_btn)

        self.open_folder_btn = QPushButton("Open Folder")
        self.open_folder_btn.clicked.connect(self.open_folder)
        btn_layout.addWidget(self.open_folder_btn)

        layout.addLayout(btn_layout)
        
        # Double click to open a file
        self.exports_list.itemDoubleClicked.connect(self.open_file)

    def refresh_exports(self):
        self.exports_list.clear()
        if not os.path.exists(self.export_dir):
            self.exports_list.addItem("-- No exports generated yet --")
            return
            
        try:
            files = os.listdir(self.export_dir)
            files = [f for f in files if f.endswith(".md") or f.endswith(".json")]
            files.sort(reverse=True)
            
            if not files:
                self.exports_list.addItem("-- No exports generated yet --")
            else:
                for f in files:
                    self.exports_list.addItem(f)
        except Exception:
            self.exports_list.addItem("-- Error reading local exports --")

    def open_folder(self):
        if not os.path.exists(self.export_dir):
            os.makedirs(self.export_dir, exist_ok=True)
        QDesktopServices.openUrl(QUrl.fromLocalFile(self.export_dir))

    def open_file(self, item):
        filename = item.text()
        if filename.startswith("--"):
            return # Ignore empty state messages
            
        file_path = os.path.join(self.export_dir, filename)
        if os.path.exists(file_path):
            QDesktopServices.openUrl(QUrl.fromLocalFile(file_path))
