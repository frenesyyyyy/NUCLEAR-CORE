from PySide6.QtWidgets import (
    QMainWindow, QWidget, QHBoxLayout, QVBoxLayout, QLabel
)
from .styles import MAIN_STYLE
from .widgets.control_panel import ControlPanel
from .widgets.log_panel import LogPanel
from .widgets.export_panel import ExportPanel
from .backend_runner import BackendRunner

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Nuclear AI GEO Optimizer")
        self.resize(1100, 700)
        self.setStyleSheet(MAIN_STYLE)
        
        # Hardcoded from nuclear_geo_optimizer.py
        self.total_nodes = 16
        self.nodes_completed = 0
        
        self.setup_ui()
        self.setup_backend()
        
    def setup_ui(self):
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        
        main_layout = QVBoxLayout(central_widget)
        main_layout.setContentsMargins(0, 0, 0, 0)
        main_layout.setSpacing(0)
        
        # Header Area
        header = QWidget()
        header.setObjectName("Header")
        header.setFixedHeight(60)
        header_layout = QHBoxLayout(header)
        title = QLabel("Nuclear AI GEO Optimizer Agency-Grade Shell")
        title.setObjectName("AppTitle")
        header_layout.addWidget(title)
        main_layout.addWidget(header)
        
        # Body Area
        body_widget = QWidget()
        body_layout = QHBoxLayout(body_widget)
        body_layout.setContentsMargins(10, 10, 10, 10)
        body_layout.setSpacing(10)
        
        # Three Main Panels
        self.control_panel = ControlPanel()
        self.control_panel.setFixedWidth(280)
        
        self.log_panel = LogPanel()
        self.log_panel.setMinimumWidth(400)
        
        self.export_panel = ExportPanel()
        self.export_panel.setFixedWidth(280)
        
        body_layout.addWidget(self.control_panel)
        body_layout.addWidget(self.log_panel)
        body_layout.addWidget(self.export_panel)
        
        main_layout.addWidget(body_widget)
        
        # Wire UI Controls -> Main Window Orchestration
        self.control_panel.run_requested.connect(self.start_run)
        self.control_panel.stop_requested.connect(self.stop_run)

    def setup_backend(self):
        """Instantiate background QProcess runner and connect its pipeline events"""
        self.runner = BackendRunner(self)
        self.runner.run_started.connect(self.on_run_started)
        self.runner.node_started.connect(self.on_node_started)
        self.runner.log_received.connect(self.on_log_received)
        self.runner.run_finished.connect(self.on_run_finished)
        self.runner.run_failed.connect(self.on_run_failed)
        
    def start_run(self, url: str, locale: str, typo: str):
        """Disables UI inputs, resets variables, and launches the QProcess"""
        self.control_panel.set_running_state(True)
        self.log_panel.clear_logs()
        self.nodes_completed = 0
        self.log_panel.progress_bar.setValue(0)
        
        self.log_panel.append_log(f"Preparing strictly local pipeline: {url} | {locale.upper()} | {typo.upper()}")
        self.runner.start_run(url, locale, typo)

    def stop_run(self):
        """Asks QProcess to terminate safely."""
        self.log_panel.append_log("\n>> Abort requested by user. Terminating background pipeline...")
        self.log_panel.set_status("Terminating...")
        self.runner.stop_run()

    # --- Backend Listeners ---
    def on_run_started(self):
        self.log_panel.set_status("Running Offline Engine...")
        self.log_panel.append_log("[OK] Background task spawned successfully.")

    def on_node_started(self, node_name: str):
        self.nodes_completed += 1
        progress = int((self.nodes_completed / self.total_nodes) * 100)
        
        self.log_panel.progress_bar.setValue(min(progress, 100))
        self.log_panel.set_status(f"Executing: {node_name}")
        
    def on_log_received(self, text: str, is_error: bool):
        # Even NO_COLOR outputs raw text, so we'll just show it securely on screen
        self.log_panel.append_log(text)
        # Auto-scroll happens naturally in PySide QTextEdit, but could force it if needed.

    def on_run_finished(self, exit_code: int):
        self.control_panel.set_running_state(False)
        
        if exit_code == 0:
            self.log_panel.progress_bar.setValue(100)
            self.log_panel.set_status("Audit Completed Successfully")
            self.log_panel.append_log("\n--- Pipeline Execution Finished (Code 0) ---")
        else:
            self.log_panel.set_status(f"Audit Aborted or Failed (Code {exit_code})")
            self.log_panel.append_log(f"\n--- Pipeline Force-Exited (Code {exit_code}) ---")
            
        # Crucial Requirement: Refresh the export view regardless of how we finished
        self.export_panel.refresh_exports()

    def on_run_failed(self, error_string: str):
        self.control_panel.set_running_state(False)
        self.log_panel.progress_bar.setValue(0)
        self.log_panel.set_status("Fatal Engine Error")
        self.log_panel.append_log(f"\n[CRITICAL INTERNAL ERROR]: {error_string}")
