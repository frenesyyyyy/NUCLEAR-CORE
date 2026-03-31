import sys
from PySide6.QtCore import QObject, Signal, QProcess, QProcessEnvironment

class BackendRunner(QObject):
    run_started = Signal()
    node_started = Signal(str)
    log_received = Signal(str, bool)  # True if stderr
    run_finished = Signal(int)
    run_failed = Signal(str)

    def __init__(self, parent=None):
        super().__init__(parent)
        self.process = QProcess(self)
        
        self.process.readyReadStandardOutput.connect(self.handle_stdout)
        self.process.readyReadStandardError.connect(self.handle_stderr)
        self.process.finished.connect(self.handle_finished)
        self.process.errorOccurred.connect(self.handle_error)

    def start_run(self, url: str, locale: str, typo: str):
        if self.process.state() != QProcess.NotRunning:
            return
            
        executable = sys.executable
        
        # We broadcast the intent completely via strict Env Vars to avoid C++ argv parsing bugs
        env = QProcessEnvironment.systemEnvironment()
        env.insert("NO_COLOR", "1")
        env.insert("PYTHONUNBUFFERED", "1")
        env.insert("NUCLEAR_GEO_BACKEND_RUN", "1")
        env.insert("NUCLEAR_GEO_URL", url)
        env.insert("NUCLEAR_GEO_LOCALE", locale)
        env.insert("NUCLEAR_GEO_TYPO", typo)
        
        self.process.setProcessEnvironment(env)
        
        args = []
        if not getattr(sys, 'frozen', False):
            # Development proxy. Target the exact module loader
            args.append(sys.argv[0])
            
        self.run_started.emit()
        self.process.start(executable, args)

    def stop_run(self):
        """
        Graceful stop request. 
        Note: Subprocesses might not gracefully catch terminate if blocked, 
        and killing interrupts writing exports.
        """
        if self.process.state() == QProcess.Running:
            # Attempt to soft-terminate. If it doesn't close elegantly, QProcess kills it.
            self.process.terminate()

    def handle_stdout(self):
        data = self.process.readAllStandardOutput().data().decode('utf-8', errors='replace')
        for line in data.splitlines():
            line = line.strip()
            if not line:
                continue
            self.log_received.emit(line, False)
            self.parse_line_for_progress(line)

    def handle_stderr(self):
        data = self.process.readAllStandardError().data().decode('utf-8', errors='replace')
        for line in data.splitlines():
            line = line.strip()
            if not line:
                continue
            self.log_received.emit(line, True)

    def parse_line_for_progress(self, line: str):
        # Sample match: [14:05:22] Executing Content Fetcher Node...
        if "Executing " in line and " Node..." in line:
            try:
                parts = line.split("Executing ")
                if len(parts) > 1:
                    node_name = parts[1].replace("...", "").strip()
                    # Strip out rich brackets if any leaked through NO_COLOR
                    node_name = node_name.replace("[bold green]", "").replace("[/bold green]", "")
                    self.node_started.emit(node_name)
            except Exception:
                pass
        
        if "CRITICAL NODE FAILURE" in line or "[bold red]" in line:
            # Let the UI know this is important, even if it's on stdout
            self.log_received.emit(f"⚠️ {line}", True)
            
    def handle_finished(self, exit_code, exit_status):
        self.run_finished.emit(exit_code)

    def handle_error(self, error):
        self.run_failed.emit(self.process.errorString())
