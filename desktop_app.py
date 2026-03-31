import sys
import os
import multiprocessing
from PySide6.QtWidgets import QApplication

def handle_backend_mode():
    """Intercepts execution loop gracefully matching via indestructible Environment Variables."""
    if os.environ.get("NUCLEAR_GEO_BACKEND_RUN") == "1":
        # Force the CLI syntax manually so `argparse` doesn't crash inside the backend
        url = os.environ.get("NUCLEAR_GEO_URL", "")
        locale = os.environ.get("NUCLEAR_GEO_LOCALE", "en")
        typo = os.environ.get("NUCLEAR_GEO_TYPO", "tech")
        
        sys.argv = [
            "nuclear_geo_optimizer.py", 
            "--url", url, 
            "--locale", locale, 
            "--typo", typo
        ]
        
        # Safely import the engine logic inline
        import nuclear_geo_optimizer
        
        # Override the pipeline execution to catch PyInstaller-specific anomalies
        try:
            nuclear_geo_optimizer.main()
        except SystemExit as exc:
            # Let normal sys.exit(0) operations pass directly out
            sys.exit(exc.code)
        except Exception as e:
            import traceback
            print(f"[bold red]CRITICAL UI-SPANNER FAILURE:[/bold red] {e}", file=sys.stderr)
            traceback.print_exc()
            sys.exit(1)
            
        # Ensure we always exit and NEVER reach down into main() GUI loops
        sys.exit(0)

def main():
    app = QApplication(sys.argv)
    
    # Initialize the main UI shell
    from ui.main_window import MainWindow
    window = MainWindow()
    window.show()
    
    sys.exit(app.exec())

if __name__ == "__main__":
    # Required for nested PyInstaller threads or subprocesses on Windows
    multiprocessing.freeze_support()
    handle_backend_mode()
    main()
