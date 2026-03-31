import sys
import os

def get_exports_dir():
    """
    Returns the persistent exports directory.
    When packaged via PyInstaller, it places /exports next to the .exe.
    When running in dev, it places /exports in the project root.
    """
    if getattr(sys, 'frozen', False):
        # We are running inside a built PyInstaller executable
        base_dir = os.path.dirname(sys.executable)
    else:
        # We are in development (ui/utils.py is two levels deep)
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        
    exports_dir = os.path.join(base_dir, "exports")
    os.makedirs(exports_dir, exist_ok=True)
    return exports_dir
