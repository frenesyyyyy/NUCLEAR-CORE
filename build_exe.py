import os
import subprocess
import sys

def build():
    print("-------------------------------------------")
    print("Preparing Nuclear AI GEO Optimizer for PyInstaller")
    print("-------------------------------------------")
    
    # 1. Install pyinstaller dependency
    print(" -> Ensuring PyInstaller is installed...")
    try:
        import PyInstaller
    except ImportError:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "pyinstaller"])
        
    # 2. Setup standard Build Command
    #   --noconsole: hides python cmd popup
    #   --add-data: bundles required backend logic internally
    print(" -> Packaging Executable...")
    cmd = [
        "pyinstaller",
        "--name", "NuclearAI-Optimizer",
        "--windowed",     # Crucial for professional desktop feel
        "--noconfirm",    # Overwrite prior dist/ without prompt
        "--clean",        
        # Include custom Python backend directories/packages manually
        "--add-data", f"nodes{os.pathsep}nodes",
        "--add-data", f"ui{os.pathsep}ui",
        "desktop_app.py"
    ]
    
    subprocess.check_call(cmd)
    
    print("\n-------------------------------------------")
    print("✅ BUILD COMPLETE!")
    print(f"✅ Executable located in: {os.path.join(os.getcwd(), 'dist', 'NuclearAI-Optimizer')}")
    print("✅ Data exported during runtime will dynamically populate in an /exports folder resting cleanly next to the exe.")
    print("-------------------------------------------")

if __name__ == "__main__":
    build()
