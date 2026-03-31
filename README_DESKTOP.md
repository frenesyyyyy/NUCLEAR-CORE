# Nuclear AI GEO Optimizer Desktop App

This PySide6 shell transforms the CLI pipeline into a fully self-contained desktop software that stores all exports safely to your local machine (`/exports`), with no cloud requirements.

## Development Setup
To configure and launch the app in standard Python development mode (useful for editing `nodes/` etc.):

1. Open a terminal to this directory.
2. Initialize and activate a local virtual environment:
   ```bash
   python -m venv venv
   venv\Scripts\activate
   ```
3. Install required packages from the codebase:
   ```bash
   pip install -r requirements.txt
   pip install pyside6 pyinstaller
   ```
4. Run the desktop application natively:
   ```bash
   python desktop_app.py
   ```
   > 🎯 *Note: In development, your exported data will land in `./exports` directly next to the python files.*

---

## 🚀 Building the Windows Executable (`.exe`)

This repository is pre-configured to bundle entirely into a single packaged Windows Application folder. This means you can distribute it over USB or zip privately to your analysts without needing Python installed on their machines.

1. Ensure your virtual environment is active and `PyInstaller` is installed.
2. Run the specialized build script:
   ```bash
   python build_exe.py
   ```
3. Wait generally 1-2 minutes for PyInstaller to collect the backend nodes, PySide UI hooks, and environment configurations.

### Where is the Software?
1. Open the newly created `dist/NuclearAI-Optimizer` folder.
2. Double-click **`NuclearAI-Optimizer.exe`**!
3. Upon your first successful run:
   - PyInstaller detects its active execution path.
   - All `Markdown` and `JSON` outputs from your pipelines will safely auto-generate dynamically in a folder called `exports/` seamlessly alongside your real `.exe`.
   - The UI "Open Folder" button securely maps directly to this new local export directory.

### Safety Guarantees
- No pipeline engines were re-written into typical UI Thread loops.
- `desktop_app.py` directly manipulates `sys.argv` routing commands locally inside the same trusted compiled instance.
- Rich-text CLI UI color warnings automatically unbuffer to native plain text to populate your UI log safely via NO_COLOR overrides.
