"""Build a standalone .exe into the Release/ folder."""
import subprocess
import shutil
import os
import sys
import stat


def _remove_readonly(func, path, _exc):
    """Error handler for shutil.rmtree: clear read-only flag and retry."""
    os.chmod(path, stat.S_IWRITE)
    func(path)

ROOT = os.path.dirname(os.path.abspath(__file__))
RELEASE_DIR = os.path.join(ROOT, "Release")
BUILD_DIR = os.path.join(ROOT, "build")
SPEC_FILE = os.path.join(ROOT, "TimeMachineControl.spec")
INFO_SRC = os.path.join(ROOT, "info")
INFO_DST = os.path.join(RELEASE_DIR, "info")

# Pre-clean build artifacts with read-only override (needed when OneDrive is syncing)
for path in [BUILD_DIR, INFO_DST]:
    if os.path.isdir(path):
        shutil.rmtree(path, onerror=_remove_readonly)
if os.path.isfile(SPEC_FILE):
    os.chmod(SPEC_FILE, stat.S_IWRITE)
    os.remove(SPEC_FILE)

# Use the same Python that is running this script
python = sys.executable
subprocess.check_call([
    python, "-m", "PyInstaller",
    os.path.join(ROOT, "gui_pyqt5.py"),
    "--name", "TimeMachineControl",
    "--onefile",
    "--windowed",
    "--exclude-module", "PyQt6",
    "--exclude-module", "PySide6",
    "--exclude-module", "PySide2",
    "--distpath", RELEASE_DIR,
    "--workpath", BUILD_DIR,
    "--specpath", ROOT,
    "--noconfirm",
])

# Copy the info/ folder next to the exe so auto-load CSV works
if os.path.isdir(INFO_SRC):
    shutil.copytree(INFO_SRC, INFO_DST)

print(f"\nBuild complete. Executable is at: {os.path.join(RELEASE_DIR, 'TimeMachineControl.exe')}")
