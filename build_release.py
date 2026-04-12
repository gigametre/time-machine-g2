"""Build a standalone .exe into the Release/ folder."""
import subprocess
import shutil
import os
import sys

ROOT = os.path.dirname(os.path.abspath(__file__))
RELEASE_DIR = os.path.join(ROOT, "Release")
INFO_SRC = os.path.join(ROOT, "info")
INFO_DST = os.path.join(RELEASE_DIR, "info")

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
    "--workpath", os.path.join(ROOT, "build"),
    "--specpath", ROOT,
    "--clean",
    "--noconfirm",
])

# Copy the info/ folder next to the exe so auto-load CSV works
if os.path.isdir(INFO_SRC):
    if os.path.isdir(INFO_DST):
        shutil.rmtree(INFO_DST)
    shutil.copytree(INFO_SRC, INFO_DST)

print(f"\nBuild complete. Executable is at: {os.path.join(RELEASE_DIR, 'TimeMachineControl.exe')}")
