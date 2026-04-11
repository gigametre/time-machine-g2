import sys
import csv
import time
import re
import os
import html
import json
import asyncio
import threading
import glob
from datetime import datetime
from dataclasses import dataclass, asdict
from typing import List, Optional, Tuple

import serial
from serial.tools import list_ports

from qasync import QEventLoop, asyncSlot
from PyQt5.QtCore import Qt, QTimer, pyqtSignal
from PyQt5.QtGui import QFont, QPixmap, QPainter, QPen, QColor, QGuiApplication, QStandardItemModel, QStandardItem

from pathlib import Path
from logging_utils import get_session_logger


# Configure logging using SessionLogger
session_id = datetime.now().strftime("%Y%m%d_%H%M%S")
session_dir = Path("logs") / f"session_{session_id}"
session_dir.mkdir(parents=True, exist_ok=True)
logger = get_session_logger(session_dir)


#TODO: consider using qasync and asyncio for better async handling and cancellation support
#TODO: consider adding a "live parsing" mode that incrementally parses and updates the table as data comes in during live capture, rather than waiting until the end
#TODO: Add a better GUI to graphically show athletes finishing.
#TODO: Add ability to correlate Event # to actual event names by allowing user to load a CSV with event/heat metadata, and then showing that metadata in the table and using it for default CSV export names.
#TODO: Add ability to correlate lane numbers to athlete names by allowing user to load a CSV with event/heat/lane/athlete metadata, and then showing athlete names in the table and using them for CSV export.
from PyQt5.QtWidgets import (
    QAction,
    QActionGroup,
    QApplication,
    QMainWindow,
    QWidget,
    QMessageBox,
    QFileDialog,
    QInputDialog,
    QLabel,
    QPushButton,
    QComboBox,
    QSpinBox,
    QDoubleSpinBox,
    QVBoxLayout,
    QHBoxLayout,
    QGridLayout,
    QGroupBox,
    QTextEdit,
    QTableWidget,
    QTableWidgetItem,
    QSplitter,
    QStatusBar,
    QRadioButton,
    QButtonGroup,
    QHeaderView,
    QToolBar,
    QCheckBox,
    QLineEdit,
    QScrollArea,
    QFrame,
    QLCDNumber,
)


# -----------------------------
# Utility helpers
# -----------------------------
def get_local_date_string() -> str:
    return datetime.now().strftime("%Y-%m-%d")


def format_bytes_mixed_ascii_hex(raw: bytes) -> str:
    out = []

    for b in raw:
        if b == 0x0D:
            out.append("\r")
        elif b == 0x0A:
            out.append("\n")
        elif b == 0x09:
            out.append("\t")
        elif 0x20 <= b <= 0x7E:
            out.append(chr(b))
        else:
            out.append(f"[0x{b:02X}]")

    return "".join(out)


def raw_bytes_to_hex(raw: bytes) -> str:
    return " ".join(f"{b:02X}" for b in raw)


CONTROL_KEEP = {0x09, 0x0A, 0x0D}
CONTROL_DROP = {
    0x00, 0x01, 0x02, 0x03, 0x04,
    0x05, 0x06, 0x07, 0x08,
    0x0B, 0x0C,
    0x0E, 0x0F,
    0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17,
    0x18, 0x19, 0x1A, 0x1B, 0x1C, 0x1D, 0x1E, 0x1F,
    0x7F,
}


def sanitize_device_bytes(raw: bytes) -> str:
    cleaned = bytearray()
    for b in raw:
        if b in CONTROL_KEEP:
            cleaned.append(b)
        elif 32 <= b <= 126:
            cleaned.append(b)
        elif b in CONTROL_DROP:
            continue
        else:
            continue

    text = cleaned.decode("ascii", errors="ignore")
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    return text


def clean_lines_for_parsing(text: str) -> List[str]:
    lines = []
    for line in text.split("\n"):
        s = line.strip()
        if s:
            lines.append(s)
    return lines


# -----------------------------
# Parsing
# -----------------------------
@dataclass
class ParsedRow:
    row_type: str
    event: str
    event_type: str
    heat: str
    date: str
    lane: str
    place: str
    cumulative_time: str
    split_time: str
    raw_line: str
    team_name: str = "N/A"
    bib: str = "N/A"
    first_name: str = "N/A"
    last_name: str = "N/A"
    gender: str = "N/A"
    age_group: str = "N/A"
    timestamp: str = ""


HEADER_EVENT_RE = re.compile(r"^EVENT\s+(\d{3})$", re.IGNORECASE)
HEADER_HEAT_RE = re.compile(r"^HEAT\s*(\d{2})$|^HEAT\s+(\d{2})$", re.IGNORECASE)
HEADER_HEAT_ONLY_RE = re.compile(r"^(\d{2})$")
HEADER_HEAT_T_RE = re.compile(r"^T\s*(\d{2})$", re.IGNORECASE)
HEADER_LT_RE = re.compile(r"^LT\s+(\d{2}:\d{2}:\d{2}\.\d{2})$", re.IGNORECASE)
HEADER_DATE_RE = re.compile(r"^DATE\s+(.+)$", re.IGNORECASE)
TIMER_COUNT_RE = re.compile(r"^\d{6}$")
RESULT_RE = re.compile(
    r"^([0-9A-Z]{1,3})\s+(\d{2})\s+(\d{2}:\d{2}:\d{2}\.\d{2})\s+(\d{2}:\d{2}\.\d{2})$",
    re.IGNORECASE
)

def _decode_timer_count(s: str) -> str:
    """Decode a 6-char timer string (sec_ones, sec_tens, min_ones, min_tens, hr_ones, hr_tens).
    Example: '923000' -> '03:29'
    """
    try:
        seconds = int(s[1]) * 10 + int(s[0])
        minutes = int(s[3]) * 10 + int(s[2])
        hours   = int(s[5]) * 10 + int(s[4])
        if hours > 0:
            return f"{hours:02d}:{minutes:02d}:{seconds:02d}"
        return f"{minutes:02d}:{seconds:02d}"
    except (IndexError, ValueError):
        return ""


def _timer_display_to_hhmmss(display: str) -> str:
    """Convert display format (MM:SS or HH:MM:SS) back to HHMMSS.
    Example: '03:29' -> '000329', '01:02:45' -> '010245'
    """
    try:
        parts = display.split(":")
        if len(parts) == 2:  # MM:SS
            minutes, seconds = int(parts[0]), int(parts[1])
            hours = 0
        elif len(parts) == 3:  # HH:MM:SS
            hours, minutes, seconds = int(parts[0]), int(parts[1]), int(parts[2])
        else:
            return ""
        
        # Validate ranges
        if not (0 <= hours <= 99 and 0 <= minutes <= 59 and 0 <= seconds <= 59):
            return ""
        
        return f"{hours:02d}{minutes:02d}{seconds:02d}"
    except (ValueError, IndexError):
        return ""


def get_event_type(event_code: str) -> str:
    """Return the event code as-is; real name resolved later from event_name_map."""
    return event_code.lstrip("0") or event_code


def parse_time_machine_text(text: str) -> Tuple[List[ParsedRow], dict]:
    lines = clean_lines_for_parsing(text)
    rows: List[ParsedRow] = []

    current_event = ""
    current_heat = ""

    meta = {
        "event": "",
        "heat": "",
        "lt": "",
        "date": "",
    }

    event_dates = {}
    saw_live_time = False
    fallback_date = get_local_date_string()

    for i, line in enumerate(lines):
        next_line = lines[i + 1] if i + 1 < len(lines) else ""

        m = HEADER_LT_RE.match(line)
        if m:
            saw_live_time = True
            meta["lt"] = m.group(1)
            rows.append(
                ParsedRow(
                    "live_time",
                    current_event,
                    get_event_type(current_event),
                    current_heat,
                    "",
                    "",
                    "",
                    m.group(1),
                    "",
                    line,
                )
            )
            continue

        m = HEADER_DATE_RE.match(line)
        if m:
            parsed_date = m.group(1).strip()
            meta["date"] = parsed_date

            if current_event:
                event_dates[current_event] = parsed_date

            rows.append(
                ParsedRow(
                    "date",
                    current_event,
                    get_event_type(current_event),
                    current_heat,
                    parsed_date,
                    "",
                    "",
                    "",
                    "",
                    line,
                )
            )
            continue

        m = HEADER_EVENT_RE.match(line)
        if m:
            current_event = m.group(1)
            meta["event"] = current_event

            rows.append(
                ParsedRow(
                    "event_header",
                    current_event,
                    get_event_type(current_event),
                    current_heat,
                    event_dates.get(current_event, ""),
                    "",
                    "",
                    "",
                    "",
                    line,
                )
            )
            continue

        m = HEADER_HEAT_RE.match(line)
        if m:
            current_heat = m.group(1) or m.group(2)
            meta["heat"] = current_heat

            row_date = event_dates.get(current_event, "")
            if not row_date and saw_live_time:
                row_date = fallback_date

            rows.append(
                ParsedRow(
                    "heat_header",
                    current_event,
                    get_event_type(current_event),
                    current_heat,
                    row_date,
                    "",
                    "",
                    "",
                    "",
                    f"HEAT {current_heat}",
                )
            )
            continue

        m = HEADER_HEAT_T_RE.match(line)
        if m and RESULT_RE.match(next_line):
            current_heat = m.group(1)
            meta["heat"] = current_heat

            row_date = event_dates.get(current_event, "")
            if not row_date and saw_live_time:
                row_date = fallback_date

            rows.append(
                ParsedRow(
                    "heat_header",
                    current_event,
                    get_event_type(current_event),
                    current_heat,
                    row_date,
                    "",
                    "",
                    "",
                    "",
                    f"HEAT {current_heat}",
                )
            )
            continue

        m = HEADER_HEAT_ONLY_RE.match(line)
        if m and saw_live_time and RESULT_RE.match(next_line):
            current_heat = m.group(1)
            meta["heat"] = current_heat

            row_date = event_dates.get(current_event, "")
            if not row_date and saw_live_time:
                row_date = fallback_date

            rows.append(
                ParsedRow(
                    "heat_header",
                    current_event,
                    get_event_type(current_event),
                    current_heat,
                    row_date,
                    "",
                    "",
                    "",
                    "",
                    f"HEAT {current_heat}",
                )
            )
            continue

        if line.upper().startswith("START OF RETRANSMIT"):
            row_date = event_dates.get(current_event, "")
            if not row_date and saw_live_time:
                row_date = fallback_date

            rows.append(
                ParsedRow(
                    "marker",
                    current_event,
                    get_event_type(current_event),
                    current_heat,
                    row_date,
                    "",
                    "",
                    "",
                    "",
                    line,
                )
            )
            continue

        if line.upper().startswith("END OF RETRANSMIT"):
            row_date = event_dates.get(current_event, "")
            if not row_date and saw_live_time:
                row_date = fallback_date

            rows.append(
                ParsedRow(
                    "marker",
                    current_event,
                    get_event_type(current_event),
                    current_heat,
                    row_date,
                    "",
                    "",
                    "",
                    "",
                    line,
                )
            )
            continue

        m = RESULT_RE.match(line)
        if m:
            lane = m.group(1)
            place = m.group(2)
            cumulative = m.group(3)
            split = m.group(4)

            row_date = event_dates.get(current_event, "")
            if not row_date and saw_live_time:
                row_date = fallback_date

            rows.append(
                ParsedRow(
                    "result",
                    current_event,
                    get_event_type(current_event),
                    current_heat,
                    row_date,
                    lane,
                    place,
                    cumulative,
                    split,
                    line,
                )
            )
            continue

        row_date = event_dates.get(current_event, "")
        if not row_date and saw_live_time:
            row_date = fallback_date

        rows.append(
            ParsedRow(
                "raw",
                current_event,
                get_event_type(current_event),
                current_heat,
                row_date,
                "",
                "",
                "",
                "",
                line,
            )
        )

    return rows, meta


# -----------------------------
# Backend client
# -----------------------------

class TimeMachineClient:
    def __init__(
        self,
        port: str,
        baudrate: int = 9600,
        bytesize: int = serial.EIGHTBITS,
        parity: str = serial.PARITY_NONE,
        stopbits: int = serial.STOPBITS_ONE,
        timeout: float = 0.2,
        logger=logger,
        inter_byte_delay: float = 0.01,
    ):
        self.ser = serial.Serial(
            port=port,
            baudrate=baudrate,
            bytesize=bytesize,
            parity=parity,
            stopbits=stopbits,
            timeout=timeout,
            xonxoff=False,
            rtscts=False,
            dsrdtr=False,
        )
        self.inter_byte_delay = inter_byte_delay
        self._lock = threading.Lock()

    def close(self):
        if self.ser and self.ser.is_open:
            self.ser.close()

    def _write_slow(self, data: bytes):
        with self._lock:
            for b in data:
                self.ser.write(bytes([b]))
                self.ser.flush()
                time.sleep(self.inter_byte_delay)

    def read_available(self) -> bytes:
        with self._lock:
            waiting = self.ser.in_waiting
            if waiting:
                return self.ser.read(waiting)
            return b""

    def retransmit(self, event_num: int = 0, heat_num: int = 0, start_time: Optional[str] = None):
        if not (0 <= event_num <= 255):
            raise ValueError("event_num must be 0..255")
        if not (0 <= heat_num <= 99):
            raise ValueError("heat_num must be 0..99")

        base = bytes([0x05]) + f"{event_num:03d}{heat_num:02d}".encode("ascii")

        if start_time is None:
            cmd = base + b"\r\n"
        else:
            if len(start_time) != 6 or not start_time.isdigit():
                raise ValueError("start_time must be 'HHMMSS'")
            cmd = base + bytes([0x15]) + start_time.encode("ascii") + b"\r\n"

        self._write_slow(cmd)

    def set_event_heat(self, event_num: int, heat_num: int):
        """
        0x06 + ASCII 3-digit event + ASCII 2-digit heat + CR + LF
        """
        if not (1 <= event_num <= 999):
            raise ValueError("event_num must be 1..999")
        if not (1 <= heat_num <= 99):
            raise ValueError("heat_num must be 1..99")

        cmd = (
            bytes([0x06]) +
            f"{event_num:03d}{heat_num:02d}".encode("ascii") +
            b"\r\n"
        )
        self._write_slow(cmd)

    def send_timer_command(self, command_id: int, payload: bytes = b""):
        cmd = bytes([command_id]) + payload + b"\r\n"
        self._write_slow(cmd)

    def timer_start(self, start_hhmmss: str = "000000"):
        # 0x82 + ASCII [1s sec, 10s sec, 1s min, 10s min, 1s hr, 10s hr] + CR/LF.
        if len(start_hhmmss) != 6 or not start_hhmmss.isdigit():
            raise ValueError("start_hhmmss must be 6 digits in HHMMSS format")
        encoded = (
            f"{start_hhmmss[5]}{start_hhmmss[4]}"
            f"{start_hhmmss[3]}{start_hhmmss[2]}"
            f"{start_hhmmss[1]}{start_hhmmss[0]}"
        ).encode("ascii")
        self.send_timer_command(0x82, encoded)

    def timer_stop(self):
        # Stop requires zeroed time payload fields: 0x80 + "000000" + CR/LF.
        self.send_timer_command(0x80, b"000000")

    def timer_reset(self):
        # Reset command currently uses 0x80 + "000000" + CR/LF.
        self.send_timer_command(0x80, b"000000")


# -----------------------------
# Collapse button
# -----------------------------
class CheckableComboBox(QComboBox):
    """QComboBox with checkable items for multi-selection."""
    selectionChanged = pyqtSignal()

    def __init__(self, parent=None):
        super().__init__(parent)
        self._model = QStandardItemModel(self)
        self.setModel(self._model)
        self.setEditable(False)
        self.view().pressed.connect(self._on_item_pressed)
        self._update_text()

    def addCheckItem(self, text: str, checked: bool = False):
        item = QStandardItem(text)
        item.setFlags(Qt.ItemIsEnabled | Qt.ItemIsUserCheckable)
        item.setCheckState(Qt.Checked if checked else Qt.Unchecked)
        self._model.appendRow(item)
        self._update_text()

    def _on_item_pressed(self, index):
        item = self._model.itemFromIndex(index)
        if item is None:
            return
        item.setCheckState(Qt.Unchecked if item.checkState() == Qt.Checked else Qt.Checked)
        self._update_text()
        self.selectionChanged.emit()

    def checkedItems(self) -> List[str]:
        checked = []
        for i in range(self._model.rowCount()):
            item = self._model.item(i)
            if item and item.checkState() == Qt.Checked:
                checked.append(item.text())
        return checked

    def setCheckedItems(self, items: List[str]):
        item_set = set(items)
        for i in range(self._model.rowCount()):
            item = self._model.item(i)
            if item:
                item.setCheckState(Qt.Checked if item.text() in item_set else Qt.Unchecked)
        self._update_text()

    def clearItems(self):
        self._model.clear()
        self._update_text()

    def _update_text(self):
        checked = self.checkedItems()
        if checked:
            self.setToolTip(", ".join(checked))
        else:
            self.setToolTip("No opponents selected")
        # Show count summary in the combo display
        if not checked:
            display = "Select opponents..."
        elif len(checked) <= 2:
            display = ", ".join(checked)
        else:
            display = f"{checked[0]}, {checked[1]} +{len(checked) - 2} more"
        # Use the combo's line edit or override paintEvent; simplest: set placeholder-like text
        self.setCurrentIndex(-1)
        # We'll show text via the combo's internal display
        idx = self.findText(display)
        if idx < 0:
            self.setEditText(display) if self.isEditable() else None

    def paintEvent(self, event):
        """Override to show summary text instead of a single item."""
        from PyQt5.QtWidgets import QStylePainter, QStyleOptionComboBox, QStyle
        painter = QStylePainter(self)
        opt = QStyleOptionComboBox()
        self.initStyleOption(opt)
        checked = self.checkedItems()
        if not checked:
            opt.currentText = "Select opponents..."
        elif len(checked) <= 2:
            opt.currentText = ", ".join(checked)
        else:
            opt.currentText = f"{checked[0]}, {checked[1]} +{len(checked) - 2} more"
        painter.drawComplexControl(QStyle.CC_ComboBox, opt)
        painter.drawControl(QStyle.CE_ComboBoxLabel, opt)


class CollapseButton(QPushButton):
    """Small button that draws three stacked v-chevrons for collapse/expand."""

    def __init__(self, expanded=True, parent=None):
        super().__init__(parent)
        self._expanded = expanded
        self.setCheckable(True)
        self.setChecked(expanded)
        self.setFixedSize(24, 24)
        self.setCursor(Qt.PointingHandCursor)
        self.toggled.connect(self._on_toggled)

    def _on_toggled(self, checked):
        self._expanded = checked
        self.update()

    def paintEvent(self, event):
        p = QPainter(self)
        p.setRenderHint(QPainter.Antialiasing)
        p.setPen(QPen(QColor("#1f5665"), 1.5))

        cx = self.width() / 2
        cy = self.height() / 2

        if self._expanded:
            # Three v chevrons pointing down, stacked vertically
            for dy in [-6, 0, 6]:
                y = cy + dy
                p.drawLine(int(cx - 4), int(y - 2), int(cx), int(y + 2))
                p.drawLine(int(cx), int(y + 2), int(cx + 4), int(y - 2))
        else:
            # Three > chevrons in a row (>>>)
            for dx in [-7, 0, 7]:
                x = cx + dx
                p.drawLine(int(x - 2), int(cy - 3), int(x + 2), int(cy))
                p.drawLine(int(x + 2), int(cy), int(x - 2), int(cy + 3))

        p.end()


# -----------------------------
# GUI
# -----------------------------
class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Time Machine Downloader")
        self.resize(1700, 950)
        self.ui_scale = self._compute_ui_scale()
        if self.ui_scale >= 1.8:
            self.text_scale = "xlarge"
        elif self.ui_scale >= 1.3:
            self.text_scale = "large"
        else:
            self.text_scale = "medium"

        self.last_raw_bytes = b""
        self.last_rows: List[ParsedRow] = []
        self.live_raw_buffer = bytearray()

        self.live_text_buffer = ""
        self.live_current_event = ""
        self.live_current_heat = ""
        self.live_current_date = ""
        self.live_saw_live_time = False
        self.live_in_retransmit = False
        self.live_expect_retransmit_event = False
        self.live_retransmit_event = ""
        self.live_retransmit_heat = ""

        self.bib_lookup = {}  # bib -> {'first_name', 'last_name', 'gender', 'age_group', 'team_name'}
        self.event_name_map = {}  # event_number_str -> event_name_str
        self.event_meta_map = {}  # event_number_str -> {'gender': ..., 'age_group': ...}
        self.home_team = "MountOlive"
        self.opponent_teams: List[str] = []
        self._table_bold = False        # toggles each time event or heat changes
        self._table_last_group = None   # (event, heat) of the last rendered row

        self.live_task: Optional[asyncio.Task] = None
        self.live_client: Optional[TimeMachineClient] = None
        self.download_task: Optional[asyncio.Task] = None
        self.timer_running = False
        self._timer_user_stopped = False  # True after explicit stop; suppresses LT auto-detect until next explicit start
        self.lt_seen_counter = 0
        self.last_lt_value = ""
        self._reset_response_count = 0  # tracks received reset-ack lines (0100000/01000000)
        self._reset_ack_streak = 0  # consecutive reset-ack count; heat increments once at 2
        self.live_timer_display = ""  # last decoded timer count from device (e.g. "03:29")
        self._raw_view_dirty = False  # set when live chunk arrives; cleared by throttled redraw

        # Defer session dir creation — may be replaced by a restored session
        self.log_session_dir = None
        self.log_file_path = None
        self.live_capture_log_path = None
        self.session_results_csv_path = None

        self._build_ui()
        self.refresh_ports()
        self._auto_load_events_csv()
        self._auto_load_bib_csv()

        # Check for a recoverable previous session *before* creating a new log dir
        self._pending_restore = self._find_recoverable_session()

        if self._pending_restore:
            # Show restore prompt first; it will create or reuse session dir
            QTimer.singleShot(200, self._prompt_session_restore)
        else:
            self._init_new_session_dir()
            self.status.showMessage(f"Logging to {self.log_session_dir}")
            # Auto-start live capture after event loop is running.
            # 800ms delay gives Bluetooth COM ports time to finish initializing.
            QTimer.singleShot(800, self.start_live_capture)

    # -----------------------------
    # Logging
    # -----------------------------
    def _init_new_session_dir(self):
        """Create a fresh session directory and set all log paths."""
        self.log_session_dir = self.create_log_session_dir()
        self.log_file_path = self.create_session_log_file()
        self.live_capture_log_path = self.create_live_capture_log_file()
        self.session_results_csv_path = os.path.join(self.log_session_dir, "session_results.csv")

    def create_log_session_dir(self) -> str:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        base_dir = os.path.abspath("logs")
        session_dir = os.path.join(base_dir, f"session_{timestamp}")
        os.makedirs(session_dir, exist_ok=True)
        return session_dir

    def create_session_log_file(self) -> str:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        return os.path.join(self.log_session_dir, f"time_machine_raw_log_{timestamp}.txt")

    def create_live_capture_log_file(self) -> str:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        return os.path.join(self.log_session_dir, f"time_machine_live_capture_log_{timestamp}.txt")

    def append_to_raw_log(self, raw: bytes):
        if not raw:
            return
        with open(self.log_file_path, "a", encoding="utf-8") as f:
            f.write(format_bytes_mixed_ascii_hex(raw))

    def append_to_live_capture_log(self, data: bytes):
        if not data:
            return
        with open(self.live_capture_log_path, "a", encoding="utf-8") as f:
            f.write(format_bytes_mixed_ascii_hex(data))

    def append_live_capture_start_log(self):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        port = self.selected_port()
        baud = self.selected_baud()
        with open(self.live_capture_log_path, "a", encoding="utf-8") as f:
            f.write(f"\n===== LIVE CAPTURE STARTED | {timestamp} | port={port} | baud={baud} =====\n")

    def append_live_capture_stop_log(self):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        port = self.selected_port()
        baud = self.selected_baud()
        with open(self.live_capture_log_path, "a", encoding="utf-8") as f:
            f.write(f"\n===== LIVE CAPTURE STOPPED | {timestamp} | port={port} | baud={baud} =====\n")

    # -----------------------------
    # Session save / restore
    # -----------------------------
    def _save_session_state(self):
        """Persist session state to JSON so it can be restored after a crash or close."""
        state = {
            "last_rows": [asdict(r) for r in self.last_rows],
            "live_raw_buffer": self.live_raw_buffer.hex(),
            "live_current_event": self.live_current_event,
            "live_current_heat": self.live_current_heat,
            "live_current_date": self.live_current_date,
            "live_saw_live_time": self.live_saw_live_time,
            "event_spin": self.event_spin.value(),
            "heat_spin": self.heat_spin.value(),
            "session_dir": self.log_session_dir,
        }
        path = os.path.join(self.log_session_dir, "session_state.json")
        try:
            with open(path, "w", encoding="utf-8") as f:
                json.dump(state, f)
        except Exception:
            pass  # Never block close

    @staticmethod
    def _find_recoverable_session() -> Optional[str]:
        """Return the path of the most recent session_state.json, or None."""
        base_dir = os.path.abspath("logs")
        if not os.path.isdir(base_dir):
            return None
        candidates = sorted(glob.glob(os.path.join(base_dir, "session_*", "session_state.json")))
        return candidates[-1] if candidates else None

    def _restore_session(self, state_path: str):
        """Load a previous session from its session_state.json."""
        with open(state_path, "r", encoding="utf-8") as f:
            state = json.load(f)

        # Reuse the original session directory (append to its logs)
        prev_dir = state.get("session_dir", "")
        if prev_dir and os.path.isdir(prev_dir):
            self.log_session_dir = prev_dir
            self.session_results_csv_path = os.path.join(prev_dir, "session_results.csv")
            # Reuse the existing live-capture log (append mode) rather than creating a new one
            existing_logs = glob.glob(os.path.join(prev_dir, "time_machine_live_capture_log_*.txt"))
            if existing_logs:
                self.live_capture_log_path = sorted(existing_logs)[-1]
            else:
                self.live_capture_log_path = self.create_live_capture_log_file()
            existing_raw = glob.glob(os.path.join(prev_dir, "time_machine_raw_log_*.txt"))
            if existing_raw:
                self.log_file_path = sorted(existing_raw)[-1]
            else:
                self.log_file_path = self.create_session_log_file()
        else:
            # Previous dir is gone — fall back to a new session dir
            self._init_new_session_dir()

        # Restore parsed rows
        self.last_rows = [ParsedRow(**d) for d in state.get("last_rows", [])]

        # Restore raw buffer
        hex_buf = state.get("live_raw_buffer", "")
        if hex_buf:
            self.live_raw_buffer = bytearray.fromhex(hex_buf)
            self.last_raw_bytes = bytes(self.live_raw_buffer)

        # Restore live parsing state
        self.live_current_event = state.get("live_current_event", "")
        self.live_current_heat = state.get("live_current_heat", "")
        self.live_current_date = state.get("live_current_date", "")
        self.live_saw_live_time = state.get("live_saw_live_time", False)

        # Restore spin boxes
        self.event_spin.setValue(state.get("event_spin", 1))
        self.heat_spin.setValue(state.get("heat_spin", 1))

        # Repopulate table and raw view
        self.populate_table(self.last_rows)
        self.update_raw_view()

        self.status.showMessage(f"Restored previous session from {os.path.basename(self.log_session_dir)}")

    def _prompt_session_restore(self):
        """Ask the user whether to resume the previous session or start fresh."""
        state_path = self._pending_restore
        self._pending_restore = None

        if not state_path or not os.path.isfile(state_path):
            self._init_new_session_dir()
            self.status.showMessage(f"Logging to {self.log_session_dir}")
            QTimer.singleShot(600, self.start_live_capture)
            return

        # Extract directory name for the dialog text
        session_dir = os.path.dirname(state_path)
        dir_name = os.path.basename(session_dir)
        # Count result rows for context
        try:
            with open(state_path, "r", encoding="utf-8") as f:
                state = json.load(f)
            result_count = sum(1 for r in state.get("last_rows", []) if r.get("row_type") == "result")
        except Exception:
            result_count = 0

        msg = QMessageBox(self)
        msg.setWindowTitle("Restore Previous Session")
        msg.setIcon(QMessageBox.Question)
        msg.setText(
            f"A previous session was found:\n"
            f"  {dir_name}\n"
            f"  ({result_count} result{'s' if result_count != 1 else ''} captured)\n\n"
            f"Would you like to resume that session or start fresh?"
        )
        resume_btn = msg.addButton("Resume Session", QMessageBox.AcceptRole)
        msg.addButton("Start Fresh", QMessageBox.RejectRole)
        msg.exec_()

        if msg.clickedButton() == resume_btn:
            try:
                self._restore_session(state_path)
            except Exception as e:
                QMessageBox.warning(self, "Restore Failed", f"Could not restore session:\n{e}")
                # Fall back to a new session dir on failure
                if not self.log_session_dir:
                    self._init_new_session_dir()
        else:
            # User chose Start Fresh — create a new session dir
            self._init_new_session_dir()

        self.status.showMessage(f"Logging to {self.log_session_dir}")
        QTimer.singleShot(600, self.start_live_capture)

    # -----------------------------
    # UI
    # -----------------------------
    def _compute_ui_scale(self) -> float:
        screen = QGuiApplication.primaryScreen()
        if screen is None:
            return 1.0

        geo = screen.availableGeometry()
        dpr = screen.devicePixelRatio()
        # Physical pixels give the real resolution.
        phys_w = geo.width() * dpr
        phys_h = geo.height() * dpr
        # Scale around a 1920x1080 baseline using physical pixels.
        scale_w = phys_w / 1920.0
        scale_h = phys_h / 1080.0
        return max(0.85, min(3.0, min(scale_w, scale_h)))

    def _build_ui(self):
        self._build_toolbar()
        self._build_menu()

        central = QWidget()
        central.setObjectName("centralRoot")
        self.setCentralWidget(central)
        outer = QVBoxLayout(central)
        outer.setContentsMargins(8, 8, 8, 8)
        outer.setSpacing(6)

        split = QSplitter(Qt.Horizontal)
        split.setHandleWidth(6)
        outer.addWidget(split)
        self.main_splitter = split

        # ── Left panel ──────────────────────────────────────────
        left = QWidget()
        left_layout = QVBoxLayout(left)
        left_layout.setContentsMargins(0, 0, 0, 0)
        left_layout.setSpacing(6)

        # --- Connection & Live Capture (combined) ---
        self.conn_group = QGroupBox()
        conn_layout = QVBoxLayout(self.conn_group)
        conn_layout.setContentsMargins(8, 8, 8, 8)
        conn_layout.setSpacing(6)

        self.port_combo = QComboBox()
        self.baud_combo = QComboBox()
        self.baud_combo.addItems(["9600", "19200", "38400", "57600", "115200"])
        self.baud_combo.setCurrentText("9600")

        self._led_on = False
        self._led_timer = QTimer(self)
        self._led_timer.setInterval(500)
        self._led_timer.timeout.connect(self._blink_led)

        # Activity indicator (blinks green on data received)
        self._activity_indicator_on = False
        self._activity_timer = QTimer(self)
        self._activity_timer.setInterval(100)
        self._activity_timer.timeout.connect(self._blink_activity_indicator)

        self.live_start_btn = QPushButton("Connect")
        self.live_start_btn.setProperty("kind", "accent")
        self.live_start_btn.setProperty("compact", True)

        conn_grid = QGridLayout()
        conn_grid.setHorizontalSpacing(8)
        conn_grid.setVerticalSpacing(6)
        conn_grid.setColumnStretch(1, 1)
        lbl_port = QLabel("Port")
        lbl_port.setObjectName("fieldLabel")
        lbl_baud = QLabel("Baud")
        lbl_baud.setObjectName("fieldLabel")
        conn_grid.addWidget(lbl_port, 0, 0)
        conn_grid.addWidget(self.port_combo, 0, 1, 1, 2)
        conn_grid.addWidget(lbl_baud, 1, 0)
        conn_grid.addWidget(self.baud_combo, 1, 1, 1, 2)
        conn_layout.addLayout(conn_grid)

        conn_layout.addWidget(self.live_start_btn)

        # --- Event / Heat / Download ---
        self.dl_group = QGroupBox()

        self.event_spin = QSpinBox()
        self.event_spin.setRange(1, 999)
        self.event_spin.setValue(1)

        self.heat_spin = QSpinBox()
        self.heat_spin.setRange(1, 99)
        self.heat_spin.setValue(1)

        self.read_seconds_spin = QDoubleSpinBox()
        self.read_seconds_spin.setRange(0.5, 60.0)
        self.read_seconds_spin.setSingleStep(0.5)
        self.read_seconds_spin.setDecimals(1)
        self.read_seconds_spin.setValue(60.0)

        self.set_event_heat_btn = QPushButton("Set E/H")
        self.set_event_heat_btn.setProperty("kind", "secondary")

        self.download_btn = QPushButton("Download E/H")
        self.download_btn.setProperty("kind", "primary")

        def _field_label(text):
            lbl = QLabel(text)
            lbl.setObjectName("fieldLabel")
            return lbl

        dl_layout = QVBoxLayout(self.dl_group)
        dl_layout.setContentsMargins(8, 8, 8, 8)
        dl_layout.setSpacing(6)

        # Row 1: Event / Heat fields in a grid (timeout moved to Edit menu)
        fields_grid = QGridLayout()
        fields_grid.setHorizontalSpacing(8)
        fields_grid.setVerticalSpacing(4)
        fields_grid.setColumnStretch(1, 1)
        fields_grid.setColumnStretch(3, 1)
        fields_grid.addWidget(_field_label("Event"), 0, 0)
        fields_grid.addWidget(self.event_spin, 0, 1)
        fields_grid.addWidget(_field_label("Heat"), 0, 2)
        fields_grid.addWidget(self.heat_spin, 0, 3)
        dl_layout.addLayout(fields_grid)

        # Row 2: Action buttons
        dl_btn_row = QHBoxLayout()
        dl_btn_row.setSpacing(8)
        dl_btn_row.addWidget(self.set_event_heat_btn, 1)
        dl_btn_row.addWidget(self.download_btn, 1)
        dl_layout.addLayout(dl_btn_row)

        # --- Timer Control ---
        # Timer controls (buttons) moved to banner; timer log moved to comm_log section
        self.timer_toggle_btn = QPushButton("Start Timer")
        self.timer_toggle_btn.setProperty("kind", "accent")
        self.timer_reset_btn = QPushButton("Reset")
        self.timer_reset_btn.setProperty("kind", "secondary")
        # Hidden input for timer start time (always defaults to 000000)
        self.timer_start_time_input = QLineEdit("000000")
        self.timer_start_time_input.setVisible(False)
        self.timer_log_text = QTextEdit()
        self.timer_log_text.setObjectName("timerLogText")
        self.timer_log_text.setReadOnly(True)
        self.timer_log_text.setLineWrapMode(QTextEdit.WidgetWidth)

        # --- Bib Lookup ---
        self.bib_group = QGroupBox()
        bib_layout = QVBoxLayout(self.bib_group)
        bib_layout.setContentsMargins(8, 8, 8, 8)
        bib_layout.setSpacing(6)

        # CSV load buttons — uniform width, stacked with status labels
        self.load_bib_csv_btn = QPushButton("Load Bib CSV")
        self.load_bib_csv_btn.setProperty("kind", "secondary")
        self.bib_csv_label = QLabel("No bib CSV loaded")
        self.bib_csv_label.setObjectName("bibCsvLabel")
        self.bib_csv_label.setWordWrap(True)
        self.bib_csv_label.setMinimumWidth(0)

        self.upload_events_btn = QPushButton("Load Events CSV")
        self.upload_events_btn.setProperty("kind", "secondary")
        self.upload_events_btn.setToolTip(
            "Load a CSV mapping event numbers to event names.\n"
            "Required columns: event number, event name\n"
            "Example:\n  event number,event name\n  1,800m\n  7,4x100 relay\n  13,100m"
        )
        self.events_csv_help_btn = QPushButton("?")
        self.events_csv_help_btn.setFixedWidth(24)
        self.events_csv_help_btn.setProperty("kind", "secondary")
        self.events_csv_help_btn.setToolTip("Show expected CSV format")
        self.events_csv_label = QLabel("No events CSV loaded")
        self.events_csv_label.setObjectName("eventsCsvLabel")
        self.events_csv_label.setWordWrap(True)
        self.events_csv_label.setMinimumWidth(0)

        csv_grid = QGridLayout()
        csv_grid.setHorizontalSpacing(8)
        csv_grid.setVerticalSpacing(6)
        csv_grid.setColumnStretch(0, 1)
        csv_grid.setColumnStretch(1, 0)
        csv_grid.addWidget(self.load_bib_csv_btn, 0, 0, 1, 2)
        csv_grid.addWidget(self.bib_csv_label, 1, 0, 1, 2)

        events_btn_row = QHBoxLayout()
        events_btn_row.setSpacing(4)
        events_btn_row.addWidget(self.upload_events_btn, 1)
        events_btn_row.addWidget(self.events_csv_help_btn, 0)
        csv_grid.addLayout(events_btn_row, 2, 0, 1, 2)
        csv_grid.addWidget(self.events_csv_label, 3, 0, 1, 2)
        bib_layout.addLayout(csv_grid)

        # Separator line
        csv_sep = QFrame()
        csv_sep.setFrameShape(QFrame.HLine)
        csv_sep.setFrameShadow(QFrame.Sunken)
        csv_sep.setStyleSheet("color: #c0d6dd;")
        bib_layout.addWidget(csv_sep)

        # Home / Opponents team selectors
        self.teams_group = QGroupBox()
        teams_layout = QVBoxLayout(self.teams_group)
        teams_layout.setContentsMargins(8, 8, 8, 8)
        teams_layout.setSpacing(6)

        team_grid = QGridLayout()
        team_grid.setHorizontalSpacing(8)
        team_grid.setVerticalSpacing(6)
        team_grid.setColumnStretch(1, 1)

        home_lbl = QLabel("Home:")
        home_lbl.setObjectName("fieldLabel")
        self.home_team_combo = QComboBox()
        self.home_team_combo.setEditable(True)
        self.home_team_combo.addItem("MountOlive")
        self.home_team_combo.setCurrentText("MountOlive")
        team_grid.addWidget(home_lbl, 0, 0)
        team_grid.addWidget(self.home_team_combo, 0, 1)

        opp_lbl = QLabel("Opponents:")
        opp_lbl.setObjectName("fieldLabel")
        self.opponent_combo = CheckableComboBox()
        team_grid.addWidget(opp_lbl, 1, 0)
        team_grid.addWidget(self.opponent_combo, 1, 1)

        teams_layout.addLayout(team_grid)

        self.bib_allowed_label = QLabel("")
        self.bib_allowed_label.setObjectName("bibAgeLabel")
        # Keep these widgets alive for compatibility with existing logic
        self.bib_lane_spin = QSpinBox()
        self.bib_lane_spin.setRange(1, 99)
        self.bib_lane_spin.setVisible(False)
        self.bib_bib_combo = QComboBox()
        self.bib_bib_combo.setEnabled(False)
        self.bib_bib_combo.addItem("Load bib CSV first", "")
        self.bib_bib_combo.setVisible(False)
        self.bib_assign_btn = QPushButton("Assign")
        self.bib_assign_btn.setProperty("kind", "primary")
        self.bib_assign_btn.setVisible(False)

        compact_mode = self.ui_scale <= 0.95

        left_layout.addWidget(self._make_collapsible_section("Connection & Live", self.conn_group, False))
        left_layout.addWidget(self._make_collapsible_section("Event / Heat", self.dl_group))
        left_layout.addWidget(self._make_collapsible_section("Teams", self.teams_group, True))
        left_layout.addWidget(self._make_collapsible_section("Config", self.bib_group, False))
        left_layout.addStretch(1)

        # Wrap left panel in a scroll area so it works on small screens
        left_scroll = QScrollArea()
        left_scroll.setWidgetResizable(True)
        left_scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        left_scroll.setFrameShape(left_scroll.NoFrame)
        left_scroll.setMinimumWidth(int(260 * self.ui_scale))
        left_scroll.setWidget(left)

        # ── Right panel ─────────────────────────────────────────
        right = QWidget()
        right_layout = QVBoxLayout(right)
        right_layout.setContentsMargins(0, 0, 0, 0)
        right_layout.setSpacing(6)

        right_split = QSplitter(Qt.Vertical)
        right_split.setHandleWidth(6)

        table_group = QGroupBox()
        table_layout = QVBoxLayout(table_group)

        parsed_results_lbl = QLabel("Parsed Results")
        parsed_results_lbl.setObjectName("sectionTitle")
        parsed_results_lbl.setAlignment(Qt.AlignLeft)
        table_layout.addWidget(parsed_results_lbl)

        self.live_banner_indicator = QLabel("Not Connected")
        self.live_banner_indicator.setObjectName("liveBannerIndicator")
        self.live_banner_indicator.setFixedWidth(int(100 * self.ui_scale))
        self.live_banner_indicator.setAlignment(Qt.AlignCenter)
        self.live_banner_indicator.setStyleSheet(
            "color: #7a7a7a; font-weight: bold; font-size: 9pt;"
        )

        self.activity_indicator = QLabel("● Data")
        self.activity_indicator.setObjectName("activityIndicator")
        self.activity_indicator.setMinimumWidth(int(68 * self.ui_scale))
        self.activity_indicator.setAlignment(Qt.AlignCenter)
        self.activity_indicator.setStyleSheet(
            "color: #4a7a4a; font-weight: bold; font-size: 12pt;"
        )

        self.timer_display_banner = QLCDNumber()
        self.timer_display_banner.setObjectName("timerDisplayBanner")
        self.timer_display_banner.setDigitCount(8)  # HH:MM:SS
        self.timer_display_banner.setSegmentStyle(QLCDNumber.Filled)
        self.timer_display_banner.setMinimumWidth(int(150 * self.ui_scale))
        self.timer_display_banner.setMinimumHeight(int(32 * self.ui_scale))
        self.timer_display_banner.display("00:00")

        self.event_heat_banner = QLabel("Event: —   Heat: —")
        self.event_heat_banner.setObjectName("eventHeatBanner")
        self.event_heat_banner.setAlignment(Qt.AlignCenter)
        self.event_heat_banner.setWordWrap(False)
        self.event_heat_banner.setStyleSheet(
            "font-weight: bold; font-size: 13pt; padding: 4px 8px;"
            "background: transparent; border-radius: 4px;"
        )

        self.date_banner = QLabel(f"Date: {get_local_date_string()}")
        self.date_banner.setObjectName("dateBanner")
        self.date_banner.setAlignment(Qt.AlignCenter)
        self.date_banner.setWordWrap(False)
        self.date_banner.setStyleSheet(
            "font-weight: bold; font-size: 13pt; padding: 4px 8px;"
            "background: transparent; border-radius: 4px;"
        )

        self.banner_widget = QWidget()
        self.banner_widget.setObjectName("bannerWidget")
        self.banner_widget.setStyleSheet(
            "QWidget#bannerWidget { background: palette(midlight); border-radius: 4px; }"
        )

        # Segment 1: status lights
        self.status_cluster = QWidget()
        self.status_cluster.setObjectName("statusCluster")
        status_layout = QHBoxLayout(self.status_cluster)
        status_layout.setContentsMargins(6, 2, 6, 2)
        status_layout.setSpacing(6)
        status_layout.addWidget(self.activity_indicator)
        status_layout.addWidget(self.live_banner_indicator)

        # Segment 2: timer and controls
        self.timer_cluster = QWidget()
        self.timer_cluster.setObjectName("timerCluster")
        timer_layout = QHBoxLayout(self.timer_cluster)
        timer_layout.setContentsMargins(6, 2, 6, 2)
        timer_layout.setSpacing(6)
        timer_layout.addWidget(self.timer_display_banner, 0)
        timer_layout.addWidget(self.timer_toggle_btn, 0)
        timer_layout.addWidget(self.timer_reset_btn, 0)
        timer_layout.addWidget(self.date_banner)

        # Segment 3: event/heat
        self.event_cluster = QWidget()
        self.event_cluster.setObjectName("eventCluster")
        event_layout = QHBoxLayout(self.event_cluster)
        event_layout.setContentsMargins(8, 2, 8, 2)
        event_layout.setSpacing(4)
        event_layout.addWidget(self.event_heat_banner)

        self.banner_divider_1 = QFrame()
        self.banner_divider_1.setObjectName("bannerDivider")
        self.banner_divider_1.setFrameShape(QFrame.VLine)
        self.banner_divider_1.setFrameShadow(QFrame.Plain)

        self.banner_divider_2 = QFrame()
        self.banner_divider_2.setObjectName("bannerDivider")
        self.banner_divider_2.setFrameShape(QFrame.VLine)
        self.banner_divider_2.setFrameShadow(QFrame.Plain)

        self.banner_inner = QGridLayout(self.banner_widget)
        self.banner_inner.setContentsMargins(4, 2, 4, 2)
        self.banner_inner.setHorizontalSpacing(8)
        self.banner_inner.setVerticalSpacing(4)
        self._banner_compact_mode = None
        self._update_banner_layout_from_width()

        table_layout.addWidget(self.banner_widget)

        self.table = QTableWidget(0, 15)
        self.table.setHorizontalHeaderLabels(
            ["Date", "Time", "Event", "Event \nType", "Heat", "Lane", "Bib", "Team", "First \nName", "Last \nName", "Finish\nTime", "Gender", "Age \nGroup", "Place/\nLap", "Split \nTime"]
        )
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.Interactive)
        self.table.horizontalHeader().setStretchLastSection(False)
        self.table.horizontalHeader().setDefaultAlignment(Qt.AlignCenter)
        self.table.horizontalHeader().setMinimumSectionSize(50)
        # Bib/Team get initial widths from _apply_professional_theme(); all columns remain user-adjustable.
        self.table.setSortingEnabled(True)
        self.table.setEditTriggers(QTableWidget.DoubleClicked | QTableWidget.EditKeyPressed | QTableWidget.AnyKeyPressed)
        self.table.setAlternatingRowColors(True)
        self.table.verticalHeader().setVisible(False)
        # Compact row height for Excel-like appearance
        compact_row_h = max(18, int(20 * self.ui_scale))
        self.table.verticalHeader().setDefaultSectionSize(compact_row_h)
        self.table.verticalHeader().setMinimumSectionSize(max(16, compact_row_h - 2))
        # Remove spacing between rows
        self.table.setShowGrid(True)
        self.table.setGridStyle(Qt.SolidLine)
        self.table.cellChanged.connect(self.on_table_cell_changed)
        table_layout.addWidget(self.table)

        # Raw output with inline display-mode toggle
        raw_out_group = QGroupBox("Raw Output")
        raw_out_layout = QVBoxLayout(raw_out_group)
        raw_out_layout.setSpacing(4)

        raw_mode_row = QHBoxLayout()
        raw_mode_row.setContentsMargins(0, 0, 0, 0)
        raw_mode_row.setSpacing(10)

        self.cleaned_ascii_radio = QRadioButton("ASCII")
        self.cleaned_ascii_radio.setChecked(True)
        self.hex_radio = QRadioButton("Hex")
        self.wrap_check = QCheckBox("Wrap")
        self.wrap_check.setChecked(False)

        self.raw_mode_group = QButtonGroup(self)
        self.raw_mode_group.addButton(self.cleaned_ascii_radio)
        self.raw_mode_group.addButton(self.hex_radio)

        raw_mode_row.addWidget(self.cleaned_ascii_radio)
        raw_mode_row.addWidget(self.hex_radio)
        raw_mode_row.addWidget(self.wrap_check)
        raw_mode_row.addStretch()
        raw_out_layout.addLayout(raw_mode_row)

        self.raw_text = QTextEdit()
        self.raw_text.setReadOnly(True)
        self.raw_text.setLineWrapMode(QTextEdit.NoWrap)
        raw_out_layout.addWidget(self.raw_text)

        # Comm log pane — sits beside Raw Output
        comm_log_group = QGroupBox()
        comm_log_layout = QVBoxLayout(comm_log_group)
        comm_log_layout.setContentsMargins(6, 6, 6, 6)
        comm_log_layout.setSpacing(4)
        
        comm_log_lbl = QLabel("Comm Log")
        comm_log_lbl.setObjectName("sectionTitle")
        comm_log_lbl.setAlignment(Qt.AlignLeft)
        comm_log_layout.addWidget(comm_log_lbl)
        comm_log_layout.addWidget(self.timer_log_text)

        # Bottom pane: Raw Output | Comm Log side by side
        bottom_split = QSplitter(Qt.Horizontal)
        bottom_split.setHandleWidth(6)
        bottom_split.addWidget(raw_out_group)
        bottom_split.addWidget(comm_log_group)
        bottom_split.setSizes([700, 300])

        right_split.addWidget(table_group)
        right_split.addWidget(bottom_split)
        right_split.setSizes([650, 220])

        right_layout.addWidget(right_split)

        split.addWidget(left_scroll)
        split.addWidget(right)
        split.setSizes([int(260 * self.ui_scale), 1520])
        split.setStretchFactor(0, 0)
        split.setStretchFactor(1, 1)

        self.status = QStatusBar()
        self.setStatusBar(self.status)

        self.cleaned_ascii_radio.toggled.connect(self.update_raw_view)
        self.hex_radio.toggled.connect(self.update_raw_view)
        self.wrap_check.toggled.connect(self.update_wrap_mode)

        self.load_bib_csv_btn.clicked.connect(self.load_bib_csv)
        self.upload_events_btn.clicked.connect(self.load_events_csv)
        self.events_csv_help_btn.clicked.connect(self.show_events_csv_format_help)
        self.bib_assign_btn.clicked.connect(self.assign_bib_to_lane)
        self.event_spin.valueChanged.connect(self.on_event_selection_changed)
        self.home_team_combo.currentTextChanged.connect(self._on_home_team_changed)
        self.opponent_combo.selectionChanged.connect(self._on_opponents_changed)

        self.set_event_heat_btn.clicked.connect(self.set_event_heat_selected)
        self.download_btn.clicked.connect(self.download_selected)
        self.live_start_btn.clicked.connect(self._on_connect_toggle)
        self.timer_toggle_btn.clicked.connect(self.on_timer_toggle_clicked)
        self.timer_reset_btn.clicked.connect(self.on_timer_reset_clicked)

        # Reflow banner when splitter sizes change.
        split.splitterMoved.connect(lambda _pos, _idx: self._update_banner_layout_from_width())

        self._apply_professional_theme()
        self._update_banner_layout_from_width()

    def on_live_start_clicked(self):
        self.start_live_capture()

    def on_live_stop_clicked(self):
        self.stop_live_capture()

    def on_timer_toggle_clicked(self):
        self.toggle_timer()

    def on_timer_reset_clicked(self):
        self.reset_timer()

    def _on_connect_toggle(self):
        """Toggle between starting and stopping live capture."""
        if self._has_live_connection() or (self.live_task and not self.live_task.done()) or self.live_start_btn.text() == "Disconnect":
            self.stop_live_capture()
        else:
            self.start_live_capture()

    def _set_led(self, on: bool):
        if on:
            self.live_banner_indicator.setText("● LIVE")
            self.live_banner_indicator.setStyleSheet(
                "color: #e03030; font-weight: bold; font-size: 11pt;"
            )
        else:
            self.live_banner_indicator.setText("● LIVE")
            self.live_banner_indicator.setStyleSheet(
                "color: #4a2020; font-weight: bold; font-size: 11pt;"
            )

    def _hide_led(self):
        self.live_banner_indicator.setText("Not Connected")
        self.live_banner_indicator.setStyleSheet(
            "color: #7a7a7a; font-weight: bold; font-size: 9pt;"
        )

    def _blink_led(self):
        self._led_on = not self._led_on
        self._set_led(self._led_on)

    def _flash_activity_indicator(self):
        """Flash the activity indicator green briefly."""
        self._activity_indicator_on = True
        self.activity_indicator.setStyleSheet(
            "color: #2ecc71; font-weight: bold; font-size: 12pt;"
        )
        self._activity_timer.stop()
        self._activity_timer.start()

    def _blink_activity_indicator(self):
        """Turn off the activity indicator after a brief flash."""
        self._activity_indicator_on = False
        self.activity_indicator.setStyleSheet(
            "color: #4a7a4a; font-weight: bold; font-size: 12pt;"
        )
        self._activity_timer.stop()

    def _update_timer_button_visual(self):
        if self.timer_running:
            self.timer_toggle_btn.setText("Stop Timer")
            self.timer_toggle_btn.setProperty("kind", "danger")
        else:
            self.timer_toggle_btn.setText("Start Timer")
            self.timer_toggle_btn.setProperty("kind", "accent")

        self.timer_toggle_btn.style().unpolish(self.timer_toggle_btn)
        self.timer_toggle_btn.style().polish(self.timer_toggle_btn)

    def log_timer_command(self, message: str):
        ts = datetime.now().strftime("%H:%M:%S")
        self.timer_log_text.append(f"[{ts}] {message}")
        bar = self.timer_log_text.verticalScrollBar()
        bar.setValue(bar.maximum())

    def _build_toolbar(self):
        # Toolbar is kept for spacing/style but actions moved to File menu
        pass

    def _make_collapsible_section(self, title: str, group: QGroupBox, expanded: bool = True) -> QWidget:
        """Wrap a QGroupBox in a collapsible section with a CollapseButton header."""
        group.setTitle("")

        section = QWidget()
        section.setObjectName("collapsibleSection")
        v_layout = QVBoxLayout(section)
        v_layout.setContentsMargins(0, 0, 0, 0)
        v_layout.setSpacing(2)

        header = QWidget()
        header.setObjectName("sectionHeader")
        h_layout = QHBoxLayout(header)
        h_layout.setContentsMargins(12, 4, 12, 4)
        h_layout.setSpacing(8)

        btn = CollapseButton(expanded)
        btn.setObjectName("collapseBtn")
        lbl = QLabel(title)
        lbl.setObjectName("sectionTitle")

        h_layout.addWidget(btn)
        h_layout.addWidget(lbl)
        h_layout.addStretch()

        group.setVisible(expanded)
        btn.toggled.connect(group.setVisible)
        header.setCursor(Qt.PointingHandCursor)
        header.mousePressEvent = lambda e: btn.toggle()

        v_layout.addWidget(header)
        v_layout.addWidget(group)

        return section

    def _update_banner_layout_from_width(self):
        """Dynamically switch banner between wide and compact layouts based on available width."""
        if not hasattr(self, "banner_inner") or not hasattr(self, "banner_widget"):
            return

        available_width = self.banner_widget.width() or self.width()
        compact_mode = available_width < int(980 * self.ui_scale)

        if self._banner_compact_mode == compact_mode:
            return
        self._banner_compact_mode = compact_mode

        # Remove all current items from the grid before re-adding in new positions.
        while self.banner_inner.count():
            self.banner_inner.takeAt(0)

        # Reset stretches to avoid stale layout behavior.
        for col in range(6):
            self.banner_inner.setColumnStretch(col, 0)
        for row in range(2):
            self.banner_inner.setRowStretch(row, 0)

        if compact_mode:
            self.timer_display_banner.setMinimumWidth(int(104 * self.ui_scale))
            self.banner_divider_2.setVisible(False)

            # Single compact row to keep Event/Heat aligned with status/timer.
            self.banner_inner.addWidget(self.status_cluster, 0, 0)
            self.banner_inner.addWidget(self.banner_divider_1, 0, 1)
            self.banner_inner.addWidget(self.timer_cluster, 0, 2)
            self.banner_inner.setColumnStretch(3, 1)
            self.banner_inner.addWidget(self.event_cluster, 0, 4, Qt.AlignRight)
        else:
            self.timer_display_banner.setMinimumWidth(int(150 * self.ui_scale))
            self.banner_divider_2.setVisible(True)

            self.banner_inner.addWidget(self.status_cluster, 0, 0)
            self.banner_inner.addWidget(self.banner_divider_1, 0, 1)
            self.banner_inner.addWidget(self.timer_cluster, 0, 2)
            self.banner_inner.setColumnStretch(3, 1)
            self.banner_inner.addWidget(self.banner_divider_2, 0, 4)
            self.banner_inner.addWidget(self.event_cluster, 0, 5)

    def resizeEvent(self, event):
        super().resizeEvent(event)
        self._update_banner_layout_from_width()

    def _build_menu(self):
        menu = self.menuBar()

        file_menu = menu.addMenu("File")
        save_csv_action = QAction("Save Table CSV", self)
        save_csv_action.setShortcut("Ctrl+S")
        save_csv_action.triggered.connect(self.save_table_to_csv)
        file_menu.addAction(save_csv_action)

        save_raw_action = QAction("Save Raw Output", self)
        save_raw_action.triggered.connect(self.save_raw_output)
        file_menu.addAction(save_raw_action)

        edit_menu = menu.addMenu("Edit")
        set_timeout_action = QAction("Set Download Timeout...", self)
        set_timeout_action.triggered.connect(self.set_download_timeout)
        edit_menu.addAction(set_timeout_action)

        view_menu = menu.addMenu("View")
        text_size_menu = view_menu.addMenu("Text Size")

        self.text_size_group = QActionGroup(self)
        self.text_size_group.setExclusive(True)
        self.text_size_actions = {}

        options = [
            ("Small", "small"),
            ("Medium", "medium"),
            ("Large", "large"),
            ("Extra Large", "xlarge"),
        ]

        for label, value in options:
            action = QAction(label, self)
            action.setCheckable(True)
            action.triggered.connect(lambda _checked=False, v=value: self.set_text_scale(v))
            self.text_size_group.addAction(action)
            text_size_menu.addAction(action)
            self.text_size_actions[value] = action

        self.text_size_actions[self.text_scale].setChecked(True)

        view_menu.addSeparator()
        clear_action = QAction("Clear Results", self)
        clear_action.triggered.connect(self.clear_results)
        view_menu.addAction(clear_action)

    def set_download_timeout(self):
        current = self.read_seconds_spin.value()
        value, ok = QInputDialog.getDouble(
            self,
            "Set Download Timeout",
            "Timeout (seconds):",
            current,
            0.5,
            60.0,
            1,
        )
        if not ok:
            return

        self.read_seconds_spin.setValue(value)
        self.status.showMessage(f"Download timeout set to {value:.1f}s")

    def set_text_scale(self, scale: str):
        if scale not in {"small", "medium", "large", "xlarge"}:
            return

        self.text_scale = scale
        self._apply_professional_theme()

        if hasattr(self, "text_size_actions"):
            self.text_size_actions[scale].setChecked(True)

        if hasattr(self, "status"):
            self.status.showMessage(f"Text size set to {scale.title()}")

    def _timer_action_blocked(self) -> bool:
        if self.download_task and not self.download_task.done():
            QMessageBox.warning(self, "Download Active", "Wait for the current download to finish before sending timer control commands.")
            self.log_timer_command("Blocked: download in progress")
            return True

        return False

    async def _wait_for_lt_confirmation(self, client: TimeMachineClient, timeout_seconds: float = 1.5) -> bool:
        deadline = asyncio.get_running_loop().time() + timeout_seconds
        start_counter = self.lt_seen_counter

        while asyncio.get_running_loop().time() < deadline:
            # If live loop parsed a new LT line, confirmation is complete.
            if self.lt_seen_counter > start_counter:
                return True

            # When not in live capture, read directly for LT confirmation.
            if not (self.live_task and not self.live_task.done() and client is self.live_client):
                chunk = await self._read_available_once(client)
                if chunk:
                    text = sanitize_device_bytes(chunk)
                    m = HEADER_LT_RE.search(text)
                    if m:
                        self.last_lt_value = m.group(1)
                        self.lt_seen_counter += 1
                        return True

            await asyncio.sleep(0.02)

        return False

    @asyncSlot()
    async def toggle_timer(self):
        if self._timer_action_blocked():
            return

        port = self.selected_port()
        if not port:
            QMessageBox.warning(self, "No COM Port", "Please select a COM port.")
            self.log_timer_command("Blocked: no COM port selected")
            return

        self.timer_toggle_btn.setEnabled(False)
        client = None
        using_live_connection = False
        try:
            if self._has_live_connection():
                client = self.live_client
                using_live_connection = True
            else:
                client = await self._open_client(port, self.selected_baud(), 0.2)

            if self.timer_running:
                await asyncio.to_thread(client.timer_stop)
                self.timer_running = False
                self._timer_user_stopped = True
                self.status.showMessage("Timer stop command sent (0x80 + 000000 + CR/LF)")
                if using_live_connection:
                    self.log_timer_command("Sent STOP on live COM: 0x80 + 000000 + CR/LF")
                else:
                    self.log_timer_command("Sent STOP: 0x80 + 000000 + CR/LF")
            else:
                # Use the current timer value from the device (live_timer_display)
                timer_display_used = self.live_timer_display or "0:00"
                if self.live_timer_display:
                    start_hhmmss = _timer_display_to_hhmmss(self.live_timer_display)
                else:
                    start_hhmmss = "000000"

                if not start_hhmmss:
                    raise ValueError(f"Invalid timer value from device: '{self.live_timer_display}'. Expected format: MM:SS or HH:MM:SS")

                event_num, heat_num = self.get_current_or_default_event_heat()
                self.event_spin.setValue(event_num)
                self.heat_spin.setValue(heat_num)
                await asyncio.to_thread(client.set_event_heat, event_num, heat_num)

                encoded = (
                    f"{start_hhmmss[5]}{start_hhmmss[4]}"
                    f"{start_hhmmss[3]}{start_hhmmss[2]}"
                    f"{start_hhmmss[1]}{start_hhmmss[0]}"
                )

                await asyncio.to_thread(client.timer_start, start_hhmmss)
                confirmed = await self._wait_for_lt_confirmation(client, 1.5)
                if not confirmed:
                    self.timer_running = False
                    self._update_timer_button_visual()
                    self.status.showMessage("Timer start not confirmed")
                    self.log_timer_command("Start failed: LT confirmation not received")
                    QMessageBox.critical(
                        self,
                        "Timer Start Failed",
                        "Timer start was not confirmed. Expected an LT message such as 'LT 00:00:00.03'.",
                    )
                    return

                self.timer_running = True
                self._timer_user_stopped = False
                self.status.showMessage("Timer start confirmed by LT message")
                if using_live_connection:
                    self.log_timer_command(
                        f"Set E/H {event_num:03d}/{heat_num:02d}; START on live COM using timer {timer_display_used} (0x82 + {encoded} + CR/LF) | confirmed LT {self.last_lt_value}"
                    )
                else:
                    self.log_timer_command(
                        f"Set E/H {event_num:03d}/{heat_num:02d}; START using timer {timer_display_used} (0x82 + {encoded} + CR/LF) | confirmed LT {self.last_lt_value}"
                    )

            self._update_timer_button_visual()
        except Exception as e:
            err_str = str(e)
            if "permissionerror" in err_str.lower() or "access is denied" in err_str.lower():
                QMessageBox.critical(
                    self,
                    "COM Port In Use",
                    f"Cannot open {port}: access was denied.\n\n"
                    "The port may still be held from a previous session.\n"
                    "Please click 'Connect' to establish the live connection first, "
                    "then use the timer controls.",
                )
            else:
                QMessageBox.critical(self, "Timer Control Error", err_str)
            self.status.showMessage("Failed to send timer start/stop command")
            self.log_timer_command(f"Error (start/stop): {e}")
        finally:
            if client is not None and not using_live_connection:
                await asyncio.to_thread(client.close)
            self.timer_toggle_btn.setEnabled(True)

    @asyncSlot()
    async def reset_timer(self):
        if self._timer_action_blocked():
            return

        port = self.selected_port()
        if not port:
            QMessageBox.warning(self, "No COM Port", "Please select a COM port.")
            self.log_timer_command("Blocked: no COM port selected")
            return

        self.timer_reset_btn.setEnabled(False)
        client = None
        using_live_connection = False
        try:
            if self._has_live_connection():
                client = self.live_client
                using_live_connection = True
            else:
                client = await self._open_client(port, self.selected_baud(), 0.2)

            await asyncio.to_thread(client.timer_reset)
            self.timer_running = False
            self._update_timer_button_visual()
            self.status.showMessage("Timer reset command sent (0x80 + 000000 + CR/LF)")
            if using_live_connection:
                self.log_timer_command("Sent RESET on live COM: 0x80 + 000000 + CR/LF")
            else:
                self.log_timer_command("Sent RESET: 0x80 + 000000 + CR/LF")
        except Exception as e:
            QMessageBox.critical(self, "Timer Reset Error", str(e))
            self.status.showMessage("Failed to send timer reset command")
            self.log_timer_command(f"Error (reset): {e}")
        finally:
            if client is not None and not using_live_connection:
                await asyncio.to_thread(client.close)
            self.timer_reset_btn.setEnabled(True)

    def _apply_professional_theme(self):
        s = self.ui_scale
        control_h = max(20, int(18 * s))
        button_h = max(20, int(18 * s))
        input_padding_v = max(1, int(2 * s))
        input_padding_h = max(4, int(5 * s))

        # Font sizes scale with ui_scale so high-DPI screens get larger text.
        def _fs(base: int) -> int:
            return max(base, int(base * s))

        size_map = {
            "small": {
                "base": 8,
                "group": 9,
                "label": 9,
                "input": 9,
                "radio": 9,
                "table": 8,
                "header": 8,
                "raw": 9,
            },
            "medium": {
                "base": _fs(9),
                "group": _fs(10),
                "label": _fs(10),
                "input": _fs(10),
                "radio": _fs(10),
                "table": _fs(9),
                "header": _fs(9),
                "raw": _fs(11),
            },
            "large": {
                "base": _fs(13),
                "group": _fs(15),
                "label": _fs(15),
                "input": _fs(15),
                "radio": _fs(15),
                "table": _fs(14),
                "header": _fs(14),
                "raw": _fs(18),
            },
            "xlarge": {
                "base": _fs(15),
                "group": _fs(17),
                "label": _fs(17),
                "input": _fs(17),
                "radio": _fs(17),
                "table": _fs(16),
                "header": _fs(16),
                "raw": _fs(21),
            },
        }
        sizes = size_map.get(self.text_scale, size_map["medium"])

        # Apply one cohesive stylesheet to keep every control visually consistent.
        self.setStyleSheet(f"""
            QMainWindow {{
                background: qlineargradient(x1:0, y1:0, x2:1, y2:1,
                                            stop:0 #f3f7fb, stop:1 #eaf4f1);
            }}

            QWidget#centralRoot {{
                background: transparent;
            }}

            QWidget#sectionHeader {{
                background: #dff0f6;
                border: 1px solid #b6d0d9;
                border-radius: 10px;
            }}

            QLabel#sectionTitle {{
                color: #0f4f5e;
                font-weight: 700;
                font-size: {sizes['group']}px;
                background: transparent;
                border: none;
            }}

            QPushButton#collapseBtn {{
                border: none;
                background: transparent;
                min-height: 0px;
                padding: 0px;
            }}

            QGroupBox {{
                color: #123640;
                font-size: {sizes['group']}px;
                font-weight: 700;
                border: 1px solid #b6d0d9;
                border-radius: 10px;
                margin-top: 0px;
                padding: 8px;
                background: #f9fcfd;
            }}

            QGroupBox::title {{
                height: 0px;
                padding: 0px;
                margin: 0px;
            }}

            QLabel {{
                color: #24424c;
                font-size: {sizes['label']}px;
            }}

            QLabel#liveLedLabel {{
                color: #9c4a2e;
                font-weight: 700;
                font-size: {sizes['label']}px;
            }}

            QLabel#fieldLabel {{
                color: #0f4f5e;
                font-weight: 700;
                font-size: {sizes['label']}px;
                font-family: "Segoe UI Semibold", "Segoe UI", sans-serif;
            }}

            QLabel#bibCsvLabel, QLabel#bibAgeLabel {{
                color: #5a7a84;
                font-size: {max(sizes['label'] - 2, 10)}px;
            }}

            QTextEdit#timerLogText {{
                background: #f4fafc;
                border: 1px solid #bfd8e0;
                border-radius: 8px;
                color: #1a4550;
                font-family: Consolas, 'Courier New', monospace;
                font-size: {sizes['table']}px;
            }}

            QComboBox, QSpinBox, QDoubleSpinBox, QLineEdit {{
                border: 1px solid #b6d0d9;
                border-radius: 8px;
                background: #ffffff;
                color: #14333d;
                min-height: {control_h}px;
                padding: {input_padding_v}px {input_padding_h}px;
                selection-background-color: #2c9fbf;
                font-size: {sizes['input']}px;
            }}

            QComboBox:hover, QSpinBox:hover, QDoubleSpinBox:hover, QLineEdit:hover {{
                border: 1px solid #7eb7c7;
            }}

            QComboBox:focus, QSpinBox:focus, QDoubleSpinBox:focus, QLineEdit:focus {{
                border: 2px solid #2c9fbf;
            }}

            QComboBox::drop-down {{
                subcontrol-origin: padding;
                subcontrol-position: top right;
                width: 26px;
                border-left: 1px solid #c7dde4;
                border-top-right-radius: 8px;
                border-bottom-right-radius: 8px;
                background: #eef7fa;
            }}

            QComboBox QAbstractItemView {{
                border: 1px solid #9ec5d2;
                selection-background-color: #d9eef5;
                selection-color: #123640;
                background: #ffffff;
            }}

            QPushButton {{
                border-radius: 9px;
                border: 1px solid #4f8fa3;
                min-height: {button_h}px;
                padding: {input_padding_v + 1}px {input_padding_h + 4}px;
                font-weight: 600;
                color: #10353f;
                background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                                            stop:0 #f8fdff, stop:1 #d9ecf3);
            }}

            QPushButton:hover {{
                background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                                            stop:0 #ffffff, stop:1 #cde7f0);
            }}

            QPushButton:pressed {{
                padding-top: 7px;
                padding-left: 15px;
                background: #c1e0ea;
            }}

            QPushButton[kind="primary"] {{
                color: #ffffff;
                border: 1px solid #1f6a80;
                background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                                            stop:0 #2f95b3, stop:1 #216f86);
            }}

            QPushButton[kind="primary"]:hover {{
                background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                                            stop:0 #3ba3c3, stop:1 #247892);
            }}

            QPushButton[kind="secondary"] {{
                color: #1a4f5e;
                border: 1px solid #8db9c6;
                background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                                            stop:0 #f0f8fb, stop:1 #dbedf3);
            }}

            QPushButton[kind="secondary"]:hover {{
                background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                                            stop:0 #f8fcfe, stop:1 #cde5ed);
                border: 1px solid #6aa3b4;
            }}

            QPushButton[kind="accent"] {{
                color: #ffffff;
                border: 1px solid #1f7c65;
                background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                                            stop:0 #31af8d, stop:1 #1f8a6e);
            }}

            QPushButton[kind="accent"]:hover {{
                background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                                            stop:0 #39c19b, stop:1 #239477);
            }}

            QPushButton[kind="danger"] {{
                color: #ffffff;
                border: 1px solid #9c4a2e;
                background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                                            stop:0 #d87952, stop:1 #b85f39);
            }}

            QPushButton[kind="danger"]:hover {{
                background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                                            stop:0 #e18861, stop:1 #c86840);
            }}

            QPushButton:disabled {{
                color: #6e8a93;
                background: #e6edf0;
                border: 1px solid #c8d6db;
            }}

            QRadioButton, QCheckBox {{
                color: #28464f;
                spacing: 8px;
                font-size: {sizes['radio']}px;
            }}

            QTableWidget {{
                border: 1px solid #b7cfd8;
                border-radius: 10px;
                background: #ffffff;
                alternate-background-color: #f4fbfe;
                gridline-color: #d6e5ea;
                color: #17313a;
                selection-background-color: #bfe4ef;
                selection-color: #102a32;
                font-size: {sizes['table']}px;
                padding: 0px;
                margin: 0px;
            }}

            QTableWidget::item {{
                padding: 0px 2px;
                margin: 0px;
                border: 0px;
            }}

            QHeaderView::section {{
                background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                                            stop:0 #e7f5fb, stop:1 #d4e9f2);
                color: #114451;
                border: 0px;
                border-right: 1px solid #bcd4dd;
                border-bottom: 1px solid #bcd4dd;
                padding: 1px 2px;
                font-weight: 700;
                font-size: {sizes['header']}px;
            }}

            QTextEdit {{
                border: 1px solid #b7cfd8;
                border-radius: 10px;
                background: #f9fdff;
                color: #123640;
                selection-background-color: #d6edf6;
                font-family: Consolas, 'Courier New', monospace;
                font-size: {sizes['raw']}px;
            }}

            QWidget#statusCluster,
            QWidget#timerCluster,
            QWidget#eventCluster {{
                background: #eef6fa;
                border: 1px solid #c9dce4;
                border-radius: 6px;
            }}

            QFrame#bannerDivider {{
                color: #b8ccd6;
                background: #b8ccd6;
                min-width: 1px;
                max-width: 1px;
                margin: 2px 0;
            }}

            QLCDNumber#timerDisplayBanner {{
                color: #ff5a45;
                background: #11161a;
                border: 1px solid #3a4f5a;
                border-radius: 4px;
                padding: 2px;
            }}

            QToolBar#mainToolbar {{
                background: qlineargradient(x1:0, y1:0, x2:1, y2:0,
                                            stop:0 #ecf7fb, stop:1 #dbeef5);
                border: 1px solid #b9d2db;
                border-radius: 10px;
                spacing: 8px;
                padding: 5px;
            }}

            QToolButton {{
                color: #114451;
                border-radius: 7px;
                padding: 5px 10px;
                font-weight: 600;
                background: transparent;
            }}

            QToolButton:hover {{
                background: #cfe8f1;
            }}

            QStatusBar {{
                background: #f2f8fa;
                color: #2a4d57;
                border-top: 1px solid #cadde4;
            }}
        """)

        base_font = QFont("Segoe UI", sizes["base"])
        self.setFont(base_font)

        # Set Bib column to exactly 9 characters wide using the actual table font.
        # Re-apply fixed column widths using the now-correct font metrics.
        self._bib_col_font_size = sizes["table"]
        self._apply_fixed_column_widths()

    # -----------------------------
    # Port helpers
    # -----------------------------
    def _apply_fixed_column_widths(self):
        """Apply startup widths for all columns, then custom Bib/Team widths.
        Columns remain user-adjustable because header resize mode is Interactive."""
        if not hasattr(self, "table"):
            return
        from PyQt5.QtGui import QFontMetrics as _QFM
        font_size = getattr(self, "_bib_col_font_size", 9)
        table_font = QFont("Segoe UI", font_size)
        fm = _QFM(table_font)

        # Start every column near the old auto-sized look (header-content based).
        for col in range(self.table.columnCount()):
            header_item = self.table.horizontalHeaderItem(col)
            header_text = header_item.text() if header_item is not None else ""
            parts = [p.strip() for p in header_text.split("\n")]
            max_part = max((fm.horizontalAdvance(p) for p in parts if p), default=0)
            self.table.setColumnWidth(col, max(50, max_part + 18))

        # Keep Bib at 9 characters and Team 25% wider baseline.
        bib_width = fm.horizontalAdvance("W" * 9) + 16  # 9 chars + padding
        self.table.setColumnWidth(5, bib_width)
        self.table.setColumnWidth(6, int(1.25 * max(120, int(102 * self.ui_scale))))

    # -----------------------------
    def selected_port(self) -> str:
        val = self.port_combo.currentData()
        return val if isinstance(val, str) else ""

    def selected_baud(self) -> int:
        return int(self.baud_combo.currentText())

    def refresh_ports(self):
        current = self.selected_port()
        self.port_combo.clear()

        ports = list(list_ports.comports())

        def port_sort_key(p):
            m = re.match(r"COM(\d+)$", p.device, re.IGNORECASE)
            if m:
                return (0, int(m.group(1)))
            return (1, p.device.upper())

        ports.sort(key=port_sort_key)

        for p in ports:
            self.port_combo.addItem(f"{p.device} — {p.description}", p.device)

        if not ports:
            self.port_combo.addItem("No ports found", "")
        else:
            # Find the first Bluetooth serial port to use as default
            bt_index = None
            for i, p in enumerate(ports):
                if "standard serial over bluetooth" in (p.description or "").lower():
                    bt_index = i
                    break

            if current:
                for i in range(self.port_combo.count()):
                    if self.port_combo.itemData(i) == current:
                        self.port_combo.setCurrentIndex(i)
                        break
                else:
                    self.port_combo.setCurrentIndex(bt_index if bt_index is not None else 0)
            else:
                self.port_combo.setCurrentIndex(bt_index if bt_index is not None else 0)

        self.status.showMessage("COM ports refreshed")

    def get_event_allowed_age_groups(self, event_num: int) -> List[str]:
        if event_num < 1:
            return []
        for key in (str(event_num), str(event_num).zfill(3)):
            meta = self.event_meta_map.get(key)
            if meta and meta.get("age_group"):
                return [meta["age_group"]]
        # Fallback if no CSV loaded
        age_group_cycle = ["9&10", "11&12", "13-15", "16-18"]
        return [age_group_cycle[(event_num - 1) % len(age_group_cycle)]]

    def bib_sort_key(self, bib: str):
        if bib.isdigit():
            return int(bib)
        return bib

    def update_bib_dropdown_options(self):
        allowed_groups = self.get_event_allowed_age_groups(self.event_spin.value())

        if not self.bib_lookup:
            self.bib_bib_combo.clear()
            self.bib_bib_combo.addItem("Load bib CSV first", "")
            self.bib_bib_combo.setEnabled(False)
            self.bib_allowed_label.setText("Allowed age groups: N/A")
            return

        allowed_bibs = [
            bib for bib, info in self.bib_lookup.items()
            if not allowed_groups or info.get("age_group") in allowed_groups
        ]
        allowed_bibs.sort(key=self.bib_sort_key)

        self.bib_bib_combo.clear()
        if allowed_bibs:
            self.bib_bib_combo.addItem("Select bib", "")
            for bib in allowed_bibs:
                self.bib_bib_combo.addItem(bib, bib)
            self.bib_bib_combo.setEnabled(True)
        else:
            self.bib_bib_combo.addItem("No bibs found for event age group", "")
            self.bib_bib_combo.setEnabled(False)

        age_display = ", ".join(allowed_groups) if allowed_groups else "All"
        self.bib_allowed_label.setText(f"Allowed age groups: {age_display}")

    def on_event_selection_changed(self, value: int):
        self.update_bib_dropdown_options()

    def _set_heat_italic(self, italic: bool):
        """Set the heat spinbox font to italic (auto-incremented) or normal (device data)."""
        f = self.heat_spin.font()
        f.setItalic(italic)
        self.heat_spin.setFont(f)

    def _sync_event_heat_controls(self):
        if self.live_current_event.isdigit():
            event_num = min(max(int(self.live_current_event), 1), 999)
            if self.event_spin.value() != event_num:
                self.event_spin.setValue(event_num)

        if self.live_current_heat.isdigit():
            heat_num = min(max(int(self.live_current_heat), 1), 99)
            if self.heat_spin.value() != heat_num:
                self.heat_spin.setValue(heat_num)
            self._set_heat_italic(False)  # device confirmed the heat — normal style

        self._update_banner_from_live_state()

    def _update_banner_from_live_state(self):
        """Update the event/heat banner from current live state or spinner values."""
        event_str = self.live_current_event.lstrip("0") if self.live_current_event else ""
        heat_str = self.live_current_heat.lstrip("0") if self.live_current_heat else ""

        if not event_str:
            event_str = str(self.event_spin.value())
        if not heat_str:
            heat_str = str(self.heat_spin.value())

        event_raw = self.live_current_event or str(self.event_spin.value())
        event_name = self._lookup_event_name(event_raw)
        compact_mode = bool(getattr(self, "_banner_compact_mode", False))
        name_part = f" — {event_name}" if event_name else ""

        # Update timer display banner (digital clock style)
        timer_text = self.live_timer_display if self.live_timer_display else "00:00"
        if len(timer_text) == 4 and ":" in timer_text:
            timer_text = f"0{timer_text}"
        self.timer_display_banner.display(timer_text)

        # Update event/heat banner, keeping heat italic in sync with auto-increment state.
        event_text = html.escape(event_str)
        name_text = html.escape(name_part)
        heat_text = html.escape(heat_str)
        if self.heat_spin.font().italic():
            heat_text = f"<i>{heat_text}</i>"
        self.event_heat_banner.setText(f"Event: {event_text}{name_text}   Heat: {heat_text}")

        # Update date banner — use device date if available, otherwise OS date.
        display_date = self.live_current_date or get_local_date_string()
        self.date_banner.setText(f"Date: {display_date}")

    def get_current_or_default_event_heat(self) -> Tuple[int, int]:
        event_text = (self.live_current_event or "").strip()
        heat_text = (self.live_current_heat or "").strip()

        if not event_text or not heat_text:
            for row in reversed(self.last_rows):
                if (not event_text) and row.event.isdigit():
                    event_text = row.event
                if (not heat_text) and row.heat.isdigit():
                    heat_text = row.heat
                if event_text and heat_text:
                    break

        if not event_text and self.event_spin.value() >= 1:
            event_text = str(self.event_spin.value())
        if not heat_text and self.heat_spin.value() >= 1:
            heat_text = str(self.heat_spin.value())

        event_num = int(event_text) if event_text.isdigit() else 1
        heat_num = int(heat_text) if heat_text.isdigit() else 1
        event_num = min(max(event_num, 1), 999)
        heat_num = min(max(heat_num, 1), 99)
        return event_num, heat_num

    # -----------------------------
    # Async serial helpers
    # -----------------------------
    async def _open_client(self, port: str, baud: int, timeout: float) -> TimeMachineClient:
        return await asyncio.to_thread(
            TimeMachineClient,
            port,
            baud,
            serial.EIGHTBITS,
            serial.PARITY_NONE,
            serial.STOPBITS_ONE,
            timeout,
            0.01,
        )

    def _has_live_connection(self) -> bool:
        return (
            self.live_client is not None
            and self.live_client.ser is not None
            and self.live_client.ser.is_open
        )

    async def _read_available_once(self, client: TimeMachineClient) -> bytes:
        return await asyncio.to_thread(client.read_available)

    async def _download_until_end(self, client: TimeMachineClient, timeout_seconds: float) -> bytes:
        deadline = asyncio.get_running_loop().time() + timeout_seconds
        out = bytearray()

        while asyncio.get_running_loop().time() < deadline:
            chunk = await self._read_available_once(client)
            if chunk:
                out.extend(chunk)
                cleaned_text = sanitize_device_bytes(bytes(out))
                if "END OF RETRANSMIT" in cleaned_text.upper():
                    break
            else:
                await asyncio.sleep(0.01)

        return bytes(out)

    async def _wait_for_retransmit_markers_in_live(self, start_index: int, timeout_seconds: float) -> bool:
        deadline = asyncio.get_running_loop().time() + timeout_seconds
        saw_start = False

        while asyncio.get_running_loop().time() < deadline:
            chunk = bytes(self.live_raw_buffer[start_index:])
            if chunk:
                text = sanitize_device_bytes(chunk).upper()
                if "START OF RETRANSMIT" in text:
                    saw_start = True
                if saw_start and "END OF RETRANSMIT" in text:
                    return True
            await asyncio.sleep(0.02)

        return False

    async def _live_capture_loop(self, client: TimeMachineClient):
        self.status.showMessage("Live capture running")
        try:
            while True:
                chunk = await self._read_available_once(client)
                if chunk:
                    self.on_live_chunk(chunk)
                else:
                    await asyncio.sleep(0.01)
        except asyncio.CancelledError:
            raise

    # -----------------------------
    # Async actions
    # -----------------------------
    @asyncSlot()
    async def download_selected(self):
        port = self.selected_port()
        if not port:
            QMessageBox.warning(self, "No COM Port", "Please select a COM port.")
            return

        if self.download_task and not self.download_task.done():
            return

        self.download_btn.setEnabled(False)

        event_num = self.event_spin.value()
        heat_num = self.heat_spin.value()
        read_seconds = self.read_seconds_spin.value()

        self.status.showMessage(f"Downloading event {event_num}, heat {heat_num}...")

        client = None
        try:
            if self._has_live_connection():
                client = self.live_client
                start_index = len(self.live_raw_buffer)
                before = sum(1 for r in self.last_rows if r.row_type == "result")

                await asyncio.to_thread(client.retransmit, event_num, heat_num, None)
                done = await self._wait_for_retransmit_markers_in_live(start_index, read_seconds + 0.5)

                after = sum(1 for r in self.last_rows if r.row_type == "result")
                added = max(0, after - before)

                if done:
                    self.status.showMessage(
                        f"Live retransmit complete: event {event_num}, heat {heat_num}, +{added} valid rows"
                    )
                else:
                    self.status.showMessage(
                        f"Live retransmit timed out: event {event_num}, heat {heat_num}, +{added} valid rows"
                    )
            else:
                client = await self._open_client(port, self.selected_baud(), 0.2)

                await asyncio.to_thread(
                    client.retransmit,
                    event_num,
                    heat_num,
                    None,
                )

                raw = await asyncio.wait_for(
                    self._download_until_end(client, read_seconds),
                    timeout=read_seconds + 0.5,
                )

                self.on_download_ok(raw)

        except asyncio.TimeoutError:
            self.on_download_failed("Download timed out")
        except Exception as e:
            self.on_download_failed(str(e))
        finally:
            if client is not None and client is not self.live_client:
                await asyncio.to_thread(client.close)
            self.download_btn.setEnabled(True)

    @asyncSlot()
    async def set_event_heat_selected(self):
        port = self.selected_port()
        if not port:
            QMessageBox.warning(self, "No COM Port", "Please select a COM port.")
            return

        event_num = self.event_spin.value()
        heat_num = self.heat_spin.value()

        self.status.showMessage(f"Setting event {event_num}, heat {heat_num}...")

        client = None
        using_live_connection = False
        try:
            if self._has_live_connection():
                client = self.live_client
                using_live_connection = True
            else:
                client = await self._open_client(port, self.selected_baud(), 0.2)

            await asyncio.to_thread(client.set_event_heat, event_num, heat_num)

            self.live_current_event = f"{event_num:03d}"
            self.live_current_heat = f"{heat_num:02d}"
            self._update_banner_from_live_state()

            if using_live_connection:
                self.status.showMessage(f"Set event {event_num}, heat {heat_num} command sent on live COM")
                self.log_timer_command(f"Set E/H on live COM: {event_num:03d}/{heat_num:02d}")
            else:
                self.status.showMessage(f"Set event {event_num}, heat {heat_num} command sent")
        except Exception as e:
            QMessageBox.critical(self, "Set Event/Heat Error", str(e))
            self.status.showMessage("Failed to set event/heat")
        finally:
            if client is not None and not using_live_connection:
                await asyncio.to_thread(client.close)

    @asyncSlot()
    async def start_live_capture(self):
        port = self.selected_port()
        if not port:
            # Only show a warning if this was triggered by the user (button click),
            # not the auto-connect at boot.
            if self.live_start_btn.text() == "Connect" and self.live_start_btn.isEnabled():
                QMessageBox.warning(self, "No COM Port", "Please select a COM port.")
            return

        if self.live_task and not self.live_task.done():
            return

        if self._has_live_connection():
            self.status.showMessage("Live connection already open")
            return

        self.live_raw_buffer.clear()
        self.last_raw_bytes = b""
        self.live_text_buffer = ""
        self.live_current_date = ""
        self.live_saw_live_time = False
        self.live_in_retransmit = False
        self.live_expect_retransmit_event = False
        self.live_retransmit_event = ""
        self.live_retransmit_heat = ""
        self._reset_response_count = 0
        self._reset_ack_streak = 0
        self.live_timer_display = ""
        self._raw_view_dirty = False
        self.update_raw_view()

        self.append_live_capture_start_log()

        self.live_start_btn.setText("Disconnect")
        self.live_start_btn.setProperty("kind", "danger")
        self.live_start_btn.style().unpolish(self.live_start_btn)
        self.live_start_btn.style().polish(self.live_start_btn)
        self.download_btn.setEnabled(False)
        self._led_on = True
        self._set_led(True)
        self._led_timer.start()

        client = None
        try:
            client = await self._open_client(port, self.selected_baud(), 0.1)
            self.live_client = client
            self.log_timer_command(f"Live COM opened: {port} @ {self.selected_baud()}")

            async def runner():
                loop_error = None
                try:
                    await self._live_capture_loop(client)
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    loop_error = e
                finally:
                    try:
                        if client is not None:
                            await asyncio.to_thread(client.close)
                    except Exception:
                        pass
                    self.live_client = None
                    # If loop died unexpectedly (not cancelled by stop_live_capture),
                    # reset the UI so the user knows they need to reconnect.
                    if self.live_start_btn.text() != "Connect":
                        err_msg = str(loop_error) if loop_error else "Connection lost"
                        self.log_timer_command(f"Live connection dropped: {err_msg}")
                        self.status.showMessage(f"Connection lost: {err_msg}")
                        self.live_start_btn.setText("Connect")
                        self.live_start_btn.setProperty("kind", "accent")
                        self.live_start_btn.style().unpolish(self.live_start_btn)
                        self.live_start_btn.style().polish(self.live_start_btn)
                        self.live_start_btn.setEnabled(True)
                        self.download_btn.setEnabled(True)
                        self._led_timer.stop()
                        self._hide_led()

            self.live_task = asyncio.create_task(runner())
        except Exception as e:
            if client is not None:
                await asyncio.to_thread(client.close)
            self.live_client = None
            self.log_timer_command(f"Live COM open failed: {e}")
            self.live_start_btn.setText("Connect")
            self.live_start_btn.setProperty("kind", "accent")
            self.live_start_btn.style().unpolish(self.live_start_btn)
            self.live_start_btn.style().polish(self.live_start_btn)
            self.live_start_btn.setEnabled(True)
            self.download_btn.setEnabled(True)
            self._led_timer.stop()
            self._hide_led()
            err_str = str(e)
            if "semaphore" in err_str.lower() or "errno 121" in err_str.lower() or "oserror(22" in err_str.lower():
                QMessageBox.critical(
                    self,
                    "Time Machine Not Powered On",
                    "Could not connect to the Time Machine G2.\n\n"
                    "The device does not appear to be powered on.\n"
                    "Please turn on the Time Machine G2 and try again.",
                )
            else:
                QMessageBox.critical(self, "Live Capture Error", err_str)

    @asyncSlot()
    async def stop_live_capture(self):
        self.live_start_btn.setEnabled(False)
        self.status.showMessage("Stopping live capture...")

        client_to_close = self.live_client

        try:
            if self.live_task and not self.live_task.done():
                self.live_task.cancel()
                try:
                    await asyncio.wait_for(self.live_task, timeout=1.5)
                except asyncio.CancelledError:
                    pass
                except asyncio.TimeoutError:
                    # Keep UI responsive even if serial read cancellation is delayed.
                    self.status.showMessage("Live capture stop is taking longer than expected")
                except Exception as e:
                    self.log_timer_command(f"Live task cancel warning: {e}")

            # Always close the COM handle on stop, even if task cancellation was delayed.
            try:
                if client_to_close is not None:
                    ser = getattr(client_to_close, "ser", None)
                    if ser is not None and ser.is_open:
                        await asyncio.to_thread(client_to_close.close)
            except Exception as e:
                self.log_timer_command(f"Live COM close warning: {e}")
        finally:
            self.live_task = None
            self.live_client = None
            self.log_timer_command("Live COM closed")

            self.append_live_capture_stop_log()

            self.live_start_btn.setText("Connect")
            self.live_start_btn.setProperty("kind", "accent")
            self.live_start_btn.style().unpolish(self.live_start_btn)
            self.live_start_btn.style().polish(self.live_start_btn)
            self.live_start_btn.setEnabled(True)
            self.download_btn.setEnabled(True)

            # Ensure all indicators return to idle state.
            self._led_timer.stop()
            self._hide_led()
            self._activity_timer.stop()
            self._blink_activity_indicator()

        self.last_raw_bytes = bytes(self.live_raw_buffer)
        cleaned = sanitize_device_bytes(self.last_raw_bytes)
        rows, _ = parse_time_machine_text(cleaned)
        self.last_rows = rows
        self.populate_table(rows)
        self.update_raw_view()
        self.status.showMessage("Live capture stopped")

    # -----------------------------
    # Data handlers
    # -----------------------------
    def append_table_row(self, row: ParsedRow):
        hidden_types = {"live_time", "heat_header", "event_header", "marker", "raw"}
        if row.row_type in hidden_types:
            return

        self.table.setSortingEnabled(False)

        r = self.table.rowCount()
        self.table.insertRow(r)

        group = (row.event, row.heat)
        if group != self._table_last_group:
            self._table_bold = not self._table_bold
            self._table_last_group = group

        event_name = self._lookup_event_name(row.event)
        meta = self._lookup_event_meta(row.event)
        gender = row.gender if row.gender and row.gender != "N/A" else meta.get("gender", "")
        age_group = row.age_group if row.age_group and row.age_group != "N/A" else meta.get("age_group", "")
        values = [
            get_local_date_string(),
            row.timestamp.split(' ')[1] if ' ' in row.timestamp else row.timestamp,
            row.event,
            event_name or row.event_type,
            row.heat,
            row.lane,
            row.bib,
            row.team_name,
            row.first_name,
            row.last_name,
            row.cumulative_time,
            gender,
            age_group,
            row.place,
            row.split_time,
        ]

        for c, value in enumerate(values):
            if c == 6:  # Bib column - use dropdown
                combo = self._create_bib_combo(r, row.event, row.bib, self._table_bold, row.lane)
                self.table.setCellWidget(r, c, combo)
                continue
            if c == 7:  # Team column - use dropdown
                combo = self._create_team_combo(r, row.team_name, self._table_bold)
                self.table.setCellWidget(r, c, combo)
                continue
            item = QTableWidgetItem(value)
            if c in (2, 3, 5):
                item.setTextAlignment(Qt.AlignCenter)
            if self._table_bold:
                f = item.font()
                f.setBold(True)
                item.setFont(f)
            self.table.setItem(r, c, item)

        self.table.setSortingEnabled(True)
        self.table.scrollToItem(self.table.item(self.table.rowCount() - 1, 0))
        self._update_event_heat_banner(self.last_rows)
        self._write_session_results_csv()

    def process_live_chunk_incremental(self, data: bytes):
        sanitized = sanitize_device_bytes(data)
        if not sanitized:
            return

        self.live_text_buffer += sanitized

        while "\n" in self.live_text_buffer:
            line, self.live_text_buffer = self.live_text_buffer.split("\n", 1)
            line = line.strip()
            if not line:
                continue

            self.process_live_line(line)


    def process_live_line(self, line: str):
        # Detect the timer-reset acknowledgement ("0100000" or "01000000").
        # Heat increments once when 2+ consecutive reset acks arrive; extras are ignored.
        if re.match(r"^010{5,6}$", line):
            self._reset_ack_streak += 1
            if self._reset_ack_streak == 2:
                # Second consecutive reset ack → increment heat and reset timer display
                new_heat = min(self.heat_spin.value() + 1, 99)
                self.heat_spin.setValue(new_heat)
                self.live_current_heat = f"{new_heat:02d}"
                self.live_timer_display = "0:00"
                self.timer_running = False
                self._set_heat_italic(True)  # predicted increment — italicize until device confirms
                self._update_timer_button_visual()
                self._update_banner_from_live_state()
                self.log_timer_command(f"Consecutive reset ack: heat advanced to {new_heat}")
            elif self._reset_ack_streak == 1:
                self.log_timer_command("Reset ack #1: waiting for consecutive second")
            # 3rd, 4th, etc. consecutive acks are silently ignored
            return

        # Any non-reset-ack line breaks the consecutive sequence
        self._reset_ack_streak = 0

        # Detect 6-digit timer count lines (e.g. "923000" = 03:29)
        if TIMER_COUNT_RE.match(line):
            decoded = _decode_timer_count(line)
            if decoded:
                self.live_timer_display = decoded
                # If timer is not already marked as running and we're getting timer counts, it's running
                if not self.timer_running and decoded != "0:00" and not self._timer_user_stopped:
                    self.timer_running = True
                    self._update_timer_button_visual()
                self._update_banner_from_live_state()
            return

        if line.upper().startswith("START OF RETRANSMIT"):
            self.live_in_retransmit = True
            self.live_expect_retransmit_event = True
            self.live_retransmit_event = ""
            self.live_retransmit_heat = ""
            return

        if line.upper().startswith("END OF RETRANSMIT"):
            self.live_in_retransmit = False
            self.live_expect_retransmit_event = False
            self.live_retransmit_event = ""
            self.live_retransmit_heat = ""
            return

        if self.live_in_retransmit and self.live_expect_retransmit_event:
            m_event_digits = re.match(r"^(\d{3})$", line)
            if m_event_digits:
                self.live_retransmit_event = m_event_digits.group(1)
                self.live_current_event = self.live_retransmit_event
                self._sync_event_heat_controls()
                self.live_expect_retransmit_event = False
                return

        m = HEADER_LT_RE.match(line)
        if m:
            self.live_saw_live_time = True
            self.last_lt_value = m.group(1)
            self.lt_seen_counter += 1

            # Device LT updates indicate clock is actively running.
            if not self.timer_running and not self._timer_user_stopped:
                self.timer_running = True
                self._update_timer_button_visual()
                self.log_timer_command(f"Detected LT from device: {self.last_lt_value} (timer running)")
            return

        # Flash activity indicator for non-clock data (EVENT, HEAT, DATE, RESULT lines)
        self._flash_activity_indicator()

        m = HEADER_EVENT_RE.match(line)
        if m:
            self.live_current_event = m.group(1)
            self._sync_event_heat_controls()
            return

        m = HEADER_DATE_RE.match(line)
        if m:
            self.live_current_date = m.group(1).strip()
            self.date_banner.setText(f"Date: {self.live_current_date}")
            return

        m = HEADER_HEAT_RE.match(line)
        if m:
            self.live_current_heat = m.group(1) or m.group(2)
            if self.live_in_retransmit:
                self.live_retransmit_heat = self.live_current_heat
            self._sync_event_heat_controls()
            return

        m = HEADER_HEAT_T_RE.match(line)
        if m:
            self.live_current_heat = m.group(1)
            if self.live_in_retransmit:
                self.live_retransmit_heat = self.live_current_heat
            self._sync_event_heat_controls()
            return

        m = HEADER_HEAT_ONLY_RE.match(line)
        if m and (self.live_saw_live_time or self.live_in_retransmit):
            self.live_current_heat = m.group(1)
            if self.live_in_retransmit:
                self.live_retransmit_heat = self.live_current_heat
            self._sync_event_heat_controls()
            return

        m = RESULT_RE.match(line)
        if m:
            lane = m.group(1)
            place = m.group(2)
            cumulative = m.group(3)
            split = m.group(4)

            row_date = self.live_current_date or get_local_date_string()
            row_event = self.live_retransmit_event or self.live_current_event
            row_heat = self.live_retransmit_heat or self.live_current_heat

            row = ParsedRow(
                "result",
                row_event,
                get_event_type(row_event),
                row_heat,
                row_date,
                lane,
                place,
                cumulative,
                split,
                line,
                timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
            )

            self.last_rows.append(row)
            self.append_table_row(row)
            return

        # Optionally preserve non-result live lines internally
        row_date = self.live_current_date or (get_local_date_string() if self.live_saw_live_time else "")
        self.last_rows.append(
            ParsedRow(
                "raw",
                self.live_current_event,
                get_event_type(self.live_current_event),
                self.live_current_heat,
                row_date,
                "",
                "",
                "",
                "",
                line,
            )
        )
    def on_download_ok(self, raw: bytes):
        self.last_raw_bytes = raw
        cleaned_text = sanitize_device_bytes(raw)
        rows, meta = parse_time_machine_text(cleaned_text)
        download_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        for row in rows:
            if row.row_type == "result":
                row.timestamp = download_time
        self.last_rows = rows

        self.append_to_raw_log(raw)
        self.populate_table(rows)
        self.update_raw_view()

        self.status.showMessage(
            f"Download complete: event {meta.get('event', '')}, heat {meta.get('heat', '')}, {len(raw)} bytes"
        )

    def on_download_failed(self, msg: str):
        self.status.showMessage("Download failed")
        QMessageBox.critical(self, "Download Error", msg)

    def on_live_chunk(self, data: bytes):
        self.live_raw_buffer.extend(data)
        self.last_raw_bytes = bytes(self.live_raw_buffer)

        self.append_to_live_capture_log(data)
        self.process_live_chunk_incremental(data)

        # Throttle raw view updates to avoid flicker; the timer fires every 200ms
        if not self._raw_view_dirty:
            self._raw_view_dirty = True
            QTimer.singleShot(200, self._throttled_raw_view_update)

    def _throttled_raw_view_update(self):
        """Called by a one-shot timer to do a single full raw view redraw."""
        if not self._raw_view_dirty:
            return
        self._raw_view_dirty = False
        self.update_raw_view()

    # -----------------------------
    # Table / raw view
    # -----------------------------
    def populate_table(self, rows: List[ParsedRow]):
        self.table.setSortingEnabled(False)
        self.table.setRowCount(0)

        hidden_types = {"live_time", "heat_header", "event_header", "marker", "raw"}

        bold = False
        last_group = None

        for row in rows:
            if row.row_type in hidden_types:
                continue

            group = (row.event, row.heat)
            if group != last_group:
                bold = not bold
                last_group = group

            r = self.table.rowCount()
            self.table.insertRow(r)

            event_name = self._lookup_event_name(row.event)
            meta = self._lookup_event_meta(row.event)
            gender = row.gender if row.gender and row.gender != "N/A" else meta.get("gender", "")
            age_group = row.age_group if row.age_group and row.age_group != "N/A" else meta.get("age_group", "")
            values = [
                get_local_date_string(),
                row.timestamp.split(' ')[1] if ' ' in row.timestamp else row.timestamp,
                row.event,
                event_name or row.event_type,
                row.heat,
                row.lane,
                row.bib,
                row.team_name,
                row.first_name,
                row.last_name,
                row.cumulative_time,
                gender,
                age_group,
                row.place,
                row.split_time,
            ]

            for c, value in enumerate(values):
                if c == 6:  # Bib column - use dropdown
                    combo = self._create_bib_combo(r, row.event, row.bib, bold, row.lane)
                    self.table.setCellWidget(r, c, combo)
                    continue
                if c == 7:  # Team column - use dropdown
                    combo = self._create_team_combo(r, row.team_name, bold)
                    self.table.setCellWidget(r, c, combo)
                    continue
                item = QTableWidgetItem(value)
                if c in (2, 3, 5):
                    item.setTextAlignment(Qt.AlignCenter)
                if bold:
                    f = item.font()
                    f.setBold(True)
                    item.setFont(f)
                self.table.setItem(r, c, item)

        self._table_bold = bold
        self._table_last_group = last_group
        self.table.setSortingEnabled(True)
        if self.table.rowCount() > 0:
            self.table.scrollToItem(self.table.item(self.table.rowCount() - 1, 0))
        self._update_event_heat_banner(rows)
        self._write_session_results_csv()

    def _lookup_event_name(self, event_code: str) -> str:
        """Try padded, stripped, and integer-normalised keys against event_name_map."""
        stripped = event_code.lstrip("0") or event_code
        for key in (event_code, stripped):
            name = self.event_name_map.get(key, "")
            if name:
                return name
        # also try zero-padded to 3 digits in case the map has "013" but row has "13"
        try:
            padded = str(int(event_code)).zfill(3)
            name = self.event_name_map.get(padded, "")
            if name:
                return name
        except ValueError:
            pass
        return ""

    def _lookup_event_meta(self, event_code: str) -> dict:
        """Return {'gender': ..., 'age_group': ...} from event_meta_map, trying multiple key forms."""
        stripped = event_code.lstrip("0") or event_code
        for key in (event_code, stripped):
            meta = self.event_meta_map.get(key)
            if meta:
                return meta
        try:
            padded = str(int(event_code)).zfill(3)
            meta = self.event_meta_map.get(padded)
            if meta:
                return meta
        except ValueError:
            pass
        return {}

    def _get_filtered_bibs(self, event_code: str, lane: str = "") -> List[str]:
        """Return bib numbers from bib_lookup that match event's gender, age group, and competing teams."""
        meta = self._lookup_event_meta(event_code)
        event_gender = meta.get("gender", "")
        event_age_group = meta.get("age_group", "")

        def _norm_team(name: str) -> str:
            return (name or "").strip().casefold()

        # Determine allowed teams
        allowed_teams = set()
        if self.home_team:
            allowed_teams.add(_norm_team(self.home_team))
        for t in self.opponent_teams:
            allowed_teams.add(_norm_team(t))

        matching = []
        for bib, info in self.bib_lookup.items():
            # Team filter
            info_team = _norm_team(info.get("team_name", ""))
            if allowed_teams and info_team not in allowed_teams:
                continue
            # Age group must match
            if event_age_group and info.get("age_group", "") != event_age_group:
                continue
            # Gender: match exact, or allow if event is "Male/Female"
            if event_gender and event_gender != "Male/Female":
                if info.get("gender", "") != event_gender:
                    continue
            matching.append(bib)

        # Sort numerically if possible
        matching.sort(key=lambda b: (int(b) if b.isdigit() else float('inf'), b))
        return matching

    def _create_bib_combo(self, table_row: int, event_code: str, current_bib: str, bold: bool, lane: str = "") -> QComboBox:
        """Create a QComboBox for the bib column filtered by event metadata and teams."""
        combo = QComboBox()
        combo.setEditable(True)
        # Keep the combo's own size hint small so it doesn't widen the column;
        # the dropdown popup can still be wide enough to read full names.
        combo.setSizeAdjustPolicy(QComboBox.AdjustToMinimumContentsLength)
        combo.view().setMinimumWidth(max(260, int(320 * self.ui_scale)))
        filtered = self._get_filtered_bibs(event_code, lane)

        # Group bibs: home team first, then opponents
        home_bibs = []
        opp_bibs = []
        for bib in filtered:
            info = self.bib_lookup.get(bib, {})
            if info.get("team_name", "") == self.home_team:
                home_bibs.append(bib)
            else:
                opp_bibs.append(bib)

        combo.addItem("")  # blank default
        if home_bibs:
            for bib in home_bibs:
                info = self.bib_lookup.get(bib, {})
                label = f"{bib} - {info.get('last_name', '')} {info.get('first_name', '')} ({info.get('team_name', '')})"
                combo.addItem(label, bib)
        if opp_bibs:
            combo.insertSeparator(combo.count())
            for bib in opp_bibs:
                info = self.bib_lookup.get(bib, {})
                label = f"{bib} - {info.get('last_name', '')} {info.get('first_name', '')} ({info.get('team_name', '')})"
                combo.addItem(label, bib)

        # Set current value
        if current_bib:
            for i in range(combo.count()):
                if combo.itemData(i) == current_bib:
                    combo.setCurrentIndex(i)
                    # Keep the visible field compact: show only the bib after selection.
                    combo.setEditText(current_bib)
                    break
            else:
                combo.setEditText(current_bib)

        if bold:
            f = combo.font()
            f.setBold(True)
            combo.setFont(f)

        combo.setProperty("table_row", table_row)
        combo.activated.connect(lambda idx, c=combo: self._on_bib_combo_changed(c))
        combo.lineEdit().editingFinished.connect(lambda c=combo: self._on_bib_combo_changed(c))
        return combo

    def _create_team_combo(self, table_row: int, current_team: str, bold: bool) -> QComboBox:
        """Create a QComboBox for the Team column with home + opponent teams."""
        combo = QComboBox()
        combo.setEditable(True)

        teams = []
        if self.home_team:
            teams.append(self.home_team)
        for t in self.opponent_teams:
            if t != self.home_team:
                teams.append(t)

        combo.addItem("")
        for t in teams:
            combo.addItem(t)

        if current_team:
            idx = combo.findText(current_team)
            if idx >= 0:
                combo.setCurrentIndex(idx)
            else:
                combo.setEditText(current_team)

        if bold:
            f = combo.font()
            f.setBold(True)
            combo.setFont(f)

        combo.setProperty("table_row", table_row)
        combo.activated.connect(lambda idx, c=combo: self._on_team_combo_changed(c))
        combo.lineEdit().editingFinished.connect(lambda c=combo: self._on_team_combo_changed(c))
        return combo

    def _on_team_combo_changed(self, combo: QComboBox):
        """Handle team combo selection."""
        table_row = combo.property("table_row")
        team_value = combo.currentText().strip()
        if not team_value:
            return

        hidden_types = {"live_time", "heat_header", "event_header", "raw"}
        table_row_index = 0

        for parsed_row in self.last_rows:
            if parsed_row.row_type in hidden_types:
                continue
            if table_row_index == table_row:
                if parsed_row.row_type != "result":
                    return
                parsed_row.team_name = team_value
                self._write_session_results_csv()
                self.status.showMessage(f"Set team to {team_value} for row {table_row + 1}")
                return
            table_row_index += 1

    def _on_bib_combo_changed(self, combo: QComboBox):
        """Handle bib combo selection or manual entry."""
        table_row = combo.property("table_row")
        bib_value = combo.currentData()
        if bib_value is None:
            # Manual text entry
            bib_value = combo.currentText().strip()
            # If user typed "123 - Name", extract just the bib
            if " - " in bib_value:
                bib_value = bib_value.split(" - ")[0].strip()
        if not bib_value:
            return

        # Keep the visible text compact in the cell while preserving rich labels in popup.
        combo.setEditText(str(bib_value))

        hidden_types = {"live_time", "heat_header", "event_header", "raw"}
        table_row_index = 0

        for parsed_row in self.last_rows:
            if parsed_row.row_type in hidden_types:
                continue

            if table_row_index == table_row:
                if parsed_row.row_type != "result":
                    return

                info = self.bib_lookup.get(bib_value, None)
                if info is None:
                    parsed_row.bib = bib_value
                    parsed_row.team_name = "N/A"
                    parsed_row.first_name = "N/A"
                    parsed_row.last_name = "N/A"
                    parsed_row.gender = "N/A"
                    parsed_row.age_group = "N/A"
                else:
                    parsed_row.bib = bib_value
                    parsed_row.team_name = info.get("team_name", "N/A")
                    parsed_row.first_name = info.get("first_name", "N/A")
                    parsed_row.last_name = info.get("last_name", "N/A")
                    parsed_row.gender = info.get("gender", "N/A")
                    parsed_row.age_group = info.get("age_group", "N/A")

                self.table.blockSignals(True)
                # Update Team combo (column 5 is a cell widget)
                team_combo = self.table.cellWidget(table_row, 6)
                if isinstance(team_combo, QComboBox):
                    idx = team_combo.findText(parsed_row.team_name)
                    if idx >= 0:
                        team_combo.setCurrentIndex(idx)
                    else:
                        team_combo.setEditText(parsed_row.team_name)
                self.table.setItem(table_row, 7, QTableWidgetItem(parsed_row.first_name))
                self.table.setItem(table_row, 8, QTableWidgetItem(parsed_row.last_name))
                self.table.setItem(table_row, 9, QTableWidgetItem(parsed_row.cumulative_time))
                self.table.setItem(table_row, 10, QTableWidgetItem(parsed_row.gender))
                self.table.setItem(table_row, 11, QTableWidgetItem(parsed_row.age_group))
                self.table.blockSignals(False)
                self._write_session_results_csv()
                self.status.showMessage(f"Assigned bib {bib_value} to row {table_row + 1}")
                return

            table_row_index += 1

    def _update_event_heat_banner(self, rows: List[ParsedRow]):
        result_rows = [r for r in rows if r.row_type == "result"]
        date_rows = [r for r in rows if r.row_type == "date"]
        current_date = date_rows[-1].date if date_rows else self.live_current_date or get_local_date_string()
        self.date_banner.setText(f"Date: {current_date}")
        if result_rows:
            last = result_rows[-1]
            event_display = last.event.lstrip("0") or last.event
            heat_display = last.heat.lstrip("0") or last.heat
            event_name = self._lookup_event_name(last.event)
            name_part = f" — {event_name}" if event_name else ""
            self.event_heat_banner.setText(f"Event: {event_display}{name_part}   Heat: {heat_display}")
        else:
            self.event_heat_banner.setText("Event: —   Heat: —")

    def update_raw_view(self):
        scrollbar = self.raw_text.verticalScrollBar()
        was_at_bottom = scrollbar.value() >= (scrollbar.maximum() - 4)

        if self.hex_radio.isChecked():
            self.raw_text.setPlainText(raw_bytes_to_hex(self.last_raw_bytes))
        else:
            text = sanitize_device_bytes(self.last_raw_bytes)
            # Strip 6-digit timer count lines from display to reduce clutter
            filtered = "\n".join(
                ln for ln in text.splitlines()
                if not TIMER_COUNT_RE.match(ln.strip())
            )
            self.raw_text.setPlainText(filtered)

        if self.live_task and not self.live_task.done():
            scrollbar.setValue(scrollbar.maximum())
        elif was_at_bottom:
            scrollbar.setValue(scrollbar.maximum())

    def update_wrap_mode(self):
        if self.wrap_check.isChecked():
            self.raw_text.setLineWrapMode(QTextEdit.WidgetWidth)
        else:
            self.raw_text.setLineWrapMode(QTextEdit.NoWrap)

    # -----------------------------
    # CSV / save helpers
    # -----------------------------
    def build_csv_default_name(self) -> str:
        visible_rows = [
            row for row in self.last_rows
            if row.row_type not in {"live_time", "heat_header", "event_header", "raw"}
        ]

        events = sorted({row.event for row in visible_rows if row.event.isdigit()}, key=int)
        heats = sorted({row.heat for row in visible_rows if row.heat.isdigit()}, key=int)

        if not events:
            event_part = "event_unknown"
        elif len(events) == 1:
            event_part = f"event{int(events[0]):03d}"
        else:
            event_part = f"event{int(events[0]):03d}-{int(events[-1]):03d}"

        if not heats:
            heat_part = "heat_unknown"
        elif len(heats) == 1:
            heat_part = f"heat{int(heats[0]):02d}"
        else:
            heat_part = f"heat{int(heats[0]):02d}-{int(heats[-1]):02d}"

        return f"{event_part}_{heat_part}_results.csv"

    def save_table_to_csv(self):
        if not self.last_rows:
            QMessageBox.information(self, "No Data", "No table data to save.")
            return

        path, _ = QFileDialog.getSaveFileName(
            self,
            "Save Table CSV",
            os.path.join(self.log_session_dir, self.build_csv_default_name()),
            "CSV Files (*.csv)",
        )
        if not path:
            return

        hidden_types = {"live_time", "heat_header", "event_header", "raw"}

        try:
            with open(path, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(["Type", "Event", "Event Type", "Heat", "Date", "Time", "Lane", "Team", "Bib", "First Name", "Last Name", "Gender", "Age Group", "Place", "Finish\nTime", "Split Time", "Raw Line"])
                for row in self.last_rows:
                    if row.row_type in hidden_types:
                        continue
                    writer.writerow([
                        row.row_type,
                        row.event,
                        row.event_type,
                        row.heat,
                        get_local_date_string(),
                        row.timestamp.split(' ')[1] if ' ' in row.timestamp else row.timestamp,
                        row.lane,
                        row.team_name,
                        row.bib,
                        row.first_name,
                        row.last_name,
                        row.gender,
                        row.age_group,
                        row.place,
                        f'="{row.cumulative_time}"' if row.cumulative_time else "",
                        f'="{row.split_time}"' if row.split_time else "",
                        row.raw_line,
                    ])
            self.status.showMessage(f"Saved CSV: {path}")
        except Exception as e:
            QMessageBox.critical(self, "Save Error", str(e))

    def load_bib_csv(self):
        path, _ = QFileDialog.getOpenFileName(
            self,
            "Open Bib CSV",
            "",
            "CSV Files (*.csv);;All Files (*.*)",
        )
        if not path:
            return

        try:
            with open(path, "r", newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                required = {"Bib", "First Name", "Last Name", "Gender", "Team", "Age Group"}
                if not required.issubset({c.strip() for c in reader.fieldnames or []}):
                    QMessageBox.critical(self, "Invalid CSV", "CSV must contain columns: Bib, First Name, Last Name, Gender, Team, Age Group")
                    return

                self.bib_lookup.clear()
                for r in reader:
                    bib = r.get("Bib", "").strip()
                    if not bib:
                        continue
                    self.bib_lookup[bib] = {
                        "team_name": r.get("Team", "").strip() or "N/A",
                        "first_name": r.get("First Name", "").strip() or "N/A",
                        "last_name": r.get("Last Name", "").strip() or "N/A",
                        "gender": r.get("Gender", "").strip() or "N/A",
                        "age_group": r.get("Age Group", "").strip() or "N/A",
                    }

            self.bib_csv_label.setText(f"Bib CSV: {os.path.basename(path)}")
            self.bib_csv_label.setToolTip(path)
            self.status.showMessage(f"Loaded bib map with {len(self.bib_lookup)} entries")
            self.update_bib_dropdown_options()
            self._populate_team_lists()
        except Exception as e:
            QMessageBox.critical(self, "Load Error", str(e))

    def _auto_load_bib_csv(self):
        default_path = os.path.join("info", "bib_import.csv")
        if not os.path.isfile(default_path):
            return
        try:
            with open(default_path, "r", newline="", encoding="utf-8-sig") as f:
                reader = csv.DictReader(f)
                fieldnames = {c.strip() for c in reader.fieldnames or []}
                required = {"Bib", "First Name", "Last Name", "Gender", "Team", "Age Group"}
                if not required.issubset(fieldnames):
                    return
                self.bib_lookup.clear()
                for r in reader:
                    bib = r.get("Bib", "").strip()
                    if not bib:
                        continue
                    self.bib_lookup[bib] = {
                        "team_name": r.get("Team", "").strip() or "N/A",
                        "first_name": r.get("First Name", "").strip() or "N/A",
                        "last_name": r.get("Last Name", "").strip() or "N/A",
                        "gender": r.get("Gender", "").strip() or "N/A",
                        "age_group": r.get("Age Group", "").strip() or "N/A",
                    }
            self.bib_csv_label.setText(f"Bib CSV: {os.path.basename(default_path)} (auto)")
            self.bib_csv_label.setToolTip(default_path)
            self.update_bib_dropdown_options()
            self._populate_team_lists()
        except Exception:
            pass

    def _populate_team_lists(self):
        """Populate home team combo and opponent list from bib_lookup team names."""
        teams = sorted({info.get("team_name", "") for info in self.bib_lookup.values() if info.get("team_name", "") and info.get("team_name", "") != "N/A"})

        current_home = self.home_team_combo.currentText()
        self.home_team_combo.blockSignals(True)
        self.home_team_combo.clear()
        for t in teams:
            self.home_team_combo.addItem(t)
        idx = self.home_team_combo.findText(current_home)
        if idx >= 0:
            self.home_team_combo.setCurrentIndex(idx)
        else:
            mo = self.home_team_combo.findText("MountOlive")
            if mo >= 0:
                self.home_team_combo.setCurrentIndex(mo)
        self.home_team = self.home_team_combo.currentText()
        self.home_team_combo.blockSignals(False)

        prev_selected = set(self.opponent_teams)
        self.opponent_combo.blockSignals(True)
        self.opponent_combo.clearItems()
        for t in teams:
            if t == self.home_team:
                continue
            self.opponent_combo.addCheckItem(t, t in prev_selected)
        self.opponent_combo.blockSignals(False)
        self._sync_opponent_selection()

    def _on_home_team_changed(self, text: str):
        self.home_team = text
        self._populate_team_lists()
        if self.last_rows:
            self.populate_table(self.last_rows)

    def _on_opponents_changed(self):
        self._sync_opponent_selection()
        if self.last_rows:
            self.populate_table(self.last_rows)

    def _sync_opponent_selection(self):
        self.opponent_teams = self.opponent_combo.checkedItems()

    def _auto_load_events_csv(self):
        default_path = os.path.join("info", "event_import.csv")
        if not os.path.isfile(default_path):
            return
        try:
            with open(default_path, "r", newline="", encoding="utf-8-sig") as f:
                reader = csv.DictReader(f)
                fieldnames = {c.strip() for c in reader.fieldnames or []}
                if not {"event number", "event name"}.issubset(fieldnames):
                    return
                self.event_name_map.clear()
                self.event_meta_map.clear()
                for r in reader:
                    event_num = r.get("event number", "").strip()
                    event_name = r.get("event name", "").strip()
                    if event_num:
                        self.event_name_map[event_num] = event_name
                        self.event_meta_map[event_num] = {
                            "gender": r.get("gender", "").strip(),
                            "age_group": r.get("age group", "").strip(),
                        }
            self.events_csv_label.setText(f"Events CSV: {os.path.basename(default_path)} (auto)")
            self.events_csv_label.setToolTip(default_path)
        except Exception:
            pass

    def show_events_csv_format_help(self):
        QMessageBox.information(
            self,
            "Events CSV Format",
            "The Events CSV maps event numbers (as reported by the timing device) "
            "to event names.\n\n"
            "Required columns:\n"
            "  \u2022 event number \u2014 the event number (e.g. 1, 7, 13)\n"
            "  \u2022 event name   \u2014 the name of the event (e.g. 800m, 4x100 relay)\n\n"
            "Example file contents:\n"
            "  event number,event name\n"
            "  1,800m\n"
            "  7,4x100 relay\n"
            "  13,100m\n"
            "  19,400m\n\n"
            "Column names are case-sensitive.",
        )

    def load_events_csv(self):
        path, _ = QFileDialog.getOpenFileName(
            self,
            "Open Events CSV",
            "",
            "CSV Files (*.csv);;All Files (*.*)",
        )
        if not path:
            return

        try:
            with open(path, "r", newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                fieldnames = {c.strip() for c in reader.fieldnames or []}
                required = {"event number", "event name"}
                if not required.issubset(fieldnames):
                    missing = required - fieldnames
                    QMessageBox.critical(
                        self,
                        "Invalid CSV",
                        f"Missing required column(s): {', '.join(sorted(missing))}\n\n"
                        f"The CSV must have exactly these column headers:\n"
                        f"  event number, event name\n\n"
                        f"Example:\n"
                        f"  event number,event name\n"
                        f"  1,800m\n"
                        f"  7,4x100 relay\n\n"
                        f"Click the ? button next to 'Upload Events' for full details.",
                    )
                    return

                self.event_name_map.clear()
                self.event_meta_map.clear()
                for r in reader:
                    event_num = r.get("event number", "").strip()
                    event_name = r.get("event name", "").strip()
                    if event_num:
                        self.event_name_map[event_num] = event_name
                        self.event_meta_map[event_num] = {
                            "gender": r.get("gender", "").strip(),
                            "age_group": r.get("age group", "").strip(),
                        }

            self.events_csv_label.setText(f"Events CSV: {os.path.basename(path)}")
            self.events_csv_label.setToolTip(path)
            self.status.showMessage(f"Loaded event map with {len(self.event_name_map)} entries")
            # Refresh table so existing rows pick up the new event names
            if self.last_rows:
                self.populate_table(self.last_rows)
        except Exception as e:
            QMessageBox.critical(self, "Load Error", str(e))

    def assign_bib_to_lane(self):
        lane_text = str(self.bib_lane_spin.value())
        bib = self.bib_bib_combo.currentData()
        if not bib:
            QMessageBox.warning(self, "Missing Bib", "Select a bib number first.")
            return

        event_code = f"{self.event_spin.value():03d}"
        heat_code = f"{self.heat_spin.value():02d}"

        info = self.bib_lookup.get(bib, None)
        if info is None:
            QMessageBox.information(self, "Bib Not Found", f"Bib {bib} not found in lookup. Will mark as N/A.")
            info = {"team_name": "N/A", "first_name": "N/A", "last_name": "N/A", "gender": "N/A", "age_group": "N/A"}

        updated = 0
        for row in self.last_rows:
            if row.row_type != "result":
                continue
            if row.event != event_code or row.heat != heat_code or row.lane != lane_text:
                continue

            row.bib = bib
            row.team_name = info.get("team_name", "N/A")
            row.first_name = info.get("first_name", "N/A")
            row.last_name = info.get("last_name", "N/A")
            row.gender = info.get("gender", "N/A")
            row.age_group = info.get("age_group", "N/A")
            updated += 1

        if updated == 0:
            QMessageBox.warning(self, "No Match", f"No rows found for event {event_code} heat {heat_code} lane {lane_text}.")
            return

        self.populate_table(self.last_rows)
        self.status.showMessage(f"Updated {updated} row(s) for bib {bib}")

    def on_table_cell_changed(self, row, column):
        """Handle table cell edits, specifically for bib column auto-population."""
        if column != 5:  # Bib column index
            return

        item = self.table.item(row, column)
        if item is None:
            return

        bib_value = item.text().strip()
        if not bib_value or bib_value == "N/A":
            return

        hidden_types = {"live_time", "heat_header", "event_header", "raw"}
        table_row_index = 0

        for parsed_row in self.last_rows:
            if parsed_row.row_type in hidden_types:
                continue

            if table_row_index == row:
                if parsed_row.row_type != "result":
                    return

                info = self.bib_lookup.get(bib_value, None)
                if info is not None:
                    allowed_groups = self.get_event_allowed_age_groups(int(parsed_row.event))
                    if allowed_groups and info.get("age_group") not in allowed_groups:
                        QMessageBox.warning(
                            self,
                            "Invalid Bib",
                            f"Bib {bib_value} is not valid for event {parsed_row.event}. Allowed age group(s): {', '.join(allowed_groups)}."
                        )
                        return

                if info is None:
                    parsed_row.bib = bib_value
                    parsed_row.team_name = "N/A"
                    parsed_row.first_name = "N/A"
                    parsed_row.last_name = "N/A"
                    parsed_row.gender = "N/A"
                    parsed_row.age_group = "N/A"
                else:
                    parsed_row.bib = bib_value
                    parsed_row.team_name = info.get("team_name", "N/A")
                    parsed_row.first_name = info.get("first_name", "N/A")
                    parsed_row.last_name = info.get("last_name", "N/A")
                    parsed_row.gender = info.get("gender", "N/A")
                    parsed_row.age_group = info.get("age_group", "N/A")

                self.table.blockSignals(True)
                self.table.setItem(row, 5, QTableWidgetItem(parsed_row.team_name))      # Team
                self.table.setItem(row, 6, QTableWidgetItem(parsed_row.bib))            # Bib
                self.table.setItem(row, 7, QTableWidgetItem(parsed_row.first_name))     # First Name
                self.table.setItem(row, 8, QTableWidgetItem(parsed_row.last_name))      # Last Name
                self.table.setItem(row, 9, QTableWidgetItem(parsed_row.gender))         # Gender
                self.table.setItem(row, 10, QTableWidgetItem(parsed_row.age_group))     # Age Group
                self.table.blockSignals(False)
                self._write_session_results_csv()
                return

            table_row_index += 1

    def save_raw_output(self):
        if not self.last_raw_bytes:
            QMessageBox.information(self, "No Data", "No raw data to save.")
            return

        path, _ = QFileDialog.getSaveFileName(
            self,
            "Save Raw Output",
            "time_machine_raw.txt",
            "Text Files (*.txt);;All Files (*.*)",
        )
        if not path:
            return

        try:
            text = raw_bytes_to_hex(self.last_raw_bytes) if self.hex_radio.isChecked() else sanitize_device_bytes(self.last_raw_bytes)
            with open(path, "w", encoding="utf-8") as f:
                f.write(text)
            self.status.showMessage(f"Saved raw output: {path}")
        except Exception as e:
            QMessageBox.critical(self, "Save Error", str(e))

    def clear_results(self):
        self.last_raw_bytes = b""
        self.last_rows = []
        self.live_raw_buffer.clear()
        self.table.setRowCount(0)
        self.raw_text.clear()
        self.status.showMessage("Cleared")

    async def close_async(self):
        if self.live_task and not self.live_task.done():
            self.live_task.cancel()
            try:
                await self.live_task
            except asyncio.CancelledError:
                pass

    def closeEvent(self, event):
        self._save_session_state()
        self._auto_save_csv()
        loop = asyncio.get_event_loop()
        loop.create_task(self.close_async())
        event.accept()

    def _write_session_results_csv(self):
        hidden_types = {"live_time", "heat_header", "event_header", "raw"}
        rows = [r for r in self.last_rows if r.row_type not in hidden_types]
        try:
            with open(self.session_results_csv_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow([
                    "Date", "Event", "Event Type", "Heat", "Lane",
                    "Team", "Bib", "First Name", "Last Name",
                    "Gender", "Age Group", "Place", "Finish\nTime", "Split Time",
                ])
                for row in rows:
                    event_name = self._lookup_event_name(row.event)
                    meta = self._lookup_event_meta(row.event)
                    gender = row.gender if row.gender and row.gender != "N/A" else meta.get("gender", "")
                    age_group = row.age_group if row.age_group and row.age_group != "N/A" else meta.get("age_group", "")
                    writer.writerow([
                        row.date, row.event, event_name or row.event_type,
                        row.heat, row.lane, row.team_name, row.bib,
                        row.first_name, row.last_name, gender, age_group,
                        row.place,
                        f'="{row.cumulative_time}"' if row.cumulative_time else "",
                        f'="{row.split_time}"' if row.split_time else "",
                    ])
        except Exception:
            pass

    def _auto_save_csv(self):
        hidden_types = {"live_time", "heat_header", "event_header", "raw"}
        rows = [r for r in self.last_rows if r.row_type not in hidden_types]
        if not rows:
            return

        try:
            os.makedirs(self.log_session_dir, exist_ok=True)
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            base = self.build_csv_default_name().replace(".csv", "")
            path = os.path.join(self.log_session_dir, f"{base}_{ts}.csv")

            with open(path, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow([
                    "Date", "Event", "Event Type", "Heat", "Lane",
                    "Team", "Bib", "First Name", "Last Name",
                    "Gender", "Age Group", "Place", "Cumulative", "Split Time",
                ])
                for row in rows:
                    meta = self._lookup_event_meta(row.event)
                    event_name = self._lookup_event_name(row.event)
                    gender = row.gender if row.gender and row.gender != "N/A" else meta.get("gender", "")
                    age_group = row.age_group if row.age_group and row.age_group != "N/A" else meta.get("age_group", "")
                    writer.writerow([
                        row.date, row.event, event_name or row.event_type,
                        row.heat, row.lane, row.team_name, row.bib,
                        row.first_name, row.last_name, gender, age_group,
                        row.place,
                        f'="{row.cumulative_time}"' if row.cumulative_time else "",
                        f'="{row.split_time}"' if row.split_time else "",
                    ])
        except Exception:
            pass  # Never block close due to save failure


def main():
    app = QApplication(sys.argv)
    loop = QEventLoop(app)
    asyncio.set_event_loop(loop)

    window = MainWindow()
    window.showMaximized()

    with loop:
        loop.run_forever()


if __name__ == "__main__":
    main()