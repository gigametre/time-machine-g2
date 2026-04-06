import sys
import csv
import time
import re
import os
import asyncio
from datetime import datetime
from dataclasses import dataclass
from typing import List, Optional, Tuple

import serial
from serial.tools import list_ports

from qasync import QEventLoop, asyncSlot
from PyQt5.QtCore import Qt


#TODO: consider using qasync and asyncio for better async handling and cancellation support
#TODO: consider adding a "live parsing" mode that incrementally parses and updates the table as data comes in during live capture, rather than waiting until the end
#TODO: Add a better GUI to graphically show athletes finishing.
#TODO: Add ability to correlate Event # to actual event names by allowing user to load a CSV with event/heat metadata, and then showing that metadata in the table and using it for default CSV export names.
#TODO: Add ability to correlate lane numbers to athlete names by allowing user to load a CSV with event/heat/lane/athlete metadata, and then showing athlete names in the table and using them for CSV export.
from PyQt5.QtWidgets import (
    QAction,
    QApplication,
    QMainWindow,
    QWidget,
    QMessageBox,
    QFileDialog,
    QLabel,
    QPushButton,
    QComboBox,
    QSpinBox,
    QDoubleSpinBox,
    QVBoxLayout,
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
    lap: str
    cumulative_time: str
    split_time: str
    raw_line: str
    team_name: str = "N/A"
    bib: str = "N/A"
    first_name: str = "N/A"
    last_name: str = "N/A"
    gender: str = "N/A"
    age_group: str = "N/A"


HEADER_EVENT_RE = re.compile(r"^EVENT\s+(\d{3})$", re.IGNORECASE)
HEADER_HEAT_RE = re.compile(r"^HEAT\s*(\d{2})$|^HEAT\s+(\d{2})$", re.IGNORECASE)
HEADER_HEAT_ONLY_RE = re.compile(r"^(\d{2})$")
HEADER_HEAT_T_RE = re.compile(r"^T\s*(\d{2})$", re.IGNORECASE)
HEADER_LT_RE = re.compile(r"^LT\s+(\d{2}:\d{2}:\d{2}\.\d{2})$", re.IGNORECASE)
HEADER_DATE_RE = re.compile(r"^DATE\s+(.+)$", re.IGNORECASE)

RESULT_RE = re.compile(
    r"^([0-9A-Z]{1,3})\s+(\d{2})\s+(\d{2}:\d{2}:\d{2}\.\d{2})\s+(\d{2}:\d{2}\.\d{2})$",
    re.IGNORECASE
)


def get_event_type(event_code: str) -> str:
    """Convert event code to human-readable event type."""
    try:
        event_num = int(event_code)
    except ValueError:
        # Handle non-numeric event codes (like relay codes)
        if not event_code.isdigit():
            return "4x100 relay"
        return event_code

    # Define event type mapping by ranges
    if 1 <= event_num <= 4:
        return "800m"
    elif 5 <= event_num <= 8:
        return "4x100 relay"
    elif 9 <= event_num <= 12:
        return "100m"
    elif 13 <= event_num <= 16:
        return "1 mile"
    elif 17 <= event_num <= 20:
        return "400m"
    elif 21 <= event_num <= 24:
        return "200m"
    else:
        # Fallback for events outside the defined ranges
        return f"Event {event_num}"


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
            lap = m.group(2)
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
                    lap,
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

    def close(self):
        if self.ser and self.ser.is_open:
            self.ser.close()

    def _write_slow(self, data: bytes):
        for b in data:
            self.ser.write(bytes([b]))
            self.ser.flush()
            time.sleep(self.inter_byte_delay)

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


# -----------------------------
# GUI
# -----------------------------
class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Time Machine Downloader")
        self.resize(1700, 950)

        self.last_raw_bytes = b""
        self.last_rows: List[ParsedRow] = []
        self.live_raw_buffer = bytearray()

        self.live_text_buffer = ""
        self.live_current_event = ""
        self.live_current_heat = ""
        self.live_current_date = ""
        self.live_saw_live_time = False

        self.bib_lookup = {}  # bib -> {'first_name', 'last_name', 'gender', 'age_group', 'team_name'}

        self.live_task: Optional[asyncio.Task] = None
        self.download_task: Optional[asyncio.Task] = None

        self.log_session_dir = self.create_log_session_dir()
        self.log_file_path = self.create_session_log_file()
        self.live_capture_log_path = self.create_live_capture_log_file()

        self._build_ui()
        self.refresh_ports()
        self.status.showMessage(f"Logging to {self.log_session_dir}")

    # -----------------------------
    # Logging
    # -----------------------------
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
    # UI
    # -----------------------------
    def _build_ui(self):
        self._build_toolbar()

        central = QWidget()
        self.setCentralWidget(central)
        outer = QVBoxLayout(central)

        split = QSplitter(Qt.Horizontal)
        outer.addWidget(split)

        left = QWidget()
        left_layout = QVBoxLayout(left)

        conn_group = QGroupBox("Connection")
        conn_grid = QGridLayout(conn_group)

        self.port_combo = QComboBox()
        self.refresh_ports_btn = QPushButton("Refresh")
        self.baud_combo = QComboBox()
        self.baud_combo.addItems(["9600", "19200", "38400", "57600", "115200"])
        self.baud_combo.setCurrentText("9600")

        conn_grid.addWidget(QLabel("COM Port"), 0, 0)
        conn_grid.addWidget(self.port_combo, 0, 1)
        conn_grid.addWidget(self.refresh_ports_btn, 0, 2)
        conn_grid.addWidget(QLabel("Baud"), 1, 0)
        conn_grid.addWidget(self.baud_combo, 1, 1)

        dl_group = QGroupBox("Retransmit Download")
        dl_grid = QGridLayout(dl_group)

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
        self.read_seconds_spin.setValue(5.0)

        self.download_btn = QPushButton("Download Selected Event / Heat")

        self.set_event_heat_btn = QPushButton("Set Event/Heat")

        dl_grid.addWidget(QLabel("Event"), 0, 0)
        dl_grid.addWidget(self.event_spin, 0, 1)
        dl_grid.addWidget(QLabel("Heat"), 1, 0)
        dl_grid.addWidget(self.heat_spin, 1, 1)
        dl_grid.addWidget(QLabel("Read Seconds"), 2, 0)
        dl_grid.addWidget(self.read_seconds_spin, 2, 1)
        dl_grid.addWidget(self.set_event_heat_btn, 3, 0, 1, 2)
        dl_grid.addWidget(self.download_btn, 4, 0, 1, 2)

        bib_group = QGroupBox("Bib Lookup / Assignment")
        bib_grid = QGridLayout(bib_group)

        self.load_bib_csv_btn = QPushButton("Load Bib CSV")
        self.bib_csv_label = QLabel("No CSV loaded")
        self.bib_allowed_label = QLabel("Allowed age groups: N/A")
        self.bib_lane_spin = QSpinBox()
        self.bib_lane_spin.setRange(1, 99)
        self.bib_bib_combo = QComboBox()
        self.bib_bib_combo.setEnabled(False)
        self.bib_bib_combo.addItem("Load bib CSV first", "")
        self.bib_assign_btn = QPushButton("Assign Bib to Lane")

        bib_grid.addWidget(self.load_bib_csv_btn, 0, 0, 1, 2)
        bib_grid.addWidget(self.bib_csv_label, 1, 0, 1, 2)
        bib_grid.addWidget(self.bib_allowed_label, 2, 0, 1, 2)
        bib_grid.addWidget(QLabel("Lane"), 3, 0)
        bib_grid.addWidget(self.bib_lane_spin, 3, 1)
        bib_grid.addWidget(QLabel("Bib"), 4, 0)
        bib_grid.addWidget(self.bib_bib_combo, 4, 1)
        bib_grid.addWidget(self.bib_assign_btn, 5, 0, 1, 2)

        live_group = QGroupBox("Live Capture")
        live_grid = QGridLayout(live_group)

        self.live_start_btn = QPushButton("Start Live Capture")
        self.live_stop_btn = QPushButton("Stop Live Capture")
        self.live_stop_btn.setEnabled(False)

        live_grid.addWidget(self.live_start_btn, 0, 0)
        live_grid.addWidget(self.live_stop_btn, 0, 1)

        raw_group = QGroupBox("Raw Display Mode")
        raw_layout = QVBoxLayout(raw_group)

        self.cleaned_ascii_radio = QRadioButton("Cleaned ASCII")
        self.cleaned_ascii_radio.setChecked(True)
        self.hex_radio = QRadioButton("Hex")
        self.wrap_check = QCheckBox("Wrap")
        self.wrap_check.setChecked(False)

        self.raw_mode_group = QButtonGroup(self)
        self.raw_mode_group.addButton(self.cleaned_ascii_radio)
        self.raw_mode_group.addButton(self.hex_radio)

        raw_layout.addWidget(self.cleaned_ascii_radio)
        raw_layout.addWidget(self.hex_radio)
        raw_layout.addWidget(self.wrap_check)

        left_layout.addWidget(conn_group)
        left_layout.addWidget(dl_group)
        left_layout.addWidget(bib_group)
        left_layout.addWidget(live_group)
        left_layout.addWidget(raw_group)
        left_layout.addStretch(1)

        right = QWidget()
        right_layout = QVBoxLayout(right)

        right_split = QSplitter(Qt.Vertical)

        table_group = QGroupBox("Parsed Results")
        table_layout = QVBoxLayout(table_group)

        self.table = QTableWidget(0, 14)
        self.table.setHorizontalHeaderLabels(
            ["Date", "Event", "Event Type", "Heat", "Lane", "Team", "Bib", "First Name", "Last Name", "Gender", "Age Group", "Lap", "Cumulative", "Lap Time"]
        )
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.table.setSortingEnabled(True)
        self.table.setEditTriggers(QTableWidget.DoubleClicked | QTableWidget.EditKeyPressed | QTableWidget.AnyKeyPressed)
        self.table.cellChanged.connect(self.on_table_cell_changed)
        table_layout.addWidget(self.table)

        raw_out_group = QGroupBox("Raw Output")
        raw_out_layout = QVBoxLayout(raw_out_group)
        self.raw_text = QTextEdit()
        self.raw_text.setReadOnly(True)
        self.raw_text.setLineWrapMode(QTextEdit.NoWrap)
        raw_out_layout.addWidget(self.raw_text)

        right_split.addWidget(table_group)
        right_split.addWidget(raw_out_group)
        right_split.setSizes([650, 220])

        right_layout.addWidget(right_split)

        split.addWidget(left)
        split.addWidget(right)
        split.setSizes([300, 1400])

        self.status = QStatusBar()
        self.setStatusBar(self.status)

        self.refresh_ports_btn.clicked.connect(self.refresh_ports)
        self.cleaned_ascii_radio.toggled.connect(self.update_raw_view)
        self.hex_radio.toggled.connect(self.update_raw_view)
        self.wrap_check.toggled.connect(self.update_wrap_mode)

        self.load_bib_csv_btn.clicked.connect(self.load_bib_csv)
        self.bib_assign_btn.clicked.connect(self.assign_bib_to_lane)
        self.event_spin.valueChanged.connect(self.on_event_selection_changed)

        self.set_event_heat_btn.clicked.connect(self.set_event_heat_selected)
        self.download_btn.clicked.connect(self.download_selected)
        self.live_start_btn.clicked.connect(self.on_live_start_clicked)
        self.live_stop_btn.clicked.connect(self.on_live_stop_clicked)

    def on_live_start_clicked(self):
        self.start_live_capture()

    def on_live_stop_clicked(self):
        self.stop_live_capture()

    def _build_toolbar(self):
        toolbar = QToolBar("Main")
        self.addToolBar(toolbar)

        save_csv_action = QAction("Save Table CSV", self)
        save_csv_action.triggered.connect(self.save_table_to_csv)
        toolbar.addAction(save_csv_action)

        save_raw_action = QAction("Save Raw Output", self)
        save_raw_action.triggered.connect(self.save_raw_output)
        toolbar.addAction(save_raw_action)

        clear_action = QAction("Clear", self)
        clear_action.triggered.connect(self.clear_results)
        toolbar.addAction(clear_action)

    # -----------------------------
    # Port helpers
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
            if current:
                for i in range(self.port_combo.count()):
                    if self.port_combo.itemData(i) == current:
                        self.port_combo.setCurrentIndex(i)
                        break
                else:
                    self.port_combo.setCurrentIndex(0)
            else:
                self.port_combo.setCurrentIndex(0)

        self.status.showMessage("COM ports refreshed")

    def get_event_allowed_age_groups(self, event_num: int) -> List[str]:
        if event_num < 1:
            return []

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

    async def _read_available_once(self, client: TimeMachineClient) -> bytes:
        waiting = await asyncio.to_thread(lambda: client.ser.in_waiting)
        if waiting:
            return await asyncio.to_thread(client.ser.read, waiting)
        return b""

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

        if self.live_task and not self.live_task.done():
            QMessageBox.warning(self, "Live Capture Active", "Stop live capture before doing a retransmit download.")
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
            if client is not None:
                await asyncio.to_thread(client.close)
            self.download_btn.setEnabled(True)

    @asyncSlot()
    async def set_event_heat_selected(self):
        port = self.selected_port()
        if not port:
            QMessageBox.warning(self, "No COM Port", "Please select a COM port.")
            return

        if self.live_task and not self.live_task.done():
            QMessageBox.warning(self, "Live Capture Active", "Stop live capture before setting event/heat.")
            return

        event_num = self.event_spin.value()
        heat_num = self.heat_spin.value()

        self.status.showMessage(f"Setting event {event_num}, heat {heat_num}...")

        client = None
        try:
            client = await self._open_client(port, self.selected_baud(), 0.2)
            await asyncio.to_thread(client.set_event_heat, event_num, heat_num)
            self.status.showMessage(f"Set event {event_num}, heat {heat_num} command sent")
        except Exception as e:
            QMessageBox.critical(self, "Set Event/Heat Error", str(e))
            self.status.showMessage("Failed to set event/heat")
        finally:
            if client is not None:
                await asyncio.to_thread(client.close)

    @asyncSlot()
    async def start_live_capture(self):
        port = self.selected_port()
        if not port:
            QMessageBox.warning(self, "No COM Port", "Please select a COM port.")
            return

        if self.live_task and not self.live_task.done():
            return

        self.live_raw_buffer.clear()
        self.last_raw_bytes = b""
        self.last_rows = []
        self.live_text_buffer = ""
        self.live_current_event = ""
        self.live_current_heat = ""
        self.live_current_date = ""
        self.live_saw_live_time = False
        self.populate_table([])
        self.update_raw_view()

        self.append_live_capture_start_log()

        self.live_start_btn.setEnabled(False)
        self.live_stop_btn.setEnabled(True)
        self.download_btn.setEnabled(False)

        client = None
        try:
            client = await self._open_client(port, self.selected_baud(), 0.1)

            async def runner():
                try:
                    await self._live_capture_loop(client)
                finally:
                    if client is not None:
                        await asyncio.to_thread(client.close)

            self.live_task = asyncio.create_task(runner())
        except Exception as e:
            if client is not None:
                await asyncio.to_thread(client.close)
            self.live_start_btn.setEnabled(True)
            self.live_stop_btn.setEnabled(False)
            self.download_btn.setEnabled(True)
            QMessageBox.critical(self, "Live Capture Error", str(e))

    @asyncSlot()
    async def stop_live_capture(self):
        if self.live_task and not self.live_task.done():
            self.live_task.cancel()
            try:
                await self.live_task
            except asyncio.CancelledError:
                pass

        self.append_live_capture_stop_log()

        self.live_start_btn.setEnabled(True)
        self.live_stop_btn.setEnabled(False)
        self.download_btn.setEnabled(True)

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
        hidden_types = {"live_time", "heat_header", "event_header", "raw"}
        if row.row_type in hidden_types:
            return

        self.table.setSortingEnabled(False)

        r = self.table.rowCount()
        self.table.insertRow(r)

        values = [
            row.date,
            row.event,
            row.event_type,
            row.heat,
            row.lane,
            row.team_name,
            row.bib,
            row.first_name,
            row.last_name,
            row.gender,
            row.age_group,
            row.lap,
            row.cumulative_time,
            row.split_time,
        ]

        for c, value in enumerate(values):
            item = QTableWidgetItem(value)
            if c in (1, 2, 4):
                item.setTextAlignment(Qt.AlignCenter)
            if c == 6:  # Bib column - make editable
                item.setFlags(item.flags() | Qt.ItemIsEditable)
            self.table.setItem(r, c, item)

        self.table.setSortingEnabled(True)

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
        m = HEADER_LT_RE.match(line)
        if m:
            self.live_saw_live_time = True
            return

        m = HEADER_EVENT_RE.match(line)
        if m:
            self.live_current_event = m.group(1)
            return

        m = HEADER_DATE_RE.match(line)
        if m:
            self.live_current_date = m.group(1).strip()
            return

        m = HEADER_HEAT_RE.match(line)
        if m:
            self.live_current_heat = m.group(1) or m.group(2)
            return

        m = HEADER_HEAT_T_RE.match(line)
        if m:
            self.live_current_heat = m.group(1)
            return

        m = HEADER_HEAT_ONLY_RE.match(line)
        if m and self.live_saw_live_time:
            self.live_current_heat = m.group(1)
            return

        m = RESULT_RE.match(line)
        if m:
            lane = m.group(1)
            lap = m.group(2)
            cumulative = m.group(3)
            split = m.group(4)

            row_date = self.live_current_date or get_local_date_string()

            row = ParsedRow(
                "result",
                self.live_current_event,
                get_event_type(self.live_current_event),
                self.live_current_heat,
                row_date,
                lane,
                lap,
                cumulative,
                split,
                line,
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
        self.update_raw_view()

    # -----------------------------
    # Table / raw view
    # -----------------------------
    def populate_table(self, rows: List[ParsedRow]):
        self.table.setSortingEnabled(False)
        self.table.setRowCount(0)

        hidden_types = {"live_time", "heat_header", "event_header", "raw"}

        for row in rows:
            if row.row_type in hidden_types:
                continue

            r = self.table.rowCount()
            self.table.insertRow(r)

            values = [
                row.date,
                row.event,
                row.event_type,
                row.heat,
                row.lane,
                row.team_name,
                row.bib,
                row.first_name,
                row.last_name,
                row.gender,
                row.age_group,
                row.lap,
                row.cumulative_time,
                row.split_time,
            ]

            for c, value in enumerate(values):
                item = QTableWidgetItem(value)
                if c in (1, 2, 4):
                    item.setTextAlignment(Qt.AlignCenter)
                if c == 6:  # Bib column - make editable
                    item.setFlags(item.flags() | Qt.ItemIsEditable)
                self.table.setItem(r, c, item)

        self.table.setSortingEnabled(True)
        self.table.resizeColumnsToContents()

    def update_raw_view(self):
        scrollbar = self.raw_text.verticalScrollBar()
        was_at_bottom = scrollbar.value() >= (scrollbar.maximum() - 4)

        if self.hex_radio.isChecked():
            self.raw_text.setPlainText(raw_bytes_to_hex(self.last_raw_bytes))
        else:
            self.raw_text.setPlainText(sanitize_device_bytes(self.last_raw_bytes))

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
            self.build_csv_default_name(),
            "CSV Files (*.csv)",
        )
        if not path:
            return

        hidden_types = {"live_time", "heat_header", "event_header", "raw"}

        try:
            with open(path, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(["Type", "Event", "Event Type", "Heat", "Date", "Lane", "Team", "Bib", "First Name", "Last Name", "Gender", "Age Group", "Lap", "Cumulative", "Lap Time", "Raw Line"])
                for row in self.last_rows:
                    if row.row_type in hidden_types:
                        continue
                    writer.writerow([
                        row.row_type,
                        row.event,
                        row.event_type,
                        row.heat,
                        row.date,
                        row.lane,
                        row.team_name,
                        row.bib,
                        row.first_name,
                        row.last_name,
                        row.gender,
                        row.age_group,
                        row.lap,
                        row.cumulative_time,
                        row.split_time,
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
            self.status.showMessage(f"Loaded bib map with {len(self.bib_lookup)} entries")
            self.update_bib_dropdown_options()
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
        if column != 6:  # Bib column index
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
        loop = asyncio.get_event_loop()
        loop.create_task(self.close_async())
        event.accept()


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