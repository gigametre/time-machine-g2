import sys
import csv
import time
import re
from dataclasses import dataclass
from typing import List, Optional, Tuple

import serial
from serial.tools import list_ports

from PyQt6.QtCore import Qt, QThread, pyqtSignal
from PyQt6.QtGui import QAction
from PyQt6.QtWidgets import (
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
    QHBoxLayout,
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
)


# -----------------------------
# Backend client
# -----------------------------
class TimeMachineClient:
    """
    Serial client for Time Machine device.
    Uses the same command structure as your existing class.
    """

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

    def send_xon(self):
        self._write_slow(bytes([0x11]))

    def send_xoff(self):
        self._write_slow(bytes([0x13]))

    def halt_retransmit(self):
        self._write_slow(bytes([0x17]))

    def set_event_heat(self, event_num: int, heat_num: int):
        if not (0 <= event_num <= 255):
            raise ValueError("event_num must be 0..255")
        if not (0 <= heat_num <= 99):
            raise ValueError("heat_num must be 0..99")

        cmd = bytes([0x06]) + f"{event_num:03d}{heat_num:02d}".encode("ascii") + b"\r\n"
        self._write_slow(cmd)

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

    def read_available_bytes(self, duration: float = 3.0) -> bytes:
        deadline = time.time() + duration
        out = bytearray()

        while time.time() < deadline:
            waiting = self.ser.in_waiting
            if waiting:
                out.extend(self.ser.read(waiting))
            else:
                time.sleep(0.01)

        return bytes(out)

    def download_memory_bytes(
        self,
        event_num: int = 0,
        heat_num: int = 0,
        start_time: Optional[str] = None,
        read_seconds: float = 5.0,
    ) -> bytes:
        self.retransmit(event_num=event_num, heat_num=heat_num, start_time=start_time)
        return self.read_available_bytes(duration=read_seconds)


# -----------------------------
# Parsing / sanitizing
# -----------------------------
@dataclass
class ParsedRow:
    row_type: str
    event: str
    heat: str
    lane: str
    place: str
    cumulative_time: str
    split_time: str
    raw_line: str


CONTROL_KEEP = {0x09, 0x0A, 0x0D}  # tab, LF, CR
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
    """
    Remove control bytes that are likely protocol noise while preserving CR/LF/TAB.
    This prevents things like DC1/XON (0x11) from showing up as stray 'Q'-like artifacts.
    """
    cleaned = bytearray()
    for b in raw:
        if b in CONTROL_KEEP:
            cleaned.append(b)
        elif 32 <= b <= 126:
            cleaned.append(b)
        elif b in CONTROL_DROP:
            continue
        else:
            # Other extended bytes are dropped from the cleaned text view.
            continue

    text = cleaned.decode("ascii", errors="ignore")
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    return text


def raw_bytes_to_hex(raw: bytes) -> str:
    return " ".join(f"{b:02X}" for b in raw)


def clean_lines_for_parsing(text: str) -> List[str]:
    lines = []
    for line in text.split("\n"):
        s = line.strip()
        if s:
            lines.append(s)
    return lines


HEADER_EVENT_RE = re.compile(r"^EVENT\s+(\d{3})$", re.IGNORECASE)
HEADER_HEAT_RE = re.compile(r"^HEAT\s+(\d{2})$", re.IGNORECASE)
HEADER_LT_RE = re.compile(r"^LT\s+(\d{2}:\d{2}:\d{2}\.\d{2})$", re.IGNORECASE)
HEADER_DATE_RE = re.compile(r"^DATE\s+(.+)$", re.IGNORECASE)

# Example:
# 01   01 00:00:03.09  00:03.09
RESULT_RE = re.compile(
    r"^([0-9A-Z]{1,3})\s+(\d{2})\s+(\d{2}:\d{2}:\d{2}\.\d{2})\s+(\d{2}:\d{2}\.\d{2})$",
    re.IGNORECASE
)


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

    for line in lines:
        m = HEADER_LT_RE.match(line)
        if m:
            meta["lt"] = m.group(1)
            rows.append(ParsedRow("live_time", current_event, current_heat, "", "", m.group(1), "", line))
            continue

        m = HEADER_DATE_RE.match(line)
        if m:
            meta["date"] = m.group(1)
            rows.append(ParsedRow("date", current_event, current_heat, "", "", "", "", line))
            continue

        m = HEADER_EVENT_RE.match(line)
        if m:
            current_event = m.group(1)
            meta["event"] = current_event
            rows.append(ParsedRow("event_header", current_event, current_heat, "", "", "", "", line))
            continue

        m = HEADER_HEAT_RE.match(line)
        if m:
            current_heat = m.group(1)
            meta["heat"] = current_heat
            rows.append(ParsedRow("heat_header", current_event, current_heat, "", "", "", "", line))
            continue

        if line.upper().startswith("START OF RETRANSMIT"):
            rows.append(ParsedRow("marker", current_event, current_heat, "", "", "", "", line))
            continue

        if line.upper().startswith("END OF RETRANSMIT"):
            rows.append(ParsedRow("marker", current_event, current_heat, "", "", "", "", line))
            continue

        m = RESULT_RE.match(line)
        if m:
            lane_or_id = m.group(1)
            place = m.group(2)
            cumulative = m.group(3)
            split = m.group(4)
            rows.append(
                ParsedRow(
                    "result",
                    current_event,
                    current_heat,
                    lane_or_id,
                    place,
                    cumulative,
                    split,
                    line,
                )
            )
            continue

        # Keep unknown lines visible instead of losing them
        rows.append(ParsedRow("raw", current_event, current_heat, "", "", "", "", line))

    return rows, meta


# -----------------------------
# Worker threads
# -----------------------------
class DownloadWorker(QThread):
    finished_ok = pyqtSignal(bytes, list, dict)
    failed = pyqtSignal(str)

    def __init__(self, port: str, baud: int, event_num: int, heat_num: int, read_seconds: float):
        super().__init__()
        self.port = port
        self.baud = baud
        self.event_num = event_num
        self.heat_num = heat_num
        self.read_seconds = read_seconds

    def run(self):
        client = None
        try:
            client = TimeMachineClient(
                port=self.port,
                baudrate=self.baud,
                timeout=0.2,
                inter_byte_delay=0.01,
            )
            raw = client.download_memory_bytes(
                event_num=self.event_num,
                heat_num=self.heat_num,
                start_time=None,
                read_seconds=self.read_seconds,
            )
            cleaned_text = sanitize_device_bytes(raw)
            rows, meta = parse_time_machine_text(cleaned_text)
            self.finished_ok.emit(raw, rows, meta)
        except Exception as e:
            self.failed.emit(str(e))
        finally:
            if client is not None:
                try:
                    client.close()
                except Exception:
                    pass


class LiveCaptureWorker(QThread):
    chunk_received = pyqtSignal(bytes)
    failed = pyqtSignal(str)
    state_changed = pyqtSignal(str)

    def __init__(self, port: str, baud: int):
        super().__init__()
        self.port = port
        self.baud = baud
        self._running = True

    def stop(self):
        self._running = False

    def run(self):
        client = None
        try:
            client = TimeMachineClient(
                port=self.port,
                baudrate=self.baud,
                timeout=0.1,
                inter_byte_delay=0.01,
            )
            self.state_changed.emit("Live capture running")

            while self._running:
                waiting = client.ser.in_waiting
                if waiting:
                    data = client.ser.read(waiting)
                    if data:
                        self.chunk_received.emit(data)
                else:
                    time.sleep(0.01)

        except Exception as e:
            self.failed.emit(str(e))
        finally:
            if client is not None:
                try:
                    client.close()
                except Exception:
                    pass
            self.state_changed.emit("Live capture stopped")


# -----------------------------
# GUI
# -----------------------------
class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Time Machine Downloader")
        self.resize(1250, 800)

        self.last_raw_bytes = b""
        self.last_rows: List[ParsedRow] = []
        self.live_raw_buffer = bytearray()

        self.download_worker: Optional[DownloadWorker] = None
        self.live_worker: Optional[LiveCaptureWorker] = None

        self._build_ui()
        self.refresh_ports()

    def _build_ui(self):
        self._build_toolbar()

        central = QWidget()
        self.setCentralWidget(central)
        outer = QVBoxLayout(central)

        split = QSplitter(Qt.Orientation.Horizontal)
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
        self.event_spin.setRange(0, 255)
        self.event_spin.setValue(1)

        self.heat_spin = QSpinBox()
        self.heat_spin.setRange(0, 99)
        self.heat_spin.setValue(1)

        self.read_seconds_spin = QDoubleSpinBox()
        self.read_seconds_spin.setRange(0.5, 60.0)
        self.read_seconds_spin.setSingleStep(0.5)
        self.read_seconds_spin.setDecimals(1)
        self.read_seconds_spin.setValue(5.0)

        self.download_btn = QPushButton("Download Selected Event / Heat")

        dl_grid.addWidget(QLabel("Event"), 0, 0)
        dl_grid.addWidget(self.event_spin, 0, 1)
        dl_grid.addWidget(QLabel("Heat"), 1, 0)
        dl_grid.addWidget(self.heat_spin, 1, 1)
        dl_grid.addWidget(QLabel("Read Seconds"), 2, 0)
        dl_grid.addWidget(self.read_seconds_spin, 2, 1)
        dl_grid.addWidget(self.download_btn, 3, 0, 1, 2)

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
        left_layout.addWidget(live_group)
        left_layout.addWidget(raw_group)
        left_layout.addStretch(1)

        right = QWidget()
        right_layout = QVBoxLayout(right)

        right_split = QSplitter(Qt.Orientation.Vertical)

        table_group = QGroupBox("Parsed Results")
        table_layout = QVBoxLayout(table_group)

        self.table = QTableWidget(0, 8)
        self.table.setHorizontalHeaderLabels(
            ["Type", "Event", "Heat", "Lane/ID", "Place", "Cumulative", "Split", "Raw Line"]
        )
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Interactive)
        self.table.horizontalHeader().setStretchLastSection(True)
        self.table.setSortingEnabled(True)
        table_layout.addWidget(self.table)

        raw_out_group = QGroupBox("Raw Output")
        raw_out_layout = QVBoxLayout(raw_out_group)
        self.raw_text = QTextEdit()
        self.raw_text.setReadOnly(True)
        self.raw_text.setLineWrapMode(QTextEdit.LineWrapMode.NoWrap)
        raw_out_layout.addWidget(self.raw_text)

        right_split.addWidget(table_group)
        right_split.addWidget(raw_out_group)
        right_split.setSizes([450, 280])

        right_layout.addWidget(right_split)

        split.addWidget(left)
        split.addWidget(right)
        split.setSizes([320, 900])

        self.status = QStatusBar()
        self.setStatusBar(self.status)
        self.status.showMessage("Ready")

        self.refresh_ports_btn.clicked.connect(self.refresh_ports)
        self.download_btn.clicked.connect(self.download_selected)
        self.live_start_btn.clicked.connect(self.start_live_capture)
        self.live_stop_btn.clicked.connect(self.stop_live_capture)
        self.cleaned_ascii_radio.toggled.connect(self.update_raw_view)
        self.hex_radio.toggled.connect(self.update_raw_view)
        self.wrap_check.toggled.connect(self.update_wrap_mode)

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

    def refresh_ports(self):
        current = self.selected_port()
        self.port_combo.clear()

        ports = list(list_ports.comports())
        for p in ports:
            self.port_combo.addItem(f"{p.device} — {p.description}", p.device)

        if not ports:
            self.port_combo.addItem("No ports found", "")

        if current:
            for i in range(self.port_combo.count()):
                if self.port_combo.itemData(i) == current:
                    self.port_combo.setCurrentIndex(i)
                    break

        self.status.showMessage("COM ports refreshed")

    def selected_port(self) -> str:
        val = self.port_combo.currentData()
        return val if isinstance(val, str) else ""

    def selected_baud(self) -> int:
        return int(self.baud_combo.currentText())

    def download_selected(self):
        port = self.selected_port()
        if not port:
            QMessageBox.warning(self, "No COM Port", "Please select a COM port.")
            return

        if self.live_worker is not None and self.live_worker.isRunning():
            QMessageBox.warning(self, "Live Capture Active", "Stop live capture before doing a retransmit download.")
            return

        self.download_btn.setEnabled(False)

        event_num = self.event_spin.value()
        heat_num = self.heat_spin.value()
        read_seconds = self.read_seconds_spin.value()

        self.status.showMessage(f"Downloading event {event_num}, heat {heat_num}...")

        self.download_worker = DownloadWorker(
            port=port,
            baud=self.selected_baud(),
            event_num=event_num,
            heat_num=heat_num,
            read_seconds=read_seconds,
        )
        self.download_worker.finished_ok.connect(self.on_download_ok)
        self.download_worker.failed.connect(self.on_download_failed)
        self.download_worker.start()

    def on_download_ok(self, raw: bytes, rows: list, meta: dict):
        self.last_raw_bytes = raw
        self.last_rows = rows

        self.populate_table(rows)
        self.update_raw_view()

        self.download_btn.setEnabled(True)
        self.status.showMessage(
            f"Download complete: event {meta.get('event','')}, heat {meta.get('heat','')}, {len(raw)} bytes"
        )

    def on_download_failed(self, msg: str):
        self.download_btn.setEnabled(True)
        self.status.showMessage("Download failed")
        QMessageBox.critical(self, "Download Error", msg)

    def start_live_capture(self):
        port = self.selected_port()
        if not port:
            QMessageBox.warning(self, "No COM Port", "Please select a COM port.")
            return

        if self.live_worker is not None and self.live_worker.isRunning():
            return

        self.live_raw_buffer.clear()
        self.last_raw_bytes = b""
        self.last_rows = []
        self.populate_table([])
        self.update_raw_view()

        self.live_worker = LiveCaptureWorker(port=port, baud=self.selected_baud())
        self.live_worker.chunk_received.connect(self.on_live_chunk)
        self.live_worker.failed.connect(self.on_live_failed)
        self.live_worker.state_changed.connect(self.status.showMessage)
        self.live_worker.start()

        self.live_start_btn.setEnabled(False)
        self.live_stop_btn.setEnabled(True)
        self.download_btn.setEnabled(False)

    def stop_live_capture(self):
        if self.live_worker is not None:
            self.live_worker.stop()
            self.live_worker.wait(1000)

        self.live_start_btn.setEnabled(True)
        self.live_stop_btn.setEnabled(False)
        self.download_btn.setEnabled(True)

        # Final parse after stop
        self.last_raw_bytes = bytes(self.live_raw_buffer)
        cleaned = sanitize_device_bytes(self.last_raw_bytes)
        rows, _ = parse_time_machine_text(cleaned)
        self.last_rows = rows
        self.populate_table(rows)
        self.update_raw_view()

    def on_live_chunk(self, data: bytes):
        self.live_raw_buffer.extend(data)
        self.last_raw_bytes = bytes(self.live_raw_buffer)

        cleaned = sanitize_device_bytes(self.last_raw_bytes)
        rows, _ = parse_time_machine_text(cleaned)
        self.last_rows = rows

        self.populate_table(rows)
        self.update_raw_view()

    def on_live_failed(self, msg: str):
        self.live_start_btn.setEnabled(True)
        self.live_stop_btn.setEnabled(False)
        self.download_btn.setEnabled(True)
        self.status.showMessage("Live capture failed")
        QMessageBox.critical(self, "Live Capture Error", msg)

    def populate_table(self, rows: List[ParsedRow]):
        self.table.setSortingEnabled(False)
        self.table.setRowCount(0)

        for row in rows:
            r = self.table.rowCount()
            self.table.insertRow(r)

            values = [
                row.row_type,
                row.event,
                row.heat,
                row.lane,
                row.place,
                row.cumulative_time,
                row.split_time,
                row.raw_line,
            ]

            for c, value in enumerate(values):
                item = QTableWidgetItem(value)
                if c in (1, 2, 4):
                    item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                self.table.setItem(r, c, item)

        self.table.setSortingEnabled(True)
        self.table.resizeColumnsToContents()

    def update_raw_view(self):
        if self.hex_radio.isChecked():
            self.raw_text.setPlainText(raw_bytes_to_hex(self.last_raw_bytes))
        else:
            self.raw_text.setPlainText(sanitize_device_bytes(self.last_raw_bytes))

    def update_wrap_mode(self):
        if self.wrap_check.isChecked():
            self.raw_text.setLineWrapMode(QTextEdit.LineWrapMode.WidgetWidth)
        else:
            self.raw_text.setLineWrapMode(QTextEdit.LineWrapMode.NoWrap)

    def save_table_to_csv(self):
        if not self.last_rows:
            QMessageBox.information(self, "No Data", "No table data to save.")
            return

        path, _ = QFileDialog.getSaveFileName(
            self,
            "Save Table CSV",
            "time_machine_results.csv",
            "CSV Files (*.csv)",
        )
        if not path:
            return

        try:
            with open(path, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(["Type", "Event", "Heat", "Lane/ID", "Place", "Cumulative", "Split", "Raw Line"])
                for row in self.last_rows:
                    writer.writerow([
                        row.row_type,
                        row.event,
                        row.heat,
                        row.lane,
                        row.place,
                        row.cumulative_time,
                        row.split_time,
                        row.raw_line,
                    ])
            self.status.showMessage(f"Saved CSV: {path}")
        except Exception as e:
            QMessageBox.critical(self, "Save Error", str(e))

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

    def closeEvent(self, event):
        try:
            if self.live_worker is not None and self.live_worker.isRunning():
                self.live_worker.stop()
                self.live_worker.wait(1000)
        except Exception:
            pass
        event.accept()


def main():
    app = QApplication(sys.argv)
    window = MainWindow()
    window.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()