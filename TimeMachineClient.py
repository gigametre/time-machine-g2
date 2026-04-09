import serial
import time
from typing import Optional
from logging_utils import SessionLogger


class TimeMachineClient:
    """
    Client for the legacy Time Machine RS232 protocol.

    This is likely a good starting point for Time Machine G2 units that expose
    a COM port, but the exact G2 protocol should be verified against vendor docs.
    """

    def __init__(
        self,
        port: str,
        baudrate: int = 9600,
        bytesize: int = serial.EIGHTBITS,
        parity: str = serial.PARITY_NONE,
        stopbits: int = serial.STOPBITS_ONE,
        timeout: float = 1.0,
        inter_byte_delay: float = 0.01,
        logger: Optional[SessionLogger] = None,
    ):
        self.logger = logger
        if self.logger:
            self.logger.info(f"Initializing TimeMachineClient on {port}", component="serial_io")
        
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
        """
        Send bytes with a small delay between them.
        The old manual suggests command characters may need spacing.
        """
        if self.logger:
            self.logger.debug(f"TX: {data.hex()} | {repr(data)}", component="serial_io")
        for b in data:
            self.ser.write(bytes([b]))
            self.ser.flush()
            time.sleep(self.inter_byte_delay)

    def send_xon(self):
        # Ctrl-Q
        self._write_slow(bytes([0x11]))

    def send_xoff(self):
        # Ctrl-S
        self._write_slow(bytes([0x13]))

    def halt_retransmit(self):
        # Ctrl-W
        self._write_slow(bytes([0x17]))

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

    def retransmit(self, event_num: int = 0, heat_num: int = 0, start_time: str | None = None):
        """
        Retransmit command based on legacy manual:
          0x05 + EEE + HH + CRLF
        or
          0x05 + EEE + HH + 0x15 + HHMMSS + CRLF

        event_num=0 means all events
        heat_num=0 means all chutes/heats

        start_time format: 'HHMMSS' or None
        """
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

    def read_available(self, duration: float = 3.0) -> bytes:
        """
        Read everything the device sends for a fixed time window.
        """
        deadline = time.time() + duration
        out = bytearray()

        waiting = 1

        while (time.time() < deadline):
            waiting = self.ser.in_waiting
            if waiting:
                out.extend(self.ser.read(waiting))
            else:
                time.sleep(0.02)

        return bytes(out)

    def download_memory(self, event_num: int = 0, heat_num: int = 0, start_time: str | None = None,
                        read_seconds: float = 5.0) -> str:
        """
        Ask the timer to retransmit stored data and return it as text.
        """
        #self.send_xon()
        self.retransmit(event_num=event_num, heat_num=heat_num, start_time=start_time)
        raw = self.read_available(duration=read_seconds)
        return raw.decode("ascii", errors="replace")

    def fetch_event_data(self, event_num: int = 1, heat_num: int = 0, start_time: str | None = None,
                         read_seconds: float = 10.0) -> str:
        """
        Download event data for the specified event and return it.
        """
        if not (0 <= event_num <= 255):
            raise ValueError("event_num must be 0..255")
        if not (0 <= heat_num <= 99):
            raise ValueError("heat_num must be 0..99")

        text = self.download_memory(event_num=event_num, heat_num=heat_num,
                                    start_time=start_time, read_seconds=read_seconds)
        return text


if __name__ == "__main__":
    tm = TimeMachineClient(
        port="COM4",
        baudrate=9600,
        bytesize=serial.EIGHTBITS,
        parity=serial.PARITY_NONE,
        stopbits=serial.STOPBITS_ONE,
        timeout=0.5,
        inter_byte_delay=0.01,
    )

    try:
        # Example: request all stored data from all events/heats
        text = tm.download_memory(event_num=1, heat_num=0, start_time=None, read_seconds=2.0)
        print("=== DEVICE DATA START ===")
        print(text)
        print("=== DEVICE DATA END ===")
    finally:
        tm.close()