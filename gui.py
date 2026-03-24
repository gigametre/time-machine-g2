import threading
import queue
import time
import tkinter as tk
from tkinter import ttk, messagebox, filedialog
from tkinter.scrolledtext import ScrolledText

try:
    import serial
    import serial.tools.list_ports
except ImportError:
    serial = None


class SerialMonitorApp:
    def __init__(self, root: tk.Tk) -> None:
        self.root = root
        self.root.title("Timing Device Serial Monitor")
        self.root.geometry("980x650")

        self.serial_port = None
        self.reader_thread = None
        self.stop_event = threading.Event()
        self.rx_queue: queue.Queue[str] = queue.Queue()
        self.line_count = 0

        self.port_var = tk.StringVar()
        self.baud_var = tk.StringVar(value="9600")
        self.databits_var = tk.StringVar(value="8")
        self.parity_var = tk.StringVar(value="N")
        self.stopbits_var = tk.StringVar(value="1")
        self.timeout_var = tk.StringVar(value="1")
        self.status_var = tk.StringVar(value="Disconnected")
        self.autoscroll_var = tk.BooleanVar(value=True)
        self.timestamp_var = tk.BooleanVar(value=True)
        self.hex_var = tk.BooleanVar(value=False)
        self.send_var = tk.StringVar()

        self._build_ui()
        self.refresh_ports()
        self.root.protocol("WM_DELETE_WINDOW", self.on_close)
        self.root.after(100, self.process_rx_queue)

    def _build_ui(self) -> None:
        top = ttk.Frame(self.root, padding=10)
        top.pack(fill=tk.X)

        ttk.Label(top, text="Port:").grid(row=0, column=0, sticky="w", padx=3, pady=3)
        self.port_combo = ttk.Combobox(top, textvariable=self.port_var, width=18, state="readonly")
        self.port_combo.grid(row=0, column=1, sticky="w", padx=3, pady=3)

        ttk.Button(top, text="Refresh", command=self.refresh_ports).grid(row=0, column=2, padx=3, pady=3)

        ttk.Label(top, text="Baud:").grid(row=0, column=3, sticky="w", padx=3, pady=3)
        ttk.Combobox(top, textvariable=self.baud_var, width=10, state="readonly",
                     values=["1200", "2400", "4800", "9600", "19200", "38400", "57600", "115200", "230400"]
                     ).grid(row=0, column=4, sticky="w", padx=3, pady=3)

        ttk.Label(top, text="Data Bits:").grid(row=0, column=5, sticky="w", padx=3, pady=3)
        ttk.Combobox(top, textvariable=self.databits_var, width=5, state="readonly",
                     values=["5", "6", "7", "8"]).grid(row=0, column=6, sticky="w", padx=3, pady=3)

        ttk.Label(top, text="Parity:").grid(row=1, column=0, sticky="w", padx=3, pady=3)
        ttk.Combobox(top, textvariable=self.parity_var, width=5, state="readonly",
                     values=["N", "E", "O", "M", "S"]).grid(row=1, column=1, sticky="w", padx=3, pady=3)

        ttk.Label(top, text="Stop Bits:").grid(row=1, column=2, sticky="w", padx=3, pady=3)
        ttk.Combobox(top, textvariable=self.stopbits_var, width=5, state="readonly",
                     values=["1", "1.5", "2"]).grid(row=1, column=3, sticky="w", padx=3, pady=3)

        ttk.Label(top, text="Timeout (s):").grid(row=1, column=4, sticky="w", padx=3, pady=3)
        ttk.Entry(top, textvariable=self.timeout_var, width=8).grid(row=1, column=5, sticky="w", padx=3, pady=3)

        self.connect_btn = ttk.Button(top, text="Connect", command=self.toggle_connection)
        self.connect_btn.grid(row=1, column=6, padx=6, pady=3)

        options = ttk.Frame(self.root, padding=(10, 0, 10, 8))
        options.pack(fill=tk.X)
        ttk.Checkbutton(options, text="Auto-scroll", variable=self.autoscroll_var).pack(side=tk.LEFT, padx=5)
        ttk.Checkbutton(options, text="Timestamp lines", variable=self.timestamp_var).pack(side=tk.LEFT, padx=5)
        ttk.Checkbutton(options, text="Show HEX", variable=self.hex_var).pack(side=tk.LEFT, padx=5)
        ttk.Button(options, text="Clear", command=self.clear_display).pack(side=tk.LEFT, padx=10)
        ttk.Button(options, text="Save Log", command=self.save_log).pack(side=tk.LEFT, padx=5)
        ttk.Label(options, textvariable=self.status_var).pack(side=tk.RIGHT, padx=5)

        self.display = ScrolledText(self.root, wrap=tk.WORD, font=("Consolas", 10))
        self.display.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)
        self.display.configure(state=tk.DISABLED)

        bottom = ttk.LabelFrame(self.root, text="Send Command", padding=10)
        bottom.pack(fill=tk.X, padx=10, pady=(0, 10))
        ttk.Entry(bottom, textvariable=self.send_var).pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(0, 8))
        ttk.Button(bottom, text="Send", command=self.send_data).pack(side=tk.LEFT, padx=4)
        ttk.Button(bottom, text="Send CR/LF", command=lambda: self.send_data(add_newline=True)).pack(side=tk.LEFT, padx=4)

    def refresh_ports(self) -> None:
        if serial is None:
            self.port_combo["values"] = []
            self.status_var.set("pyserial not installed")
            return

        ports = [p.device for p in serial.tools.list_ports.comports()]
        self.port_combo["values"] = ports
        if ports and self.port_var.get() not in ports:
            self.port_var.set(ports[0])
        elif not ports:
            self.port_var.set("")

    def toggle_connection(self) -> None:
        if self.serial_port and self.serial_port.is_open:
            self.disconnect()
        else:
            self.connect()

    def connect(self) -> None:
        if serial is None:
            messagebox.showerror("Missing Dependency", "pyserial is not installed.\nInstall it with: pip install pyserial")
            return

        port = self.port_var.get().strip()
        if not port:
            messagebox.showwarning("No Port", "Please select a COM port.")
            return

        try:
            baudrate = int(self.baud_var.get())
            timeout = float(self.timeout_var.get())
            bytesize = {
                "5": serial.FIVEBITS,
                "6": serial.SIXBITS,
                "7": serial.SEVENBITS,
                "8": serial.EIGHTBITS,
            }[self.databits_var.get()]
            parity = {
                "N": serial.PARITY_NONE,
                "E": serial.PARITY_EVEN,
                "O": serial.PARITY_ODD,
                "M": serial.PARITY_MARK,
                "S": serial.PARITY_SPACE,
            }[self.parity_var.get()]
            stopbits = {
                "1": serial.STOPBITS_ONE,
                "1.5": serial.STOPBITS_ONE_POINT_FIVE,
                "2": serial.STOPBITS_TWO,
            }[self.stopbits_var.get()]

            self.serial_port = serial.Serial(
                port=port,
                baudrate=baudrate,
                timeout=timeout,
                bytesize=bytesize,
                parity=parity,
                stopbits=stopbits,
            )

            self.stop_event.clear()
            self.reader_thread = threading.Thread(target=self.read_loop, daemon=True)
            self.reader_thread.start()

            self.status_var.set(f"Connected to {port} @ {baudrate}")
            self.connect_btn.config(text="Disconnect")
            self.append_text(f"[INFO] Connected to {port} at {baudrate} baud\n")
        except Exception as exc:
            messagebox.showerror("Connection Error", str(exc))
            self.status_var.set("Connection failed")
            self.serial_port = None

    def disconnect(self) -> None:
        self.stop_event.set()
        try:
            if self.serial_port and self.serial_port.is_open:
                port_name = self.serial_port.port
                self.serial_port.close()
                self.append_text(f"[INFO] Disconnected from {port_name}\n")
        except Exception as exc:
            self.append_text(f"[WARN] Error while disconnecting: {exc}\n")
        finally:
            self.serial_port = None
            self.connect_btn.config(text="Connect")
            self.status_var.set("Disconnected")

    def read_loop(self) -> None:
        partial = b""
        while not self.stop_event.is_set():
            try:
                if not self.serial_port or not self.serial_port.is_open:
                    break

                waiting = self.serial_port.in_waiting
                data = self.serial_port.read(waiting or 1)
                if not data:
                    continue

                if self.hex_var.get():
                    hex_line = " ".join(f"{b:02X}" for b in data)
                    self.rx_queue.put(self.decorate_line(hex_line))
                    continue

                partial += data
                while b"" in partial:
                    line, partial = partial.split(b"", 1)
                    line = line.rstrip(b"")
                    decoded = self.format_mixed_bytes(line)
                    self.rx_queue.put(self.decorate_line(decoded))
            except Exception as exc:
                self.rx_queue.put(f"[ERROR] Serial read failed: {exc}\n")
                break

        if partial and not self.hex_var.get():
            try:
                decoded = self.format_mixed_bytes(partial)
                self.rx_queue.put(self.decorate_line(decoded))
            except Exception:
                pass

    def format_mixed_bytes(self, data: bytes) -> str:
        parts = []
        hex_run = []

        def flush_hex_run() -> None:
            nonlocal hex_run
            if hex_run:
                parts.append("<" + " ".join(f"{b:02X}" for b in hex_run) + ">")
                hex_run = []

        for b in data:
            if 32 <= b <= 126 or b == 9:
                flush_hex_run()
                parts.append(chr(b))
            else:
                hex_run.append(b)

        flush_hex_run()
        return "".join(parts)

    def decorate_line(self, text: str) -> str:
        if self.timestamp_var.get():
            stamp = time.strftime("%H:%M:%S")
            return f"[{stamp}] {text}\n"
        return text + "\n"

    def process_rx_queue(self) -> None:
        try:
            while True:
                msg = self.rx_queue.get_nowait()
                self.append_text(msg)
        except queue.Empty:
            pass
        finally:
            self.root.after(100, self.process_rx_queue)

    def append_text(self, text: str) -> None:
        self.display.configure(state=tk.NORMAL)
        self.display.insert(tk.END, text)
        self.display.configure(state=tk.DISABLED)
        self.line_count += text.count("\n")
        if self.autoscroll_var.get():
            self.display.see(tk.END)

    def clear_display(self) -> None:
        self.display.configure(state=tk.NORMAL)
        self.display.delete("1.0", tk.END)
        self.display.configure(state=tk.DISABLED)
        self.line_count = 0

    def save_log(self) -> None:
        filename = filedialog.asksaveasfilename(
            title="Save serial log",
            defaultextension=".txt",
            filetypes=[("Text Files", "*.txt"), ("Log Files", "*.log"), ("All Files", "*.*")],
        )
        if not filename:
            return
        try:
            data = self.display.get("1.0", tk.END)
            with open(filename, "w", encoding="utf-8") as f:
                f.write(data)
            self.status_var.set(f"Saved log to {filename}")
        except Exception as exc:
            messagebox.showerror("Save Error", str(exc))

    def send_data(self, add_newline: bool = False) -> None:
        if not self.serial_port or not self.serial_port.is_open:
            messagebox.showwarning("Not Connected", "Connect to a serial port first.")
            return

        text = self.send_var.get()
        if add_newline:
            text += "\r\n"

        try:
            self.serial_port.write(text.encode("utf-8", errors="replace"))
            self.append_text(f"[TX] {text!r}\n")
            self.send_var.set("")
        except Exception as exc:
            messagebox.showerror("Send Error", str(exc))

    def on_close(self) -> None:
        self.disconnect()
        self.root.destroy()


def main() -> None:
    root = tk.Tk()
    app = SerialMonitorApp(root)
    root.mainloop()


if __name__ == "__main__":
    main()
