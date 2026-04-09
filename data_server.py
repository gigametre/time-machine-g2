import argparse
import csv
import json
import os
import threading
import time
from dataclasses import dataclass
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import parse_qs, urlparse
from logging_utils import SessionLogger, get_session_logger


@dataclass
class ServerState:
    logs_dir: Path
    poll_interval_seconds: float = 1.0
    auth_token: Optional[str] = None
    last_scan_ts: float = 0.0
    latest_csv: Optional[Path] = None
    latest_rows: List[Dict[str, str]] = None
    latest_mtime: float = 0.0
    logger: Optional[SessionLogger] = None

    def __post_init__(self) -> None:
        if self.latest_rows is None:
            self.latest_rows = []
        if self.logger is None:
            # Default logger to a 'server' subdirectory in logs
            self.logger = get_session_logger(self.logs_dir / "server_logs")


STATE_LOCK = threading.Lock()
STATE: Optional[ServerState] = None


def _scan_latest_session_csv(logs_dir: Path) -> Optional[Path]:
    if not logs_dir.exists() or not logs_dir.is_dir():
        return None

    latest_csv: Optional[Path] = None
    latest_mtime = 0.0

    for child in logs_dir.iterdir():
        if not child.is_dir() or not child.name.startswith("session_"):
            continue
        candidate = child / "session_results.csv"
        if not candidate.exists():
            continue
        mtime = candidate.stat().st_mtime
        if mtime > latest_mtime:
            latest_mtime = mtime
            latest_csv = candidate

    return latest_csv


def _load_csv_rows(path: Path) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    if not path.exists():
        return rows

    with path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append({k: (v if v is not None else "") for k, v in row.items()})

    return rows


def refresh_state(force: bool = False) -> None:
    global STATE
    assert STATE is not None

    now = time.time()
    with STATE_LOCK:
        if not force and (now - STATE.last_scan_ts) < STATE.poll_interval_seconds:
            return

        STATE.last_scan_ts = now
        latest_csv = _scan_latest_session_csv(STATE.logs_dir)

        if latest_csv is None:
            STATE.latest_csv = None
            STATE.latest_rows = []
            STATE.latest_mtime = 0.0
            return

        mtime = latest_csv.stat().st_mtime
        if STATE.latest_csv == latest_csv and STATE.latest_mtime == mtime:
            return

        STATE.latest_rows = _load_csv_rows(latest_csv)
        STATE.latest_csv = latest_csv
        STATE.latest_mtime = mtime


def _safe_int(value: Optional[str], default: int) -> int:
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        return default


class DataServerHandler(BaseHTTPRequestHandler):
    server_version = "TimeMachineDataServer/1.0"

    def _send_json(self, payload: Dict[str, Any], status: int = HTTPStatus.OK) -> None:
        raw = json.dumps(payload, ensure_ascii=True, indent=2).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def _send_html(self, html: str, status: int = HTTPStatus.OK) -> None:
        raw = html.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def _request_token(self, query: Dict[str, List[str]]) -> str:
        # Accept token via query, custom header, or Bearer header.
        token = query.get("token", [""])[0].strip()
        if token:
            return token

        header_token = (self.headers.get("X-API-Token") or "").strip()
        if header_token:
            return header_token

        auth_header = (self.headers.get("Authorization") or "").strip()
        if auth_header.lower().startswith("bearer "):
            return auth_header[7:].strip()

        return ""

    def _is_authorized(self, query: Dict[str, List[str]]) -> bool:
        assert STATE is not None
        if not STATE.auth_token:
            return True
        return self._request_token(query) == STATE.auth_token

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        query = parse_qs(parsed.query)

        if not self._is_authorized(query):
            self._send_json(
                {"error": "Unauthorized", "hint": "Pass token via ?token=, X-API-Token header, or Authorization: Bearer <token>"},
                status=HTTPStatus.UNAUTHORIZED,
            )
            return

        if parsed.path == "/":
            self._send_html(self._index_html())
            return

        if parsed.path == "/health":
            self._handle_health()
            return

        if parsed.path == "/api/latest":
            self._handle_latest(query)
            return

        if parsed.path == "/api/results":
            self._handle_results(query)
            return

        self._send_json({"error": "Not found"}, status=HTTPStatus.NOT_FOUND)

    def _handle_health(self) -> None:
        refresh_state()
        assert STATE is not None
        with STATE_LOCK:
            self._send_json(
                {
                    "ok": True,
                    "logs_dir": str(STATE.logs_dir),
                    "latest_csv": str(STATE.latest_csv) if STATE.latest_csv else None,
                    "row_count": len(STATE.latest_rows),
                }
            )

    def _handle_latest(self, query: Dict[str, List[str]]) -> None:
        refresh_state()
        limit = _safe_int(query.get("limit", ["200"])[0], 200)
        if limit < 1:
            limit = 1

        assert STATE is not None
        with STATE_LOCK:
            rows = STATE.latest_rows[-limit:]
            self._send_json(
                {
                    "latest_csv": str(STATE.latest_csv) if STATE.latest_csv else None,
                    "updated_unix": STATE.latest_mtime,
                    "row_count": len(STATE.latest_rows),
                    "rows": rows,
                }
            )

    def _handle_results(self, query: Dict[str, List[str]]) -> None:
        refresh_state()
        offset = _safe_int(query.get("offset", ["0"])[0], 0)
        limit = _safe_int(query.get("limit", ["200"])[0], 200)
        if offset < 0:
            offset = 0
        if limit < 1:
            limit = 1

        assert STATE is not None
        with STATE_LOCK:
            end = offset + limit
            subset = STATE.latest_rows[offset:end]
            self._send_json(
                {
                    "latest_csv": str(STATE.latest_csv) if STATE.latest_csv else None,
                    "offset": offset,
                    "limit": limit,
                    "row_count": len(STATE.latest_rows),
                    "rows": subset,
                }
            )

    def log_message(self, fmt: str, *args: Any) -> None:
        # structured logging for HTTP requests
        assert STATE is not None
        msg = fmt % args
        STATE.logger.info(msg, component="http_server")

    def _index_html(self) -> str:
        return """<!doctype html>
<html>
  <head>
    <meta charset=\"utf-8\" />
    <title>Time Machine Data Server</title>
    <style>
      body { font-family: Segoe UI, Arial, sans-serif; margin: 24px; background: #f6f8fb; }
      h1 { margin: 0 0 10px; }
      .meta { margin-bottom: 12px; color: #334; }
      table { border-collapse: collapse; width: 100%; background: #fff; }
      th, td { border: 1px solid #cfd8e3; padding: 6px 8px; font-size: 13px; }
      th { background: #e9f0f8; text-align: left; }
      code { background: #eef3f8; padding: 2px 4px; }
    </style>
  </head>
  <body>
    <h1>Time Machine Data Server</h1>
    <div class=\"meta\" id=\"meta\">Loading...</div>
    <table id=\"results\"></table>
    <script>
      async function refresh() {
        const params = new URLSearchParams(window.location.search);
        const token = params.get('token') || '';
        const endpoint = '/api/latest?limit=200' + (token ? `&token=${encodeURIComponent(token)}` : '');
        const res = await fetch(endpoint);
        const data = await res.json();
        const meta = document.getElementById('meta');
        meta.textContent = `CSV: ${data.latest_csv || 'none'} | Rows: ${data.row_count}`;

        const table = document.getElementById('results');
        if (!data.rows || data.rows.length === 0) {
          table.innerHTML = '<tr><td>No rows yet</td></tr>';
          return;
        }

        const cols = Object.keys(data.rows[0]);
        const head = '<tr>' + cols.map(c => `<th>${c}</th>`).join('') + '</tr>';
        const body = data.rows.map(r => '<tr>' + cols.map(c => `<td>${(r[c] ?? '')}</td>`).join('') + '</tr>').join('');
        table.innerHTML = head + body;
      }

      refresh();
      setInterval(refresh, 2000);
    </script>
  </body>
</html>
"""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Serve generated Time Machine session data over HTTP.")
    parser.add_argument("--host", default="127.0.0.1", help="Host interface to bind (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=8080, help="Port to bind (default: 8080)")
    parser.add_argument(
        "--logs-dir",
        default="logs",
        help="Logs directory containing session_*/session_results.csv (default: logs)",
    )
    parser.add_argument(
        "--poll-interval",
        type=float,
        default=1.0,
        help="Seconds between file scans (default: 1.0)",
    )
    parser.add_argument(
        "--token",
        default="",
        help="Optional API token. When set, all endpoints require auth.",
    )
    return parser.parse_args()


def main() -> None:
    global STATE
    args = parse_args()

    STATE = ServerState(
        logs_dir=Path(args.logs_dir).resolve(),
        poll_interval_seconds=max(0.1, args.poll_interval),
        auth_token=(args.token.strip() or None),
    )
    refresh_state(force=True)

    server = ThreadingHTTPServer((args.host, args.port), DataServerHandler)
    print(f"Serving Time Machine data at http://{args.host}:{args.port}")
    print(f"Watching logs at: {STATE.logs_dir}")
    print("Endpoints: /health, /api/latest, /api/results")
    if STATE.auth_token:
        print("Auth: enabled (use ?token=... or Authorization Bearer / X-API-Token)")
    else:
        print("Auth: disabled")

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down...")
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
