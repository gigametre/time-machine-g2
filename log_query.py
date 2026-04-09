import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional


def query_logs(
    log_file: Path,
    level: Optional[str] = None,
    component: Optional[str] = None,
    search: Optional[str] = None,
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
):
    if not log_file.exists():
        print(f"Error: Log file {log_file} not found.")
        return

    count = 0
    with log_file.open("r", encoding="utf-8") as f:
        for line in f:
            try:
                entry = json.loads(line)
            except json.JSONDecodeError:
                continue

            # Filtering
            if level and entry.get("level") != level.upper():
                continue
            if component and entry.get("component") != component:
                continue
            if search and search.lower() not in entry.get("message", "").lower():
                continue
            
            if start_time or end_time:
                ts_str = entry.get("timestamp")
                if ts_str:
                    ts = datetime.fromisoformat(ts_str)
                    if start_time and ts < start_time:
                        continue
                    if end_time and ts > end_time:
                        continue

            # Print match
            ts = entry.get("timestamp", "")
            lvl = entry.get("level", "INFO")
            comp = entry.get("component", "unknown")
            msg = entry.get("message", "")
            ctx = entry.get("context", "")
            exc = entry.get("exception", "")

            print(f"[{ts}] {lvl:7} [{comp}] {msg}")
            if ctx:
                print(f"  Context: {ctx}")
            if exc:
                print(f"  Exception: {exc}")
            
            count += 1
    
    print(f"\n--- Found {count} matching entries ---")


def main():
    parser = argparse.ArgumentParser(description="Query structured session logs.")
    parser.add_argument("log_file", type=str, help="Path to the .log file to query")
    parser.add_argument("--level", help="Filter by log level (DEBUG, INFO, WARNING, ERROR)")
    parser.add_argument("--component", help="Filter by component name")
    parser.add_argument("--search", help="Free-text search in message")
    parser.add_argument("--start", help="Start time (ISO format, e.g., 2026-04-06T14:00:00)")
    parser.add_argument("--end", help="End time (ISO format)")

    args = parser.parse_args()

    start_dt = datetime.fromisoformat(args.start) if args.start else None
    end_dt = datetime.fromisoformat(args.end) if args.end else None

    query_logs(
        Path(args.log_file),
        level=args.level,
        component=args.component,
        search=args.search,
        start_time=start_dt,
        end_time=end_dt
    )


if __name__ == "__main__":
    main()
