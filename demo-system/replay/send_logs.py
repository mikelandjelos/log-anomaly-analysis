"""Replay logs to the demo-system Logstash endpoint over TCP JSON."""

from __future__ import annotations

import argparse
import json
import re
import socket
import time
from datetime import datetime
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Replay logs to Logstash")
    parser.add_argument("log_file", type=Path, help="Path to raw log file")
    parser.add_argument("datastream", help="Datastream label (e.g., bgl, apache)")
    parser.add_argument(
        "--host", default="127.0.0.1", help="Logstash host (default: 127.0.0.1)"
    )
    parser.add_argument(
        "--port", type=int, default=5044, help="Logstash TCP port (default: 5044)"
    )
    parser.add_argument(
        "--sleep",
        type=float,
        default=0.0,
        help="Fixed delay between lines (seconds). Ignored when timestamps are parsed.",
    )
    parser.add_argument(
        "--timestamp-regex",
        type=str,
        default=None,
        help="Regex to extract a timestamp from each line. Use a named group 'ts' or the first capturing group.",
    )
    parser.add_argument(
        "--timestamp-format",
        type=str,
        default=None,
        help="Datetime format (strptime syntax) for the extracted timestamp.",
    )
    parser.add_argument(
        "--speed",
        type=float,
        default=1.0,
        help="Speed multiplier when using timestamps (1.0 = real time).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if not args.log_file.exists():
        raise SystemExit(f"Log file not found: {args.log_file}")

    ts_pattern = re.compile(args.timestamp_regex) if args.timestamp_regex else None
    use_dynamic_sleep = bool(ts_pattern and args.timestamp_format)
    base_ts: datetime | None = None
    wall_start: float | None = None
    prev_ts: datetime | None = None

    with socket.create_connection((args.host, args.port)) as sock:
        with args.log_file.open("r", encoding="utf-8", errors="ignore") as fh:
            for line in fh:
                stripped = line.strip()
                if not stripped:
                    continue

                now_iso = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime())
                message_ts_iso = now_iso
                original_iso = None
                if use_dynamic_sleep:
                    match = ts_pattern.search(stripped)
                    ts_value = None
                    if match:
                        try:
                            ts_raw = match.groupdict().get("ts") or match.group(1)
                            ts_value = datetime.strptime(ts_raw, args.timestamp_format)
                            original_iso = ts_value.isoformat()
                        except (ValueError, IndexError):
                            ts_value = None

                    if ts_value is not None:
                        if base_ts is None:
                            base_ts = ts_value
                            wall_start = time.time()
                            message_ts_iso = datetime.utcfromtimestamp(wall_start).isoformat()
                        elif wall_start is not None and args.speed > 0:
                            elapsed = (ts_value - base_ts).total_seconds() / args.speed
                            target_time = wall_start + elapsed
                            sleep_for = target_time - time.time()
                            if sleep_for > 0:
                                time.sleep(sleep_for)
                            message_ts_iso = datetime.utcfromtimestamp(target_time).isoformat()
                        prev_ts = ts_value
                    elif args.sleep > 0:
                        time.sleep(args.sleep)
                elif args.sleep > 0:
                    time.sleep(args.sleep)

                payload = {
                    "message": stripped,
                    "datastream": args.datastream,
                    "timestamp": message_ts_iso,
                }
                if original_iso:
                    payload["original_timestamp"] = original_iso
                sock.sendall(json.dumps(payload).encode("utf-8") + b"\n")


if __name__ == "__main__":
    main()
