#!/usr/bin/env python3
# =============================================================================
# worker/core/process_monitor.py — Supervisord event listener
# =============================================================================
# Monitoruje zdarzenia procesów (crash, restart) i loguje do pliku.
# =============================================================================

import json
import sys
from datetime import datetime, timezone
from pathlib import Path


def write_event(event_type: str, data: dict) -> None:
    log_dir = Path("/app/logs")
    log_dir.mkdir(parents=True, exist_ok=True)
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    log_file = log_dir / f"process_events_{date_str}.jsonl"

    entry = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "event_type": event_type,
        **data,
    }
    with open(log_file, "a", encoding="utf-8") as f:
        f.write(json.dumps(entry, default=str) + "\n")


def main():
    while True:
        # Supervisord event protocol
        line = sys.stdin.readline()
        if not line:
            break

        headers = dict(
            pair.split(":", 1) for pair in line.strip().split(" ")
            if ":" in pair
        )

        payload_len = int(headers.get("len", 0))
        payload = sys.stdin.read(payload_len) if payload_len else ""

        event_name = headers.get("eventname", "UNKNOWN")
        payload_data = dict(
            pair.split(":", 1) for pair in payload.strip().split(" ")
            if ":" in pair
        )

        write_event(
            event_type=f"supervisor.{event_name.lower()}",
            data={
                "process": payload_data.get("processname", "?"),
                "group": payload_data.get("groupname", "?"),
                "from_state": payload_data.get("from_state", "?"),
                "raw_payload": payload[:200],
            },
        )

        # Odpowiedź OK do supervisord
        sys.stdout.write("RESULT 2\nOK")
        sys.stdout.flush()


if __name__ == "__main__":
    main()