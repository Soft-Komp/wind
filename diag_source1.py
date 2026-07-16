# -*- coding: utf-8 -*-
"""
Diagnostyka: konfiguracja polaczenia zrodla id_source=1 (fakir/database)
oraz test osiagalnosci sieciowej TCP z wnetrza kontenera api.
Haslo NIGDY nie jest wypisywane.
"""
import asyncio
import json
import socket
import time


async def main() -> None:
    print("=== DIAG SOURCE 1 CONNECTION START ===", flush=True)

    from app.db.session import get_db_context
    from app.core.encryption import decrypt_value
    from sqlalchemy import text

    id_source = 1

    async with get_db_context() as db:
        result = await db.execute(
            text(
                "SELECT [source_name], [source_type], [connection_config], "
                "       [is_active], [last_sync_at], [last_sync_status], [last_sync_message] "
                "FROM [dbo].[skw_document_sources] WHERE [id_source] = :s"
            ),
            {"s": id_source},
        )
        row = result.fetchone()
        if not row:
            print("BLAD: zrodlo id=1 nie istnieje", flush=True)
            return

        source_name, source_type, cfg_raw, is_active, last_sync_at, last_sync_status, last_sync_message = row
        print(f"[META] source_name={source_name!r} source_type={source_type!r} is_active={is_active}", flush=True)
        print(f"[META] last_sync_at={last_sync_at} last_sync_status={last_sync_status!r}", flush=True)
        print(f"[META] last_sync_message={last_sync_message!r}", flush=True)

        decrypted = decrypt_value(cfg_raw)
        config = json.loads(decrypted)

        # Wypisujemy WYLACZNIE nie-wrazliwe pola polaczenia
        safe_keys = ("host", "port", "database", "driver", "encrypt",
                     "trust_server_certificate", "view_name", "id_column",
                     "line_items_view", "line_items_id_column", "connection_string")
        print("[CONFIG] pola polaczenia (haslo pominiete):", flush=True)
        for k in safe_keys:
            if k in config:
                val = config[k]
                if k == "connection_string":
                    # connection_string moze zawierac PWD= w srodku - maskujemy
                    import re
                    val = re.sub(r"(PWD=)[^;]*", r"\1***", str(val))
                print(f"  {k} = {val!r}", flush=True)

        host = config.get("host")
        port = config.get("port", 1433)

        # Jesli podano gotowy connection_string zamiast osobnych pol - sprobuj wyciagnac SERVER=
        if not host and config.get("connection_string"):
            import re
            m = re.search(r"SERVER=([^,;]+)(?:,(\d+))?", config["connection_string"], re.IGNORECASE)
            if m:
                host = m.group(1)
                port = int(m.group(2)) if m.group(2) else 1433
                print(f"[CONFIG] wyciagnieto z connection_string: host={host!r} port={port}", flush=True)

        if not host:
            print("[BLAD] Nie udalo sie ustalic hosta do testu polaczenia TCP.", flush=True)
            return

        print(f"\n[TEST TCP] Probuje polaczyc sie z {host}:{port} (timeout=5s)", flush=True)
        t0 = time.monotonic()
        try:
            s = socket.create_connection((host, int(port)), timeout=5)
            elapsed = (time.monotonic() - t0) * 1000
            print(f"[TEST TCP] OK — polaczenie nawiazane w {elapsed:.1f} ms", flush=True)
            s.close()
        except Exception as exc:
            elapsed = (time.monotonic() - t0) * 1000
            print(f"[TEST TCP] BLAD po {elapsed:.1f} ms: {type(exc).__name__}: {exc}", flush=True)

        # Test DNS osobno (czy nazwa hosta w ogole sie rozwiazuje)
        print(f"\n[TEST DNS] Rozwiazywanie nazwy hosta '{host}'", flush=True)
        try:
            resolved = socket.gethostbyname(host)
            print(f"[TEST DNS] OK — {host} -> {resolved}", flush=True)
        except Exception as exc:
            print(f"[TEST DNS] BLAD: {type(exc).__name__}: {exc}", flush=True)

    print("=== DIAG SOURCE 1 CONNECTION END ===", flush=True)


if __name__ == "__main__":
    asyncio.run(main())
