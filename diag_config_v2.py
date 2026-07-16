# -*- coding: utf-8 -*-
"""
Diagnostyka v2: co realnie jest zapisane w connection_config zrodla id=10
PO rzekomej konfiguracji endpoint_line_items przez front. Sprawdza baze
bezposrednio (bez cache Redis, zgodnie z tym jak dziala realny endpoint),
oraz metadane updated_at zeby potwierdzic ze zapis w ogole niedawno mial
miejsce.
"""
import asyncio
import json


async def main() -> None:
    print("=== DIAG CONFIG v2 START ===", flush=True)

    from app.db.session import get_db_context
    from app.core.encryption import decrypt_value
    from sqlalchemy import text

    id_source = 10

    async with get_db_context() as db:
        result = await db.execute(
            text(
                "SELECT [connection_config], [updated_at], [is_active] "
                "FROM [dbo].[skw_document_sources] WHERE [id_source] = :s"
            ),
            {"s": id_source},
        )
        row = result.fetchone()
        if not row:
            print("BLAD: zrodlo nie istnieje", flush=True)
            return

        cfg_raw, updated_at, is_active = row
        print(f"[META] updated_at={updated_at} is_active={is_active}", flush=True)

        decrypted = decrypt_value(cfg_raw)
        config = json.loads(decrypted)

        print(f"[CONFIG] wszystkie klucze: {sorted(config.keys())}", flush=True)
        for key in ("base_url", "endpoint_list", "endpoint_detail", "endpoint_line_items", "auth_type"):
            present = key in config
            value = config.get(key)
            # auth_type nie jest wrazliwy, reszta tez nie (to sciezki URL, nie sekrety)
            print(f"[CONFIG] {key}: obecny={present!r} wartosc={value!r}", flush=True)

    print("=== DIAG CONFIG v2 END ===", flush=True)


if __name__ == "__main__":
    asyncio.run(main())
