# -*- coding: utf-8 -*-
"""
Diagnostyka: dlaczego get_adapter_by_source_id(id_source=10) zwraca None
mimo is_active=1. Uruchamiane wewnatrz kontenera api, PYTHONPATH=/app.

Loguje KAZDY krok jawnie do stdout (docker exec output) — niezaleznie
od konfiguracji loggera aplikacji, ktora okazala sie nie przechwytywac
logow z tego modulu w docker logs.
"""
import asyncio
import json
import sys
import traceback


async def main() -> None:
    print("=== DIAG START ===", flush=True)

    from app.db.session import get_db_context
    from app.core.encryption import decrypt_value
    from sqlalchemy import text

    id_source = 10

    async with get_db_context() as db:
        print(f"[1] Pobieram surowy wiersz skw_document_sources id={id_source}", flush=True)
        result = await db.execute(
            text(
                "SELECT [id_source], [source_name], [source_type], "
                "       [connection_config], [is_active], [is_test_mode] "
                "FROM [dbo].[skw_document_sources] WHERE [id_source] = :s"
            ),
            {"s": id_source},
        )
        row = result.fetchone()
        if not row:
            print("[1] BLAD: wiersz nie istnieje", flush=True)
            return

        _, source_name, source_type, cfg_raw, is_active, is_test_mode = row
        print(f"[1] OK source_name={source_name!r} source_type={source_type!r} "
              f"is_active={is_active!r} is_test_mode={is_test_mode!r} "
              f"config_raw_len={len(cfg_raw) if cfg_raw else 0}", flush=True)
        print(f"[1] source_type repr dokladny (sprawdzenie bialych znakow): {repr(source_type)}", flush=True)

        print("[2] Probuje odszyfrowac connection_config", flush=True)
        try:
            decrypted = decrypt_value(cfg_raw)
            print(f"[2] OK odszyfrowano, dlugosc={len(decrypted)}", flush=True)
        except Exception as exc:
            print(f"[2] BLAD DESZYFROWANIA: {type(exc).__name__}: {exc}", flush=True)
            traceback.print_exc()
            return

        print("[3] Probuje sparsowac JSON", flush=True)
        try:
            config = json.loads(decrypted)
            print(f"[3] OK klucze w config: {sorted(config.keys())}", flush=True)
            # Nie logujemy wartosci (auth_config moze zawierac sekrety) —
            # tylko obecnosc kluczy wymaganych przez RestApiAdapter.
            print(f"[3] base_url obecny: {'base_url' in config and bool(config.get('base_url'))}", flush=True)
            print(f"[3] endpoint_list obecny: {'endpoint_list' in config and bool(config.get('endpoint_list'))}", flush=True)
            print(f"[3] auth_type: {config.get('auth_type')!r}", flush=True)
            print(f"[3] endpoint_line_items obecny: {'endpoint_line_items' in config and bool(config.get('endpoint_line_items'))}", flush=True)
        except Exception as exc:
            print(f"[3] BLAD PARSOWANIA JSON: {type(exc).__name__}: {exc}", flush=True)
            traceback.print_exc()
            return

        print("[4] Probuje zbudowac RestApiAdapter bezposrednio", flush=True)
        try:
            from app.schemas.unified_document import _build_adapter
            adapter = _build_adapter(id_source, source_name, source_type, config, [])
            print(f"[4] _build_adapter zwrocil: {adapter!r}", flush=True)
            if adapter is not None:
                print(f"[4] hasattr get_line_items: {hasattr(adapter, 'get_line_items')}", flush=True)
        except Exception as exc:
            print(f"[4] WYJATEK PODCZAS _build_adapter: {type(exc).__name__}: {exc}", flush=True)
            traceback.print_exc()

        print("[5] Wywoluje get_adapter_by_source_id() jak robi to realny endpoint", flush=True)
        try:
            from app.schemas.unified_document import get_adapter_by_source_id
            adapter2 = await get_adapter_by_source_id(db, id_source)
            print(f"[5] get_adapter_by_source_id zwrocil: {adapter2!r}", flush=True)
        except Exception as exc:
            print(f"[5] WYJATEK: {type(exc).__name__}: {exc}", flush=True)
            traceback.print_exc()

    print("=== DIAG END ===", flush=True)


if __name__ == "__main__":
    asyncio.run(main())
