# fix_fakir_port.py
#
# NAPRAWA (2026-08-04, trzecia iteracja tej samej sprawy): pierwszy skrypt
# naprawczy (fix_fakir_connection_config.py) dopisal host/database/username/
# password, ale ZOSTAWIL bez zmian pole "port", ktore w oryginalnym,
# zepsutym configu bylo "1433" (port GPGKJASLO) - nikt tego wtedy nie
# sprawdzal, bo cala reszta i tak byla niekompletna. Srodowisko, na ktorym
# operujemy, to STOMIL (potwierdzone: port 59425 osiagalny z workera przez
# TCP, 1433 daje timeout) - "Login timeout expired" byl wynikiem proby
# polaczenia sie z niewlasciwym portem, NIE bledem configu autoryzacji.
#
# Ten skrypt zmienia WYLACZNIE klucz "port" w connection_config zrodla
# Fakir (id_source=1), z 1433 na 59425. Wszystkie pozostale klucze
# (host, database, username, password, view_name, line_items_view,
# line_items_join_column, driver, encrypt, trust_server_certificate)
# pozostaja bez zmian.
#
# POPRAWKA wzgledem poprzedniego skryptu: ten skrypt PROBUJE wywolac
# invalidate_adapter_cache() (jesli istnieje w unified_document.py) zaraz
# po zapisie, zeby uniknac powtorki z poprzedniej naprawy (stary config w
# Redis TTL 5 min, serwowany mimo poprawnego zapisu w bazie). Jesli ta
# funkcja nie istnieje pod tym importem, skrypt i tak wypisze komende
# redis-cli do recznego wykonania jako fallback.
#
# URUCHOMIENIE (identycznie jak poprzednio):
#   docker cp fix_fakir_port.py windykacja_api:/tmp/fix_fakir_port.py
#   docker exec -it -e PYTHONPATH=/app windykacja_api python /tmp/fix_fakir_port.py
#   docker exec windykacja_api rm /tmp/fix_fakir_port.py

import asyncio
import json
import sys

from app.core.config import get_settings
from app.core.encryption import decrypt_value, encrypt_value

ID_SOURCE = 1  # fakir
NEW_PORT = 59425  # STOMIL — potwierdzone jako jedyny osiagalny z workera


async def main() -> None:
    settings = get_settings()

    from sqlalchemy.ext.asyncio import create_async_engine
    from sqlalchemy import text

    db_url = settings.get_sqlalchemy_url()
    engine = create_async_engine(db_url)

    async with engine.connect() as conn:
        row = (await conn.execute(
            text(
                "SELECT [connection_config] FROM [dbo].[skw_document_sources] "
                "WHERE [id_source] = :s"
            ),
            {"s": ID_SOURCE},
        )).fetchone()

        if not row:
            print(f"BLAD: id_source={ID_SOURCE} nie istnieje.")
            sys.exit(1)

        current = json.loads(decrypt_value(row[0])) if row[0] else {}
        old_port = current.get("port")
        print(f"AKTUALNY port: {old_port!r}")

        updated = dict(current)
        updated["port"] = NEW_PORT

        print(f"NOWY port: {NEW_PORT!r}")
        print("Pozostale klucze bez zmian:", sorted(k for k in updated if k != "port"))
        print()
        confirm = input(
            f"Zapisac port={NEW_PORT} dla id_source={ID_SOURCE} (fakir)? [TAK/nie]: "
        )
        if confirm.strip().upper() != "TAK":
            print("Przerwano - nic nie zapisano.")
            sys.exit(0)

    encrypted = encrypt_value(json.dumps(updated, ensure_ascii=False))

    async with engine.begin() as conn:
        result = await conn.execute(
            text(
                "UPDATE [dbo].[skw_document_sources] "
                "SET [connection_config] = :cfg, [updated_at] = SYSUTCDATETIME() "
                "WHERE [id_source] = :s"
            ),
            {"cfg": encrypted, "s": ID_SOURCE},
        )

    if result.rowcount == 1:
        print(f"OK: port zapisany dla id_source={ID_SOURCE}.")
    else:
        print(f"OSTRZEZENIE: rowcount={result.rowcount}, sprawdz recznie.")

    await engine.dispose()

    # Proba automatycznej inwalidacji cache Redis — zeby nie powtorzyc
    # sytuacji z poprzedniej naprawy (stary config serwowany z cache przez
    # 5 min mimo poprawnego zapisu w bazie).
    try:
        from app.schemas.unified_document import invalidate_adapter_cache
        from app.core.redis_client import get_redis  # nazwa modulu do potwierdzenia — jesli inny import, ponizszy except to zlapie
        redis = await get_redis()
        await invalidate_adapter_cache(redis, ID_SOURCE)
        print("OK: cache adaptera w Redis zinwalidowany automatycznie.")
    except Exception as exc:
        print(f"UWAGA: automatyczna inwalidacja cache nie powiodla sie ({type(exc).__name__}: {exc}).")
        print("Wykonaj recznie:")
        print(f"    docker exec windykacja_redis redis-cli DEL adapter_config:{ID_SOURCE}")


if __name__ == "__main__":
    asyncio.run(main())