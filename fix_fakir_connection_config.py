# fix_fakir_connection_config.py
#
# NAPRAWA (2026-08-04): connection_config zrodla Fakir (id_source=1) w
# skw_document_sources stracil pola polaczenia (connection_string / host /
# database / username / password) - prawdopodobnie przy edycji, ktora
# dograla line_items_view/line_items_join_column (front, 04.08.2026).
# Skutek: DatabaseAdapter._validate_config() rzuca ValueError, _build_adapter()
# lapie to cicho i zwraca None -> "Brak adaptera dla tego source_type"
# (widoczne w last_sync_status='error' oraz w get_line_items -> not_applicable).
#
# POPRAWKA (2026-08-04, druga iteracja): pierwsza wersja tego skryptu uzywala
# FAKIR_DB_HOST/FAKIR_DB_DATABASE/FAKIR_DB_USER/FAKIR_DB_PASSWORD - to okazalo
# sie porzuconym legacy (zlecone frontowi kiedys, pozniej cofniete "wrzutka",
# bez aktualizacji walidatora validate_fakir_user_different_from_db_user,
# ktory nadal sugeruje rozdzielenie userow - MYLACE, do sprzatniecia osobno).
# Wedlug frontu (04.08.2026) nalezy uzywac PODSTAWOWYCH danych polaczenia
# backendu (db_host/db_name/db_user/db_password) - Fakir/WAPRO zyje w tej
# samej fizycznej bazie co reszta systemu (dbo, nie osobny serwer).
#
# Ten skrypt:
#   1. Czyta AKTUALNY connection_config zrodla Fakir (odszyfrowany).
#   2. Dokleja host/database/username/password z PODSTAWOWYCH ustawien
#      backendu (db_host / db_name / db_user / db_password).
#   3. NIE rusza view_name / line_items_view / line_items_join_column -
#      to swiadoma decyzja frontu z 04.08.2026 (zapisana w notatkach),
#      nie cofamy jej cicho.
#   4. Szyfruje przez encrypt_value() (ta sama funkcja co uzywa aplikacja)
#      i zapisuje UPDATE do skw_document_sources.
#
# Haslo NIGDY nie jest drukowane ani logowane w tym skrypcie.
#
# URUCHOMIENIE (wewnatrz kontenera API, dane nie opuszczaja kontenera):
#   docker cp fix_fakir_connection_config.py windykacja_api:/tmp/fix_fakir.py
#   docker exec windykacja_api python /tmp/fix_fakir.py
#   docker exec windykacja_api rm /tmp/fix_fakir.py
#
# Skrypt jest IDEMPOTENTNY do odczytu (najpierw pokazuje diff kluczy do
# potwierdzenia), ale zapis wykonuje dopiero po jawnym "TAK" na wejsciu
# interaktywnym - zeby nie odpalic przypadkiem bez patrzenia.

import asyncio
import json
import sys

from app.core.config import get_settings
from app.core.encryption import decrypt_value, encrypt_value


ID_SOURCE = 1  # fakir


async def main() -> None:
    settings = get_settings()

    fakir_host     = getattr(settings, "db_host", None)
    fakir_database = getattr(settings, "db_name", None)
    fakir_user     = getattr(settings, "db_user", None)
    fakir_password = getattr(settings, "db_password", None)

    if hasattr(fakir_password, "get_secret_value"):
        fakir_password = fakir_password.get_secret_value()
    if hasattr(fakir_user, "get_secret_value"):
        fakir_user = fakir_user.get_secret_value()

    missing = [
        name for name, val in [
            ("db_host", fakir_host),
            ("db_name", fakir_database),
            ("db_user", fakir_user),
            ("db_password", fakir_password),
        ] if not val
    ]
    if missing:
        print(f"BLAD: brakuje w ustawieniach backendu: {missing}")
        print("Sprawdz .env kontenera API - te wartosci musza byc ustawione.")
        sys.exit(1)

    # Import lokalny, zeby nie zgadywac sciezki modulu DB przy imporcie na starcie
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

        raw_config = row[0]
        current = json.loads(decrypt_value(raw_config)) if raw_config else {}

        print("AKTUALNE klucze w connection_config:", sorted(current.keys()))

        updated = dict(current)
        updated["host"]     = fakir_host
        updated["database"] = fakir_database
        updated["username"] = fakir_user
        updated["password"] = fakir_password

        # view_name / line_items_view / line_items_join_column / driver /
        # port / encrypt / trust_server_certificate - CELOWO bez zmian.

        print("NOWE klucze w connection_config:", sorted(updated.keys()))
        print(f"view_name pozostaje bez zmian: {updated.get('view_name')!r}")
        print()
        confirm = input(
            f"Zapisac zaktualizowany connection_config dla id_source={ID_SOURCE} (fakir)? [TAK/nie]: "
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
        print(f"OK: zapisano nowy connection_config dla id_source={ID_SOURCE}.")
        print("Nastepny krok: sprawdz logi workera / recznie uruchom sync i potwierdz last_sync_status='ok'.")
    else:
        print(f"OSTRZEZENIE: rowcount={result.rowcount}, sprawdz recznie czy zapis sie powiodl.")

    await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())