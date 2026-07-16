"""0055_encrypt_hook_operation_config

Szyfruje istniejace, jawne operation_config w dbo.skw_source_hooks (Fernet,
ten sam mechanizm co connection_config zrodel). Idempotentna — probuje
odczytac kazdy wiersz jako czysty JSON; jesli sie udaje, to jeszcze
niezaszyfrowany rekord (szyfruje go); jesli sie nie udaje (bo to juz
ciphertext z poprzedniego uruchomienia tej migracji), pomija.

WAZNE: numer rewizji do potwierdzenia wzgledem faktycznego HEAD Alembic —
0054 bylo ostatnia znana mi migracja w tej sesji, mogly dojsc kolejne.

Revision ID : 0055
Revises     : 0054
"""

import json
import logging

from alembic import op
from sqlalchemy import text

revision = "0055"
down_revision = "0054"
branch_labels = None
depends_on = None

logger = logging.getLogger("alembic.migration.0055")


def upgrade() -> None:
    from app.core.encryption import encrypt_value

    bind = op.get_bind()
    result = bind.execute(text(
        "SELECT [id_hook], [operation_config] "
        "FROM [dbo].[skw_source_hooks] "
        "WHERE [operation_config] IS NOT NULL"
    ))
    rows = result.fetchall()

    zaszyfrowano = 0
    juz_zaszyfrowane = 0

    for id_hook, raw_config in rows:
        try:
            # Jesli to sie parsuje jako JSON - jeszcze jawny tekst, trzeba zaszyfrowac.
            json.loads(raw_config)
        except (json.JSONDecodeError, TypeError):
            # Nie parsuje sie jako JSON -> zalozenie: juz ciphertext, pomijamy.
            juz_zaszyfrowane += 1
            continue

        encrypted = encrypt_value(raw_config)
        bind.execute(
            text("UPDATE [dbo].[skw_source_hooks] SET [operation_config] = :cfg WHERE [id_hook] = :id"),
            {"cfg": encrypted, "id": id_hook},
        )
        zaszyfrowano += 1

    logger.info(
        "0055: zaszyfrowano %d rekordow, %d juz bylo zaszyfrowanych",
        zaszyfrowano, juz_zaszyfrowane,
    )
    print(f"[0055] zaszyfrowano={zaszyfrowano} juz_zaszyfrowane={juz_zaszyfrowane}")


def downgrade() -> None:
    """
    UWAGA: downgrade odszyfrowuje z powrotem do jawnego tekstu — celowo,
    zeby symetrycznie cofnac upgrade. To oznacza chwilowy powrot do stanu
    sprzed poprawki bezpieczenstwa podczas rollbacku — akceptowalne tylko
    jako awaryjna procedura, nie do rutynowego uzytku.
    """
    from app.core.encryption import decrypt_value

    bind = op.get_bind()
    result = bind.execute(text(
        "SELECT [id_hook], [operation_config] "
        "FROM [dbo].[skw_source_hooks] "
        "WHERE [operation_config] IS NOT NULL"
    ))
    for id_hook, encrypted in result.fetchall():
        try:
            raw = decrypt_value(encrypted)
        except Exception:
            continue  # juz jawny tekst albo nie do odszyfrowania - pomijamy
        bind.execute(
            text("UPDATE [dbo].[skw_source_hooks] SET [operation_config] = :cfg WHERE [id_hook] = :id"),
            {"cfg": raw, "id": id_hook},
        )