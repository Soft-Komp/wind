"""${message}

Revision ID:  ${up_revision}
Revises:      ${down_revision | comma,n}
Create Date:  ${create_date}

INSTRUKCJA:
  upgrade:   alembic upgrade ${up_revision}
  downgrade: alembic downgrade ${down_revision | comma,n}

WAŻNE dla MSSQL:
  - Każda operacja DDL w MSSQL jest auto-transakcyjna.
  - Jeśli coś się wysypie, transakcja jest automatycznie rollbackowana.
  - NIGDY nie mieszaj operacji DDL i DML w jednej migracji na MSSQL —
    może to powodować problemy z transakcjami.
  - Dla zmiany kolumny NOT NULL: kolejność ma znaczenie
      1. Dodaj kolumnę jako NULL
      2. Wypełnij dane (UPDATE)
      3. Zmień na NOT NULL (ALTER COLUMN)

KONWENCJA NAZEWNICTWA:
  Nazwa migracji w -m "..." powinna być:
    - snake_case
    - opisowa: "add_skw_documents_table" nie "update2"
    - czas przeszły: "added_column_x" lub rzeczownik: "skw_initial_schema"

PRZYKŁADY OPERACJI:
  # Dodaj kolumnę
  op.add_column('skw_Users', sa.Column('PhoneNumber', sa.String(20), nullable=True), schema='dbo_ext')

  # Zmień typ kolumny
  op.alter_column('skw_Users', 'Email', existing_type=sa.String(100), type_=sa.String(200), schema='dbo_ext')

  # Dodaj indeks
  op.create_index('IX_skw_Users_Email', 'skw_Users', ['Email'], schema='dbo_ext')

  # Wykonaj surowy SQL (np. INSERT seed danych)
  op.execute("INSERT INTO [dbo_ext].[skw_SystemConfig] ...")

  # Warunkowe — sprawdź czy kolumna istnieje zanim dodasz
  # (przydatne gdy DDL i Alembic mogą być niezsynch.)
  from alembic import op as alembic_op
  bind = op.get_bind()
  inspector = sa.inspect(bind)
  columns = [c['name'] for c in inspector.get_columns('skw_Users', schema='dbo_ext')]
  if 'NewColumn' not in columns:
      op.add_column(...)
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
${imports if imports else ""}

# revision identifiers, used by Alembic.
revision: str = ${repr(up_revision)}
down_revision: Union[str, None] = ${repr(down_revision)}
branch_labels: Union[str, Sequence[str], None] = ${repr(branch_labels)}
depends_on: Union[str, Sequence[str], None] = ${repr(depends_on)}


def upgrade() -> None:
    """
    Migracja w górę — aplikuje zmiany schematu.

    Wykonywana przez: alembic upgrade head / alembic upgrade ${up_revision}
    """
    # ── TODO: Implementacja upgrade ──────────────────────────────────────────
    pass


def downgrade() -> None:
    """
    Migracja w dół — cofa zmiany schematu.

    Wykonywana przez: alembic downgrade -1 / alembic downgrade ${down_revision | comma,n}

    WAŻNE: downgrade musi być dokładnym odwróceniem upgrade().
    Jeśli downgrade jest niemożliwy lub niebezpieczny (np. utrata danych),
    rzuć wyjątek zamiast implementować:
        raise NotImplementedError("Ta migracja jest nieodwracalna — zbyt ryzykowna utrata danych")
    """
    # ── TODO: Implementacja downgrade ────────────────────────────────────────
    pass
