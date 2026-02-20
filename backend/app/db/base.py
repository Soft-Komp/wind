from __future__ import annotations

from app.db.base_class import Base  # noqa: F401

# WAŻNE: importuj wszystkie modele, żeby Alembic je zobaczył.
# Dopasuj listę do realnych plików w app/db/models/.
from app.db.models import (  # noqa: F401
    user,
    role,
    permission,
    role_permission,
    system_config,
    audit_log,
    comment,
    template,
    otp_code,
    refresh_token,
    schema_checksums,
    master_access_log,
    monit_history,
)
