"""
Model tabeli dbo_ext.skw_SystemConfig.
Dynamiczna konfiguracja aplikacji. Cachowana w Redis (TTL: 5 min).
Zmiana config_value działa bez restartu aplikacji.
"""

from sqlalchemy import String, Text
from sqlalchemy.orm import Mapped, mapped_column

from .base import AuditMixin, Base


class SystemConfig(AuditMixin, Base):
    __tablename__ = "skw_SystemConfig"
    __table_args__ = {
        "schema": "dbo_ext",
        "comment": (
            "Dynamiczna konfiguracja. Cachowana Redis TTL=5min. "
            "Zmiana bez restartu. Patrz: config_service.py"
        ),
    }

    id_config: Mapped[int] = mapped_column(
        "ID_CONFIG", primary_key=True, autoincrement=True,
    )
    config_key: Mapped[str] = mapped_column(
        "ConfigKey", String(100), nullable=False, unique=True,
        comment=(
            "Unikalny klucz. Znane klucze: "
            "cors.allowed_origins, otp.expiry_minutes, "
            "delete_token.ttl_seconds, impersonation.max_hours, "
            "master_key.enabled, master_key.pin_hash, "
            "schema_integrity.reaction, snapshot.retention_days"
        ),
    )
    config_value: Mapped[str] = mapped_column(
        "ConfigValue", Text, nullable=False,
        comment="Wartość klucza — JSON lub plain string",
    )
    description: Mapped[str | None] = mapped_column(
        "Description", String(500), nullable=True,
    )

    def __repr__(self) -> str:
        return f"<SystemConfig key={self.config_key!r}>"