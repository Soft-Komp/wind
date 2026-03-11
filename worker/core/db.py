# =============================================================================
# worker/core/db.py — Połączenie z bazą danych (worker)
# =============================================================================
# SQLAlchemy 2.0 async z aioodbc (MSSQL).
# Worker używa tych samych credentials z .env (WORKER_DB_* lub DB_*).
# ORM Models — minimalne, tylko tabele dotykane przez workera.
# =============================================================================

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import AsyncGenerator, Optional

from sqlalchemy import (
    BigInteger, Boolean, DateTime, Integer, Numeric,
    String, Text, func, text,
)
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from worker.settings import get_settings

logger = logging.getLogger("worker.db")

# ── Globalne obiekty ──────────────────────────────────────────────────────────
_engine: Optional[AsyncEngine] = None
_session_factory: Optional[async_sessionmaker[AsyncSession]] = None


# =============================================================================
# ORM Base + Models (tylko tabele używane przez workera)
# =============================================================================

class Base(DeclarativeBase):
    pass


class MonitHistory(Base):
    """skw_MonitHistory — aktualizacja statusu po wysyłce."""
    __tablename__ = "skw_MonitHistory"
    __table_args__ = {"schema": "dbo_ext"}

    id_monit: Mapped[int] = mapped_column("ID_MONIT", BigInteger, primary_key=True)
    id_kontrahenta: Mapped[Optional[int]] = mapped_column("ID_KONTRAHENTA", Integer, nullable=True)
    id_user: Mapped[Optional[int]] = mapped_column("ID_USER", Integer, nullable=True)
    monit_type: Mapped[str] = mapped_column("MonitType", String(20))
    template_id: Mapped[Optional[int]] = mapped_column("TemplateID", Integer, nullable=True)
    status: Mapped[str] = mapped_column("Status", String(20), default="pending")
    recipient: Mapped[Optional[str]] = mapped_column("Recipient", String(100), nullable=True)
    subject: Mapped[Optional[str]] = mapped_column("Subject", String(200), nullable=True)
    message_body: Mapped[Optional[str]] = mapped_column("MessageBody", Text, nullable=True)
    total_debt: Mapped[Optional[float]] = mapped_column("TotalDebt", Numeric(18, 2), nullable=True)
    invoice_numbers: Mapped[Optional[str]] = mapped_column("InvoiceNumbers", String(500), nullable=True)
    pdf_path: Mapped[Optional[str]] = mapped_column("PDFPath", String(500), nullable=True)
    external_id: Mapped[Optional[str]] = mapped_column("ExternalID", String(100), nullable=True)
    scheduled_at: Mapped[Optional[datetime]] = mapped_column("ScheduledAt", DateTime, nullable=True)
    sent_at: Mapped[Optional[datetime]] = mapped_column("SentAt", DateTime, nullable=True)
    created_at: Mapped[Optional[datetime]] = mapped_column("CreatedAt", DateTime, nullable=True)
    retry_count: Mapped[int] = mapped_column("RetryCount", Integer, default=0)
    error_message: Mapped[Optional[str]] = mapped_column("ErrorMessage", String(500), nullable=True)


class AuditLog(Base):
    """skw_AuditLog — zapis akcji workera do audit trail."""
    __tablename__ = "skw_AuditLog"
    __table_args__ = {"schema": "dbo_ext"}

    id_log: Mapped[int] = mapped_column("ID_LOG", BigInteger, primary_key=True)
    timestamp: Mapped[datetime] = mapped_column(
        "Timestamp", DateTime, default=lambda: datetime.now(timezone.utc)
    )
    user_id: Mapped[Optional[int]] = mapped_column("ID_USER", Integer, nullable=True)
    action: Mapped[str] = mapped_column("Action", String(100))
    entity_type: Mapped[Optional[str]] = mapped_column("EntityType", String(50), nullable=True)
    entity_id: Mapped[Optional[str]] = mapped_column("EntityID", String(50), nullable=True)
    old_value: Mapped[Optional[str]] = mapped_column("OldValue", Text, nullable=True)
    new_value: Mapped[Optional[str]] = mapped_column("NewValue", Text, nullable=True)
    ip_address: Mapped[Optional[str]] = mapped_column("IPAddress", String(45), nullable=True)
    user_agent: Mapped[Optional[str]] = mapped_column("UserAgent", String(255), nullable=True)
    success: Mapped[bool] = mapped_column("Success", Boolean, default=True)
    error_message: Mapped[Optional[str]] = mapped_column("ErrorMessage", String(500), nullable=True)
    username: Mapped[Optional[str]] = mapped_column("Username", String(255), nullable=True)
    action_category: Mapped[Optional[str]] = mapped_column("ActionCategory", String(100), nullable=True)
    details: Mapped[Optional[str]] = mapped_column("Details", Text, nullable=True)
    request_url: Mapped[Optional[str]] = mapped_column("RequestURL", String(500), nullable=True)
    request_method: Mapped[Optional[str]] = mapped_column("RequestMethod", String(10), nullable=True)
    request_id: Mapped[Optional[str]] = mapped_column("RequestID", String(100), nullable=True)


# =============================================================================
# Engine + Session Factory
# =============================================================================

def _build_engine(settings) -> AsyncEngine:
    """Buduje async engine dla MSSQL przez aioodbc."""
    conn_str = settings.db_connection_string

    # aioodbc URL format
    from urllib.parse import quote_plus
    encoded = quote_plus(conn_str)
    url = f"mssql+aioodbc:///?odbc_connect={encoded}"

    engine = create_async_engine(
        url,
        echo=False,
        pool_size=5,           # Worker potrzebuje mniej połączeń niż API
        max_overflow=5,
        pool_timeout=30,
        pool_recycle=1800,     # Recykl co 30 min
        pool_pre_ping=True,    # Test połączenia przed użyciem
        connect_args={
            "timeout": 30,
        },
    )
    return engine


async def init_db() -> None:
    """
    Inicjalizuje engine i session factory.
    Wywoływana w ARQ on_startup + FastAPI startup.
    """
    global _engine, _session_factory
    settings = get_settings()

    logger.info(
        "Inicjalizacja połączenia DB",
        extra={
            "host": settings.DB_HOST,
            "port": settings.DB_PORT,
            "database": settings.DB_NAME,
            "user": settings.effective_db_user,
        },
    )

    _engine = _build_engine(settings)
    _session_factory = async_sessionmaker(
        bind=_engine,
        class_=AsyncSession,
        expire_on_commit=False,
        autocommit=False,
        autoflush=False,
    )

    # Test połączenia
    try:
        async with _engine.begin() as conn:
            result = await conn.execute(text("SELECT @@VERSION"))
            version = result.scalar()
            logger.info(
                "Połączenie DB OK",
                extra={"mssql_version": str(version)[:80] if version else "unknown"},
            )
    except Exception as exc:
        logger.error(
            "Błąd inicjalizacji DB — worker wystartuje bez DB",
            extra={"error": str(exc), "error_type": type(exc).__name__},
            exc_info=True,
        )
        raise


async def close_db() -> None:
    """Zamyka engine. Wywoływana w ARQ on_shutdown."""
    global _engine
    if _engine:
        await _engine.dispose()
        logger.info("Połączenie DB zamknięte")
        _engine = None


@asynccontextmanager
async def get_session() -> AsyncGenerator[AsyncSession, None]:
    """
    Context manager dający sesję SQLAlchemy.
    Automatyczny commit lub rollback.

    Usage:
        async with get_session() as db:
            db.add(obj)
    """
    if _session_factory is None:
        raise RuntimeError("DB nie zainicjalizowana — wywołaj init_db() najpierw")

    async with _session_factory() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()


def get_engine() -> AsyncEngine:
    if _engine is None:
        raise RuntimeError("DB nie zainicjalizowana")
    return _engine