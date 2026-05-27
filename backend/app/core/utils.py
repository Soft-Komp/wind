from datetime import datetime, timezone


def dt_utc(dt: datetime | None) -> str | None:
    """Serializuje datetime do ISO 8601 z jawnym +00:00.
    Zakłada że wszystkie naive datetimes z bazy są UTC."""
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.isoformat() + "+00:00"
    return dt.isoformat()