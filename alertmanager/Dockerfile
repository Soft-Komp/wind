# =============================================================================
# alertmanager/Dockerfile
# System Windykacja — Alert Manager Service
#
# Osobny, standalone kontener asyncio — BRAK FastAPI.
# Cel: monitoring zdrowia systemu i wysyłka email-alertów.
#
# Budowanie:
#   docker compose build windykacja_alertmanager
# =============================================================================

FROM python:3.12-slim

LABEL maintainer="GPGK Jasło"
LABEL description="System Windykacja — Alert Manager (monitoring + email alerts)"
LABEL version="1.0.0"

# ---------------------------------------------------------------------------
# Zmienne środowiskowe dla Pythona
# ---------------------------------------------------------------------------
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONFAULTHANDLER=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# ---------------------------------------------------------------------------
# Sterownik ODBC 18 dla MSSQL (wymagany przez pyodbc)
# ---------------------------------------------------------------------------
RUN apt-get update && apt-get install -y --no-install-recommends \
        curl \
        gnupg2 \
        unixodbc-dev \
        gcc \
        g++ \
    && curl -sSL https://packages.microsoft.com/keys/microsoft.asc \
        | gpg --dearmor > /usr/share/keyrings/microsoft-prod.gpg \
    && echo "deb [arch=amd64 signed-by=/usr/share/keyrings/microsoft-prod.gpg] \
        https://packages.microsoft.com/debian/12/prod bookworm main" \
        > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y --no-install-recommends msodbcsql18 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# ---------------------------------------------------------------------------
# Katalog roboczy + logi (volume)
# ---------------------------------------------------------------------------
WORKDIR /app

RUN mkdir -p /app/logs && chmod 777 /app/logs

# ---------------------------------------------------------------------------
# Zależności Python
# ---------------------------------------------------------------------------
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ---------------------------------------------------------------------------
# Kod źródłowy
# ---------------------------------------------------------------------------
COPY . .

# ---------------------------------------------------------------------------
# Healthcheck Docker (sprawdza czy plik PID istnieje — proces żyje)
# ---------------------------------------------------------------------------
HEALTHCHECK --interval=60s --timeout=10s --start-period=30s --retries=3 \
    CMD test -f /app/logs/alertmanager.pid && \
        kill -0 $(cat /app/logs/alertmanager.pid) 2>/dev/null || exit 1

# ---------------------------------------------------------------------------
# Uruchomienie — brak FastAPI, czyste asyncio
# ---------------------------------------------------------------------------
CMD ["python", "-u", "main.py"]