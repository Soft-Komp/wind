# =============================================================================
# backend/Dockerfile — System Windykacja
# =============================================================================
# Obraz: Python 3.12-slim (Debian Bookworm)
# ODBC Driver 18 for SQL Server (Microsoft repo)
# Non-root user: appuser (bezpieczeństwo)
# =============================================================================

FROM python:3.12-slim

# ─────────────────────────────────────────────────────────────────────────────
# Metadane
# ─────────────────────────────────────────────────────────────────────────────
LABEL maintainer="System Windykacja"
LABEL description="FastAPI backend — System Windykacja"
LABEL python.version="3.12"

# ─────────────────────────────────────────────────────────────────────────────
# Zmienne środowiskowe dla buildu
# ─────────────────────────────────────────────────────────────────────────────
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONFAULTHANDLER=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    # Ścieżka do sterownika ODBC (dla pyodbc/aioodbc)
    ODBCINI=/etc/odbc.ini \
    ODBCSYSINI=/etc

# ─────────────────────────────────────────────────────────────────────────────
# Zależności systemowe + Microsoft ODBC Driver 18
# ─────────────────────────────────────────────────────────────────────────────
RUN apt-get update && apt-get install -y --no-install-recommends \
        # Narzędzia sieciowe (healthcheck, entrypoint)
        curl \
        # Kompilacja sterowników
        gcc \
        g++ \
        # ODBC
        unixodbc \
        unixodbc-dev \
        # GPG + repozytoria Microsoft
        gnupg \
        ca-certificates \
        apt-transport-https \
        fonts-dejavu-core \
    # Dodaj repozytorium Microsoft (Debian 12 Bookworm)
    && mkdir -p /usr/share/keyrings \
    && curl -fsSL https://packages.microsoft.com/keys/microsoft.asc \
       | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg \
    && echo "deb [signed-by=/usr/share/keyrings/microsoft-prod.gpg] \
       https://packages.microsoft.com/debian/12/prod bookworm main" \
       > /etc/apt/sources.list.d/mssql-release.list \
    # Zainstaluj ODBC Driver 18 + sqlcmd (mssql-tools18)
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y --no-install-recommends \
        msodbcsql18 \
        mssql-tools18 \
    # Usuń zbędne pakiety kompilacyjne (zmniejsz obraz)
    && apt-get purge -y gcc g++ \
    && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

ENV PATH="$PATH:/opt/mssql-tools18/bin"    
# ─────────────────────────────────────────────────────────────────────────────
# Non-root user (bezpieczeństwo)
# ─────────────────────────────────────────────────────────────────────────────
RUN groupadd --gid 1001 appgroup \
    && useradd --uid 1001 --gid appgroup --shell /bin/sh --create-home appuser

# ─────────────────────────────────────────────────────────────────────────────
# Katalogi robocze z odpowiednimi uprawnieniami
# ─────────────────────────────────────────────────────────────────────────────
RUN mkdir -p /app/logs /app/snapshots /app/archives \
    && chown -R appuser:appgroup /app

WORKDIR /app

# ─────────────────────────────────────────────────────────────────────────────
# Zależności Python (osobna warstwa — cache przy braku zmian w requirements.txt)
# ─────────────────────────────────────────────────────────────────────────────
COPY --chown=appuser:appgroup requirements.txt .

RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# ─────────────────────────────────────────────────────────────────────────────
# Kod aplikacji
# ─────────────────────────────────────────────────────────────────────────────
# UWAGA: W dev docker-compose montuje ./backend:/app (hot reload)
# więc COPY jest używane głównie dla obrazu produkcyjnego
COPY --chown=appuser:appgroup . .

# ─────────────────────────────────────────────────────────────────────────────
# Uprawnienia entrypoint
# ─────────────────────────────────────────────────────────────────────────────
RUN chmod +x /app/entrypoint.sh

# ─────────────────────────────────────────────────────────────────────────────
# Zmień na non-root user
# ─────────────────────────────────────────────────────────────────────────────
USER appuser

# ─────────────────────────────────────────────────────────────────────────────
# Healthcheck (dla docker-compose depends_on: condition: service_healthy)
# ─────────────────────────────────────────────────────────────────────────────
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# Port aplikacji
EXPOSE 8000

# ─────────────────────────────────────────────────────────────────────────────
# Entrypoint — czeka na DB/Redis, uruchamia migracje, startuje uvicorn
# ─────────────────────────────────────────────────────────────────────────────
ENTRYPOINT ["/app/entrypoint.sh"]