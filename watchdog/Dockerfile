# =============================================================================
# watchdog/Dockerfile — System Windykacja Config Watchdog
# =============================================================================
# Lżejszy niż worker/Dockerfile — brak WeasyPrint, OCR, Tesseract (niepotrzebne
# dla samego sprawdzania spójności ustawień). Zachowany ten sam wzorzec
# instalacji ODBC Driver 18, żeby połączenie z MSSQL było identyczne jak
# w backend/worker (te same wersje = zero niespodzianek przy różnicach sterownika).
# =============================================================================

FROM python:3.12-slim

LABEL maintainer="System Windykacja"
LABEL description="Watchdog — okresowa weryfikacja spójności kolekcji ustawień (ENV/DB)"
LABEL version="1.0.0"

# ── System dependencies — minimalny zestaw (bez WeasyPrint/OCR) ─────────────
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    gnupg \
    unixodbc \
    unixodbc-dev \
    tzdata \
    && rm -rf /var/lib/apt/lists/*

# ── Timezone Europe/Warsaw — identycznie jak worker ──────────────────────────
ENV TZ=Europe/Warsaw
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# ── MSSQL ODBC Driver 18 — identyczna procedura jak worker/Dockerfile ────────
RUN curl -sSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor \
        -o /usr/share/keyrings/microsoft-prod.gpg \
    && curl -sSL \
        "https://packages.microsoft.com/config/debian/12/prod.list" \
        -o /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y --no-install-recommends \
        msodbcsql18 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

RUN mkdir -p /app/watchdog
COPY . /app/watchdog/

RUN mkdir -p /app/logs

# Brak EXPOSE — Watchdog nie serwuje HTTP, to czysty proces w tle
CMD ["python", "-m", "watchdog.main"]