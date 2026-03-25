# =============================================================================
# worker/Dockerfile — System Windykacja ARQ Worker
# =============================================================================
# Python 3.12-slim + ODBC Driver 18 + WeasyPrint + supervisord
# Buduje osobny kontener workera na port 8001 (REST API)
# =============================================================================

FROM python:3.12-slim

LABEL maintainer="System Windykacja"
LABEL description="ARQ background worker — email/SMS/PDF/snapshot"
LABEL version="1.0.0"

# ── System dependencies ──────────────────────────────────────────────────────
# ODBC: curl gnupg unixodbc unixodbc-dev
# WeasyPrint: libcairo2 libpango libpangocairo libgdk-pixbuf2 libffi shared-mime-info
# supervisord: supervisor
# timezone: tzdata
# fonts PL: fonts-dejavu-core fonts-liberation
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    gnupg \
    unixodbc \
    unixodbc-dev \
    libcairo2 \
    libpango-1.0-0 \
    libpangocairo-1.0-0 \
    libgdk-pixbuf-2.0-0 \
    libffi-dev \
    shared-mime-info \
    supervisor \
    tzdata \
    fonts-dejavu-core \
    fonts-liberation \
    && rm -rf /var/lib/apt/lists/*

# ── Timezone Europe/Warsaw ────────────────────────────────────────────────────
ENV TZ=Europe/Warsaw
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# ── MSSQL ODBC Driver 18 ─────────────────────────────────────────────────────
RUN curl -sSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor \
        -o /usr/share/keyrings/microsoft-prod.gpg \
    && curl -sSL \
        "https://packages.microsoft.com/config/debian/12/prod.list" \
        -o /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y --no-install-recommends \
        msodbcsql18 \
    && rm -rf /var/lib/apt/lists/*

# ── Python deps ───────────────────────────────────────────────────────────────
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# ── App code ──────────────────────────────────────────────────────────────────
RUN mkdir -p /app/worker
COPY . /app/worker/

# ── Runtime dirs ─────────────────────────────────────────────────────────────
RUN mkdir -p /app/logs /app/snapshots /app/archives /app/pdf_cache

# ── supervisord config ────────────────────────────────────────────────────────
COPY supervisord.conf /etc/supervisor/conf.d/worker.conf

# ── Entrypoint ────────────────────────────────────────────────────────────────
RUN chmod +x /app/worker/start.sh

EXPOSE 8001

CMD ["/app/worker/start.sh"]