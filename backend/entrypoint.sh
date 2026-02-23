#!/bin/sh
# =============================================================================
# entrypoint.sh — System Windykacja API
# =============================================================================
# Kolejność startu:
#   1. Czekaj na MSSQL (retry z backoff)
#   2. Czekaj na Redis
#   3. Uruchom alembic upgrade head (migracje)
#   4. Uruchom uvicorn (z hot-reload w DEV)
#
# Zmienne środowiskowe (z .env przez docker-compose):
#   DB_HOST, DB_PORT, DB_USER, DB_PASSWORD
#   DB_TRUST_SERVER_CERTIFICATE, DB_ENCRYPT
#   REDIS_URL, APP_ENV, DEBUG
# =============================================================================
set -e

# Kolory do logów (czytelne w docker logs)
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

log_info()    { echo "${BLUE}[ENTRYPOINT]${NC} ℹ️  $1"; }
log_ok()      { echo "${GREEN}[ENTRYPOINT]${NC} ✅ $1"; }
log_warn()    { echo "${YELLOW}[ENTRYPOINT]${NC} ⚠️  $1"; }
log_error()   { echo "${RED}[ENTRYPOINT]${NC} ❌ $1"; }

# ─────────────────────────────────────────────────────────────────────────────
# KROK 1: Sprawdź wymagane zmienne środowiskowe
# ─────────────────────────────────────────────────────────────────────────────
log_info "Weryfikacja konfiguracji..."

_MISSING=""
for VAR in DB_HOST DB_PORT DB_NAME DB_USER DB_PASSWORD SECRET_KEY; do
  if [ -z "$(eval echo \$$VAR)" ]; then
    _MISSING="$_MISSING $VAR"
  fi
done

if [ -n "$_MISSING" ]; then
  log_error "Brakujące zmienne środowiskowe:$_MISSING"
  log_error "Uzupełnij plik .env i uruchom ponownie."
  exit 1
fi

log_ok "Konfiguracja OK — DB_HOST=${DB_HOST}, DB_NAME=${DB_NAME}, APP_ENV=${APP_ENV:-development}"

# ─────────────────────────────────────────────────────────────────────────────
# KROK 2: Czekaj na MSSQL
# ─────────────────────────────────────────────────────────────────────────────
log_info "Czekam na SQL Server (${DB_HOST}:${DB_PORT})..."

# TrustServerCertificate i Encrypt z .env (domyślnie bezpieczne dla dev)
_TRUST="${DB_TRUST_SERVER_CERTIFICATE:-true}"
_ENCRYPT="${DB_ENCRYPT:-false}"

# Konwersja bool Python ↔ string ODBC
if [ "$_TRUST" = "true" ] || [ "$_TRUST" = "True" ] || [ "$_TRUST" = "1" ]; then
  _ODBC_TRUST="yes"
else
  _ODBC_TRUST="no"
fi

if [ "$_ENCRYPT" = "true" ] || [ "$_ENCRYPT" = "True" ] || [ "$_ENCRYPT" = "1" ]; then
  _ODBC_ENCRYPT="yes"
else
  _ODBC_ENCRYPT="no"
fi

_MSSQL_RETRIES=30
_MSSQL_DELAY=3
_ATTEMPT=0

until python3 - << EOF
import os, sys, pyodbc

conn_str = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    "SERVER=tcp:${DB_HOST},${DB_PORT};"
    "DATABASE=${DB_NAME};"
    "UID=${DB_USER};"
    "PWD=${DB_PASSWORD};"
    "Encrypt=${_ODBC_ENCRYPT};"
    "TrustServerCertificate=${_ODBC_TRUST};"
    "Connection Timeout=5;"
)

try:
    conn = pyodbc.connect(conn_str, timeout=5)
    cursor = conn.cursor()
    cursor.execute("SELECT 1 AS ping")
    cursor.fetchone()
    conn.close()
    sys.exit(0)
except pyodbc.Error as e:
    print(f"  Błąd połączenia: {e}", flush=True)
    sys.exit(1)
EOF
do
  _ATTEMPT=$((_ATTEMPT + 1))
  if [ "$_ATTEMPT" -ge "$_MSSQL_RETRIES" ]; then
    log_error "SQL Server niedostępny po ${_MSSQL_RETRIES} próbach (${DB_HOST}:${DB_PORT})"
    log_error "Sprawdź: adres IP, port 1433, SQL Server Auth, firewall"
    exit 1
  fi
  log_warn "SQL Server niedostępny (próba ${_ATTEMPT}/${_MSSQL_RETRIES}), czekam ${_MSSQL_DELAY}s..."
  sleep "$_MSSQL_DELAY"
done

log_ok "SQL Server dostępny (${DB_HOST}:${DB_PORT}/${DB_NAME})"

# ─────────────────────────────────────────────────────────────────────────────
# KROK 3: Czekaj na Redis
# ─────────────────────────────────────────────────────────────────────────────
log_info "Czekam na Redis..."

_REDIS_RETRIES=15
_REDIS_ATTEMPT=0

until python3 - << EOF
import os, sys
try:
    import redis as _redis
    # Parsuj REDIS_URL
    url = os.getenv("REDIS_URL", "redis://redis:6379/0")
    r = _redis.from_url(url, socket_connect_timeout=3)
    r.ping()
    sys.exit(0)
except Exception as e:
    print(f"  Błąd Redis: {e}", flush=True)
    sys.exit(1)
EOF
do
  _REDIS_ATTEMPT=$((_REDIS_ATTEMPT + 1))
  if [ "$_REDIS_ATTEMPT" -ge "$_REDIS_RETRIES" ]; then
    log_error "Redis niedostępny po ${_REDIS_RETRIES} próbach"
    exit 1
  fi
  log_warn "Redis niedostępny (próba ${_REDIS_ATTEMPT}/${_REDIS_RETRIES}), czekam 2s..."
  sleep 2
done

log_ok "Redis dostępny"

# ─────────────────────────────────────────────────────────────────────────────
# KROK 4: Utwórz katalogi robocze (na wypadek gdyby volume był pusty)
# ─────────────────────────────────────────────────────────────────────────────
log_info "Tworzenie katalogów roboczych..."
mkdir -p /app/logs /app/snapshots /app/archives
log_ok "Katalogi gotowe: /app/logs, /app/snapshots, /app/archives"

# ─────────────────────────────────────────────────────────────────────────────
# KROK 5: Alembic — migracje bazy
# ─────────────────────────────────────────────────────────────────────────────
log_info "Uruchamiam migracje Alembic (alembic upgrade head)..."

if alembic upgrade head; then
  log_ok "Migracje zakończone pomyślnie"
else
  log_error "Migracje FAILED — sprawdź logi powyżej"
  log_warn "Jeśli to pierwsze uruchomienie, sprawdź czy schemat dbo_ext istnieje w bazie"
  exit 1
fi

# ─────────────────────────────────────────────────────────────────────────────
# KROK 6: Uruchom serwer
# ─────────────────────────────────────────────────────────────────────────────
log_ok "============================================="
log_ok " System Windykacja — API startuje"
log_ok " URL:  http://localhost:${API_PORT:-8000}"
log_ok " Docs: http://localhost:${API_PORT:-8000}/api/v1/docs"
log_ok " ENV:  ${APP_ENV:-development}"
log_ok "============================================="

if [ "${APP_ENV:-development}" = "development" ] || [ "${DEBUG:-false}" = "true" ]; then
  # DEV: hot reload — zmiany w kodzie → automatyczny restart
  log_info "Tryb: DEVELOPMENT (hot reload włączony)"
  exec uvicorn app.main:app \
    --host 0.0.0.0 \
    --port 8000 \
    --reload \
    --reload-dir /app \
    --log-level warning \
    --no-access-log
else
  # PROD: bez hot reload, z większą liczbą workerów
  log_info "Tryb: PRODUCTION"
  exec uvicorn app.main:app \
    --host 0.0.0.0 \
    --port 8000 \
    --workers 2 \
    --log-level warning \
    --no-access-log \
    --no-server-header
fi