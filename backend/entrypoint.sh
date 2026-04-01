#!/bin/sh
# =============================================================================
# backend/entrypoint.sh — System Windykacja API
# =============================================================================
#
# Sekwencja startu:
#   1. Weryfikacja wymaganych zmiennych środowiskowych
#   2. Tworzenie katalogów roboczych (logs, snapshots, archives)
#   3. Czekaj na MSSQL (retry z exponential backoff)
#   4. Czekaj na Redis (retry z exponential backoff)
#   5. Weryfikacja schematu dbo_ext w bazie
#   6. Alembic — migracje (upgrade head)
#   7. Seedery — dane inicjalne (idempotentne)
#   8. Start uvicorn (DEV: hot-reload | PROD: workers)
#
# Zmienne środowiskowe (z .env przez docker-compose):
#   WYMAGANE:  DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD, SECRET_KEY
#   OPCJONALNE:
#     ODBC_DRIVER        (domyślnie: ODBC Driver 18 for SQL Server)
#     REDIS_URL          (domyślnie: redis://redis:6379/0)
#     REDIS_PASSWORD     (domyślnie: puste)
#     APP_ENV            (domyślnie: development)
#     DEBUG              (domyślnie: false)
#     API_PORT           (domyślnie: 8000)
#     ALEMBIC_MODE       (domyślnie: upgrade)
#                         upgrade → alembic upgrade head (normalny flow)
#                         stamp   → alembic stamp head   (po ręcznym DDL sqlcmd)
#                         skip    → pomiń migracje (debug/emergency)
#     RUN_SEEDS          (domyślnie: auto)
#                         auto    → uruchom seed tylko gdy tabela skw_Roles jest pusta
#                         always  → zawsze (seedery muszą być idempotentne z IF NOT EXISTS)
#                         skip    → pomiń seedery
#     DB_WAIT_RETRIES    (domyślnie: 30)
#     DB_WAIT_SLEEP      (domyślnie: 3)   sekundy między próbami
#     REDIS_WAIT_RETRIES (domyślnie: 15)
#     REDIS_WAIT_SLEEP   (domyślnie: 2)
#
# UWAGA dotycząca seederów:
#   Katalog database/seeds/ jest w ROOT projektu (nie w backend/).
#   Aby seedery działały, docker-compose musi montować go do kontenera.
#   Przykład w docker-compose.yml:
#     volumes:
#       - ./database:/app/database:ro
#   Jeśli montowanie nie jest skonfigurowane → seedery są pomijane (warning).
#   Aplikacja startuje normalnie — seedery NIE są wymagane do startu.
#
# =============================================================================

set -e          # Wyjdź przy błędzie (chyba że obsługujemy ręcznie)
set -u          # Traktuj niezdefiniowane zmienne jako błąd

# =============================================================================
# KOLORY I FUNKCJE LOGOWANIA
# =============================================================================

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

# Timestamp ISO 8601 dla logów
_ts() { date '+%Y-%m-%dT%H:%M:%S%z'; }

log_info()    { printf "%s ${BLUE}[ENTRYPOINT]${NC} ℹ️  %s\n"    "$(_ts)" "$*"; }
log_ok()      { printf "%s ${GREEN}[ENTRYPOINT]${NC} ✅ %s\n"    "$(_ts)" "$*"; }
log_warn()    { printf "%s ${YELLOW}[ENTRYPOINT]${NC} ⚠️  %s\n"  "$(_ts)" "$*"; }
log_error()   { printf "%s ${RED}[ENTRYPOINT]${NC} ❌ %s\n"      "$(_ts)" "$*"; }
log_section() { printf "\n%s ${CYAN}${BOLD}[ENTRYPOINT]${NC} ══ %s ══\n\n" "$(_ts)" "$*"; }

# Wyjście z błędem — zawsze wypisz na stderr
die() {
    log_error "$*"
    exit 1
}

# =============================================================================
# KONFIGURACJA Z WARTOŚCIAMI DOMYŚLNYMI
# =============================================================================

APP_ENV="${APP_ENV:-development}"
DEBUG="${DEBUG:-false}"
API_PORT="${API_PORT:-8000}"

DB_HOST="${DB_HOST:-}"
DB_PORT="${DB_PORT:-1433}"
DB_NAME="${DB_NAME:-}"
DB_USER="${DB_USER:-}"
DB_PASSWORD="${DB_PASSWORD:-}"
ODBC_DRIVER="${ODBC_DRIVER:-ODBC Driver 18 for SQL Server}"

REDIS_URL="${REDIS_URL:-redis://redis:6379/0}"
REDIS_PASSWORD="${REDIS_PASSWORD:-}"

ALEMBIC_MODE="${ALEMBIC_MODE:-upgrade}"
RUN_SEEDS="${RUN_SEEDS:-auto}"

DB_WAIT_RETRIES="${DB_WAIT_RETRIES:-30}"
DB_WAIT_SLEEP="${DB_WAIT_SLEEP:-3}"
REDIS_WAIT_RETRIES="${REDIS_WAIT_RETRIES:-15}"
REDIS_WAIT_SLEEP="${REDIS_WAIT_SLEEP:-2}"

# Katalog seederów — database/ jest w ROOT projektu, montowany przez docker-compose
SEED_DIR="${SEED_DIR:-/app/database/seeds}"

# =============================================================================
# KROK 1: Weryfikacja wymaganych zmiennych środowiskowych
# =============================================================================

log_section "KROK 1: Weryfikacja konfiguracji"

_MISSING=""
for VAR in DB_HOST DB_NAME DB_USER DB_PASSWORD SECRET_KEY; do
    _VAL="$(eval echo "\${${VAR}:-}")"
    if [ -z "$_VAL" ]; then
        _MISSING="$_MISSING  - ${VAR}\n"
    fi
done

if [ -n "$_MISSING" ]; then
    log_error "Brakujące wymagane zmienne środowiskowe:"
    printf "%b" "$_MISSING"
    log_error "Uzupełnij plik .env i uruchom ponownie."
    die "Konfiguracja niekompletna — zatrzymuję start."
fi

# Walidacja ALEMBIC_MODE
case "$ALEMBIC_MODE" in
    upgrade|stamp|skip) ;;
    *) die "Nieprawidłowa wartość ALEMBIC_MODE='$ALEMBIC_MODE'. Dozwolone: upgrade | stamp | skip" ;;
esac

# Walidacja RUN_SEEDS
case "$RUN_SEEDS" in
    auto|always|skip) ;;
    *) die "Nieprawidłowa wartość RUN_SEEDS='$RUN_SEEDS'. Dozwolone: auto | always | skip" ;;
esac

log_ok "Konfiguracja:"
log_ok "  APP_ENV      = $APP_ENV"
log_ok "  DB_HOST      = $DB_HOST:$DB_PORT / $DB_NAME"
log_ok "  REDIS_URL    = $REDIS_URL"
log_ok "  ALEMBIC_MODE = $ALEMBIC_MODE"
log_ok "  RUN_SEEDS    = $RUN_SEEDS"
log_ok "  SEED_DIR     = $SEED_DIR"

# =============================================================================
# KROK 2: Tworzenie katalogów roboczych
# =============================================================================

log_section "KROK 2: Katalogi robocze"

mkdir -p /app/logs /app/snapshots /app/archives
log_ok "Katalogi gotowe: /app/logs, /app/snapshots, /app/archives"

# =============================================================================
# KROK 3: Czekaj na MSSQL
# =============================================================================

log_section "KROK 3: Oczekiwanie na MSSQL ($DB_HOST:$DB_PORT)"

_db_retries=0
until /opt/mssql-tools18/bin/sqlcmd \
        -S "tcp:${DB_HOST},${DB_PORT}" \
        -d "${DB_NAME}" \
        -U "${DB_USER}" \
        -P "${DB_PASSWORD}" \
        -C -Q "SELECT 1 AS probe" \
        -b -l 5 \
        > /dev/null 2>&1; do

    _db_retries=$((_db_retries + 1))

    if [ "$_db_retries" -ge "$DB_WAIT_RETRIES" ]; then
        die "MSSQL niedostępny po ${DB_WAIT_RETRIES} próbach (${DB_HOST}:${DB_PORT}/${DB_NAME}). Sprawdź połączenie sieciowe i dane logowania."
    fi

    # Exponential backoff z limitem 30 sekund
    _sleep=$((DB_WAIT_SLEEP * _db_retries))
    if [ "$_sleep" -gt 30 ]; then _sleep=30; fi

    log_warn "MSSQL nie odpowiada (próba ${_db_retries}/${DB_WAIT_RETRIES}) — czekam ${_sleep}s..."
    sleep "$_sleep"
done

log_ok "MSSQL dostępny: ${DB_HOST}:${DB_PORT} / ${DB_NAME}"

# =============================================================================
# KROK 4: Czekaj na Redis
# =============================================================================

log_section "KROK 4: Oczekiwanie na Redis"

# UWAGA: Używamy Pythona zamiast redis-cli.
# redis-cli NIE jest zainstalowany w obrazie FastAPI (python:3.12-slim).
# Python z biblioteką redis jest zawsze dostępny (jest w requirements.txt).
#
# Skrypt pomocniczy: wysyła PING i czeka na PONG.
# Obsługuje REDIS_URL z hasłem i bez (redis://:pass@host:port/db).

_redis_ping() {
    python3 - <<PYEOF
import sys, os

try:
    import redis
except ImportError:
    # redis nie zainstalowany — fallback do gniazda TCP (tylko connectivity check)
    import socket
    url = os.environ.get("REDIS_URL", "redis://redis:6379/0")
    # Wyciągnij host i port z URL
    host = "redis"
    port = 6379
    try:
        # Prosta ekstrakcja: redis://host:port/db lub redis://:pass@host:port/db
        part = url.split("//", 1)[1]          # host:port/db lub :pass@host:port/db
        if "@" in part:
            part = part.split("@", 1)[1]       # host:port/db
        part = part.split("/")[0]              # host:port
        if ":" in part:
            host, port_str = part.rsplit(":", 1)
            port = int(port_str)
        else:
            host = part
    except Exception:
        pass
    try:
        s = socket.create_connection((host, port), timeout=3)
        s.close()
        sys.exit(0)
    except Exception:
        sys.exit(1)

# redis zainstalowany — pełny PING z uwierzytelnieniem
url = os.environ.get("REDIS_URL", "redis://redis:6379/0")
password = os.environ.get("REDIS_PASSWORD") or None

try:
    client = redis.from_url(url, password=password, socket_connect_timeout=3, socket_timeout=3)
    result = client.ping()
    sys.exit(0 if result else 1)
except Exception as e:
    print(f"Redis PING błąd: {e}", file=sys.stderr)
    sys.exit(1)
PYEOF
}

_redis_retries=0
until _redis_ping; do

    _redis_retries=$((_redis_retries + 1))

    if [ "$_redis_retries" -ge "$REDIS_WAIT_RETRIES" ]; then
        die "Redis niedostępny po ${REDIS_WAIT_RETRIES} próbach (REDIS_URL=${REDIS_URL}). Sprawdź czy kontener redis działa: docker-compose ps"
    fi

    log_warn "Redis nie odpowiada (próba ${_redis_retries}/${REDIS_WAIT_RETRIES}) — czekam ${REDIS_WAIT_SLEEP}s..."
    sleep "$REDIS_WAIT_SLEEP"
done

log_ok "Redis dostępny: ${REDIS_URL}"

# =============================================================================
# KROK 5: Weryfikacja schematu dbo_ext
# =============================================================================

log_section "KROK 5: Weryfikacja schematu dbo_ext"

_SCHEMA_CHECK=$(/opt/mssql-tools18/bin/sqlcmd \
    -S "tcp:${DB_HOST},${DB_PORT}" \
    -d "${DB_NAME}" \
    -U "${DB_USER}" \
    -P "${DB_PASSWORD}" \
    -C -b -h -1 \
    -Q "SET NOCOUNT ON; SELECT CAST(COUNT(*) AS NVARCHAR(10)) FROM sys.schemas WHERE name='dbo_ext';" \
    2>/dev/null | tr -d ' \r\n' || echo "0")

if [ "$_SCHEMA_CHECK" = "0" ] || [ -z "$_SCHEMA_CHECK" ]; then
    log_warn "Schemat dbo_ext NIE ISTNIEJE w bazie ${DB_NAME}!"
    log_warn "Uruchom DDL: sqlcmd -i database/ddl/000_create_schema.sql"
    log_warn ""
    log_warn "Jeśli używasz ALEMBIC_MODE=upgrade — Alembic spróbuje utworzyć schemat."
    log_warn "Jeśli używasz ALEMBIC_MODE=stamp   — schemat MUSI istnieć wcześniej."

    if [ "$ALEMBIC_MODE" = "stamp" ]; then
        die "ALEMBIC_MODE=stamp wymaga istniejącego schematu dbo_ext. Uruchom DDL najpierw."
    fi
else
    log_ok "Schemat dbo_ext istnieje w bazie ${DB_NAME}."

    # Sprawdź ile tabel skw_ już istnieje
    _SKW_COUNT=$(/opt/mssql-tools18/bin/sqlcmd \
        -S "tcp:${DB_HOST},${DB_PORT}" \
        -d "${DB_NAME}" \
        -U "${DB_USER}" \
        -P "${DB_PASSWORD}" \
        -C -b -h -1 \
        -Q "SET NOCOUNT ON; SELECT CAST(COUNT(*) AS NVARCHAR(10)) FROM sys.tables t JOIN sys.schemas s ON t.schema_id=s.schema_id WHERE s.name='dbo_ext' AND t.name LIKE 'skw_%';" \
        2>/dev/null | tr -d ' \r\n' || echo "0")

    log_ok "Tabele skw_* w dbo_ext: ${_SKW_COUNT}/13"

    # Ostrzeżenie gdy tabele istnieją ale ALEMBIC_MODE=upgrade
    if [ "$_SKW_COUNT" -gt "0" ] && [ "$ALEMBIC_MODE" = "upgrade" ]; then
        log_warn "Wykryto ${_SKW_COUNT} tabel skw_* w bazie."
        log_warn "Jeśli DDL był wykonany ręcznie (sqlcmd), użyj ALEMBIC_MODE=stamp"
        log_warn "aby uniknąć błędu 'Table already exists'."
        log_warn "Kontynuuję z ALEMBIC_MODE=upgrade — migracja 0001 jest idempotentna (IF NOT EXISTS)."
    fi
fi

# =============================================================================
# KROK 5b: Tworzenie schematu dbo_ext (jeśli nie istnieje)
# =============================================================================

log_section "KROK 5b: Bootstrap schematu dbo_ext"

# Ścieżka do pliku DDL — montowana przez docker-compose
# volumes:
#   - ./database:/app/database:ro
_SCHEMA_DDL="${SEED_DIR%/seeds}/ddl/000_create_schema.sql"

# Sprawdź czy schemat już istnieje (to samo zapytanie co w KROKU 5)
_SCHEMA_EXISTS=$(/opt/mssql-tools18/bin/sqlcmd \
    -S "tcp:${DB_HOST},${DB_PORT}" \
    -d "${DB_NAME}" \
    -U "${DB_USER}" \
    -P "${DB_PASSWORD}" \
    -C -b -h -1 \
    -Q "SET NOCOUNT ON; SELECT CAST(ISNULL(SCHEMA_ID(N'dbo_ext'), 0) AS NVARCHAR(10));" \
    2>/dev/null | tr -d ' \r\n' || echo "0")

if [ "$_SCHEMA_EXISTS" != "0" ]; then
    log_ok "Schemat dbo_ext istnieje (schema_id=${_SCHEMA_EXISTS}) — pomijam DDL."
else
    log_info "Schemat dbo_ext NIE ISTNIEJE — uruchamiam DDL bootstrap..."

    # Czy plik DDL jest dostępny?
    if [ ! -f "$_SCHEMA_DDL" ]; then
        log_error "Brak pliku DDL: ${_SCHEMA_DDL}"
        log_error "Upewnij się że docker-compose montuje katalog database/:"
        log_error "  volumes:"
        log_error "    - ./database:/app/database:ro"
        die "Nie mogę utworzyć schematu dbo_ext — brak pliku DDL. Zatrzymuję start."
    fi

    log_info "Plik DDL: ${_SCHEMA_DDL}"

    # Wykonaj DDL — przechwytuj stdout i stderr
    _DDL_OUT=$(/opt/mssql-tools18/bin/sqlcmd \
        -S "tcp:${DB_HOST},${DB_PORT}" \
        -d "${DB_NAME}" \
        -U "${DB_USER}" \
        -P "${DB_PASSWORD}" \
        -C -b -I -r1 \
        -i "$_SCHEMA_DDL" \
        2>&1)
    _DDL_RC=$?

    # Pokaż output niezależnie od wyniku
    if [ -n "$_DDL_OUT" ]; then
        log_info "DDL output:"
        printf '%s\n' "$_DDL_OUT" | while IFS= read -r _line; do
            [ -n "$_line" ] && log_info "  $_line"
        done
    fi

    if [ "$_DDL_RC" -eq 0 ]; then
        log_ok "Schemat dbo_ext utworzony pomyślnie."
    else
        log_error "Błąd tworzenia schematu dbo_ext (exit code: ${_DDL_RC})"
        log_error "Sprawdź uprawnienia: użytkownik '${DB_USER}' musi mieć prawo CREATE SCHEMA."
        log_error "Na SQL Server: GRANT CREATE SCHEMA TO [${DB_USER}];"
        log_error "Lub uruchom ręcznie jako sa:"
        log_error "  sqlcmd -S tcp:HOST,PORT -d ${DB_NAME} -U sa -P PASS -C -b -i database/ddl/000_create_schema.sql"
        die "Bootstrap schematu dbo_ext nieudany — zatrzymuję start."
    fi

    # Weryfikacja końcowa — upewnij się że schemat faktycznie istnieje
    _SCHEMA_VERIFY=$(/opt/mssql-tools18/bin/sqlcmd \
        -S "tcp:${DB_HOST},${DB_PORT}" \
        -d "${DB_NAME}" \
        -U "${DB_USER}" \
        -P "${DB_PASSWORD}" \
        -C -b -h -1 \
        -Q "SET NOCOUNT ON; SELECT CAST(ISNULL(SCHEMA_ID(N'dbo_ext'), 0) AS NVARCHAR(10));" \
        2>/dev/null | tr -d ' \r\n' || echo "0")

    if [ "$_SCHEMA_VERIFY" = "0" ]; then
        die "Weryfikacja nieudana: schemat dbo_ext nadal nie istnieje po DDL. Sprawdź uprawnienia."
    fi

    log_ok "Weryfikacja: schemat dbo_ext istnieje (schema_id=${_SCHEMA_VERIFY})."
fi

# =============================================================================
# KROK 6: Alembic — migracje
# =============================================================================

log_section "KROK 6: Alembic (ALEMBIC_MODE=${ALEMBIC_MODE})"

cd /app

case "$ALEMBIC_MODE" in

    upgrade)
        log_info "Uruchamiam: alembic upgrade head"
        if alembic upgrade head; then
            log_ok "Migracje zakończone pomyślnie."

            # ── Cleanup wiszących checksumów ─────────────────────────────────
            # Migracja może wstawić checksums dla widoków wymagających ręcznego
            # DDL (np. 018_faktura_widoki_dbo.sql). Usunięcie "wiszących" wpisów
            # pozwala aplikacji przeliczyć checksums od nowa przy starcie.
            log_info "Czyszczenie wiszących wpisów w skw_SchemaChecksums..."
            /opt/mssql-tools18/bin/sqlcmd \
                -S "tcp:${DB_HOST},${DB_PORT}" \
                -d "${DB_NAME}" \
                -U "${DB_USER}" \
                -P "${DB_PASSWORD}" \
                -C -b -h -1 \
                -Q "
                    SET NOCOUNT ON;
                    IF OBJECT_ID(N'[dbo_ext].[skw_SchemaChecksums]', N'U') IS NOT NULL
                    BEGIN
                        DELETE FROM [dbo_ext].[skw_SchemaChecksums]
                        WHERE ObjectName NOT IN (
                            SELECT s.name + '.' + o.name
                            FROM sys.objects  o
                            JOIN sys.schemas  s ON o.schema_id = s.schema_id
                            WHERE o.type IN ('V', 'P', 'FN', 'IF', 'TF')
                        );
                        PRINT 'SchemaChecksums cleanup: ' + CAST(@@ROWCOUNT AS NVARCHAR) + ' wpisow usunieto.';
                    END
                " 2>/dev/null \
                && log_ok "SchemaChecksums cleanup — zakończony." \
                || log_warn "SchemaChecksums cleanup — błąd (niekrytyczny, kontynuuję)."
        else
            _EXIT_CODE=$?
            log_error "alembic upgrade head zakończone błędem (exit code: ${_EXIT_CODE})"
            log_error ""
            log_error "Możliwe przyczyny:"
            log_error "  1. Tabele skw_* już istnieją (DDL przez sqlcmd) → użyj ALEMBIC_MODE=stamp"
            log_error "  2. Błąd połączenia z bazą → sprawdź DB_HOST/DB_USER/DB_PASSWORD"
            log_error "  3. Brak modelu w app/db/models/ → sprawdź import w alembic/env.py"
            log_error "  4. Schemat dbo_ext nie istnieje → uruchom database/ddl/000_create_schema.sql"
            die "Migracje FAILED — zatrzymuję start."
        fi
        ;;

    stamp)
        log_info "Uruchamiam: alembic stamp head"
        log_info "(Tryb stamp: oznaczam bazę jako aktualną bez wykonywania migracji)"
        log_info "Używaj po ręcznym uruchomieniu plików DDL przez sqlcmd."
        if alembic stamp head; then
            log_ok "alembic stamp head — zakończone pomyślnie."
            log_ok "Baza danych oznaczona jako aktualna (revision: head)."
        else
            die "alembic stamp head FAILED — sprawdź konfigurację alembic.ini i env.py."
        fi
        ;;

    skip)
        log_warn "ALEMBIC_MODE=skip — pomijam migracje Alembic."
        log_warn "UWAGA: Schema może być nieaktualna. Używaj tylko do debugowania."
        ;;

esac

# Pokaż aktualną wersję po migracji
_CURRENT=$(alembic current 2>/dev/null | head -1 || echo "nieznana")
log_ok "Aktualna wersja Alembic: ${_CURRENT}"

# =============================================================================
# KROK 7: Seedery — dane inicjalne
# =============================================================================

log_section "KROK 7: Seedery (RUN_SEEDS=${RUN_SEEDS})"

# Lista seederów w kolejności (seedery muszą być idempotentne: IF NOT EXISTS / MERGE)
SEED_FILES="
01_roles.sql
02_permissions.sql
03_role_permissions.sql
04_admin_user.sql
05_system_config.sql
06_templates_permissions.sql
07_default_templates.sql
08_faktura_permissions.sql
09_faktura_role_permissions.sql
10_system_config_faktura.sql
11_alert_config.sql
"

_should_run_seeds=false

case "$RUN_SEEDS" in
    always)
        _should_run_seeds=true
        log_info "RUN_SEEDS=always — uruchamiam seedery bezwarunkowo."
        ;;

    skip)
        log_info "RUN_SEEDS=skip — pomijam seedery."
        ;;

    auto)
        # Sprawdź czy tabela skw_Roles ma jakiekolwiek dane
        _ROLES_COUNT=$(/opt/mssql-tools18/bin/sqlcmd \
            -S "tcp:${DB_HOST},${DB_PORT}" \
            -d "${DB_NAME}" \
            -U "${DB_USER}" \
            -P "${DB_PASSWORD}" \
            -C -b -h -1 \
            -Q "SET NOCOUNT ON; IF OBJECT_ID(N'[dbo_ext].[skw_Roles]','U') IS NOT NULL SELECT CAST(COUNT(*) AS NVARCHAR(10)) FROM [dbo_ext].[skw_Roles] ELSE SELECT N'0';" \
            2>/dev/null | tr -d ' \r\n' || echo "0")

        if [ "$_ROLES_COUNT" = "0" ]; then
            log_info "RUN_SEEDS=auto — tabela skw_Roles jest pusta → uruchamiam seedery."
            _should_run_seeds=true
        else
            log_info "RUN_SEEDS=auto — tabela skw_Roles ma ${_ROLES_COUNT} rekordów → pomijam seedery."
        fi
        ;;
esac

if [ "$_should_run_seeds" = "true" ]; then

    # ── Katalog seederów istnieje? ────────────────────────────────────────────
    if [ ! -d "$SEED_DIR" ]; then
        log_warn "Katalog seederów nie istnieje: ${SEED_DIR}"
        log_warn "Upewnij się że docker-compose montuje database/:"
        log_warn "  volumes:"
        log_warn "    - ./database:/app/database:ro"
        log_warn "Pomijam seedery — aplikacja startuje bez danych inicjalnych."

    else
        # ── Inicjalizacja katalogów logów ─────────────────────────────────────
        # WAŻNE: używamy || true żeby set -e nie zabił skryptu przy błędzie IO
        SEED_LOG_DIR="/app/logs/seeds"
        _RUN_TS=$(date '+%Y%m%d_%H%M%S')
        _RUN_DATE=$(date '+%Y-%m-%dT%H:%M:%S%z')
        _REPORT_FILE="${SEED_LOG_DIR}/report_$(date '+%Y-%m-%d').jsonl"

        # Spróbuj utworzyć katalog — raportuj błąd ale nie zabijaj startu
        if mkdir -p "$SEED_LOG_DIR" 2>/dev/null; then
            _LOGS_AVAILABLE=true
            log_info "Logi seederów → ${SEED_LOG_DIR}/"
            log_info "Raport JSONL   → ${_REPORT_FILE}"
        else
            _LOGS_AVAILABLE=false
            log_warn "Nie można utworzyć katalogu logów: ${SEED_LOG_DIR}"
            log_warn "Logi seederów będą tylko na konsoli."
            log_warn "Sprawdź uprawnienia do /app/logs/ (Docker volume?)"
        fi

        # ── Pliki tymczasowe — tworzymy je od razu (touch) ───────────────────
        _TMP_OUT="/tmp/sw_seed_out_$$"
        _TMP_ERR="/tmp/sw_seed_err_$$"
        touch "$_TMP_OUT" "$_TMP_ERR" 2>/dev/null || {
            # /tmp niedostępny? Ekstremalny edge-case, ale obsługujemy
            _TMP_OUT="/app/logs/seed_tmp_out_$$"
            _TMP_ERR="/app/logs/seed_tmp_err_$$"
            touch "$_TMP_OUT" "$_TMP_ERR" 2>/dev/null || true
        }

        # Cleanup przy wyjściu z kontenera
        trap 'rm -f "$_TMP_OUT" "$_TMP_ERR" 2>/dev/null; trap - EXIT INT TERM' \
            EXIT INT TERM

        # ── Funkcja pomocnicza: dopisz wpis do JSONL raportu ─────────────────
        _report_jsonl() {
            # Argumenty: seed status exit_code duration_ts error log_path
            _r_seed="$1"; _r_status="$2"; _r_code="$3"
            _r_dur="$4";  _r_err="$5";   _r_log="$6"
            if [ "$_LOGS_AVAILABLE" = "true" ]; then
                printf '{"ts":"%s","run_ts":"%s","seed":"%s","status":"%s","exit_code":%s,"duration_sec":"%s","error":"%s","log":"%s"}\n' \
                    "$(date '+%Y-%m-%dT%H:%M:%S%z')" \
                    "$_RUN_TS" \
                    "$_r_seed" \
                    "$_r_status" \
                    "$_r_code" \
                    "$_r_dur" \
                    "$_r_err" \
                    "$_r_log" \
                    >> "$_REPORT_FILE" 2>/dev/null || true
            fi
        }

        # ── Liczniki ──────────────────────────────────────────────────────────
        _seed_ok=0
        _seed_fail=0
        _seed_skip=0

        # ════════════════════════════════════════════════════════════════════
        # PĘTLA — każdy seed oddzielnie
        # ════════════════════════════════════════════════════════════════════
        for SEED_FILE in $SEED_FILES; do

            # Trim whitespace (newline z wieloliniowej zmiennej)
            SEED_FILE=$(printf '%s' "$SEED_FILE" | tr -d ' \t\r\n')
            [ -z "$SEED_FILE" ] && continue

            SEED_PATH="${SEED_DIR}/${SEED_FILE}"
            _SEED_NAME="${SEED_FILE%.sql}"   # np. "01_roles"

            # ── Plik seeda istnieje? ──────────────────────────────────────────
            if [ ! -f "$SEED_PATH" ]; then
                log_warn "[SEED] Pomijam — brak pliku: ${SEED_PATH}"
                _seed_skip=$((_seed_skip + 1))
                _report_jsonl "$SEED_FILE" "SKIP" "0" "0" "file_not_found" ""
                continue
            fi

            # ── Ścieżka do pliku logu (tylko jeśli katalog dostępny) ──────────
            if [ "$_LOGS_AVAILABLE" = "true" ]; then
                _SEED_LOG="${SEED_LOG_DIR}/${_RUN_TS}_${_SEED_NAME}.log"
            else
                _SEED_LOG=""
            fi

            # ── Nagłówek do konsoli ───────────────────────────────────────────
            log_info "[SEED] ══════════════════════════════════════════"
            log_info "[SEED] Uruchamiam : ${SEED_FILE}"
            log_info "[SEED] Ścieżka   : ${SEED_PATH}"
            [ -n "$_SEED_LOG" ] && log_info "[SEED] Log       : ${_SEED_LOG}"

            # ── Nagłówek do pliku logu (jeśli dostępny) ───────────────────────
            if [ -n "$_SEED_LOG" ]; then
                {
                    printf "================================================================\n"
                    printf " SEED LOG: %s\n" "$SEED_FILE"
                    printf "================================================================\n"
                    printf " Czas:      %s\n" "$(date '+%Y-%m-%dT%H:%M:%S%z')"
                    printf " Plik:      %s\n" "$SEED_PATH"
                    printf " Serwer:    tcp:%s,%s / %s\n" "$DB_HOST" "$DB_PORT" "$DB_NAME"
                    printf " User:      %s\n" "$DB_USER"
                    printf "================================================================\n\n"
                } > "$_SEED_LOG" 2>/dev/null || {
                    log_warn "[SEED] Nie moge zapisac naglowka do: ${_SEED_LOG}"
                    _SEED_LOG=""
                }
            fi

            # ── Wyczyść pliki tymczasowe ──────────────────────────────────────
            : > "$_TMP_OUT" 2>/dev/null || true
            : > "$_TMP_ERR" 2>/dev/null || true

            _T_START="$(date '+%Y-%m-%dT%H:%M:%S%z')"

            # ── Uruchomienie sqlcmd ───────────────────────────────────────────
            # KLUCZOWE: "|| true" na końcu bloku if/else — set -e nie propaguje
            # -r1 : komunikaty błędów → stderr (oddzielone od stdout)
            # -b  : EXIT on SQL error (returncode != 0)
            # -I  : QUOTED_IDENTIFIER ON (wymagane przez nasze MERGE)
            # -C  : TrustServerCertificate (dev / self-signed)
            if /opt/mssql-tools18/bin/sqlcmd \
                    -S "tcp:${DB_HOST},${DB_PORT}" \
                    -d "${DB_NAME}" \
                    -U "${DB_USER}" \
                    -P "${DB_PASSWORD}" \
                    -C -b -I -r1 \
                    -i "$SEED_PATH" \
                    > "$_TMP_OUT" 2> "$_TMP_ERR"; then

                # ════════════════════════
                # SUKCES
                # ════════════════════════
                log_ok "[SEED] ${SEED_FILE} — OK"

                # Pokaż PRINT-y z SQL (postęp seeda, liczba wierszy itp.)
                if [ -s "$_TMP_OUT" ]; then
                    log_info "[SEED] ── SQL output (PRINT) ──────────────────"
                    while IFS= read -r _ln; do
                        [ -n "$_ln" ] && log_info "[SEED]   $_ln"
                    done < "$_TMP_OUT"
                fi

                # Dopisz stdout do pliku logu
                if [ -n "$_SEED_LOG" ]; then
                    {
                        printf "\n--- STDOUT ---\n"
                        cat "$_TMP_OUT" 2>/dev/null || true
                        printf "\n--- WYNIK: OK ---\n"
                        printf "Czas zakonczenia: %s\n" "$(date '+%Y-%m-%dT%H:%M:%S%z')"
                    } >> "$_SEED_LOG" 2>/dev/null || true
                fi

                _seed_ok=$((_seed_ok + 1))
                _report_jsonl "$SEED_FILE" "OK" "0" "$_T_START" "" "$_SEED_LOG"

            else
                # ════════════════════════
                # BŁĄD SQLCMD
                # ════════════════════════
                _RC=$?

                log_error "[SEED] ══════════════════════════════════════════"
                log_error "[SEED] BŁĄD: ${SEED_FILE} (sqlcmd exit code: ${_RC})"
                log_error "[SEED] ══════════════════════════════════════════"

                # Błędy SQL Server idą na stderr dzięki -r1
                if [ -s "$_TMP_ERR" ]; then
                    log_error "[SEED] ── SQL Server ERROR (stderr) ───────────"
                    while IFS= read -r _ln; do
                        [ -n "$_ln" ] && log_error "[SEED]   $_ln"
                    done < "$_TMP_ERR"
                    log_error "[SEED] ──────────────────────────────────────"
                else
                    log_error "[SEED] (brak wyjscia na stderr — sprawdz polaczenie DB)"
                fi

                # Kontekst przed błędem (stdout — to co się zdążyło wykonać)
                if [ -s "$_TMP_OUT" ]; then
                    log_error "[SEED] ── Output przed bledem (stdout) ───────"
                    while IFS= read -r _ln; do
                        [ -n "$_ln" ] && log_error "[SEED]   $_ln"
                    done < "$_TMP_OUT"
                    log_error "[SEED] ──────────────────────────────────────"
                fi

                # Dopisz pełne logi do pliku
                if [ -n "$_SEED_LOG" ]; then
                    {
                        printf "\n--- STDOUT (przed bledem) ---\n"
                        cat "$_TMP_OUT" 2>/dev/null || true
                        printf "\n--- STDERR (bledy SQL) ---\n"
                        cat "$_TMP_ERR" 2>/dev/null || true
                        printf "\n--- WYNIK: ERROR (exit_code=%d) ---\n" "$_RC"
                        printf "Czas zakonczenia: %s\n" "$(date '+%Y-%m-%dT%H:%M:%S%z')"
                    } >> "$_SEED_LOG" 2>/dev/null || true
                    log_error "[SEED] Pelny log: ${_SEED_LOG}"
                fi

                log_error "[SEED] ══════════════════════════════════════════"

                # Zbierz treść błędu do raportu JSONL (pierwsze 5 linii)
                _ERR_BRIEF=$(head -5 "$_TMP_ERR" 2>/dev/null \
                    | tr '\n' '|' \
                    | sed "s/\"/'/g; s/[[:cntrl:]]//g" \
                    || true)

                _seed_fail=$((_seed_fail + 1))
                _report_jsonl "$SEED_FILE" "ERROR" "$_RC" "$_T_START" \
                    "$_ERR_BRIEF" "$_SEED_LOG"

                # ── Krytyczność seeda ─────────────────────────────────────────
                # 01_roles + 02_permissions = system nieużywalny bez nich
                # 03-05 = dane opcjonalne, system ruszy z ostrzeżeniem
                case "$SEED_FILE" in
                    01_roles.sql|02_permissions.sql)
                        log_error "[SEED] SEED KRYTYCZNY — zatrzymuje start!"
                        log_error "[SEED] Bez ról/uprawnień autentykacja nie działa."
                        log_error ""
                        log_error "[SEED] Jak debugować:"
                        log_error "[SEED]   docker exec -it windykacja_api /opt/mssql-tools18/bin/sqlcmd \\"
                        log_error "[SEED]     -S tcp:\${DB_HOST},\${DB_PORT} -d \${DB_NAME} \\"
                        log_error "[SEED]     -U \${DB_USER} -P \${DB_PASSWORD} \\"
                        log_error "[SEED]     -C -b -I -r1 -i ${SEED_PATH}"
                        log_error ""
                        log_error "[SEED] Typowe przyczyny:"
                        log_error "[SEED]   1. Schemat dbo_ext nie istnieje (ALEMBIC_MODE=skip bez wcześniejszego DDL)"
                        log_error "[SEED]   2. Tabele skw_* nie istnieją (migracje nie wykonane)"
                        log_error "[SEED]   3. Brak uprawnień INSERT na dbo_ext dla użytkownika ${DB_USER}"
                        log_error "[SEED]   4. Błąd składniowy w pliku SQL (GO bez LF?)"

                        # Wymuś cleanup przed exitem
                        rm -f "$_TMP_OUT" "$_TMP_ERR" 2>/dev/null || true
                        trap - EXIT INT TERM
                        exit 1
                        ;;
                    *)
                        log_warn "[SEED] Seed niekrytyczny — kontynuuje."
                        log_warn "[SEED] System uruchomi sie, ale dane inicjalne moga byc niekompletne."
                        ;;
                esac

            fi  # if sqlcmd

        done  # for SEED_FILE

        # ── Podsumowanie ──────────────────────────────────────────────────────
        log_info "[SEED] ══════════════════════════════════════════"
        log_ok   "Seedery: ${_seed_ok} OK | ${_seed_fail} BLEDOW | ${_seed_skip} POMINIETYCH"
        [ -n "$_REPORT_FILE" ] && [ "$_LOGS_AVAILABLE" = "true" ] && \
            log_info "[SEED] Raport: ${_REPORT_FILE}"
        log_info "[SEED] ══════════════════════════════════════════"

        if [ "$_seed_fail" -gt 0 ]; then
            log_warn "Niektoré seedery nie powiodly sie."
            log_warn "Sprawdz logi powyzej lub: docker exec windykacja_api cat ${_REPORT_FILE}"
        fi

        # Cleanup plików tymczasowych
        rm -f "$_TMP_OUT" "$_TMP_ERR" 2>/dev/null || true
        trap - EXIT INT TERM

    fi  # if [ ! -d "$SEED_DIR" ]
fi  # if [ "$_should_run_seeds" = "true" ]

# =============================================================================
# KROK 7b: Seedery modułowe — zawsze uruchamiane (MERGE idempotentne)
# Niezależne od RUN_SEEDS — moduły mogą być dodawane po inicjalnym setupie.
# Warunek uruchomienia: skw_Permissions istnieje (migracje wykonane).
# =============================================================================

log_section "KROK 7b: Seedery modułowe (zawsze)"

MODULE_SEED_FILES="
08_faktura_permissions.sql
09_faktura_role_permissions.sql
10_system_config_faktura.sql
"

# Sprawdź czy tabela skw_Permissions istnieje — jeśli nie, seedery nie zadziałają
_PERMS_EXISTS=$(/opt/mssql-tools18/bin/sqlcmd \
    -S "tcp:${DB_HOST},${DB_PORT}" \
    -d "${DB_NAME}" \
    -U "${DB_USER}" \
    -P "${DB_PASSWORD}" \
    -C -b -h -1 \
    -Q "SET NOCOUNT ON; SELECT CAST(COUNT(*) AS NVARCHAR(10)) FROM sys.tables t JOIN sys.schemas s ON t.schema_id=s.schema_id WHERE s.name='dbo_ext' AND t.name='skw_Permissions';" \
    2>/dev/null | tr -d ' \r\n' || echo "0")

if [ "$_PERMS_EXISTS" = "0" ]; then
    log_warn "[SEED-MOD] Tabela skw_Permissions nie istnieje — pomijam seedery modułowe."
    log_warn "[SEED-MOD] Uruchom migracje Alembic przed seedami modułowymi."
else
    log_info "[SEED-MOD] Tabela skw_Permissions istnieje — uruchamiam seedery modułowe."

    if [ ! -d "$SEED_DIR" ]; then
        log_warn "[SEED-MOD] Brak katalogu seederów: ${SEED_DIR} — pomijam."
    else
        _mod_ok=0
        _mod_fail=0
        _mod_skip=0

        for MOD_SEED in $MODULE_SEED_FILES; do
            MOD_SEED=$(printf '%s' "$MOD_SEED" | tr -d ' \t\r\n')
            [ -z "$MOD_SEED" ] && continue

            MOD_PATH="${SEED_DIR}/${MOD_SEED}"

            if [ ! -f "$MOD_PATH" ]; then
                log_warn "[SEED-MOD] Brak pliku: ${MOD_PATH} — pomijam."
                _mod_skip=$((_mod_skip + 1))
                continue
            fi

            log_info "[SEED-MOD] Uruchamiam: ${MOD_SEED}"

            if /opt/mssql-tools18/bin/sqlcmd \
                    -S "tcp:${DB_HOST},${DB_PORT}" \
                    -d "${DB_NAME}" \
                    -U "${DB_USER}" \
                    -P "${DB_PASSWORD}" \
                    -C -b -I -r1 \
                    -i "$MOD_PATH" \
                    > /tmp/sw_mod_out_$$ 2> /tmp/sw_mod_err_$$; then

                log_ok "[SEED-MOD] ${MOD_SEED} — OK"
                if [ -s /tmp/sw_mod_out_$$ ]; then
                    while IFS= read -r _ln; do
                        [ -n "$_ln" ] && log_info "[SEED-MOD]   $_ln"
                    done < /tmp/sw_mod_out_$$
                fi
                _mod_ok=$((_mod_ok + 1))
            else
                _RC=$?
                log_warn "[SEED-MOD] ${MOD_SEED} — BŁĄD (exit: ${_RC}) — niekrytyczny, kontynuuję."
                if [ -s /tmp/sw_mod_err_$$ ]; then
                    while IFS= read -r _ln; do
                        [ -n "$_ln" ] && log_warn "[SEED-MOD]   $_ln"
                    done < /tmp/sw_mod_err_$$
                fi
                _mod_fail=$((_mod_fail + 1))
            fi

            rm -f /tmp/sw_mod_out_$$ /tmp/sw_mod_err_$$ 2>/dev/null || true
        done

        log_ok "[SEED-MOD] Seedery modułowe: ${_mod_ok} OK | ${_mod_fail} BŁĘDÓW | ${_mod_skip} POMINIĘTYCH"
    fi
fi

# =============================================================================
# KROK 7c: DDL Fakir Write User — tworzenie użytkownika SQL dla Fakira
# Uruchamiany tylko jeśli FAKIR_DB_PASSWORD jest ustawiony w środowisku.
# =============================================================================
if [ -n "${FAKIR_DB_PASSWORD:-}" ]; then
    log_section "KROK 7c: Fakir Write User DDL"
    _FAKIR_DDL="/app/database/ddl/021_fakir_write_user.sql"

    if [ ! -f "$_FAKIR_DDL" ]; then
        log_warn "[FAKIR-DDL] Brak pliku: ${_FAKIR_DDL} — pomijam."
    else
        _FAKIR_OUT=$(/opt/mssql-tools18/bin/sqlcmd \
            -S "tcp:${DB_HOST},${DB_PORT}" \
            -d "${DB_NAME}" \
            -U "${DB_USER}" \
            -P "${DB_PASSWORD}" \
            -v FAKIR_PASSWORD="${FAKIR_DB_PASSWORD}" \
            -C -b -I -r1 \
            -i "$_FAKIR_DDL" \
            2>&1)
        _FAKIR_RC=$?

        if [ -n "$_FAKIR_OUT" ]; then
            while IFS= read -r _ln; do
                [ -n "$_ln" ] && log_info "[FAKIR-DDL]   $_ln"
            done <<EOF
$_FAKIR_OUT
EOF
        fi

        if [ "$_FAKIR_RC" -eq 0 ]; then
            log_ok "[FAKIR-DDL] 021_fakir_write_user.sql — OK"
        else
            log_warn "[FAKIR-DDL] 021_fakir_write_user.sql — BŁĄD (exit: ${_FAKIR_RC}) — niekrytyczny, kontynuuję."
        fi
    fi
else
    log_info "[FAKIR-DDL] FAKIR_DB_PASSWORD nie ustawiony — pomijam tworzenie użytkownika Fakir."
fi

# =============================================================================
# KROK 8: Start serwera
# =============================================================================

log_section "KROK 8: Start uvicorn"

log_ok "════════════════════════════════════════════"
log_ok " System Windykacja — API"
log_ok " URL:  http://0.0.0.0:${API_PORT}"
log_ok " Docs: http://0.0.0.0:${API_PORT}/api/v1/docs"
log_ok " ENV:  ${APP_ENV}"
log_ok " DB:   ${DB_HOST}:${DB_PORT}/${DB_NAME}"
log_ok "════════════════════════════════════════════"

if [ "$APP_ENV" = "development" ] || [ "$DEBUG" = "true" ]; then
    # ── DEV: hot reload — zmiana kodu → automatyczny restart ─────────────────
    log_info "Tryb: DEVELOPMENT (hot reload włączony)"
    exec uvicorn app.main:app \
        --host 0.0.0.0 \
        --port "${API_PORT}" \
        --reload \
        --reload-dir /app/app \
        --log-level warning \
        --no-access-log
else
    # ── PROD: bez hot reload, większa liczba workerów ─────────────────────────
    log_info "Tryb: PRODUCTION"
    exec uvicorn app.main:app \
        --host 0.0.0.0 \
        --port "${API_PORT}" \
        --workers 2 \
        --log-level warning \
        --no-access-log \
        --no-server-header \
        --proxy-headers \
        --forwarded-allow-ips "*"
fi