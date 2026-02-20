# ============================================================
#  System Windykacja - Inicjalizacja struktury projektu
#  Kompatybilny z PowerShell 5.x i 7+
#  Uruchom BĘDĄC w katalogu windykacja-system\
#  Przykład: cd C:\Projects\windykacja-system && .\init_project.ps1
# ============================================================

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function New-Dir  { param($p) New-Item -ItemType Directory -Force -Path $p | Out-Null; Write-Host "  [DIR]   $p" -ForegroundColor Cyan }
function New-File { param($p) New-Item -ItemType File     -Force -Path $p | Out-Null; Write-Host "  [FILE]  $p" -ForegroundColor Yellow }

Write-Host "`n===================================================" -ForegroundColor Magenta
Write-Host "  System Windykacja - tworzenie struktury" -ForegroundColor Magenta
Write-Host "  Katalog: $(Get-Location)" -ForegroundColor Magenta
Write-Host "===================================================`n" -ForegroundColor Magenta

# ─────────────────────────────────────────────
# 1. KATALOGI
# ─────────────────────────────────────────────
Write-Host "[1/3] Tworzenie katalogow..." -ForegroundColor White

$dirs = @(
    # Backend - warstwy aplikacji
    "backend/app/api",
    "backend/app/core",
    "backend/app/db/models",
    "backend/app/schemas",
    "backend/app/services",
    "backend/app/middleware",
    # Alembic
    "backend/alembic/versions",
    # Logi (wolumen Docker - tylko .gitkeep)
    "backend/logs",
    # Worker
    "worker/tasks",
    "worker/utils",
    "worker/templates",
    # Baza danych - pliki SQL
    "database/procedures",
    "database/views",
    "database/seeds",
    # Dane - montowane jako wolumeny Docker
    "snapshots",
    "archives",
    "logs"
)

foreach ($d in $dirs) { New-Dir $d }

# ─────────────────────────────────────────────
# 2. PLIKI PYTHON - BACKEND
# ─────────────────────────────────────────────
Write-Host "`n[2/3] Tworzenie plikow..." -ForegroundColor White

# Entry point
New-File "backend/main.py"

# __init__.py - wszystkie pakiety
$initDirs = @(
    "backend/app",
    "backend/app/api",
    "backend/app/core",
    "backend/app/db",
    "backend/app/db/models",
    "backend/app/schemas",
    "backend/app/services",
    "backend/app/middleware"
)
foreach ($d in $initDirs) { New-File "$d/__init__.py" }

# API - endpointy
New-File "backend/app/api/auth.py"           # login, logout, refresh, OTP, impersonacja, master-access
New-File "backend/app/api/users.py"          # CRUD uzytkownikow
New-File "backend/app/api/roles.py"          # CRUD rol
New-File "backend/app/api/permissions.py"    # CRUD uprawnien + macierz
New-File "backend/app/api/debtors.py"        # dluznici z WAPRO (read-only przez widoki)
New-File "backend/app/api/events.py"         # SSE stream
New-File "backend/app/api/system.py"         # SystemConfig, CORS, health, snapshoty

# Core - infrastruktura
New-File "backend/app/core/config.py"             # Settings z .env (pydantic-settings)
New-File "backend/app/core/security.py"           # JWT, bcrypt, delete-token, impersonacja
New-File "backend/app/core/redis.py"              # Redis async client + pub/sub
New-File "backend/app/core/allowed_procedures.py" # Rejestr dozwolonych SP - whitelist
New-File "backend/app/core/cors.py"               # Dynamiczny CORS z bazy / cache Redis
New-File "backend/app/core/schema_integrity.py"   # Weryfikacja checksumow przy starcie
New-File "backend/app/core/logging.py"            # Konfiguracja loggera (JSON Lines, rotacja)
New-File "backend/app/core/exceptions.py"         # Centralne wyjatki HTTP + handlery

# DB - dostep do danych
New-File "backend/app/db/session.py"              # SQLAlchemy async session factory
New-File "backend/app/db/wapro.py"                # pyodbc - polaczenie read-only z WAPRO

# DB Models - SQLAlchemy ORM (schemat dbo_ext)
New-File "backend/app/db/models/user.py"
New-File "backend/app/db/models/role.py"
New-File "backend/app/db/models/permission.py"
New-File "backend/app/db/models/role_permission.py"
New-File "backend/app/db/models/refresh_token.py"
New-File "backend/app/db/models/otp_codes.py"
New-File "backend/app/db/models/audit_log.py"
New-File "backend/app/db/models/monit_history.py"
New-File "backend/app/db/models/system_config.py"
New-File "backend/app/db/models/schema_checksums.py"
New-File "backend/app/db/models/master_access_log.py"  # Tylko DBA / SSMS - brak endpointu

# Schemas - Pydantic v2 (extra='forbid' wszedzie)
New-File "backend/app/schemas/auth.py"
New-File "backend/app/schemas/users.py"
New-File "backend/app/schemas/roles.py"
New-File "backend/app/schemas/permissions.py"
New-File "backend/app/schemas/debtors.py"
New-File "backend/app/schemas/events.py"
New-File "backend/app/schemas/system.py"
New-File "backend/app/schemas/common.py"       # Wspolne: ResponseEnvelope, PaginatedResponse, ErrorDetail

# Services - logika biznesowa
New-File "backend/app/services/auth_service.py"
New-File "backend/app/services/impersonation_service.py"  # Impersonacja + master key
New-File "backend/app/services/user_service.py"
New-File "backend/app/services/role_service.py"
New-File "backend/app/services/permission_service.py"
New-File "backend/app/services/debtor_service.py"
New-File "backend/app/services/event_service.py"
New-File "backend/app/services/audit_service.py"
New-File "backend/app/services/snapshot_service.py"
New-File "backend/app/services/archive_service.py"
New-File "backend/app/services/config_service.py"
New-File "backend/app/services/otp_service.py"

# Middleware
New-File "backend/app/middleware/audit_middleware.py"   # Automatyczny AuditLog dla kazdego requestu
New-File "backend/app/middleware/cors_middleware.py"

# Alembic
New-File "backend/alembic/env.py"
New-File "backend/alembic/script.py.mako"
New-File "backend/alembic.ini"

# Konfiguracja backendu
New-File "backend/requirements.txt"
New-File "backend/Dockerfile"
New-File "backend/.env"                        # Lokalne - w .gitignore

# Logi - .gitkeep zachowuje katalog w repo, zawartosc ignorowana
New-File "backend/logs/.gitkeep"

# ─────────────────────────────────────────────
# WORKER
# ─────────────────────────────────────────────
New-File "worker/main.py"                      # Entry point ARQ worker

$workerInit = @(
    "worker",
    "worker/tasks",
    "worker/utils"
)
foreach ($d in $workerInit) { New-File "$d/__init__.py" }

New-File "worker/tasks/email.py"               # send_bulk_emails - SMTP zewnetrzny
New-File "worker/tasks/sms.py"                 # send_bulk_sms - SMSAPI
New-File "worker/tasks/pdf.py"                 # generate_pdf - ReportLab -> BytesIO
New-File "worker/tasks/snapshots.py"           # daily_snapshot - ARQ cron 02:00

New-File "worker/utils/events.py"              # Synchroniczny publish do Redis Pub/Sub
New-File "worker/utils/db.py"                  # Polaczenie DB dla workera (sync pyodbc)

New-File "worker/requirements.txt"
New-File "worker/Dockerfile"

# ─────────────────────────────────────────────
# DATABASE - pliki SQL
# ─────────────────────────────────────────────
New-File "database/views/.gitkeep"
New-File "database/procedures/.gitkeep"
New-File "database/seeds/.gitkeep"
New-File "database/seeds/01_roles.sql"
New-File "database/seeds/02_permissions.sql"
New-File "database/seeds/03_role_permissions.sql"
New-File "database/seeds/04_admin_user.sql"
New-File "database/seeds/05_system_config.sql"

# ─────────────────────────────────────────────
# WOLUMENY DOCKER - .gitkeep
# ─────────────────────────────────────────────
New-File "snapshots/.gitkeep"
New-File "archives/.gitkeep"
New-File "logs/.gitkeep"

# ─────────────────────────────────────────────
# ROOT - konfiguracja projektu
# ─────────────────────────────────────────────
New-File "docker-compose.yml"
New-File ".env.example"
New-File "README.md"

# ─────────────────────────────────────────────
# 3. .GITIGNORE
# ─────────────────────────────────────────────
Write-Host "`n[3/3] Generowanie .gitignore..." -ForegroundColor White

$gitignore = @'
# ============================================================
#  System Windykacja - .gitignore
# ============================================================

# ── Python ──────────────────────────────────────────────────
__pycache__/
*.py[cod]
*$py.class
*.so
*.egg
*.egg-info/
dist/
build/
.eggs/
pip-wheel-metadata/
*.whl

# ── Srodowiska wirtualne ─────────────────────────────────────
.venv/
venv/
env/
ENV/
.python-version

# ── Zmienne srodowiskowe - NIGDY nie commituj .env ──────────
.env
backend/.env
worker/.env
!.env.example

# ── Logi aplikacji - zawartosc ignorowana, katalog sledzony ─
logs/*.log
logs/*.jsonl
backend/logs/*.log
backend/logs/*.jsonl
worker/logs/*.log
worker/logs/*.jsonl
# Zachowaj katalogi w repo
!logs/.gitkeep
!backend/logs/.gitkeep

# ── Snapshoty i archiwa - tylko pliki ignorowane ─────────────
snapshots/**/*.json.gz
snapshots/**/*.json
archives/**/*.json.gz
archives/**/*.json
!snapshots/.gitkeep
!archives/.gitkeep

# ── Redis ────────────────────────────────────────────────────
dump.rdb
redis.log

# ── IDE ──────────────────────────────────────────────────────
.vscode/
.idea/
*.swp
*.swo
.DS_Store
Thumbs.db

# ── Narzedzia statyczne Python ───────────────────────────────
.mypy_cache/
.ruff_cache/
.pytest_cache/
.coverage
htmlcov/
.tox/

# ── Docker ───────────────────────────────────────────────────
# docker-compose.override.yml moze zawierac lokalne nadpisania
docker-compose.override.yml

# ── Alembic - nie ignorujemy wersji migracji ─────────────────
# backend/alembic/versions/ - sledzony w repo

# ── Tymczasowe PDF (jezeli kiedys beda zapisywane) ───────────
backend/pdfs/
*.pdf

# ── Certyfikaty SSL (jesli beda lokalne) ─────────────────────
*.pem
*.key
*.crt
*.p12
'@

Set-Content -Path ".gitignore" -Value $gitignore -Encoding UTF8
Write-Host "  [FILE]  .gitignore" -ForegroundColor Yellow

# ─────────────────────────────────────────────
# PODSUMOWANIE
# ─────────────────────────────────────────────
Write-Host "`n===================================================" -ForegroundColor Magenta

$dirCount  = ($dirs).Count
$fileCount = (Get-ChildItem -Recurse -File | Measure-Object).Count

Write-Host "  Gotowe!" -ForegroundColor Green
Write-Host "  Katalogów utworzonych : $dirCount" -ForegroundColor Green
Write-Host "  Plików w projekcie    : $fileCount" -ForegroundColor Green
Write-Host "`n  Nastepne kroki:" -ForegroundColor White
Write-Host "    1. Uzupelnij .env.example -> skopiuj do backend\.env" -ForegroundColor Gray
Write-Host "    2. git init && git add . && git commit -m 'init: struktura projektu'" -ForegroundColor Gray
Write-Host "    3. docker-compose up --build" -ForegroundColor Gray
Write-Host "===================================================`n" -ForegroundColor Magenta


