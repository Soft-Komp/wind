#!/usr/bin/env bash
# ============================================================
# projekt_raport.sh
# Windykacja GPGK — Generator raportu struktury projektu
# Umieść w katalogu głównym projektu (obok backend/, frontend/ itd.)
# Uruchom: bash projekt_raport.sh
# Wynik:   projekt_raport_YYYY-MM-DD_HH-MM.txt  (gotowy do wklejenia)
# ============================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TIMESTAMP="$(date '+%Y-%m-%d_%H-%M')"
OUTFILE="${SCRIPT_DIR}/projekt_raport_${TIMESTAMP}.txt"

echo "[RAPORT] Generuję raport projektu..."
echo "[RAPORT] Plik wyjściowy: ${OUTFILE}"

# ─── Pomocnicze funkcje ───────────────────────────────────────────────────────

section() {
    echo "" >> "${OUTFILE}"
    echo "── $1 $(printf '%0.s─' {1..50})" >> "${OUTFILE}"
    echo "" >> "${OUTFILE}"
}

file_block() {
    local label="$1"
    local filepath="$2"
    echo "" >> "${OUTFILE}"
    echo "┌─ ${label} $(printf '%0.s─' {1..50})" >> "${OUTFILE}"
    if [[ -f "${filepath}" ]]; then
        cat "${filepath}" >> "${OUTFILE}"
    else
        echo "[BRAK: ${filepath}]" >> "${OUTFILE}"
    fi
}

mask_env_file() {
    local filepath="$1"
    if [[ ! -f "${filepath}" ]]; then
        echo "[BRAK: ${filepath}]" >> "${OUTFILE}"
        return
    fi
    while IFS= read -r line || [[ -n "$line" ]]; do
        # Pomiń puste linie i komentarze — przekaż bez zmian
        if [[ "$line" =~ ^[[:space:]]*# ]] || [[ -z "$line" ]]; then
            echo "$line" >> "${OUTFILE}"
            continue
        fi
        # Pobierz klucz (część przed =)
        KEY="${line%%=*}"
        # Maskuj wartości wrażliwych kluczy
        if echo "${KEY}" | grep -qiE 'PASSWORD|SECRET|KEY|TOKEN|HASH|PASS'; then
            echo "${KEY}=***MASKED***" >> "${OUTFILE}"
        else
            echo "$line" >> "${OUTFILE}"
        fi
    done < "${filepath}"
}

# ─── Nagłówek ─────────────────────────────────────────────────────────────────

cat > "${OUTFILE}" << EOF
================================================================
 WINDYKACJA GPGK — RAPORT STRUKTURY PROJEKTU
 Wygenerowano: $(date '+%Y-%m-%d %H:%M:%S')
 Hostname: $(hostname)
 Katalog bazowy: ${SCRIPT_DIR}
================================================================
EOF

# ─── GIT ─────────────────────────────────────────────────────────────────────

section "GIT STATUS"
if git -C "${SCRIPT_DIR}" rev-parse --is-inside-work-tree &>/dev/null; then
    git -C "${SCRIPT_DIR}" status >> "${OUTFILE}" 2>&1
    echo "" >> "${OUTFILE}"
    echo "── GIT LOG (ostatnie 10 commitów) ──" >> "${OUTFILE}"
    git -C "${SCRIPT_DIR}" log --oneline -10 >> "${OUTFILE}" 2>&1
    echo "" >> "${OUTFILE}"
    echo "── GIT BRANCH ──" >> "${OUTFILE}"
    git -C "${SCRIPT_DIR}" branch -a >> "${OUTFILE}" 2>&1
else
    echo "[BRAK: katalog nie jest repozytorium git lub git nie zainstalowany]" >> "${OUTFILE}"
fi

# ─── Struktura katalogów ─────────────────────────────────────────────────────

section "STRUKTURA KATALOGÓW (3 poziomy, bez pycache/node_modules)"
if command -v tree &>/dev/null; then
    tree -L 3 -I '__pycache__|node_modules|*.pyc|.git|dist|build' \
        "${SCRIPT_DIR}" >> "${OUTFILE}" 2>&1
else
    # Fallback gdy tree nie zainstalowane
    find "${SCRIPT_DIR}" -maxdepth 3 \
        -not -path '*/__pycache__/*' \
        -not -path '*/node_modules/*' \
        -not -path '*/.git/*' \
        -not -path '*/dist/*' \
        -not -name '*.pyc' \
        | sort >> "${OUTFILE}" 2>&1
fi

# ─── Migracje Alembic ─────────────────────────────────────────────────────────

section "MIGRACJE ALEMBIC (versions/)"
ALEMBIC_DIR="${SCRIPT_DIR}/backend/alembic/versions"
if [[ -d "${ALEMBIC_DIR}" ]]; then
    ls -1 "${ALEMBIC_DIR}"/*.py 2>/dev/null | xargs -I{} basename {} | sort >> "${OUTFILE}" || \
        echo "[BRAK plików .py w versions/]" >> "${OUTFILE}"
else
    echo "[BRAK katalogu backend/alembic/versions/]" >> "${OUTFILE}"
fi

# Aktualny head z alembic (jeśli możliwe — w środowisku Docker nie zadziała)
echo "" >> "${OUTFILE}"
echo "── Alembic current (próba):" >> "${OUTFILE}"
if command -v docker &>/dev/null; then
    docker exec windykacja_api alembic current 2>/dev/null >> "${OUTFILE}" || \
        echo "[Kontener windykacja_api niedostępny lub alembic niedostępny]" >> "${OUTFILE}"
else
    echo "[docker niedostępny w tym środowisku]" >> "${OUTFILE}"
fi

# ─── DDL i Seeds ─────────────────────────────────────────────────────────────

section "PLIKI DDL (database/ddl/)"
DDL_DIR="${SCRIPT_DIR}/database/ddl"
if [[ -d "${DDL_DIR}" ]]; then
    ls -1 "${DDL_DIR}"/*.sql 2>/dev/null | xargs -I{} basename {} | sort >> "${OUTFILE}" || \
        echo "[BRAK plików .sql]" >> "${OUTFILE}"
else
    echo "[BRAK katalogu database/ddl/]" >> "${OUTFILE}"
fi

section "PLIKI SEED (database/seeds/)"
SEEDS_DIR="${SCRIPT_DIR}/database/seeds"
if [[ -d "${SEEDS_DIR}" ]]; then
    ls -1 "${SEEDS_DIR}"/*.sql 2>/dev/null | xargs -I{} basename {} | sort >> "${OUTFILE}" || \
        echo "[BRAK plików .sql]" >> "${OUTFILE}"
else
    echo "[BRAK katalogu database/seeds/]" >> "${OUTFILE}"
fi

# ─── Kluczowe pliki — pełna zawartość ────────────────────────────────────────

file_block "requirements.txt" "${SCRIPT_DIR}/backend/requirements.txt"
file_block "docker-compose.yml" "${SCRIPT_DIR}/docker-compose.yml"

echo "" >> "${OUTFILE}"
echo "┌─ .env.example (pełna zawartość) ──────────────────────────" >> "${OUTFILE}"
ENV_EXAMPLE=""
[[ -f "${SCRIPT_DIR}/backend/.env.example" ]] && ENV_EXAMPLE="${SCRIPT_DIR}/backend/.env.example"
[[ -f "${SCRIPT_DIR}/.env.example" ]] && ENV_EXAMPLE="${SCRIPT_DIR}/.env.example"
if [[ -n "${ENV_EXAMPLE}" ]]; then
    cat "${ENV_EXAMPLE}" >> "${OUTFILE}"
else
    echo "[BRAK .env.example]" >> "${OUTFILE}"
fi

echo "" >> "${OUTFILE}"
echo "┌─ .env.docker (klucze — wartości wrażliwe zamaskowane) ────" >> "${OUTFILE}"
ENV_DOCKER=""
[[ -f "${SCRIPT_DIR}/backend/.env.docker" ]] && ENV_DOCKER="${SCRIPT_DIR}/backend/.env.docker"
[[ -f "${SCRIPT_DIR}/.env.docker" ]] && ENV_DOCKER="${SCRIPT_DIR}/.env.docker"
if [[ -n "${ENV_DOCKER}" ]]; then
    mask_env_file "${ENV_DOCKER}"
else
    echo "[BRAK .env.docker]" >> "${OUTFILE}"
fi

file_block "alembic/env.py" "${SCRIPT_DIR}/backend/alembic/env.py"
file_block "app/db/base.py" "${SCRIPT_DIR}/backend/app/db/base.py"
file_block "worker/main.py" "${SCRIPT_DIR}/backend/worker/main.py"
file_block "app/api/router.py" "${SCRIPT_DIR}/backend/app/api/router.py"
file_block "app/core/config.py" "${SCRIPT_DIR}/backend/app/core/config.py"
file_block "entrypoint.sh" "${SCRIPT_DIR}/backend/entrypoint.sh"
file_block "Dockerfile (backend)" "${SCRIPT_DIR}/backend/Dockerfile"

# ─── Listy plików po kategoriach ─────────────────────────────────────────────

section "MODELE ORM (app/db/models/)"
MODELS_DIR="${SCRIPT_DIR}/backend/app/db/models"
if [[ -d "${MODELS_DIR}" ]]; then
    find "${MODELS_DIR}" -name "*.py" | sed "s|${SCRIPT_DIR}/||" | sort >> "${OUTFILE}"
else
    echo "[BRAK katalogu app/db/models/]" >> "${OUTFILE}"
fi

section "SERWISY (app/services/)"
SVC_DIR="${SCRIPT_DIR}/backend/app/services"
if [[ -d "${SVC_DIR}" ]]; then
    find "${SVC_DIR}" -name "*.py" | sed "s|${SCRIPT_DIR}/||" | sort >> "${OUTFILE}"
else
    echo "[BRAK katalogu app/services/]" >> "${OUTFILE}"
fi

section "API ROUTERY (app/api/)"
API_DIR="${SCRIPT_DIR}/backend/app/api"
if [[ -d "${API_DIR}" ]]; then
    find "${API_DIR}" -name "*.py" | sed "s|${SCRIPT_DIR}/||" | sort >> "${OUTFILE}"
else
    echo "[BRAK katalogu app/api/]" >> "${OUTFILE}"
fi

section "WORKER TASKS (worker/)"
WORKER_DIR="${SCRIPT_DIR}/backend/worker"
if [[ -d "${WORKER_DIR}" ]]; then
    find "${WORKER_DIR}" -name "*.py" | sed "s|${SCRIPT_DIR}/||" | sort >> "${OUTFILE}"
else
    echo "[BRAK katalogu worker/]" >> "${OUTFILE}"
fi

section "NGINX (nginx/)"
NGINX_DIR="${SCRIPT_DIR}/nginx"
if [[ -d "${NGINX_DIR}" ]]; then
    find "${NGINX_DIR}" | sed "s|${SCRIPT_DIR}/||" | sort >> "${OUTFILE}"
else
    echo "[BRAK katalogu nginx/ — nie dodano jeszcze]" >> "${OUTFILE}"
fi

section "PLIKI KONFIGURACYJNE (Jinja2 templates, email itp.)"
TEMPLATES_DIR="${SCRIPT_DIR}/backend/app/templates"
if [[ -d "${TEMPLATES_DIR}" ]]; then
    find "${TEMPLATES_DIR}" | sed "s|${SCRIPT_DIR}/||" | sort >> "${OUTFILE}"
else
    echo "[BRAK katalogu app/templates/]" >> "${OUTFILE}"
fi

section "DOCKER STATUS (jeśli dostępny)"
if command -v docker &>/dev/null; then
    echo "── docker ps:" >> "${OUTFILE}"
    docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" 2>/dev/null >> "${OUTFILE}" || \
        echo "[docker ps niedostępny]" >> "${OUTFILE}"
    echo "" >> "${OUTFILE}"
    echo "── docker images (windykacja_*):" >> "${OUTFILE}"
    docker images | grep windykacja 2>/dev/null >> "${OUTFILE}" || \
        echo "[brak obrazów windykacja]" >> "${OUTFILE}"
else
    echo "[docker niedostępny]" >> "${OUTFILE}"
fi

section "LOGI — ROZMIARY (jeśli dostępne)"
LOGS_DIR="${SCRIPT_DIR}/backend/logs"
if [[ -d "${LOGS_DIR}" ]]; then
    ls -lh "${LOGS_DIR}" >> "${OUTFILE}" 2>&1
else
    echo "[BRAK katalogu logs/ — dostępne tylko w kontenerze]" >> "${OUTFILE}"
fi

# ─── Stopka ───────────────────────────────────────────────────────────────────

cat >> "${OUTFILE}" << 'EOF'

================================================================
 KONIEC RAPORTU
 Wklej całą zawartość tego pliku do Claude.
================================================================
EOF

echo ""
echo "[OK] Raport zapisany do: ${OUTFILE}"
echo "[OK] Rozmiar: $(du -h "${OUTFILE}" | cut -f1)"
echo ""
echo "Wklej zawartość pliku do Claude na początku nowego czatu."