#!/bin/bash
# =============================================================================
# start.sh — Entrypoint workera
# =============================================================================
set -euo pipefail

echo "[START] System Windykacja Worker — $(date '+%Y-%m-%d %H:%M:%S %Z')"
echo "[START] TZ: ${TZ:-nie ustawiona}"
echo "[START] Python: $(python --version)"
echo "[START] PYTHONPATH: ${PYTHONPATH:-/app}"

# ── Tworzenie katalogów runtime ───────────────────────────────────────────────
mkdir -p /app/logs /app/snapshots /app/archives /app/pdf_cache
echo "[START] Katalogi runtime gotowe"

# ── Czekaj na Redis ──────────────────────────────────────────────────────────
REDIS_HOST="${REDIS_HOST:-redis}"
REDIS_PORT="${REDIS_PORT:-6379}"
echo "[START] Czekam na Redis ${REDIS_HOST}:${REDIS_PORT}..."
for i in $(seq 1 30); do
    if python -c "
import socket
s = socket.socket()
s.settimeout(2)
s.connect(('${REDIS_HOST}', ${REDIS_PORT}))
s.close()
print('OK')
" 2>/dev/null; then
        echo "[START] Redis dostępny po ${i}s"
        break
    fi
    echo "[START] Redis niedostępny, próba ${i}/30..."
    sleep 2
done

# ── Uruchom supervisord ───────────────────────────────────────────────────────
echo "[START] Uruchamiam supervisord (ARQ Worker + API :8001)..."
exec /usr/bin/supervisord -n -c /etc/supervisor/conf.d/worker.conf