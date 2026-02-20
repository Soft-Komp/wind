#!/bin/sh
set -e

echo "⏳ Czekam na SQL Server (${DB_HOST:-mssql}:${DB_PORT:-1433})..."

until python3 - << 'EOF'
import os
import sys
import pyodbc

host = os.getenv("DB_HOST", "mssql")
port = os.getenv("DB_PORT", "1433")
user = os.getenv("DB_USER", "sa")
password = os.getenv("DB_PASSWORD", "YourStrong!Passw0rd")

conn_str = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    f"SERVER=tcp:{host},{port};"
    f"UID={user};PWD={password};"
    "Encrypt=no;TrustServerCertificate=yes;"
)

try:
    conn = pyodbc.connect(conn_str, timeout=5)
    conn.close()
except pyodbc.Error:
    sys.exit(1)

sys.exit(0)
EOF
do
  echo "SQL Server jeszcze nie jest gotowy, czekam..."
  sleep 3
done

echo "✅ SQL Server dostępny."

python3 -m app.init_db
alembic upgrade head

exec uvicorn app.main:app --host 0.0.0.0 --port 8000