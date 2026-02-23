import os
import pyodbc
from dotenv import load_dotenv
from argon2 import PasswordHasher

load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

ADMIN_EMAIL = os.getenv("ADMIN_INITIAL_EMAIL", "admin@local")
ADMIN_PASSWORD = os.getenv("ADMIN_INITIAL_PASSWORD", "Admin123!")

ph = PasswordHasher()
password_hash = ph.hash(ADMIN_PASSWORD)

conn_str = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    f"SERVER=tcp:{DB_HOST},{DB_PORT};"
    f"DATABASE={DB_NAME};"
    f"UID={DB_USER};"
    f"PWD={DB_PASSWORD};"
    "Encrypt=no;"
)

conn = pyodbc.connect(conn_str)
cur = conn.cursor()

cur.execute("""
IF NOT EXISTS (
    SELECT 1 FROM dbo_ext.Users WHERE Username='admin'
)
BEGIN
    INSERT INTO dbo_ext.Users (
        Username, Email, PasswordHash, FullName, IsActive,
        RoleID, CreatedAt, FailedLoginAttempts
    )
    VALUES (
        'admin', ?, ?, 'Administrator Systemu', 1,
        1, GETDATE(), 0
    )
END
""", ADMIN_EMAIL, password_hash)

conn.commit()
cur.close()
conn.close()

print("Admin seeded successfully.")