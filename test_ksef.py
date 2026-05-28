Set-Content -Path .\test_ksef.py -Encoding UTF8 -Value @'
import asyncio
from sqlalchemy import text
from app.db.session import AsyncSessionLocal

async def t():
    async with AsyncSessionLocal() as db:
        r = await db.execute(text(
            "SELECT KSEF_ID, ID_BUF_DOKUMENT, NUMER, NazwaKontrahenta "
            "FROM dbo.skw_faktury_akceptacja_naglowek "
            "WHERE KSEF_ID = N'6842027416-20260410-596ACB000003-4E'"
        ))
        row = r.fetchone()
        print("WYNIK:", dict(row._mapping) if row else "BRAK WIERSZY - widok nie widzi tego rekordu")

asyncio.run(t())
'@