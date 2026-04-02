# System Windykacja — Pierwsze uruchomienie

## ⚠️ WYMAGANE przed `docker compose up`

Schemat `dbo_ext` i wszystkie tabele `skw_*` muszą istnieć w bazie **zanim**
uruchomisz kontener. To jednorazowy krok wykonywany przez DBA lub dewelopera
z dostępem do SQL Server.

---

## KROK 1 — Utwórz tabele w bazie (jednorazowo)

Połącz się z bazą `GPGKJASLO` w dowolnym narzędziu:

| Narzędzie       | Jak uruchomić skrypt                          |
|-----------------|-----------------------------------------------|
| **SSMS**        | File → Open → `database/SETUP_DATABASE.sql` → F5 |
| **DBeaver**     | File → Open SQL Script → Run Script (Ctrl+Alt+X) |
| **Azure Data Studio** | File → Open → Run (Ctrl+Shift+E)        |
| **sqlcmd** (CLI) | Komenda poniżej                              |

```powershell
# sqlcmd z poziomu Windows (poza Dockerem):
sqlcmd -S tcp:sv2016\slq2022,59421 -d GPGKJASLO -U sa -P "HASLO" -C -b -I -i database/SETUP_DATABASE.sql
```

Po wykonaniu skryptu powinieneś zobaczyć w konsoli:
```
✅ SUKCES: Wszystkie 13 tabel skw_* istnieją. Możesz uruchomić docker compose.
```

---

## KROK 2 — Skonfiguruj `.env`

W pliku `.env` ustaw **dokładnie tak**:

```env
ALEMBIC_MODE=stamp
RUN_SEEDS=auto
```

- `ALEMBIC_MODE=stamp` — Alembic oznaczy bazę jako aktualną **bez wykonywania DDL**.
  Tabele zostały już utworzone ręcznie w KROKU 1.
- `RUN_SEEDS=auto` — Seedery uruchomią się automatycznie jeśli tabele są puste.

---

## KROK 3 — Uruchom kontenery

```powershell
docker compose up
```

Przy pierwszym uruchomieniu kontener wykona:
1. Sprawdzenie połączenia z bazą
2. `alembic stamp head` — oznacza rewizję `0001` jako wykonaną (bez DDL)
3. Seedery — wstawia role, uprawnienia, użytkownika admin, konfigurację systemu
4. Start aplikacji FastAPI

---

## Weryfikacja po starcie

```powershell
# Sprawdź czy tabele istnieją:
docker exec windykacja_api /opt/mssql-tools18/bin/sqlcmd `
  -S "tcp:sv2016\slq2022,59421" -d GPGKJASLO -U sa -P "HASLO" -C `
  -Q "SELECT name FROM sys.tables t JOIN sys.schemas s ON t.schema_id=s.schema_id WHERE s.name='dbo_ext' ORDER BY name;"

# Sprawdź logi kontenera:
docker compose logs windykacja_api --tail=50
```

---

## Kolejne uruchomienia (po pierwszym)

```env
ALEMBIC_MODE=stamp   # zostaw — nie zmienia się
RUN_SEEDS=auto       # zostaw — seedery są idempotentne (nie duplikują danych)
```

```powershell
docker compose up       # lub: docker compose restart windykacja_api
```

---

## Migracje schematu (zmiany w przyszłości)

Gdy pojawi się nowa migracja Alembic (nowe kolumny, tabele):

```powershell
# 1. Zatrzymaj kontener
docker compose down

# 2. W .env zmień tryb:
#    ALEMBIC_MODE=upgrade

# 3. Uruchom — Alembic wykona tylko nowe migracje
docker compose up

# 4. Po sukcesie — wróć do stamp:
#    ALEMBIC_MODE=stamp
```

---

## Struktura bazy danych

```
GPGKJASLO
├── dbo.*                    ← tabele WAPRO ERP (tylko odczyt, nie ruszamy)
└── dbo_ext.*                ← tabele Systemu Windykacja
    ├── skw_Roles            (4 role: Admin, Manager, User, ReadOnly)
    ├── skw_Permissions      (83 uprawnienia w 11 kategoriach)
    ├── skw_RolePermissions  (macierz uprawnień)
    ├── skw_Users            (użytkownicy systemu)
    ├── skw_RefreshTokens    (tokeny JWT - HttpOnly cookies)
    ├── skw_OtpCodes         (kody OTP do resetu hasła)
    ├── skw_Templates        (szablony email/sms/print - Jinja2)
    ├── skw_AuditLog         (pełny audit trail wszystkich operacji)
    ├── skw_MonitHistory     (historia wysłanych monitów)
    ├── skw_MasterAccessLog  (log dostępu - tylko INSERT)
    ├── skw_SystemConfig     (konfiguracja systemu - cache Redis 5min)
    ├── skw_SchemaChecksums  (sumy kontrolne widoków i procedur)
    └── skw_Comments         (komentarze do dłużników)
```

---

## Troubleshooting

| Problem | Przyczyna | Rozwiązanie |
|---------|-----------|-------------|
| `Invalid object name 'dbo_ext.skw_Roles'` | KROK 1 nie wykonany | Uruchom `SETUP_DATABASE.sql` |
| `Schemat dbo_ext nie istnieje` | j.w. | j.w. |
| Seedery się nie uruchamiają | `RUN_SEEDS=never` w `.env` | Zmień na `RUN_SEEDS=auto` |
| Alembic próbuje tworzyć tabele | `ALEMBIC_MODE=upgrade` zamiast `stamp` | Zmień na `ALEMBIC_MODE=stamp` |
| Alembic — `Multiple head revisions` | Stary plik w `alembic/versions/` | Usuń `_archive/` i stare pliki `001_*.py` |


docker exec -it windykacja_api python /app/database/setup.py --set-admin-password

docker logs windykacja_api 2>&1 | Select-String "xyz"

docker exec -e SELFTEST_PASSWORD="xyz" windykacja_api python -m tests.runner --filter test_health_ok --verbose


- nie dodało kluczy w [skw_SystemConfig] dotyczących uruchamiania modułów
- nie uaktualniło orphaned
USE [GPGKJASLO];
GO

-- 1. Usuwamy stare ograniczenie
ALTER TABLE [dbo_ext].[skw_faktura_akceptacja] 
DROP CONSTRAINT [CHK_sfa_status_wewnetrzny];
GO

-- 2. Dodajemy nowe ograniczenie z uwzględnieniem statusu 'orphaned'
ALTER TABLE [dbo_ext].[skw_faktura_akceptacja] 
ADD CONSTRAINT [CHK_sfa_status_wewnetrzny] 
CHECK ([status_wewnetrzny] IN (N'anulowana', N'zaakceptowana', N'w_toku', N'nowe', N'orphaned'));
GO

Rzeczyzwiazane z fakturami mają taką odpowiedź.
{ "data": [], "total": 1, "page": 1, "limit": 50 }

Mabyć przekształcona w coś takiego:
{
  "code": 200,
  "app_code": "faktury.list",
  "errors": [],
  "data": {
    "data": [],
    "total": 1,
    "page": 1,
    "limit": 50
  }
}

Generalnie ma dostać envelope.
