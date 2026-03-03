# Archiwum — Stare Migracje (przed prefixem skw_)

> Data archiwizacji: 2026-03-02
> Powód: Refaktoryzacja nazewnictwa tabel — dodanie prefiksu `skw_`

---

## Co tu jest i dlaczego

Pliki w tym katalogu to **stare migracje Alembic** z okresu przed wprowadzeniem
prefiksu `skw_` dla wszystkich tabel systemu Windykacja.

Tabele sprzed migracji (bez prefiksu):
```
dbo_ext.Roles               →  dbo_ext.skw_Roles
dbo_ext.Permissions         →  dbo_ext.skw_Permissions
dbo_ext.Users               →  dbo_ext.skw_Users
dbo_ext.RolePermissions     →  dbo_ext.skw_RolePermissions
dbo_ext.RefreshTokens       →  dbo_ext.skw_RefreshTokens
dbo_ext.OtpCodes            →  dbo_ext.skw_OtpCodes
dbo_ext.AuditLog            →  dbo_ext.skw_AuditLog
dbo_ext.Templates           →  dbo_ext.skw_Templates
dbo_ext.MonitHistory        →  dbo_ext.skw_MonitHistory
dbo_ext.SystemConfig        →  dbo_ext.skw_SystemConfig
dbo_ext.SchemaChecksums     →  dbo_ext.skw_SchemaChecksums
dbo_ext.MasterAccessLog     →  dbo_ext.skw_MasterAccessLog
dbo_ext.Comments            →  dbo_ext.skw_Comments
```

---

## Zarchiwizowane pliki

| Plik | Opis | Status |
|------|------|--------|
| `001_create_dbo_ext_initial.py` | Tworzenie wszystkich tabel (bez skw_) | ⚠️ PRZESTARZAŁY |
| `002_add_wapro_performance_indexes.py` | Indeksy wydajnościowe WAPRO | ⚠️ PRZESTARZAŁY |
| `003_fix_otpcodes_code_length.py` | Korekta długości kolumny Code w OtpCodes | ⚠️ PRZESTARZAŁY |
| `004_merge_heads.py` | Scalenie gałęzi migracji | ⚠️ PRZESTARZAŁY |
| `20260223_XXXX_add_missing_user_columns.py` | Brakujące kolumny Users | ⚠️ PRZESTARZAŁY |
| `e5f6a7b8c9d0_add_audit_log_request_id.py` | RequestId w AuditLog | ⚠️ PRZESTARZAŁY |

---

## Co zastąpiło te migracje

Jedna czysta migracja inicjalna:
```
alembic/versions/0001_skw_initial_schema.py
```

Zawiera pełen stan schematu po refaktoryzacji — wszystkie 13 tabel `skw_*`
z kompletną strukturą, constraintami i indeksami.

---

## Dlaczego NIE usunięto tych plików

1. **Historia** — dokumentują ewolucję schematu
2. **Audit trail** — wiemy co było zmieniane i kiedy
3. **Odniesienie** — jeśli ktoś pyta "dlaczego kolumna X ma długość Y", odpowiedź może być tutaj

---

## Czy można je uruchomić?

**NIE.** Stare migracje odwołują się do tabel bez prefiksu `skw_`.
Uruchomienie ich na bazie z tabelami `skw_*` spowoduje błąd FK lub duplikat.

---

## Instrukcja dla nowych środowisk (fresh install)

```bash
# 1. Uruchom pliki DDL ręcznie
sqlcmd -S <host> -d WAPRO -i database/ddl/000_create_schema.sql
sqlcmd -S <host> -d WAPRO -i database/ddl/001_roles.sql
# ... (wszystkie pliki DDL 000-014)

# 2. Powiedz Alembicowi że jesteśmy na aktualnej wersji (NIE wykonuj upgrade!)
cd backend/
alembic stamp head

# 3. Gotowe — alembic current pokaże 0001_skw_initial_schema
alembic current
```

---

*Archiwum jest read-only. Nie modyfikuj tych plików.*
