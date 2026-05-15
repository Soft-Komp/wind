@echo off
chcp 65001 >nul 2>&1
setlocal EnableDelayedExpansion

:: ============================================================
:: projekt_raport.bat  --  Windykacja GPGK
:: Umiec w katalogu glownym projektu (obok backend\)
:: Uruchom:  projekt_raport.bat
:: Wynik:    projekt_raport_YYYY-MM-DD_HH-MM.txt
:: ============================================================

set "ROOT=%~dp0"
if "%ROOT:~-1%"=="\" set "ROOT=%ROOT:~0,-1%"

:: Zbuduj timestamp YYYY-MM-DD_HH-MM
:: DATE format PL: DD.MM.YYYY lub DD-MM-YYYY lub DD/MM/YYYY
for /f "tokens=1-3 delims=.-/ " %%a in ("%DATE%") do (
    set "P1=%%a"
    set "P2=%%b"
    set "P3=%%c"
)
:: Rok to ta czesc ktora ma 4 cyfry
set "YY=%P3%"
set "MM=%P2%"
set "DD=%P1%"
if "!YY:~2,2!"=="" (
    set "YY=%P1%"
    set "MM=%P2%"
    set "DD=%P3%"
)
set "HHMM=%TIME:~0,5%"
set "HHMM=!HHMM::=-!"
set "HHMM=!HHMM: =0!"
set "TS=!YY!-!MM!-!DD!_!HHMM!"
set "OUT=%ROOT%\projekt_raport_%TS%.txt"

echo Generuje raport projektu...
echo Wynik: %OUT%
echo.

echo ================================================================ > "%OUT%"
echo  WINDYKACJA GPGK -- RAPORT STRUKTURY PROJEKTU >> "%OUT%"
echo  Data: %DATE%  Czas: %TIME% >> "%OUT%"
echo  Katalog bazowy: %ROOT% >> "%OUT%"
echo ================================================================ >> "%OUT%"

call :SEC "GIT STATUS"
git -C "%ROOT%" status >> "%OUT%" 2>&1
if errorlevel 1 echo [BRAK GIT lub nie w repo] >> "%OUT%"

call :SEC "GIT LOG (ostatnie 10 commitow)"
git -C "%ROOT%" log --oneline -10 >> "%OUT%" 2>&1
if errorlevel 1 echo [BRAK HISTORII] >> "%OUT%"

call :SEC "GIT BRANCH"
git -C "%ROOT%" branch -a >> "%OUT%" 2>&1
if errorlevel 1 echo [BRAK] >> "%OUT%"

call :SEC "MIGRACJE ALEMBIC (backend\alembic\versions\)"
if exist "%ROOT%\backend\alembic\versions" (
    dir /B /O:N "%ROOT%\backend\alembic\versions\*.py" >> "%OUT%" 2>&1
) else (
    echo [BRAK katalogu] >> "%OUT%"
)

echo. >> "%OUT%"
echo  -- alembic current przez docker: >> "%OUT%"
docker exec windykacja_api alembic current >> "%OUT%" 2>&1

call :SEC "PLIKI DDL (database\ddl\)"
if exist "%ROOT%\database\ddl" (
    dir /B /O:N "%ROOT%\database\ddl\*.sql" >> "%OUT%" 2>&1
) else (
    echo [BRAK] >> "%OUT%"
)

call :SEC "PLIKI SEED (database\seeds\)"
if exist "%ROOT%\database\seeds" (
    dir /B /O:N "%ROOT%\database\seeds\*.sql" >> "%OUT%" 2>&1
) else (
    echo [BRAK] >> "%OUT%"
)

call :SEC "MODELE ORM (app\db\models\)"
if exist "%ROOT%\backend\app\db\models" (
    dir /S /B "%ROOT%\backend\app\db\models\*.py" >> "%OUT%" 2>&1
) else (
    echo [BRAK] >> "%OUT%"
)

call :SEC "SERWISY (app\services\)"
if exist "%ROOT%\backend\app\services" (
    dir /S /B "%ROOT%\backend\app\services\*.py" >> "%OUT%" 2>&1
) else (
    echo [BRAK] >> "%OUT%"
)

call :SEC "API ROUTERY (app\api\)"
if exist "%ROOT%\backend\app\api" (
    dir /S /B "%ROOT%\backend\app\api\*.py" >> "%OUT%" 2>&1
) else (
    echo [BRAK] >> "%OUT%"
)

call :SEC "WORKER TASKS (worker\)"
if exist "%ROOT%\backend\worker" (
    dir /S /B "%ROOT%\backend\worker\*.py" >> "%OUT%" 2>&1
) else (
    echo [BRAK] >> "%OUT%"
)

call :SEC "NGINX (nginx\)"
if exist "%ROOT%\nginx" (
    dir /S /B "%ROOT%\nginx\" >> "%OUT%" 2>&1
) else (
    echo [BRAK katalogu nginx\ -- nie dodano jeszcze] >> "%OUT%"
)

call :SEC "TEMPLATES (app\templates\)"
if exist "%ROOT%\backend\app\templates" (
    dir /S /B "%ROOT%\backend\app\templates\" >> "%OUT%" 2>&1
) else (
    echo [BRAK] >> "%OUT%"
)

call :SEC "DOCKER PS"
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" >> "%OUT%" 2>&1

call :FILE "requirements.txt"            "%ROOT%\backend\requirements.txt"
call :FILE "docker-compose.yml"          "%ROOT%\docker-compose.yml"
call :FILE ".env.example"               "%ROOT%\backend\.env.example"
call :FILE "alembic\env.py"             "%ROOT%\backend\alembic\env.py"
call :FILE "app\db\base.py"             "%ROOT%\backend\app\db\base.py"
call :FILE "worker\main.py"             "%ROOT%\backend\worker\main.py"
call :FILE "app\api\router.py"          "%ROOT%\backend\app\api\router.py"
call :FILE "app\core\config.py"         "%ROOT%\backend\app\core\config.py"
call :FILE "entrypoint.sh"              "%ROOT%\backend\entrypoint.sh"
call :FILE "Dockerfile"                 "%ROOT%\backend\Dockerfile"

:: .env.docker z maskowaniem hasel
call :SEC ".env.docker (hasla zamaskowane)"
set "ENVF=%ROOT%\backend\.env.docker"
if not exist "!ENVF!" set "ENVF=%ROOT%\.env.docker"
if exist "!ENVF!" (
    for /F "usebackq tokens=1,* delims==" %%K in ("!ENVF!") do (
        set "_K=%%K"
        set "_V=%%L"
        echo !_K! | findstr /I "PASSWORD SECRET KEY TOKEN HASH PASS" >nul 2>&1
        if !errorlevel!==0 (
            echo !_K!=***MASKED*** >> "%OUT%"
        ) else (
            echo !_K!=!_V! >> "%OUT%"
        )
    )
) else (
    echo [BRAK .env.docker] >> "%OUT%"
)

echo. >> "%OUT%"
echo ================================================================ >> "%OUT%"
echo  KONIEC RAPORTU - wklej ten plik do Claude >> "%OUT%"
echo ================================================================ >> "%OUT%"

echo.
echo [OK] Raport zapisany: %OUT%
echo.
start notepad "%OUT%"
goto :EOF

:: ──────────────────────────────────────────────────────────────
:SEC
echo. >> "%OUT%"
echo ===== %~1 ===== >> "%OUT%"
echo. >> "%OUT%"
goto :EOF

:FILE
echo. >> "%OUT%"
echo ----- %~1 ----- >> "%OUT%"
if exist "%~2" (
    type "%~2" >> "%OUT%"
) else (
    echo [BRAK: %~2] >> "%OUT%"
)
echo. >> "%OUT%"
goto :EOF