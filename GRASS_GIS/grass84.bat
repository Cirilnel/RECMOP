@echo off
rem Ottengo la cartella dove si trova questo script
set SCRIPT_DIR=%~dp0
rem Tolgo eventuale barra finale (opzionale)
set SCRIPT_DIR=%SCRIPT_DIR:~0,-1%

rem Setto GISBASE alla cartella GRASS_GIS relativa allo script
set GISBASE=%SCRIPT_DIR%

call "%GISBASE%\etc\env.bat"

cd "%USERPROFILE%"
"%GRASS_PYTHON%" "%GISBASE%\etc\grass84.py" %*

if %ERRORLEVEL% GEQ 1 pause
