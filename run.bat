@echo off
rem ==============================================================================
rem James YouTrack Mail Server - Windows Launcher
rem ==============================================================================
setlocal enabledelayedexpansion

set "BASE_DIR=%~dp0"
if "%BASE_DIR:~-1%"=="\" set "BASE_DIR=%BASE_DIR:~0,-1%"

set "CONF_DIR=%BASE_DIR%\conf"
if not exist "%CONF_DIR%" (
    if exist "%BASE_DIR%\sample-configuration" (
        set "CONF_DIR=%BASE_DIR%\sample-configuration"
    )
)

set "JVM_PROPS=%CONF_DIR%\jvm.properties"

rem Default memory and GC settings
set "JVM_HEAP_MIN=3g"
set "JVM_HEAP_MAX=3g"
set "JVM_GC_TYPE=ZGC"
set "JVM_GC_GENERATIONAL=true"

if exist "%JVM_PROPS%" (
    for /f "usebackq tokens=1,* delims==" %%A in ("%JVM_PROPS%") do (
        set "KEY=%%A"
        set "VAL=%%B"
        rem Trim spaces
        for /f "tokens=* delims= " %%K in ("!KEY!") do set "KEY=%%K"
        for /f "tokens=* delims= " %%V in ("!VAL!") do set "VAL=%%V"
        
        if not "!KEY:~0,1!"=="#" (
            if /i "!KEY!"=="jvm.heap.min" set "JVM_HEAP_MIN=!VAL!"
            if /i "!KEY!"=="jvm.heap.max" set "JVM_HEAP_MAX=!VAL!"
            if /i "!KEY!"=="jvm.gc.type" set "JVM_GC_TYPE=!VAL!"
            if /i "!KEY!"=="jvm.gc.generational" set "JVM_GC_GENERATIONAL=!VAL!"
        )
    )
)

rem Build JVM GC arguments
set "GC_OPTS=-XX:+UseZGC"
if /i "%JVM_GC_TYPE%"=="G1" (
    set "GC_OPTS=-XX:+UseG1GC"
) else if /i "%JVM_GC_TYPE%"=="G1GC" (
    set "GC_OPTS=-XX:+UseG1GC"
) else (
    set "GC_OPTS=-XX:+UseZGC"
    if /i "%JVM_GC_GENERATIONAL%"=="true" set "GC_OPTS=-XX:+UseZGC -XX:+ZGenerational"
)

set "JVM_OPTS=-Xms%JVM_HEAP_MIN% -Xmx%JVM_HEAP_MAX% %GC_OPTS% %EXTRA_JVM_OPTS%"

set "JAR_PATH=%BASE_DIR%\target\james-server-youtrackdb-app.jar"
if not exist "%JAR_PATH%" (
    set "JAR_PATH=%BASE_DIR%\james-server-youtrackdb-app.jar"
)

if not exist "%JAR_PATH%" (
    echo ERROR: james-server-youtrackdb-app.jar not found in %BASE_DIR% or target\ >&2
    exit /b 1
)

echo Starting James YouTrack Mail Server...
echo JVM Options: %JVM_OPTS%
java %JVM_OPTS% -Dworking.directory="%BASE_DIR%" -jar "%JAR_PATH%" %*
