@echo off
set "CONF_DIR=conf"

set "JAR_FILE=target\james-server-youtrackdb-app.jar"
if not exist "%JAR_FILE%" set "JAR_FILE=james-server-youtrackdb-app.jar"

if "%JAVA_OPTS%"=="" set JAVA_OPTS=-XX:+UseZGC -XX:+ZGenerational -Xms3g -Xmx3g

java %JAVA_OPTS% -Dworking.directory=. -Dextra.props="%CONF_DIR%\jvm.properties" -jar "%JAR_FILE%" %*
