#!/usr/bin/env sh
CONF_DIR="${CONF_DIR:-conf}"
[ ! -d "$CONF_DIR" ] && CONF_DIR="sample-configuration"

JAR_FILE="target/james-server-youtrackdb-app.jar"
[ ! -f "$JAR_FILE" ] && JAR_FILE="james-server-youtrackdb-app.jar"

exec java ${JAVA_OPTS:--XX:+UseZGC -XX:+ZGenerational -Xms3g -Xmx3g} \
     -Dworking.directory=. \
     -Dextra.props="${CONF_DIR}/jvm.properties" \
     -jar "$JAR_FILE" "$@"
