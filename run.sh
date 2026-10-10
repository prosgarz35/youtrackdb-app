#!/usr/bin/env bash
# ==============================================================================
# James YouTrack Mail Server - Linux / macOS Launcher
# ==============================================================================
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$SCRIPT_DIR"

# Locate configuration directory
CONF_DIR="${BASE_DIR}/conf"
if [ ! -d "$CONF_DIR" ] && [ -d "${BASE_DIR}/sample-configuration" ]; then
    CONF_DIR="${BASE_DIR}/sample-configuration"
fi

JVM_PROPS="${CONF_DIR}/jvm.properties"

# Default memory and GC settings
JVM_HEAP_MIN="3072m"
JVM_HEAP_MAX="3072m"
JVM_GC_TYPE="ZGC"
JVM_GC_GENERATIONAL="true"

# Read options from jvm.properties if present
if [ -f "$JVM_PROPS" ]; then
    while IFS='=' read -r key val || [ -n "$key" ]; do
        # Strip comments and surrounding whitespace
        key=$(echo "$key" | tr -d '\r' | sed 's/^[ \t]*//;s/[ \t]*$//')
        val=$(echo "$val" | tr -d '\r' | sed 's/^[ \t]*//;s/[ \t]*$//')
        [[ "$key" =~ ^#.* ]] && continue
        [ -z "$key" ] && continue

        case "$key" in
            jvm.heap.min) JVM_HEAP_MIN="$val" ;;
            jvm.heap.max) JVM_HEAP_MAX="$val" ;;
            jvm.gc.type) JVM_GC_TYPE="$val" ;;
            jvm.gc.generational) JVM_GC_GENERATIONAL="$val" ;;
        esac
    done < "$JVM_PROPS"
fi

# Build JVM GC arguments
GC_OPTS=""
case "$JVM_GC_TYPE" in
    ZGC|zgc)
        GC_OPTS="-XX:+UseZGC"
        if [ "$JVM_GC_GENERATIONAL" = "true" ]; then
            GC_OPTS="${GC_OPTS} -XX:+ZGenerational"
        fi
        ;;
    G1|g1|G1GC|g1gc)
        GC_OPTS="-XX:+UseG1GC"
        ;;
    Parallel|parallel|ParallelGC)
        GC_OPTS="-XX:+UseParallelGC"
        ;;
    Serial|serial|SerialGC)
        GC_OPTS="-XX:+UseSerialGC"
        ;;
    *)
        GC_OPTS="-XX:+UseZGC -XX:+ZGenerational"
        ;;
esac

JVM_OPTS="-Xms${JVM_HEAP_MIN} -Xmx${JVM_HEAP_MAX} ${GC_OPTS} ${EXTRA_JVM_OPTS:-}"

# Locate JAR file
JAR_PATH="${BASE_DIR}/target/james-server-youtrackdb-app.jar"
if [ ! -f "$JAR_PATH" ]; then
    JAR_PATH="${BASE_DIR}/james-server-youtrackdb-app.jar"
fi

if [ ! -f "$JAR_PATH" ]; then
    echo "ERROR: james-server-youtrackdb-app.jar not found in ${BASE_DIR} or target/" >&2
    exit 1
fi

echo "Starting James YouTrack Mail Server..."
echo "JVM Options: ${JVM_OPTS}"
exec java ${JVM_OPTS} -Dworking.directory="${BASE_DIR}" -jar "${JAR_PATH}" "$@"
