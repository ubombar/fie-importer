#!/usr/bin/env bash

set -euo pipefail

log() {
	printf '[%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*"
}

if [[ $# -ne 2 ]]; then
	echo "Usage: $0 <capture-dir> <name>" >&2
	exit 1
fi

CAPTURE_DIR="${1%/}"
NAME="$2"

if [[ ! -d "$CAPTURE_DIR" ]]; then
	echo "Capture directory does not exist: $CAPTURE_DIR" >&2
	exit 1
fi

DIR_NAME="$(basename "$CAPTURE_DIR")"
IMPORT_NAME="${DIR_NAME}_${NAME}"

EVENTS_DIR="$CAPTURE_DIR/events"
FIES_DIR="$CAPTURE_DIR/fies"

if [[ ! -d "$EVENTS_DIR" ]]; then
	echo "Events directory does not exist: $EVENTS_DIR" >&2
	exit 1
fi

if [[ ! -d "$FIES_DIR" ]]; then
	echo "FIEs directory does not exist: $FIES_DIR" >&2
	exit 1
fi

: "${CH_USER:?CH_USER is not set}"
: "${CH_PASSWORD:?CH_PASSWORD is not set}"

CLICKHOUSE_ADDRESS="${CLICKHOUSE_ADDRESS:-localhost:9000}"
CLICKHOUSE_DATABASE="${CLICKHOUSE_DATABASE:-phase2_analysis}"

log "Starting FIE import"
log "Capture directory: $CAPTURE_DIR"
log "Events directory: $EVENTS_DIR"
log "FIEs directory: $FIES_DIR"
log "ClickHouse address: $CLICKHOUSE_ADDRESS"
log "ClickHouse database: $CLICKHOUSE_DATABASE"
log "Import name: $IMPORT_NAME"

go run . \
	--events-dir "$EVENTS_DIR" \
	--fies-dir "$FIES_DIR" \
	--clickhouse-address "$CLICKHOUSE_ADDRESS" \
	--clickhouse-database "$CLICKHOUSE_DATABASE" \
	--name "$IMPORT_NAME"

log "Import completed successfully"
