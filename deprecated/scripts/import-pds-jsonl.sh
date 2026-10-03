#!/usr/bin/env bash
# Load a ProbingDirective JSONL file (the one inserted into the orchestrator)
# into a new ClickHouse table.
#
# The orchestrator ignores the probing_directive_id of the file and assigns its
# own IDs, consecutive in insertion order. So the ID that the FIEs carry is the
# 0-based line number of the PD, stored here as probing_directive_id. The ID
# written in the file is kept as source_probing_directive_id.

set -euo pipefail

: "${CH_USER:?CH_USER is not set}"
: "${CH_PASSWORD:?CH_PASSWORD is not set}"

CLICKHOUSE_ADDRESS="${CLICKHOUSE_ADDRESS:-localhost:9000}"
CLICKHOUSE_DATABASE="${CLICKHOUSE_DATABASE:-pam_campaign}"

log() { printf '[%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*" >&2; }
usage() {
	log "usage: $0 <table> <pds-jsonl-file>"
	exit 1
}

[[ $# -eq 2 ]] || usage

TABLE="$1"
JSONL_FILE="$2"

[[ -f "$JSONL_FILE" ]] || {
	log "Error: no such file: $JSONL_FILE"
	exit 1
}

HOST="${CLICKHOUSE_ADDRESS%:*}"
PORT="${CLICKHOUSE_ADDRESS##*:}"

CH=(clickhouse-client --host "$HOST" --port "$PORT" --database "$CLICKHOUSE_DATABASE" --user "$CH_USER" --password "$CH_PASSWORD")

# No IF NOT EXISTS: an existing table fails the run instead of being appended to.
log "Creating table $CLICKHOUSE_DATABASE.$TABLE..."
"${CH[@]}" --query "
	CREATE TABLE \`${TABLE}\`
	(
		probing_directive_id        UInt64,
		source_probing_directive_id UInt64,
		agent_id                    LowCardinality(String),
		ip_version                  UInt8,
		protocol                    UInt8,
		destination_address         IPv6,
		near_ttl                    UInt8
	)
	ENGINE = MergeTree
	ORDER BY probing_directive_id
"

# awk renames the file's ID and puts the line number in front of each object.
log "Inserting $JSONL_FILE..."
awk '{
	sub(/"probing_directive_id":/, "\"source_probing_directive_id\":")
	printf "{\"probing_directive_id\":%d,%s\n", NR - 1, substr($0, 2)
}' "$JSONL_FILE" |
	"${CH[@]}" \
		--async_insert=0 \
		--max_execution_time=0 \
		--input_format_skip_unknown_fields=1 \
		--query "
			INSERT INTO \`${TABLE}\`
			(
				probing_directive_id,
				source_probing_directive_id,
				agent_id,
				ip_version,
				protocol,
				destination_address,
				near_ttl
			)
			FORMAT JSONEachRow
		"

COUNT=$("${CH[@]}" --query "SELECT count() FROM \`${TABLE}\`")
LINES=$(wc -l <"$JSONL_FILE" | tr -d ' ')
log "Inserted $COUNT rows from $LINES lines"
[[ "$COUNT" == "$LINES" ]] || {
	log "Error: row count does not match line count"
	exit 1
}
