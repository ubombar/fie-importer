#!/usr/bin/env bash
# Export a PD table (for example the output of select-cover-pds.sh) as a
# ProbingDirective JSONL file for the orchestrator. Read-only on ClickHouse.
#
# The probing_directive_id written to the file is the one of the table, so the
# file can be traced back to it (and to its likely_null_near tag). The
# orchestrator ignores it and assigns its own IDs, the 0-based line numbers.
# import-pds-jsonl.sh keeps both when the file is loaded again.
#
# Line order: by near TTL, then by a hash of (agent, destination). Two PDs of
# one agent for the same destination at adjacent near TTLs send the same probe,
# and the agent drops the second one if both arrive within the same second.
# This order puts such PDs one whole TTL block apart instead of side by side.
#
# Only ICMP (1) and ICMPv6 (58) PDs are supported, with zero half words: the PD
# tables do not store the half words.

set -euo pipefail

: "${CH_USER:?CH_USER is not set}"
: "${CH_PASSWORD:?CH_PASSWORD is not set}"

CLICKHOUSE_ADDRESS="${CLICKHOUSE_ADDRESS:-localhost:9000}"
CLICKHOUSE_DATABASE="${CLICKHOUSE_DATABASE:-pam_campaign}"

log() { printf '[%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*" >&2; }
usage() {
	log "usage: $0 <pds-table> <output-jsonl-file>"
	exit 1
}

[[ $# -eq 2 ]] || usage

TABLE="$1"
OUTPUT="$2"

[[ ! -e "$OUTPUT" ]] || {
	log "Error: $OUTPUT already exists; remove or rename it first"
	exit 1
}

HOST="${CLICKHOUSE_ADDRESS%:*}"
PORT="${CLICKHOUSE_ADDRESS##*:}"

CH=(clickhouse-client --host "$HOST" --port "$PORT" --database "$CLICKHOUSE_DATABASE" --user "$CH_USER" --password "$CH_PASSWORD" --max_execution_time=0)

UNSUPPORTED=$("${CH[@]}" --query "SELECT count() FROM \`${TABLE}\` WHERE protocol NOT IN (1, 58)")
[[ "$UNSUPPORTED" == "0" ]] || {
	log "Error: $UNSUPPORTED PDs have a protocol other than ICMP (1) or ICMPv6 (58)"
	exit 1
}

log "Exporting $CLICKHOUSE_DATABASE.$TABLE to $OUTPUT..."
"${CH[@]}" --query "
	SELECT format(
		'{{\"probing_directive_id\":{},\"ip_version\":{},\"protocol\":{},\"agent_id\":\"{}\",\"destination_address\":\"{}\",\"near_ttl\":{},\"next_header\":{{\"{}\":{{\"first_half_word\":0,\"second_half_word\":0}}}}}}',
		toString(probing_directive_id),
		toString(ip_version),
		toString(protocol),
		toString(agent_id),
		replaceOne(toString(destination_address), '::ffff:', ''),
		toString(near_ttl),
		if(protocol = 1, 'icmp_next_header', 'icmpv6_next_header')
	)
	FROM \`${TABLE}\`
	ORDER BY near_ttl, cityHash64(agent_id, destination_address), probing_directive_id
	FORMAT TSVRaw
" >"$OUTPUT"

ROWS=$("${CH[@]}" --query "SELECT count() FROM \`${TABLE}\`")
LINES=$(wc -l <"$OUTPUT" | tr -d ' ')
log "Wrote $LINES lines for $ROWS rows"
[[ "$LINES" == "$ROWS" ]] || {
	log "Error: line count does not match row count"
	exit 1
}
