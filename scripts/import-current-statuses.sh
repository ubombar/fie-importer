#!/usr/bin/env bash

set -euo pipefail

: "${CH_USER:?CH_USER is not set}"
: "${CH_PASSWORD:?CH_PASSWORD is not set}"

CLICKHOUSE_ADDRESS="${CLICKHOUSE_ADDRESS:-localhost:9000}"
CLICKHOUSE_DATABASE="${CLICKHOUSE_DATABASE:-phase2_analysis}"

if [[ $# -ne 2 ]]; then
	echo "usage: $0 <table> <events-dir>" >&2
	exit 1
fi

TABLE="$1"
EVENTS_DIR="$2"

HOST="${CLICKHOUSE_ADDRESS%:*}"
PORT="${CLICKHOUSE_ADDRESS##*:}"

clickhouse-client \
	--host "$HOST" \
	--port "$PORT" \
	--database "$CLICKHOUSE_DATABASE" \
	--user "$CH_USER" \
	--password "$CH_PASSWORD" \
	--query "
		CREATE TABLE \`${TABLE}\`
		(
			timestamp                          DateTime64(9, 'UTC'),
			current_pd_count                   UInt64,
			cumulative_insertions              UInt64,
			cumulative_issuances               UInt64,
			cumulative_updates                 UInt64,
			aggregate_requested_rate           Float64,
			aggregate_period_between_issuances Float64,
			realized_issuance_rate             Float64,
			realized_update_rate               Float64,
			distinct_impacted_addrs            UInt64,
			period_min                         Float64,
			period_max                         Float64,
			pds_clamped_at_min                 UInt64,
			pds_clamped_at_max                 UInt64,
			pds_with_full_history              UInt64,
			update_channel_occupancy           UInt64,
			insert_channel_occupancy           UInt64,
			cumulative_late_occurrences        UInt64
		)
		ENGINE = MergeTree
		ORDER BY timestamp
	"

fie-importer current-status \
	--events-dir "$EVENTS_DIR" |
	clickhouse-client \
		--host "$HOST" \
		--port "$PORT" \
		--database "$CLICKHOUSE_DATABASE" \
		--user "$CH_USER" \
		--password "$CH_PASSWORD" \
		--async_insert=0 \
		--query "
			INSERT INTO \`${TABLE}\`
			FORMAT JSONEachRow
		"
