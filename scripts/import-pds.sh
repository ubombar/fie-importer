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
			probing_directive_id UInt64,
			agent_id             LowCardinality(String),
			protocol             UInt8,
			destination_address  IPv6,
			near_ttl             UInt8,
			ip_version           UInt8 MATERIALIZED if(startsWith(toString(destination_address), '::ffff:'), 4, 6)
		)
		ENGINE = MergeTree
		ORDER BY 
		(
			probing_directive_id,
			agent_id,
			destination_address
		)
	"

fie-importer pds \
	--events-dir "$EVENTS_DIR" |
	jq -c '{
		probing_directive_id,
		agent_id,
		protocol,
		destination_address,
		near_ttl
	}' |
	clickhouse-client \
		--host "$HOST" \
		--port "$PORT" \
		--database "$CLICKHOUSE_DATABASE" \
		--user "$CH_USER" \
		--password "$CH_PASSWORD" \
		--async_insert=0 \
		--query "
			INSERT INTO \`${TABLE}\`
			(
				probing_directive_id,
				agent_id,
				protocol,
				destination_address,
				near_ttl
			)
			FORMAT JSONEachRow
		"
