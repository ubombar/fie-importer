#!/usr/bin/env bash

set -euo pipefail

: "${CH_USER:?CH_USER is not set}"
: "${CH_PASSWORD:?CH_PASSWORD is not set}"

CLICKHOUSE_ADDRESS="${CLICKHOUSE_ADDRESS:-localhost:9000}"
CLICKHOUSE_DATABASE="${CLICKHOUSE_DATABASE:-phase2_analysis}"

if [[ $# -ne 2 ]]; then
	echo "usage: $0 <table> <parquet-file>" >&2
	exit 1
fi

TABLE="$1"
PARQUET_FILE="$2"

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
            sequence_number UInt64,
            probing_directive_id UInt64,
            near_reply_address Nullable(IPv6),
            far_reply_address Nullable(IPv6),
            capture_timestamp DateTime
        )
        ENGINE = MergeTree
        ORDER BY sequence_number
    "

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
        sequence_number,
        probing_directive_id,
        near_reply_address,
        far_reply_address,
        capture_timestamp
    )
    SELECT
        sequence_number,
        probing_directive_id,
        if(isNull(near_reply_address), NULL, CAST(CAST(near_reply_address AS FixedString(16)) AS IPv6)),
        if(isNull(far_reply_address), NULL, CAST(CAST(far_reply_address AS FixedString(16)) AS IPv6)),
        capture_timestamp
    FROM input(
        'sequence_number UInt64,
         probing_directive_id UInt32,
         near_reply_address Nullable(String),
         far_reply_address Nullable(String),
         capture_timestamp UInt32'
    )
    FORMAT Parquet
" <"$PARQUET_FILE"
