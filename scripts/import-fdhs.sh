#!/usr/bin/env bash

set -euo pipefail

: "${CH_USER:?CH_USER is not set}"
: "${CH_PASSWORD:?CH_PASSWORD is not set}"

CLICKHOUSE_ADDRESS="${CLICKHOUSE_ADDRESS:-localhost:9000}"
CLICKHOUSE_DATABASE="${CLICKHOUSE_DATABASE:-phase2_analysis}"

if [[ $# -ne 3 ]]; then
	echo "usage: $0 <pds-table> <fies-table> <fdhs-table>" >&2
	exit 1
fi

PDS_TABLE="$1"
FIES_TABLE="$2"
FDHS_TABLE="$3"

HOST="${CLICKHOUSE_ADDRESS%:*}"
PORT="${CLICKHOUSE_ADDRESS##*:}"

CH=(clickhouse-client --host "$HOST" --port "$PORT" --database "$CLICKHOUSE_DATABASE" --user "$CH_USER" --password "$CH_PASSWORD" --max_execution_time=0)

EXISTS=$("${CH[@]}" --query "EXISTS TABLE \`${FDHS_TABLE}\`")
if [[ "$EXISTS" == "1" ]]; then
	echo "table '${FDHS_TABLE}' already exists; drop or rename it manually before running this script" >&2
	exit 1
fi

echo "creating table '${FDHS_TABLE}'" >&2
"${CH[@]}" --query "
    CREATE TABLE \`${FDHS_TABLE}\`
    ENGINE = MergeTree
    ORDER BY (near_address, destination_address, capture_timestamp, sequence_number)
    AS SELECT
        f.probing_directive_id AS probing_directive_id,
        f.sequence_number AS sequence_number,
        f.capture_timestamp AS capture_timestamp,
        assumeNotNull(f.near_reply_address) AS near_address,
        p.destination_address AS destination_address,
        p.near_ttl AS ttl,
        f.far_reply_address AS far_address,
        p.ip_version AS ip_version,
        p.agent_id AS agent_id
    FROM \`${FIES_TABLE}\` AS f
    INNER JOIN \`${PDS_TABLE}\` AS p USING (probing_directive_id)
    WHERE f.near_reply_address IS NOT NULL
    SETTINGS max_bytes_before_external_sort = 8000000000, max_bytes_before_external_group_by = 8000000000
"

COUNT=$("${CH[@]}" --query "SELECT count() FROM \`${FDHS_TABLE}\`")
echo "created '${FDHS_TABLE}' with ${COUNT} rows" >&2
