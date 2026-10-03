#!/usr/bin/env bash
# Select the PDs that cover every FDH seen in a run and write them to a new
# ClickHouse table.
#
# An FDH (forwarding decision handle) is a (near_address, destination_address)
# pair; the agent is not part of it. A null near address is kept as a handle of
# its own, (NULL, destination_address), and the PD selected for it is tagged
# with likely_null_near = 1.
#
# Assumption: finding the minimum number of PDs that cover all FDHs is the set
# cover problem, which is NP-hard. This script uses a heuristic instead: for
# each FDH it keeps the PD that yielded it most often. It decides per FDH and
# gives no credit to a PD that covers several FDHs, so on a run where PDs are
# issued several times the result can be larger than the minimum. On a run
# where every PD is issued once, a PD yields exactly one FDH and the result is
# the minimum: one PD per FDH.
#
# Ties on the occurrence count are broken by preferring a PD whose far side
# replied, then by a hash of the PD ID, which spreads the picks across agents.

set -euo pipefail

: "${CH_USER:?CH_USER is not set}"
: "${CH_PASSWORD:?CH_PASSWORD is not set}"

CLICKHOUSE_ADDRESS="${CLICKHOUSE_ADDRESS:-localhost:9000}"
CLICKHOUSE_DATABASE="${CLICKHOUSE_DATABASE:-pam_campaign}"

log() { printf '[%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*" >&2; }
usage() {
	log "usage: $0 <pds-table> <fies-table> <cover-table>"
	log "  creates <cover-table>: the selected rows of <pds-table>, plus covered_fdhs and likely_null_near"
	exit 1
}

[[ $# -eq 3 ]] || usage

PDS_TABLE="$1"
FIES_TABLE="$2"
COVER_TABLE="$3"

HOST="${CLICKHOUSE_ADDRESS%:*}"
PORT="${CLICKHOUSE_ADDRESS##*:}"

CH=(clickhouse-client --host "$HOST" --port "$PORT" --database "$CLICKHOUSE_DATABASE" --user "$CH_USER" --password "$CH_PASSWORD" --max_execution_time=0)

# One row per (FDH, PD) with how often the PD yielded the FDH.
OCCURRENCES="
	SELECT
		f.near_reply_address AS near_address,
		p.destination_address AS destination_address,
		f.probing_directive_id AS probing_directive_id,
		count() AS occurrences,
		countIf(f.far_reply_address IS NOT NULL) AS far_replies
	FROM \`${FIES_TABLE}\` AS f
	INNER JOIN \`${PDS_TABLE}\` AS p USING (probing_directive_id)
	GROUP BY near_address, destination_address, probing_directive_id
"

# No IF NOT EXISTS: an existing table fails the run instead of being replaced.
log "Creating table $CLICKHOUSE_DATABASE.$COVER_TABLE..."
"${CH[@]}" --query "
	CREATE TABLE \`${COVER_TABLE}\`
	ENGINE = MergeTree
	ORDER BY probing_directive_id
	AS SELECT
		p.*,
		s.covered_fdhs AS covered_fdhs,
		s.likely_null_near AS likely_null_near
	FROM \`${PDS_TABLE}\` AS p
	INNER JOIN
	(
		-- A PD can win several FDHs; keep it once.
		SELECT
			probing_directive_id,
			toUInt32(count()) AS covered_fdhs,
			toUInt8(countIf(near_address IS NULL) > 0) AS likely_null_near
		FROM
		(
			-- The winning PD of each FDH.
			SELECT
				near_address,
				destination_address,
				argMax(probing_directive_id, (occurrences, far_replies > 0, cityHash64(probing_directive_id))) AS probing_directive_id
			FROM (${OCCURRENCES})
			GROUP BY near_address, destination_address
		)
		GROUP BY probing_directive_id
	) AS s USING (probing_directive_id)
"

log "Checking the selection..."
FDHS=$("${CH[@]}" --query "
	SELECT count() FROM (SELECT 1 FROM (${OCCURRENCES}) GROUP BY near_address, destination_address)
")
COVERED=$("${CH[@]}" --query "
	SELECT count() FROM
	(
		SELECT 1 FROM (${OCCURRENCES})
		WHERE probing_directive_id IN (SELECT probing_directive_id FROM \`${COVER_TABLE}\`)
		GROUP BY near_address, destination_address
	)
")
log "FDHs in the run: $FDHS, FDHs yielded by the selected PDs: $COVERED"

"${CH[@]}" --query "
	SELECT
		count() AS selected_pds,
		(SELECT count() FROM \`${PDS_TABLE}\`) AS all_pds,
		round(100 * selected_pds / all_pds, 2) AS selected_pct,
		countIf(likely_null_near = 1) AS likely_null_near_pds,
		sum(covered_fdhs) AS covered_fdhs
	FROM \`${COVER_TABLE}\`
	FORMAT Vertical
" >&2

"${CH[@]}" --query "
	SELECT agent_id, count() AS selected_pds, countIf(likely_null_near = 1) AS likely_null_near_pds
	FROM \`${COVER_TABLE}\`
	GROUP BY agent_id
	ORDER BY agent_id
	FORMAT PrettyCompactMonoBlock
" >&2

[[ "$COVERED" == "$FDHS" ]] || {
	log "Error: the selected PDs do not cover every FDH"
	exit 1
}
log "Done: every FDH is covered"
