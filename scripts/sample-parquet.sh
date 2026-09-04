#!/usr/bin/env bash

set -euo pipefail

EVENTS_DIR="../campaign4_snapshots/20260829_134155_s1/events"
FIES_DIR="../campaign4_snapshots/20260829_134155_s1/fies"
OUTPUT_FILE="./sample.parquet"
PERCENTAGE="1"
SEED="42"

usage() {
	cat <<EOF
Usage: $0 [options]

Options:
  --events-dir DIR      Events directory
                        Default: ../campaign4_snapshots/20260829_134155_s1/events

  --fies-dir DIR        FIEs directory
                        Default: ../campaign4_snapshots/20260829_134155_s1/fies

  --output FILE, -o FILE
                        Output Parquet file
                        Default: ./sample.parquet

  --percentage N, -p N  Percentage of PDs to sample
                        Default: 1

  --seed N              Random seed
                        Default: 42

  --help, -h            Show this help
EOF
}

while [[ $# -gt 0 ]]; do
	case "$1" in
	--events-dir)
		EVENTS_DIR="$2"
		shift 2
		;;
	--fies-dir)
		FIES_DIR="$2"
		shift 2
		;;
	--output | -o)
		OUTPUT_FILE="$2"
		shift 2
		;;
	--percentage | -p)
		PERCENTAGE="$2"
		shift 2
		;;
	--seed)
		SEED="$2"
		shift 2
		;;
	--help | -h)
		usage
		exit 0
		;;
	*)
		echo "unknown argument: $1" >&2
		usage >&2
		exit 1
		;;
	esac
done

TMP_PDS="$(mktemp)"
TMP_IDS="$(mktemp)"
trap 'rm -f "$TMP_PDS" "$TMP_IDS"' EXIT

echo "Loading probing directives..." >&2

fie-importer pds \
	--events-dir "$EVENTS_DIR" \
	>"$TMP_PDS"

TOTAL="$(wc -l <"$TMP_PDS" | tr -d ' ')"

echo "Total PDs: $TOTAL" >&2
echo "Sampling ${PERCENTAGE}% with seed $SEED..." >&2

python3 - "$TMP_PDS" "$TMP_IDS" "$PERCENTAGE" "$SEED" <<'PY'
import json
import random
import sys

input_file, output_file, percentage, seed = sys.argv[1:]

percentage = float(percentage)
seed = int(seed)

with open(input_file) as f:
    pds = [json.loads(line) for line in f if line.strip()]

n = round(len(pds) * percentage / 100.0)

rng = random.Random(seed)
sample = rng.sample(pds, n)

with open(output_file, "w") as f:
    for pd in sample:
        f.write(str(pd["probing_directive_id"]) + "\n")

print(f"Selected {n} / {len(pds)} PDs", file=sys.stderr)
PY

PDIDS="$(paste -sd, "$TMP_IDS")"

echo "Generating $OUTPUT_FILE..." >&2

fie-importer parquet \
	--fies-dir "$FIES_DIR" \
	--pdids "$PDIDS" \
	--output "$OUTPUT_FILE"

echo "Done: $OUTPUT_FILE" >&2
