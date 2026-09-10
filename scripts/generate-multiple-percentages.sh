#!/usr/bin/env bash

FIES_DIR="../campaign4_snapshots/20260829_134155_s1/fies"
OUT_DIR="/Volumes/Backup/sampled"
SEED=42

for PCT in 1 5 25 50 100; do
	OUTPUT="${OUT_DIR}/20260829_134155_165h_${PCT}pct_smp2.parquet"
	LOG="${OUT_DIR}/20260829_134155_165h_${PCT}pct_smp2.log"

	nohup fie-importer parquet \
		--fies-dir "$FIES_DIR" \
		--seed "$SEED" \
		--pdid-percent "$PCT" \
		-o "$OUTPUT" \
		&>"$LOG" &

	echo "$PCT% started: PID=$! log=$LOG"
done
