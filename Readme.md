# fie-importer

`fie-importer` is a small, single-purpose command-line utility for importing the output of a Retina experiment into ClickHouse.

It combines the three sources produced or used during an experiment:

- **FIE captures** — FIEs captured by the Retina orchestrator.
- **Retina events** — events emitted by the orchestrator during the experiment.
- **Probing Directives (PDs)** — the `pds.jsonl` file containing the probing directives used to generate the FIEs.

The importer reads these sources, enriches the captured FIEs with the corresponding PD and event metadata, converts them to the `fies3` schema, and inserts the resulting records into a ClickHouse database.

## Purpose

This utility is intended for post-processing completed Retina experiments. It is not part of the live capture path and does not modify the original capture files.

Conceptually, the import pipeline is:

```text
pds.jsonl ──────────┐
                    │
Retina events ──────┼──► enrich FIEs ──► fies3 ──► ClickHouse
                    │
FIE captures ───────┘
```

In particular, information that is not directly available in a captured FIE, such as the source address associated with the experiment, can be recovered from the corresponding Retina events before insertion.

## Usage

```bash
fie-importer \
    --pds ./pds.jsonl \
    --capture-dir ./captures/experiment \
    --clickhouse-address localhost:9000 \
    --clickhouse-database retina
```

The exact command-line options can be listed with:

```bash
fie-importer --help
```

## Input

### Probing Directives

The PD input is a JSONL file containing the probing directives used for the experiment:

```text
pds.jsonl
```

Each line contains one probing directive.

### Capture directory

The capture directory contains the data produced by the Retina orchestrator during the experiment, including the captured FIE databases and Retina event files.

For example:

```text
capture/
├── events/
│   └── events-20260825T180000Z.jsonl
└── fies/
    ├── fies-20260825T180000Z.duckdb
    ├── fies-20260826T000000Z.duckdb
    └── ...
```

## Output

The importer transforms the captured data into the `fies3` schema and inserts the resulting rows into the configured ClickHouse database.

The original PD, event, and capture files are left unchanged.

## Scope

`fie-importer` is deliberately a simple, single-use post-processing tool. Its job is limited to converting the artifacts of a completed Retina experiment into the ClickHouse representation used for subsequent analysis.

It is not intended to provide real-time ingestion, capture FIEs directly from Retina, or replace the orchestrator's capture mechanism.
