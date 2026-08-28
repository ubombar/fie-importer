# fie-importer

`fie-importer` is a small, single-purpose command-line utility for importing the artifacts of a completed Retina experiment into ClickHouse.

It combines three sources:

- the Retina events emitted during the experiment;
- the FIEs captured by the Retina orchestrator.

The importer creates two ClickHouse tables containing the PDs and the enriched FIEs, ready for subsequent analysis.

## Usage

```bash
fie-importer \
    --events-dir ./captures/experiment \
    --fies-dir ./captures/experiment \
    --clickhouse-address localhost:9000 \
    --clickhouse-database retina \
    --label myexperiment
```

ClickHouse credentials are read from the environment:

```bash
export CH_USER="default"
export CH_PASSWORD="password"
```

## Command-line arguments

| Flag                    | Description                                                                                         |
| ----------------------- | --------------------------------------------------------------------------------------------------- |
| `--events-dir`          | Directory containing the Retina event captures. Only files matching `events-*.jsonl` are processed. |
| `--fies-dir`            | Directory containing the FIE captures. Only DuckDB capture files are processed.                     |
| `--clickhouse-address`  | Address of the destination ClickHouse server, for example `localhost:9000`.                         |
| `--clickhouse-database` | ClickHouse database into which the tables are created.                                              |
| `--name`                | Name identifying the experiment. Used to construct the destination table names.                     |

`--events-dir` and `--fies-dir` may point to the same directory. Each input loader only considers files relevant to it.

## Environment variables

ClickHouse authentication is configured through:

| Variable      | Description          |
| ------------- | -------------------- |
| `CH_USER`     | ClickHouse username. |
| `CH_PASSWORD` | ClickHouse password. |

Credentials are intentionally not passed as command-line arguments.

## Input

### Probing Directives

`--pds` points to a JSONL file containing the Probing Directives used to generate the experiment traffic.

```text
pds.jsonl
```

Each line contains one Probing Directive.

### Retina events

`--events-dir` points to the directory containing events emitted by the Retina orchestrator.

The importer only considers files matching:

```text
events-*.jsonl
```

For example:

```text
events-20260825T180000Z.jsonl
events-20260826T000000Z.jsonl
```

The event stream provides additional information required when constructing the final FIE representation, including the FIE source address.

### FIE captures

`--fies-dir` points to the directory containing the DuckDB FIE capture files produced by the Retina capturer.

The importer discovers the DuckDB capture files in this directory and processes them in post-processing rather than connecting to a live Retina stream.

## Import process

The importer performs the following pipeline:

```text
pds.jsonl ───────────────────────────────► <name>__pds
                                                │
Retina events ──► experiment metadata           │
                         │                      │
                         ▼                      │
DuckDB FIEs ──────► enrich/transform FIEs ◄─────┘
                         │
                         ▼
                    <name>__fies
```

The importer:

1. reads the Probing Directives from `pds.jsonl`;
2. reads the Retina event files from `--events-dir`;
3. extracts the metadata required to enrich the captured FIEs;
4. reads the DuckDB FIE capture files from `--fies-dir`;
5. transforms the PD and FIE records into their ClickHouse schemas;
6. creates the destination tables;
7. inserts the resulting records into ClickHouse.

The source files are never modified.

## Output tables

For an import using:

```bash
--clickhouse-database retina
--name myexperiment
```

the importer creates:

```text
retina.myexperiment__pds
retina.myexperiment__fies
```

### `<name>__pds`

The PD table contains the Probing Directives used by the experiment.

```sql
CREATE TABLE <database>.<name>__pds
(
    `probing_directive_id` UInt64,
    `agent_id`             String,
    `ip_version`           UInt8,
    `protocol`             UInt8,
    `destination_address`  IPv6,
    `near_ttl`             UInt8,
    `next_header_type`     Enum8(
        'icmp'   = 1,
        'icmpv6' = 2
    ),
    `first_half_word`      UInt16,
    `second_half_word`     UInt16,
    `event_timestamp`      DateTime64(9, 'UTC')
)
ENGINE = MergeTree
ORDER BY
(
    destination_address,
    agent_id,
    probing_directive_id
)
SETTINGS
    index_granularity = 8192;
```

### `<name>__fies`

The FIE table contains the captured FIEs enriched with the information required for analysis.

```sql
CREATE TABLE <database>.<name>__fies
(
    `sequence_number`         UInt64,
    `agent_id`                String,
    `probing_directive_id`    UInt64,
    `ip_version`              UInt8,
    `protocol`                UInt8,
    `source_address`          IPv6,
    `destination_address`     IPv6,
    `near_probe_ttl`          UInt8,
    `near_reply_address`      IPv6,
    `near_sent_timestamp`     DateTime,
    `near_received_timestamp` DateTime,
    `far_probe_ttl`           UInt8,
    `far_reply_address`       IPv6,
    `far_sent_timestamp`      DateTime,
    `far_received_timestamp`  DateTime,
    `production_timestamp`    DateTime
)
ENGINE = MergeTree
ORDER BY
(
    near_reply_address,
    destination_address,
    agent_id,
    production_timestamp
)
SETTINGS
    index_granularity = 8192;
```

## Example directory layout

The FIE and event captures may share a directory:

```text
experiment/
├── pds.jsonl
└── captures/
    ├── events-20260825T180000Z.jsonl
    ├── fies-20260825T180000Z.duckdb
    ├── fies-20260826T000000Z.duckdb
    └── ...
```

The corresponding import is:

```bash
export CH_USER="default"
export CH_PASSWORD="password"

fie-importer \
    --pds ./experiment/pds.jsonl \
    --events-dir ./experiment/captures \
    --fies-dir ./experiment/captures \
    --clickhouse-address localhost:9000 \
    --clickhouse-database retina \
    --name myexperiment
```

This produces:

```text
retina.myexperiment__pds
retina.myexperiment__fies
```

## Scope

`fie-importer` is deliberately a post-processing utility.

It does not capture live FIEs, subscribe to the Retina event stream, or perform real-time ClickHouse ingestion. Its purpose is to take the artifacts of a completed Retina experiment, combine them into a consistent representation, and load that representation into ClickHouse for analysis.
