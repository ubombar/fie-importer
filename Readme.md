# fie-importer

`fie-importer` is a small, single-purpose command-line utility for importing the artifacts of a completed Retina experiment into ClickHouse.

It combines two artifact sources:

- the Retina events emitted during the experiment;
- the FIEs captured by the Retina orchestrator.

The importer creates three ClickHouse tables containing the Probing Directives, agent connection terms, and enriched FIEs, ready for subsequent analysis.

## Usage

```bash
fie-importer \
    --events-dir ./captures/experiment \
    --fies-dir ./captures/experiment \
    --clickhouse-address localhost:9000 \
    --clickhouse-database retina \
    --name myexperiment
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

The event stream provides the Probing Directives used during the experiment and the agent connection and disconnection events required to reconstruct agent terms.

Agent terms are also used while reconstructing FIEs, including determining the source address associated with an agent at the time an FIE was captured.

### FIE captures

`--fies-dir` points to the directory containing the DuckDB FIE capture files produced by the Retina capturer.

The importer discovers the DuckDB capture files in this directory and processes them in post-processing rather than connecting to a live Retina stream.

## Output tables

For an import using:

```bash
--clickhouse-database retina
--name myexperiment
```

the importer creates:

```text
retina.myexperiment__pds
retina.myexperiment__agent_terms
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

### `<name>__agent_terms`

The agent-term table contains the periods during which agents were connected and active during the experiment.

Each term records the agent address and port observed when the agent connected, along with the beginning and end of that connection period.

An `end_time` of `NULL` indicates that no corresponding disconnection event was observed before the event capture ended.

```sql
CREATE TABLE <database>.<name>__agent_terms
(
    `agent_id`       String,
    `agent_ip`       IPv6,
    `agent_port`     UInt16,
    `beginning_time` DateTime64(9, 'UTC'),
    `end_time`       Nullable(DateTime64(9, 'UTC'))
)
ENGINE = MergeTree
ORDER BY
(
    agent_id,
    beginning_time
)
SETTINGS
    index_granularity = 8192;
```

### `<name>__fies`

The FIE table contains the captured FIEs enriched with the information required for analysis.

The global `sequence_number` is reconstructed from the preserved capture order of the DuckDB FIE records.

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
└── captures/
    ├── events-20260825T180000Z.jsonl
    ├── events-20260826T000000Z.jsonl
    ├── fies-20260825T180000Z.duckdb
    ├── fies-20260826T000000Z.duckdb
    └── ...
```

The corresponding import is:

```bash
export CH_USER="default"
export CH_PASSWORD="password"

fie-importer \
    --events-dir ./experiment/captures \
    --fies-dir ./experiment/captures \
    --clickhouse-address localhost:9000 \
    --clickhouse-database retina \
    --name myexperiment
```

This produces:

```text
retina.myexperiment__pds
retina.myexperiment__agent_terms
retina.myexperiment__fies
```

## Scope

`fie-importer` is deliberately a post-processing utility.

It does not capture live FIEs, subscribe to the Retina event stream, or perform real-time ClickHouse ingestion. Its purpose is to take the artifacts of a completed Retina experiment, combine them into a consistent representation, and load that representation into ClickHouse for analysis.
