# fie-importer Design

## 1. Overview

`fie-importer` is a post-processing utility that converts the artifacts of a completed Retina experiment into two ClickHouse tables:

```text
<name>__pds
<name>__fies
```

The importer consumes three inputs:

```text
pds.jsonl
events-*.jsonl
fies-*.duckdb
```

and combines them into the ClickHouse representation required for analysis.

The importer is intentionally designed as a single-run batch program. It does not provide live ingestion, incremental synchronization, or daemon behavior.

## 2. Goals

The importer must:

- read the PDs used for the experiment;
- read Retina events associated with the experiment;
- extract metadata required to enrich FIEs;
- read all FIE capture databases from the provided directory;
- transform PDs and FIEs into the destination ClickHouse schemas;
- create the two destination tables;
- insert the resulting records efficiently;
- fail explicitly if the input is inconsistent or incomplete.

The importer must not modify the original PD, event, or DuckDB files.

## 3. Non-goals

The importer is not intended to:

- capture live FIEs;
- subscribe to Retina event streams;
- continuously watch a directory for new files;
- synchronize ClickHouse with ongoing experiments;
- retry indefinitely after errors;
- provide general-purpose ETL functionality;
- support arbitrary input schemas.

The importer operates on the known Retina experiment formats.

## 4. Command-line interface

The importer is invoked as:

```bash
fie-importer \
    --pds ./pds.jsonl \
    --events-dir ./captures/experiment \
    --fies-dir ./captures/experiment \
    --clickhouse-address localhost:9000 \
    --clickhouse-database retina \
    --name myexperiment
```

The ClickHouse username and password are read from:

```text
CH_USER
CH_PASSWORD
```

The resulting tables are:

```text
retina.myexperiment__pds
retina.myexperiment__fies
```

### 4.1 Input discovery

`--events-dir` only considers files matching:

```text
events-*.jsonl
```

`--fies-dir` only considers DuckDB capture files.

The directories may point to the same location.

Input files should be sorted by filename before processing to make execution deterministic.

## 5. High-level architecture

The importer consists of four logical stages:

```text
                 ┌─────────────────┐
                 │   pds.jsonl     │
                 └────────┬────────┘
                          │
                          ▼
                 ┌─────────────────┐
                 │   PD loader     │
                 └────────┬────────┘
                          │
                          ▼
                     PD metadata
                          │
                          │
events-*.jsonl ──► Event loader ──► Event metadata
                          │
                          │
                          ▼
fies-*.duckdb ──► FIE reader ──► enrichment ──► ClickHouse batch writer
```

PD and event metadata are expected to be small enough to keep in memory.

FIEs are expected to be the dominant dataset and must therefore be streamed rather than loaded fully into memory.

## 6. Processing phases

### 6.1 Configuration validation

Before processing any data, the importer validates:

- `--pds` is provided;
- `--events-dir` is provided;
- `--fies-dir` is provided;
- `--clickhouse-address` is provided;
- `--clickhouse-database` is provided;
- `--name` is provided;
- the PD file exists;
- the event directory exists;
- the FIE directory exists;
- `CH_USER` is available if required by the ClickHouse deployment;
- `CH_PASSWORD` is available if required by the ClickHouse deployment.

The importer must reject an invalid table name before sending SQL to ClickHouse.

The experiment name should therefore be restricted to a conservative identifier format such as:

```text
[a-zA-Z0-9_]+
```

### 6.2 Input discovery

The importer discovers:

```text
eventsDir/events-*.jsonl
```

and all supported DuckDB capture files in `fiesDir`.

Files are sorted lexicographically before processing.

Because Retina filenames contain UTC timestamps in sortable format, lexicographic ordering also provides chronological processing.

Example:

```text
fies-20260825T180000Z.duckdb
fies-20260826T000000Z.duckdb
fies-20260826T060000Z.duckdb
```

### 6.3 Load PDs

The PD JSONL file is read line-by-line.

Each line contains one probing directive.

The importer extracts the fields required by the destination PD schema and builds any lookup structures required later when processing FIEs.

The primary identifier is:

```text
probing_directive_id
```

A natural in-memory representation is:

```go
map[uint64]PDMetadata
```

where the key is the probing directive ID.

The PD dataset is expected to be sufficiently small to retain in memory.

Duplicate probing directive IDs should be treated as an error unless the input format explicitly permits them.

### 6.4 Load events

All `events-*.jsonl` files are read line-by-line.

Each JSON object is decoded using its event `type`.

Only event types required by the importer need to be interpreted. Other Retina event types may be ignored.

The primary purpose of this stage is to recover metadata that is not stored directly in the captured FIE representation.

In particular, the importer must recover the `source_address` required by the destination FIE schema.

The resulting event-derived metadata should be stored in an in-memory lookup keyed by the identifier required to associate it with the corresponding PD or FIE.

The exact lookup structure depends on the event carrying the source-address information, but conceptually:

```go
map[uint64]EventMetadata
```

where:

```text
key = probing_directive_id
```

and `EventMetadata` contains at least:

```text
source_address
```

If source-address information changes over time for the same PD, the lookup must instead preserve the required temporal relationship rather than storing a single value. The implementation should follow the semantics of the actual Retina event that carries the address.

### 6.5 Create ClickHouse tables

After configuration and inputs have been validated, the importer connects to ClickHouse and creates:

```text
<name>__pds
<name>__fies
```

in the configured database.

Table creation should use explicit schemas compiled into the program rather than dynamically inferred schemas.

The importer should fail if the destination tables already exist.

This avoids accidentally appending an experiment to an existing dataset or partially duplicating an earlier import.

The user can explicitly remove the old tables before re-running an import.

## 7. PD transformation

Each input PD is transformed into one row of:

```text
<name>__pds
```

with the destination fields:

```text
probing_directive_id
agent_id
ip_version
protocol
destination_address
near_ttl
next_header_type
first_half_word
second_half_word
event_timestamp
```

The transformation should be explicit and strongly typed.

Values should not be converted through generic maps where a concrete Go structure can be used.

Enum values for:

```text
next_header_type
```

must be validated before insertion.

Supported values are:

```text
icmp
icmpv6
```

## 8. FIE processing

FIE processing is intentionally streaming.

For every DuckDB capture file:

```text
open database
    ↓
query FIE rows
    ↓
scan row
    ↓
lookup PD metadata
    ↓
lookup event metadata
    ↓
construct destination FIE
    ↓
append to ClickHouse batch
    ↓
flush batch periodically
```

The importer must never load an entire DuckDB FIE capture into memory.

### 8.1 DuckDB reads

DuckDB databases are opened read-only where possible.

Only the required columns should be selected.

The query should avoid expensive sorting or transformations inside DuckDB unless required.

The importer performs the final schema transformation in Go.

### 8.2 FIE enrichment

Each captured FIE is enriched with metadata obtained from the PD and event datasets.

The final destination row contains:

```text
sequence_number
agent_id
probing_directive_id
ip_version
protocol
source_address
destination_address
near_probe_ttl
near_reply_address
near_sent_timestamp
near_received_timestamp
far_probe_ttl
far_reply_address
far_sent_timestamp
far_received_timestamp
production_timestamp
```

The source address comes from the event-derived metadata.

Other fields come either directly from the FIE capture or from the corresponding PD where required.

### 8.3 Missing metadata

A captured FIE referencing an unknown probing directive is considered inconsistent input.

The importer should fail rather than silently insert an incomplete row.

Similarly, if a FIE requires source-address metadata and the corresponding event information cannot be found, the importer should fail with enough context to diagnose the problem.

For example:

```text
cannot enrich FIE: probing_directive_id=12345 has no source address
```

Silently using zero values would make the resulting research dataset difficult to trust and should therefore be avoided.

## 9. ClickHouse insertion

Rows are inserted using ClickHouse native batching.

The importer should prepare one batch for each destination table.

Conceptually:

```go
batch, err := conn.PrepareBatch(ctx, "INSERT INTO ...")
```

Rows are appended until the configured or internal batch threshold is reached.

The batch is then sent and a new batch is created.

A reasonable initial batch size is on the order of:

```text
100,000 rows
```

This can later become configurable if required.

### 9.1 PD inserts

PDs are expected to be relatively small and can normally be inserted in one or a small number of batches.

### 9.2 FIE inserts

FIEs are potentially very large and must be inserted incrementally while the DuckDB files are being scanned.

At no point should all FIEs exist in memory simultaneously.

## 10. Error handling

The importer follows fail-fast semantics.

Any of the following should terminate the import with a non-zero exit status:

- malformed PD JSON;
- malformed event JSON;
- unsupported required event data;
- duplicate PD identifiers;
- invalid IP addresses;
- missing PD metadata;
- missing event metadata required for enrichment;
- inability to read a DuckDB capture;
- DuckDB query failure;
- ClickHouse connection failure;
- table creation failure;
- batch insertion failure.

Errors should include enough context to locate the failing input.

For example:

```text
events-20260825T180000Z.jsonl:381: cannot decode event: ...
```

or:

```text
fies-20260826T000000Z.duckdb: probing_directive_id=4821: source address not found
```

## 11. Partial imports

ClickHouse inserts are not assumed to provide a transaction covering the entire experiment.

Therefore, if the importer fails after some batches have already been inserted, the destination tables may contain a partial import.

The importer should report this clearly.

Re-running against the same table should not be used as a recovery mechanism because it could duplicate already inserted rows.

The expected recovery procedure is:

```text
DROP TABLE <name>__pds
DROP TABLE <name>__fies
```

and then re-run the importer.

For this reason, refusing to overwrite existing tables is an important safety property.

## 12. Memory model

The importer divides data into two classes.

### In-memory data

Expected to remain comparatively small:

```text
PD metadata
event-derived metadata
current ClickHouse batch
```

### Streaming data

Potentially very large:

```text
FIEs
```

The memory complexity should therefore be approximately:

```text
O(number of PDs + number of relevant events + batch size)
```

rather than:

```text
O(number of FIEs)
```

This is necessary for multi-day captures.

## 13. Concurrency

The first implementation should remain mostly sequential.

There is little benefit in introducing concurrency before the basic import pipeline is measured.

The initial model should be:

```text
load PDs
load events
insert PDs
for each DuckDB file:
    read FIEs
    enrich
    insert batches
```

This has several advantages:

- deterministic execution;
- simple error handling;
- bounded memory usage;
- no ordering or synchronization complexity;
- easier diagnosis of malformed data.

If throughput later proves insufficient, DuckDB reading and ClickHouse insertion can be pipelined through a bounded channel without changing the external interface.

## 14. Determinism

Given identical:

- PD input;
- event files;
- FIE capture files;
- importer version;

the importer should produce logically identical destination tables.

Input files should therefore be processed in deterministic order.

No transformation should depend on the wall-clock time at which the importer runs.

## 15. Logging and progress

The importer should provide lightweight progress information to stderr.

For example:

```text
loading PDs...
loaded 2,500,000 PDs

loading events...
loaded 10,421 events

creating ClickHouse tables...

importing fies-20260825T180000Z.duckdb...
inserted 12,300,000 FIEs

importing fies-20260826T000000Z.duckdb...
inserted 24,700,000 FIEs

done
PDs:  2,500,000
FIEs: 24,700,000
```

Progress logging should not occur per row.

## 16. Suggested internal structure

Although the first implementation may live in a single Go file, the logical structure should remain clear.

A reasonable set of functions is:

```go
func parseConfig() (Config, error)

func discoverEventFiles(dir string) ([]string, error)
func discoverFIEFiles(dir string) ([]string, error)

func loadPDs(path string) ([]PDRow, map[uint64]PDMetadata, error)
func loadEvents(paths []string) (EventIndex, error)

func connectClickHouse(cfg Config) (driver.Conn, error)
func createTables(ctx context.Context, conn driver.Conn, cfg Config) error

func insertPDs(ctx context.Context, conn driver.Conn, rows []PDRow) error

func importFIEFile(
    ctx context.Context,
    conn driver.Conn,
    path string,
    pds map[uint64]PDMetadata,
    events EventIndex,
) (uint64, error)

func main()
```

The exact types may change as the Retina PD and event schemas are incorporated, but the separation of responsibilities should remain.

## 17. Data flow summary

The complete import flow is:

```text
                   ┌──────────────┐
                   │ CLI / ENV    │
                   └──────┬───────┘
                          │
                          ▼
                   validate config
                          │
             ┌────────────┴────────────┐
             │                         │
             ▼                         ▼
       load pds.jsonl           load events-*.jsonl
             │                         │
             ▼                         ▼
        PD metadata               Event metadata
             │                         │
             └────────────┬────────────┘
                          │
                          ▼
                  create CH tables
                          │
                          ▼
                      insert PDs
                          │
                          ▼
                discover DuckDB files
                          │
                          ▼
                    for each file
                          │
                          ▼
                     stream FIEs
                          │
                          ▼
                        enrich
                          │
                          ▼
                    batch insert
                          │
                          ▼
                         done
```

## 18. Design principle

The importer should remain intentionally boring.

Its job is not to provide a flexible data platform. Its job is to take one well-defined Retina experiment representation, validate it strictly, transform it deterministically, and load it efficiently into ClickHouse.

The implementation should therefore prefer:

```text
explicit schemas
explicit transformations
bounded memory
sequential processing
fail-fast validation
simple recovery
```

over abstraction or generality that is not required by the experiment pipeline.
