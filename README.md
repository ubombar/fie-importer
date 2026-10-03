# fie-importer

`fie-importer` imports Retina captures into ClickHouse: the FIEs with `upload-fies` and the PDs they answer with `upload-pds`. Version 2 reads the **fies2a** capture files the orchestrator writes since `retina-orchestrator` `research-v1.6.0`: one zstd Parquet file per hour, `fies2a-<interval start>.parquet`, sorted by PD ID. The format is described in `FIES2.md` in the orchestrator repository.

Version 1 (the `parquet`, `pds` and `current-status` commands, for the original `fies` format) is kept under [`deprecated/`](deprecated/NOTICE.md) for reference only.

## Installation

```bash
go install ./cmd/fie-importer
```

DuckDB is linked in, so cgo is required. `make build` formats, lints and builds; `make test` runs the tests.

## upload-fies

```bash
fie-importer upload-fies <table> --fies-dir <dir> [filters] [options]
```

Creates the table `<table>` and uploads the FIEs of every `fies2a-*.parquet` file in `<dir>` into it. The table must not exist. Staging files (`*.staging.duckdb`) and unfinished files (`*.tmp`) are ignored.

| Flag | Default | Meaning |
| --- | --- | --- |
| `--fies-dir` | (required) | directory containing the `fies2a-*.parquet` files |
| `--pdids` | | only these PD IDs, comma-separated |
| `--pdids-file` | | only the PD IDs in this file, one per line (blank lines and `#` comments ignored); combined with `--pdids` |
| `--pdid-percent` | | only a deterministic sample of this percentage of the PDs, in (0, 100]; exclusive with the PD lists |
| `--seed` | `0` | seed of the sample: the same seed always selects the same PDs, in every file and every table |
| `--start` | | only FIEs captured at or after this time, RFC 3339 (`2026-10-03T11:00:00Z`) |
| `--end` | | only FIEs captured before this time |
| `--offset` | `0` | skip this many filtered rows |
| `--limit` | `-1` | upload at most this many rows; `-1` is all |
| `--batch-size` | `1000000` | rows per insert |
| `--drop-on-fail` | `true` | drop the table if the upload fails or is interrupted; `--drop-on-fail=false` keeps what was inserted |
| `--dry-run` | `false` | count what would be uploaded, without touching ClickHouse |
| `--clickhouse-address` | `$CLICKHOUSE_ADDRESS` or `localhost:9000` | ClickHouse native protocol address |
| `--clickhouse-database` | `$CLICKHOUSE_DATABASE` or `pam_campaign` | database the table is created in |
| `--clickhouse-secure` | `false` | connect with TLS |

The ClickHouse user and password are read from `CH_USER` and `CH_PASSWORD` only, so the password never appears in the process list.

Filters combine with AND. `--offset` and `--limit` count the rows that pass the filters, in a fixed order: files by interval start, rows in file order (`pd_id`, then capture time). The same flags always select the same rows. Whole files are skipped while the offset covers them; only the file where the offset lands is read from the middle. Files outside `--start`/`--end` are not read at all, and a PD ID filter skips Parquet row groups whose PD ID range does not match.

After the upload, the row count in ClickHouse is checked against the rows sent; a mismatch is a failure.

### Examples

A 10% sample of the PDs over one hour:

```bash
CH_USER=… CH_PASSWORD=… fie-importer upload-fies us_east1_10pct \
    --fies-dir captures/20261003_103333/fies \
    --pdid-percent 10 --seed 1 \
    --start 2026-10-03T11:00:00Z --end 2026-10-03T12:00:00Z
```

How much a filter selects, without uploading:

```bash
fie-importer upload-fies x --fies-dir captures/20261003_103333/fies --pdids-file sample.txt --dry-run
```

### Progress

On a terminal, progress is redrawn in place: a bar for the whole upload, the row rate, the approximate transfer rate, the time left, and the file being read with its own bar. When stderr is not a terminal, a plain line is printed every 5 seconds instead. `NO_COLOR` turns colors off.

```text
▶ upload-fies → pam_campaign.tty_demo PD sample 50% (seed 0) · time 2026-10-03 11:05:00Z → …
 ▕██████████████████████████████▌                 ▏  63.6%  6.0M / 9.4M rows
   2.0M rows/s · ≈139.6 MB/s to ClickHouse · elapsed 0:04 · ETA 0:02
   file 5/6 fies2a-20261003T114000Z.parquet  ▕█████▋      ▏  48%
✔ uploaded 9,433,682 rows into pam_campaign.tty_demo in 5.3s
  121,103 distinct PDs · captured 2026-10-03 11:05:00 → 2026-10-03 11:59:59 · 6 files · 1,763,484 rows/s · verified in ClickHouse
```

## upload-pds

```bash
fie-importer upload-pds <table> --pds-file <file|-> [options]
```

Creates the table `<table>` and uploads a PD file into it. The file is JSON Lines of `retina-commons` `ProbingDirective`s, the format the orchestrator's insert API takes, plain or gzip-compressed (detected from the content); `-` reads stdin, so a tarball can be piped: `tar -xzOf pds.jsonl.tar.gz | fie-importer upload-pds t --pds-file -`.

| Flag | Default | Meaning |
| --- | --- | --- |
| `--pds-file` | (required) | JSONL PD file, plain or gzip; `-` for stdin |
| `--first-id` | `0` | PD ID of the first line, for a file inserted into an orchestrator that already had PDs |
| `--batch-size` | `1000000` | rows per insert |
| `--drop-on-fail` | `true` | drop the table if the upload fails or is interrupted |
| `--dry-run` | `false` | parse and check the whole file, print counts per IP version, protocol and agent, without touching ClickHouse |
| `--clickhouse-*` | | as for `upload-fies`; credentials from `CH_USER` and `CH_PASSWORD` |

- **PD IDs follow the file order**: the first non-blank line gets `--first-id`, the next one `--first-id + 1`, and so on, which is how the orchestrator assigns them. Blank lines are skipped and take no ID. The file's own `probing_directive_id` and `ip_version` are ignored, as the orchestrator ignores them.
- **A PD the orchestrator would refuse stops the upload**, naming its line and PD ID, so that the IDs in the table never drift from the orchestrator's. The checks are the orchestrator's: a valid destination address, a non-empty `agent_id`, and protocol 1 (ICMP), 17 (UDP) or 58 (ICMPv6) with its matching next header. TTLs are not checked, since the orchestrator accepts any.
- Progress follows the bytes read from the file (compressed bytes for gzip).

### The PD table

```sql
CREATE TABLE <table> (
    pd_id            UInt32,
    agent_id         LowCardinality(String),
    ip_version       UInt8,
    protocol         UInt8,
    destination_addr IPv6,
    near_ttl         UInt8,
    first_half_word  UInt16,
    second_half_word UInt16
) ENGINE = MergeTree
ORDER BY pd_id
```

| Column | Meaning |
| --- | --- |
| `pd_id` | joins with the FIE table's `pd_id` |
| `ip_version` | 4 or 6, from the destination address |
| `protocol` | 1 ICMP, 17 UDP, 58 ICMPv6 |
| `destination_addr` | IPv4 as IPv4-mapped IPv6, like the FIE table |
| `near_ttl` | TTL of the near probe; the far probe is `near_ttl + 1` |
| `first_half_word`, `second_half_word` | what the agent probes with: for UDP the source and destination ports; for ICMP and ICMPv6 the first half-word and **0**, since the agent always sends these probes with a zero second half-word (`retina-agent` `caracalDstPort`) |

## The FIE table

```sql
CREATE TABLE <table> (
    source_format        LowCardinality(String),
    pd_id                UInt32,
    capture_time         DateTime64(6, 'UTC'),
    fie_transit_s        Nullable(UInt8),
    near_reply_addr      Nullable(IPv6),
    far_reply_addr       Nullable(IPv6),
    near_probe_sent_time Nullable(DateTime64(6, 'UTC')),
    near_reply_recv_time Nullable(DateTime64(6, 'UTC')),
    far_probe_sent_time  Nullable(DateTime64(6, 'UTC')),
    far_reply_recv_time  Nullable(DateTime64(6, 'UTC'))
) ENGINE = MergeTree
ORDER BY (pd_id, capture_time)
```

The table holds FIEs only. Anything a PD implies (agent, destination, TTLs, protocol) is not copied in; join with the PDs for it. The schema is meant to fit later capture formats unchanged: every time is absolute with microsecond precision, and `source_format` tells a field the format does not have apart from a value that is missing.

| Column | Meaning | From fies2a |
| --- | --- | --- |
| `source_format` | capture format of the row | `'2a'` |
| `pd_id` | PD the FIE answers | `pd_id` |
| `capture_time` | when the orchestrator received the FIE | interval start + `capture_second`, whole seconds |
| `fie_transit_s` | seconds from the agent building the FIE to the orchestrator receiving it | `fie_transit_s` |
| `near_reply_addr`, `far_reply_addr` | reply addresses; IPv4 as IPv4-mapped IPv6 | the 4- or 16-byte addresses; NULL when there was no reply |
| `near_probe_sent_time`, `far_probe_sent_time` | when each probe was sent | always NULL: fies2a does not carry send times |
| `near_reply_recv_time`, `far_reply_recv_time` | when each reply was captured | `capture_time − fie_transit_s − *_reply_age_s`, whole seconds; NULL when there was no reply or `fie_transit_s` is NULL |

RTT is not stored: compute it from the sent and received times once a format provides send times. In fies2a, ages and transit of 255 s or more are stored as 255, so the rare received time derived from them is only a bound.
