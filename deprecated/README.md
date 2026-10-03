# fie-importer

`fie-importer` converts captured Retina FIE data into a compact Parquet representation.

The Parquet output can either be written to a local file or streamed through stdout, allowing the output to be piped directly to another process or a remote machine over SSH.

## Installation

Install the CLI with:

```bash
go install ./cmd/fie-importer
```

Make sure your Go binary directory is in your `PATH`:

```bash
export PATH="$PATH:$(go env GOPATH)/bin"
```

Verify the installation:

```bash
fie-importer --help
```

## Parquet Export

The `parquet` command reads compressed FIE capture files and exports them as Parquet.

```bash
fie-importer parquet --fies-dir <directory> [--output <file>] [--batch-size <rows>]
```

### Options

```text
--fies-dir     Directory containing compressed FIE files (required)
-o, --output   Output Parquet file. If omitted, Parquet is written to stdout.
--batch-size   Number of rows written per batch (default: 100000)
```

Progress information is written to stderr and includes the number of ingested rows, elapsed time, ETA, completion percentage, current output size, and projected final size.

## Write to a Local File

```bash
fie-importer parquet \
    --fies-dir ~/Documents/Repositories/campaign4_snapshots/20260829_134155_s1/fies \
    --output 20260829_134155_s1.parquet
```

The output can also be redirected directly from stdout:

```bash
fie-importer parquet \
    --fies-dir ~/Documents/Repositories/campaign4_snapshots/20260829_134155_s1/fies \
    > 20260829_134155_s1.parquet
```

## Stream Directly to a Remote Machine

Because Parquet is written to stdout when `--output` is omitted, the output can be streamed directly over SSH without creating a local Parquet file:

```bash
fie-importer parquet \
    --fies-dir ~/Documents/Repositories/campaign4_snapshots/20260829_134155_s1/fies \
    | ssh dev.lip6.fr 'cat > /storage/ufuk/campaign4/20260829_134155/20260829_134155_s1.parquet'
```

The data path is therefore:

```text
compressed FIE files
        |
        v
   fie-importer
        |
        | Parquet / stdout
        v
       SSH
        |
        v
remote Parquet file
```

Progress remains visible in the local terminal because status information is written to stderr while only the Parquet data is written to stdout.

## Example Progress

```text
parquet export started with 20192898 FIEs total.
total=20192898 ingested=10200000 since=5s ETA=5s completed=50.51% size=292.4 MiB projected=578.9 MiB
```

## Parquet Schema

The compact Parquet representation contains:

```go
type ParquetLiteFIE struct {
    SequenceNumber     uint64 `parquet:"sequence_number"`
    ProbingDirectiveID uint32 `parquet:"probing_directive_id"`
    NearReplyAddress   []byte `parquet:"near_reply_address,optional"`
    FarReplyAddress    []byte `parquet:"far_reply_address,optional"`
    CaptureTimestamp   uint32 `parquet:"capture_timestamp"`
}
```

IPv4 addresses are stored using 4 bytes and IPv6 addresses using 16 bytes. Missing reply addresses are stored as null values.

`capture_timestamp` is stored as Unix time in seconds.
