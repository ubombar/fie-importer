# Deprecated: fie-importer v1

This directory holds fie-importer v1 as it was at `v1.0.1`: the `parquet`, `pds` and `current-status` commands, their streams and the scripts around them, all for the original `fies` capture format. It is kept for reference only and is not maintained.

It is its own Go module (`go.mod` here), so it still builds on its own and stays out of the v2 build:

```bash
cd deprecated && go build ./...
```

v2 (the repository root) imports fies2a captures into ClickHouse with `fie-importer upload-fies`.
