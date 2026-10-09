package fies

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// writeNamed writes one 10-minute fies2a file named name: PDs 1..5, each
// captured at seconds 0, 100, ..., 500 of the file.
func writeNamed(t *testing.T, dir, name, start string) {
	t.Helper()
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	//nolint:gosec // test data
	q := fmt.Sprintf(`COPY (
		SELECT p::UINTEGER AS pd_id, s::USMALLINT AS capture_second,
			'\xC0\x00\x02\x01'::BLOB AS near_reply_addr, NULL::BLOB AS far_reply_addr,
			0::UTINYINT AS near_reply_age_s, NULL::UTINYINT AS far_reply_age_s, 1::UTINYINT AS fie_transit_s
		FROM range(1, 6) a(p), range(0, 600, 100) b(s) ORDER BY pd_id, capture_second)
		TO '%s' (FORMAT parquet, KV_METADATA {'retina.fies.format': '2a',
			'retina.fies.interval_start': '%s', 'retina.fies.interval_seconds': '600'})`,
		filepath.Join(dir, name), start)
	if _, err := db.Exec(q); err != nil {
		t.Fatal(err)
	}
}

func mergeOpts(dir string) *MergeOptions {
	return &MergeOptions{Output: filepath.Join(dir, "merged.parquet"), RowGroupSize: 7}
}

func TestMerge(t *testing.T) {
	dir := t.TempDir()
	// The names sort opposite to the intervals: the metadata decides the order.
	writeNamed(t, dir, "fies2a-b.parquet", "2026-10-03T11:00:00Z")
	writeNamed(t, dir, "fies2a-a.parquet", "2026-10-03T11:10:00Z")
	src, _ := plan(t, dir, &Filter{})
	out := t.TempDir()
	opts := mergeOpts(out)
	res, err := src.Merge(context.Background(), opts)
	if err != nil {
		t.Fatal(err)
	}
	if res.Rows != 60 || res.Files != 2 || res.Bytes == 0 {
		t.Fatalf("result = %+v, want 60 rows in 2 files", res)
	}
	if _, err := os.Stat(opts.Output + ".tmp"); err == nil {
		t.Fatal("the temporary file was left behind")
	}
	if err := src.Verify(context.Background(), opts.Output); err != nil {
		t.Fatal(err)
	}

	checkMerged(t, opts.Output)
}

func checkMerged(t *testing.T, path string) {
	t.Helper()
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	start := time.Date(2026, 10, 3, 11, 0, 0, 0, time.UTC).Unix()
	var first, firstBuild, firstNear, last int64
	var far sql.NullInt64
	err = db.QueryRow(`SELECT
		(SELECT capture_time FROM read_parquet(?) LIMIT 1),
		(SELECT fie_build_time FROM read_parquet(?) LIMIT 1),
		(SELECT near_reply_time FROM read_parquet(?) LIMIT 1),
		(SELECT far_reply_time FROM read_parquet(?) LIMIT 1),
		(SELECT capture_time FROM read_parquet(?) OFFSET 59 LIMIT 1)`, path, path, path, path, path).
		Scan(&first, &firstBuild, &firstNear, &far, &last)
	if err != nil {
		t.Fatal(err)
	}
	// Transit 1 s and age 0 s; no far reply.
	if first != start || firstBuild != start-1 || firstNear != start-1 || far.Valid {
		t.Fatalf("first row = %d %d %d %v", first, firstBuild, firstNear, far)
	}
	if last != start+600+500 {
		t.Fatalf("last capture_time = %d, want %d", last, start+600+500)
	}
	var format, rows string
	if err := db.QueryRow(`SELECT
		(SELECT decode(value) FROM parquet_kv_metadata(?) WHERE decode(key) = 'retina.fies.format'),
		(SELECT decode(value) FROM parquet_kv_metadata(?) WHERE decode(key) = 'retina.fies.rows')`, path, path).
		Scan(&format, &rows); err != nil {
		t.Fatal(err)
	}
	if format != Format2m || rows != "60" {
		t.Fatalf("metadata = %q, %q", format, rows)
	}
}

func TestMerge_ExistingOutput(t *testing.T) {
	dir := t.TempDir()
	writeNamed(t, dir, "fies2a-a.parquet", "2026-10-03T11:00:00Z")
	src, _ := plan(t, dir, &Filter{})
	opts := mergeOpts(t.TempDir())
	if err := os.WriteFile(opts.Output, []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := src.Merge(context.Background(), opts); err == nil || !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("err = %v, want already exists", err)
	}
	if data, _ := os.ReadFile(opts.Output); string(data) != "old" {
		t.Fatal("the existing output was touched")
	}
	opts.Overwrite = true
	if _, err := src.Merge(context.Background(), opts); err != nil {
		t.Fatal(err)
	}
}

func TestCheckLayout(t *testing.T) {
	dir := t.TempDir()
	writeNamed(t, dir, "fies2a-a.parquet", "2026-10-03T11:00:00Z")
	writeNamed(t, dir, "fies2a-b.parquet", "2026-10-03T11:30:00Z")
	src, _ := plan(t, dir, &Filter{})
	gaps, err := src.CheckLayout()
	if err != nil || len(gaps) != 1 {
		t.Fatalf("gaps = %v, err = %v, want one gap", gaps, err)
	}

	dir = t.TempDir()
	writeNamed(t, dir, "fies2a-a.parquet", "2026-10-03T11:00:00Z")
	writeNamed(t, dir, "fies2a-b.parquet", "2026-10-03T11:05:00Z")
	src, _ = plan(t, dir, &Filter{})
	if _, err := src.CheckLayout(); err == nil || !strings.Contains(err.Error(), "overlap") {
		t.Fatalf("err = %v, want overlap", err)
	}
	if _, err := src.Merge(context.Background(), mergeOpts(t.TempDir())); err == nil {
		t.Fatal("merging overlapping files must fail")
	}
}

func TestVerify_DetectsDisorder(t *testing.T) {
	dir := t.TempDir()
	writeNamed(t, dir, "fies2a-a.parquet", "2026-10-03T11:00:00Z")
	writeNamed(t, dir, "fies2a-b.parquet", "2026-10-03T11:10:00Z")
	src, _ := plan(t, dir, &Filter{})
	opts := mergeOpts(t.TempDir())
	if _, err := src.Merge(context.Background(), opts); err != nil {
		t.Fatal(err)
	}
	bad := filepath.Join(filepath.Dir(opts.Output), "bad.parquet")
	if _, err := src.db.Exec(fmt.Sprintf(`COPY (SELECT * FROM read_parquet(%s) ORDER BY capture_time DESC)
		TO %s (FORMAT parquet)`, quote(opts.Output), quote(bad))); err != nil { //nolint:gosec // test paths
		t.Fatal(err)
	}
	if err := src.Verify(context.Background(), bad); err == nil {
		t.Fatal("a reversed file passed verification")
	}
}
