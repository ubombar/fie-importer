package fies

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"
)

// Format2m is the retina.fies.format of a merged file.
const Format2m = "2m"

// MergeOptions configures Merge.
type MergeOptions struct {
	// Output is the merged file. It is written to Output+".tmp" first.
	Output    string
	Overwrite bool
	// MemoryLimit and TempDir are DuckDB's, for example "16GB". Empty keeps
	// DuckDB's defaults. Threads 0 uses all cores.
	MemoryLimit  string
	TempDir      string
	Threads      int
	RowGroupSize int
}

// MergeResult describes a finished merge.
type MergeResult struct {
	Rows  int64
	Bytes int64
	Files int
}

// CheckOutput fails when path exists and overwrite is false.
func CheckOutput(path string, overwrite bool) error {
	if _, err := os.Stat(path); err == nil && !overwrite {
		return fmt.Errorf("%s already exists (use --overwrite to replace it)", path)
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

// CheckLayout fails when two files overlap in time and returns a warning for
// every gap between consecutive files. Files are in interval order.
func (s *Source) CheckLayout() (gaps []string, err error) {
	for i := 1; i < len(s.files); i++ {
		prev, cur := &s.files[i-1], &s.files[i]
		end := prev.IntervalStart.Add(prev.Interval)
		switch {
		case cur.IntervalStart.Before(end):
			return nil, fmt.Errorf("%s and %s overlap: the first covers until %s",
				prev.Path, cur.Path, end.Format(time.RFC3339))
		case cur.IntervalStart.After(end):
			gaps = append(gaps, fmt.Sprintf("no data between %s and %s", end.Format(time.RFC3339), cur.IntervalStart.Format(time.RFC3339)))
		}
	}
	return gaps, nil
}

// Merge writes the rows of all files, in file order and without sorting, to
// one Parquet file of the merged format: capture_second becomes an absolute
// Unix time and the ages and transit become absolute times too. Plan must
// have been called.
func (s *Source) Merge(ctx context.Context, opts *MergeOptions) (*MergeResult, error) {
	if len(s.files) == 0 {
		return nil, errors.New("no files to merge")
	}
	if opts.RowGroupSize < 1 {
		return nil, fmt.Errorf("row group size must be at least 1: got %d", opts.RowGroupSize)
	}
	if err := CheckOutput(opts.Output, opts.Overwrite); err != nil {
		return nil, err
	}
	if _, err := s.CheckLayout(); err != nil {
		return nil, err
	}
	var total int64
	for i := range s.files {
		total += s.files[i].Rows
	}
	if err := s.applySettings(ctx, opts); err != nil {
		return nil, err
	}
	tmp := opts.Output + ".tmp"
	_ = os.Remove(tmp)
	if _, err := s.db.ExecContext(ctx, s.mergeSQL(tmp, opts, total)); err != nil {
		_ = os.Remove(tmp)
		return nil, fmt.Errorf("cannot write %s: %w", tmp, err)
	}
	var got int64
	if err := s.db.QueryRowContext(ctx, "SELECT num_rows FROM parquet_file_metadata(?)", tmp).Scan(&got); err != nil {
		_ = os.Remove(tmp)
		return nil, fmt.Errorf("cannot read back %s: %w", tmp, err)
	}
	if got != total {
		_ = os.Remove(tmp)
		return nil, fmt.Errorf("merged file has %d rows, the inputs have %d", got, total)
	}
	if err := os.Rename(tmp, opts.Output); err != nil {
		return nil, err
	}
	info, err := os.Stat(opts.Output)
	if err != nil {
		return nil, err
	}
	return &MergeResult{Rows: total, Bytes: info.Size(), Files: len(s.files)}, nil
}

func (s *Source) applySettings(ctx context.Context, opts *MergeOptions) error {
	// Without this DuckDB may reorder rows to write faster.
	stmts := []string{"SET preserve_insertion_order = true"}
	if opts.Threads > 0 {
		stmts = append(stmts, fmt.Sprintf("SET threads = %d", opts.Threads))
	}
	if opts.MemoryLimit != "" {
		stmts = append(stmts, "SET memory_limit = "+quote(opts.MemoryLimit))
	}
	if opts.TempDir != "" {
		stmts = append(stmts, "SET temp_directory = "+quote(opts.TempDir))
	}
	for _, stmt := range stmts {
		if _, err := s.db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("%s: %w", stmt, err)
		}
	}
	return nil
}

// mergeSQL is one COPY over a single read_parquet of all files, which DuckDB
// reads in list order. There is no ORDER BY and no join, which would reorder
// rows: nothing is sorted. The start of each file is looked up from the
// filename, which is constant within a chunk of rows, so the lookup costs next
// to nothing. (One SELECT per file joined by UNION ALL gives the same rows
// about four times slower.)
func (s *Source) mergeSQL(tmp string, opts *MergeOptions, total int64) string {
	paths := make([]string, len(s.files))
	var starts strings.Builder
	for i := range s.files {
		f := &s.files[i]
		paths[i] = quote(f.Path)
		fmt.Fprintf(&starts, " WHEN %s THEN %d", paths[i], f.IntervalStart.Unix())
	}
	first, last := &s.files[0], &s.files[len(s.files)-1]
	meta := fmt.Sprintf("{'retina.fies.format': '%s', 'retina.fies.rows': '%d', 'retina.fies.files': '%d', "+
		"'retina.fies.interval_start': '%s', 'retina.fies.interval_end': '%s'}",
		Format2m, total, len(s.files),
		first.IntervalStart.Format(time.RFC3339), last.IntervalStart.Add(last.Interval).Format(time.RFC3339))
	//nolint:gosec // built from typed values and quoted paths
	return fmt.Sprintf(`COPY (SELECT pd_id, t AS capture_time,
			t - fie_transit_s AS fie_build_time,
			t - fie_transit_s - near_reply_age_s AS near_reply_time,
			t - fie_transit_s - far_reply_age_s AS far_reply_time,
			near_reply_addr, far_reply_addr
		FROM (SELECT *, (CASE filename%s END)::BIGINT + capture_second::BIGINT AS t
			FROM read_parquet([%s], filename = true)))
		TO %s (FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE %d, KV_METADATA %s)`,
		starts.String(), strings.Join(paths, ", "), quote(tmp), opts.RowGroupSize, meta)
}

func quote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

// Verify reads the merged file at path and checks it against the inputs: each
// 15-minute (or other equal-length) segment must hold as many rows as its
// input file, segments must follow each other in time, and rows within a
// segment must be sorted by (pd_id, capture_time). It reads the whole file.
// Plan must have been called.
func (s *Source) Verify(ctx context.Context, path string) error {
	origin, secs := s.files[0].IntervalStart.Unix(), int64(s.files[0].Interval/time.Second)
	want := make(map[int64]int64, len(s.files))
	for i := range s.files {
		f := &s.files[i]
		offset := f.IntervalStart.Unix() - origin
		if int64(f.Interval/time.Second) != secs || offset%secs != 0 {
			return errors.New("cannot verify: the files are not equal-length, aligned intervals")
		}
		want[offset/secs] = f.Rows
	}
	//nolint:gosec // built from typed values
	q := fmt.Sprintf(`SELECT seg, count(*),
			count(*) FILTER (WHERE pseg IS NOT NULL AND seg < pseg),
			count(*) FILTER (WHERE pseg = seg AND (pd_id < ppd OR (pd_id = ppd AND capture_time < pt)))
		FROM (SELECT pd_id, capture_time, (capture_time - %[1]d) // %[2]d AS seg,
				lag(pd_id) OVER () AS ppd, lag(capture_time) OVER () AS pt,
				lag((capture_time - %[1]d) // %[2]d) OVER () AS pseg
			FROM read_parquet(?))
		GROUP BY seg`, origin, secs)
	rows, err := s.db.QueryContext(ctx, q, path)
	if err != nil {
		return fmt.Errorf("cannot verify %s: %w", path, err)
	}
	defer func() { _ = rows.Close() }()
	var problems []string
	for rows.Next() {
		var seg, n, back, unsorted int64
		if err := rows.Scan(&seg, &n, &back, &unsorted); err != nil {
			return err
		}
		if w, ok := want[seg]; !ok || w != n {
			problems = append(problems, fmt.Sprintf("segment %d has %d rows, want %d", seg, n, want[seg]))
		}
		delete(want, seg)
		if back > 0 {
			problems = append(problems, fmt.Sprintf("segment %d starts %d times after a later one", seg, back))
		}
		if unsorted > 0 {
			problems = append(problems, fmt.Sprintf("segment %d has %d rows out of (pd_id, capture_time) order", seg, unsorted))
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	for seg, w := range want {
		if w > 0 {
			problems = append(problems, fmt.Sprintf("segment %d has no rows, want %d", seg, w))
		}
	}
	if len(problems) > 0 {
		return fmt.Errorf("%s failed verification: %s", path, strings.Join(problems[:min(len(problems), 5)], "; "))
	}
	return nil
}
