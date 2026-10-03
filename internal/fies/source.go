package fies

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/duckdb/duckdb-go/v2"
)

// Format2a is the source_format of rows read from fies2a files.
const Format2a = "2a"

// File is one fies2a capture file.
type File struct {
	Path          string
	IntervalStart time.Time
	Interval      time.Duration
	// Rows is the number of rows passing the filter, set by Plan.
	Rows int64
}

// Filter selects rows. Zero values select everything.
type Filter struct {
	// PDIDs keeps only these PD IDs. Exclusive with Percent.
	PDIDs []uint32
	// Percent keeps a deterministic sample of the PDs: those whose
	// hash(pd_id, Seed) falls in the first Percent% of 1,000,000 buckets.
	// 0 means no sampling.
	Percent float64
	Seed    uint64
	// Start and End keep capture times in [Start, End). Zero means unbounded.
	Start, End time.Time
}

// Validate reports whether the filter is usable.
func (f *Filter) Validate() error {
	if len(f.PDIDs) > 0 && f.Percent != 0 {
		return errors.New("a PD ID list and a PD percentage cannot be combined")
	}
	if f.Percent < 0 || f.Percent > 100 {
		return fmt.Errorf("PD percentage must be in (0, 100]: got %v", f.Percent)
	}
	if !f.Start.IsZero() && !f.End.IsZero() && !f.Start.Before(f.End) {
		return fmt.Errorf("start %v must be before end %v", f.Start, f.End)
	}
	return nil
}

// Source reads fies2a files with an in-memory DuckDB.
type Source struct {
	db     *sql.DB
	filter Filter
	files  []File
}

// Open lists the fies2a files of dir, checks their metadata, drops those
// outside the filter's time window, and prepares the filter.
func Open(ctx context.Context, dir string, filter *Filter) (*Source, error) {
	if err := filter.Validate(); err != nil {
		return nil, err
	}
	paths, err := filepath.Glob(filepath.Join(dir, "fies2a-*.parquet"))
	if err != nil {
		return nil, err
	}
	if len(paths) == 0 {
		return nil, fmt.Errorf("no fies2a-*.parquet files in %s", dir)
	}
	connector, err := duckdb.NewConnector("", nil)
	if err != nil {
		return nil, fmt.Errorf("cannot start DuckDB: %w", err)
	}
	s := &Source{db: sql.OpenDB(connector), filter: *filter}
	// The PD set and its temporary table live on one connection.
	s.db.SetMaxOpenConns(1)
	if err := s.init(ctx, connector, paths); err != nil {
		_ = s.Close()
		return nil, err
	}
	return s, nil
}

func (s *Source) init(ctx context.Context, connector *duckdb.Connector, paths []string) error {
	for _, path := range paths {
		f, err := s.readMetadata(ctx, path)
		if err != nil {
			return err
		}
		if s.overlaps(&f) {
			s.files = append(s.files, f)
		}
	}
	slices.SortFunc(s.files, func(a, b File) int { return a.IntervalStart.Compare(b.IntervalStart) })
	if len(s.filter.PDIDs) == 0 {
		return nil
	}
	if _, err := s.db.ExecContext(ctx, "CREATE TEMP TABLE pd_filter (pd_id UINTEGER PRIMARY KEY)"); err != nil {
		return fmt.Errorf("cannot create PD filter table: %w", err)
	}
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	return conn.Raw(func(raw any) error {
		appender, err := duckdb.NewAppenderFromConn(raw.(driver.Conn), "", "pd_filter")
		if err != nil {
			return fmt.Errorf("cannot fill PD filter table: %w", err)
		}
		ids := slices.Compact(slices.Sorted(slices.Values(s.filter.PDIDs)))
		for _, id := range ids {
			if err := appender.AppendRow(id); err != nil {
				_ = appender.Close()
				return err
			}
		}
		return appender.Close()
	})
}

func (s *Source) readMetadata(ctx context.Context, path string) (File, error) {
	rows, err := s.db.QueryContext(ctx, "SELECT decode(key), decode(value) FROM parquet_kv_metadata(?)", path)
	if err != nil {
		return File{}, fmt.Errorf("cannot read metadata of %s: %w", path, err)
	}
	defer func() { _ = rows.Close() }()
	meta := map[string]string{}
	for rows.Next() {
		var k, v string
		if err := rows.Scan(&k, &v); err != nil {
			return File{}, err
		}
		meta[k] = v
	}
	if err := rows.Err(); err != nil {
		return File{}, err
	}
	if got := meta["retina.fies.format"]; got != Format2a {
		return File{}, fmt.Errorf("%s: format %q, want %q", path, got, Format2a)
	}
	start, err := time.Parse(time.RFC3339, meta["retina.fies.interval_start"])
	if err != nil {
		return File{}, fmt.Errorf("%s: invalid interval start: %w", path, err)
	}
	seconds, err := strconv.ParseInt(meta["retina.fies.interval_seconds"], 10, 64)
	if err != nil || seconds <= 0 {
		return File{}, fmt.Errorf("%s: invalid interval length %q", path, meta["retina.fies.interval_seconds"])
	}
	return File{Path: path, IntervalStart: start.UTC(), Interval: time.Duration(seconds) * time.Second}, nil
}

func (s *Source) overlaps(f *File) bool {
	end := f.IntervalStart.Add(f.Interval)
	if !s.filter.Start.IsZero() && !end.After(s.filter.Start) {
		return false
	}
	return s.filter.End.IsZero() || f.IntervalStart.Before(s.filter.End)
}

// where returns the WHERE clause selecting the filter's rows in file f.
func (s *Source) where(f *File) string {
	conds := []string{"true"}
	if len(s.filter.PDIDs) > 0 {
		conds = append(conds, "pd_id IN (SELECT pd_id FROM pd_filter)")
	}
	if s.filter.Percent > 0 && s.filter.Percent < 100 {
		const buckets = 1_000_000
		threshold := uint64(s.filter.Percent / 100 * buckets)
		conds = append(conds, fmt.Sprintf("hash(pd_id, %d) %% %d < %d", s.filter.Seed, buckets, threshold))
	}
	// capture time = interval start + capture_second, whole seconds.
	if !s.filter.Start.IsZero() {
		if from := ceilSeconds(s.filter.Start.Sub(f.IntervalStart)); from > 0 {
			conds = append(conds, fmt.Sprintf("capture_second >= %d", from))
		}
	}
	if !s.filter.End.IsZero() {
		if to := ceilSeconds(s.filter.End.Sub(f.IntervalStart)); to < int64(f.Interval/time.Second) {
			conds = append(conds, fmt.Sprintf("capture_second < %d", to))
		}
	}
	return strings.Join(conds, " AND ")
}

func ceilSeconds(d time.Duration) int64 {
	return int64(math.Ceil(d.Seconds()))
}

// Files returns the files in the time window, oldest first.
func (s *Source) Files() []File { return s.files }

// Plan counts the rows of each file that pass the filter.
func (s *Source) Plan(ctx context.Context) error {
	for i := range s.files {
		f := &s.files[i]
		q := fmt.Sprintf("SELECT count(*) FROM read_parquet(?) WHERE %s", s.where(f)) //nolint:gosec // built from typed values
		if err := s.db.QueryRowContext(ctx, q, f.Path).Scan(&f.Rows); err != nil {
			return fmt.Errorf("cannot count rows of %s: %w", f.Path, err)
		}
	}
	return nil
}

// DistinctPDs counts the distinct PD IDs passing the filter, over all files.
func (s *Source) DistinctPDs(ctx context.Context) (int64, error) {
	if len(s.files) == 0 {
		return 0, nil
	}
	parts := make([]string, len(s.files))
	args := make([]any, len(s.files))
	for i := range s.files {
		parts[i] = fmt.Sprintf("SELECT pd_id FROM read_parquet(?) WHERE %s", s.where(&s.files[i]))
		args[i] = s.files[i].Path
	}
	var n int64
	q := "SELECT count(DISTINCT pd_id) FROM (" + strings.Join(parts, " UNION ALL ") + ")" //nolint:gosec // built from typed values
	if err := s.db.QueryRowContext(ctx, q, args...).Scan(&n); err != nil {
		return 0, fmt.Errorf("cannot count PDs: %w", err)
	}
	return n, nil
}

// Read is one file's share of an upload: rows [Offset, Offset+Limit) of the
// rows of File passing the filter, in file order.
type Read struct {
	File   *File
	Offset int64
	Limit  int64
}

// Window applies a global offset and limit (limit < 0 means none) to the
// planned files: whole files are skipped while the offset covers them, and
// only the file where the offset lands is read from the middle.
func Window(files []File, offset, limit int64) []Read {
	var reads []Read
	for i := range files {
		if limit == 0 {
			break
		}
		f := &files[i]
		if offset >= f.Rows {
			offset -= f.Rows
			continue
		}
		n := f.Rows - offset
		if limit > 0 {
			n = min(n, limit)
			limit -= n
		}
		reads = append(reads, Read{File: f, Offset: offset, Limit: n})
		offset = 0
	}
	return reads
}

// Rows calls fn for every row of r, in file order, until fn returns an error.
func (s *Source) Rows(ctx context.Context, r Read, fn func(*Raw2a) error) error {
	// DuckDB keeps file order without ORDER BY (preserve_insertion_order is on).
	//nolint:gosec // built from typed values
	q := fmt.Sprintf(`SELECT pd_id, capture_second, near_reply_addr, far_reply_addr,
		near_reply_age_s, far_reply_age_s, fie_transit_s
		FROM read_parquet(?) WHERE %s LIMIT %d OFFSET %d`, s.where(r.File), r.Limit, r.Offset)
	rows, err := s.db.QueryContext(ctx, q, r.File.Path)
	if err != nil {
		return fmt.Errorf("cannot read %s: %w", r.File.Path, err)
	}
	defer func() { _ = rows.Close() }()
	var raw Raw2a
	var near, far []byte
	var nearAge, farAge, transit sql.Null[uint8]
	for rows.Next() {
		if err := rows.Scan(&raw.PDID, &raw.CaptureSecond, &near, &far, &nearAge, &farAge, &transit); err != nil {
			return fmt.Errorf("cannot read %s: %w", r.File.Path, err)
		}
		raw.NearReplyAddr, raw.FarReplyAddr = near, far
		raw.NearReplyAgeS, raw.FarReplyAgeS, raw.FIETransitS = ptr(nearAge), ptr(farAge), ptr(transit)
		if err := fn(&raw); err != nil {
			return err
		}
	}
	return rows.Err()
}

func ptr(n sql.Null[uint8]) *uint8 {
	if !n.Valid {
		return nil
	}
	v := n.V
	return &v
}

// Close releases DuckDB.
func (s *Source) Close() error {
	return s.db.Close()
}
