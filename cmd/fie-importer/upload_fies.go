package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"golang.org/x/sync/errgroup"

	"fie-importer/internal/clickhouse"
	"fie-importer/internal/fies"
	"fie-importer/internal/progress"
)

// approxRowBytes is the approximate size of one row in ClickHouse's native
// format before compression, used for the transfer rate shown.
const approxRowBytes = 85

type uploadOptions struct {
	table      string
	fiesDir    string
	pdids      []string
	pdidsFile  string
	percent    float64
	seed       uint64
	start, end string
	offset     int64
	limit      int64
	batchSize  int
	dropOnFail bool
	dryRun     bool
	clickhouse clickhouse.Config
}

func newUploadFIEsCommand() *cobra.Command {
	var o uploadOptions
	cmd := &cobra.Command{
		Use:   "upload-fies <table>",
		Short: "Upload fies2a capture files into a new ClickHouse FIE table",
		Long: `Upload fies2a capture files into a new ClickHouse FIE table.

The table is created and must not exist. Rows can be filtered by PD ID (a list
or a deterministic sample), by capture time, and cut with --offset and --limit,
which count filtered rows in file order. If the upload fails, the table is
dropped unless --drop-on-fail=false.

ClickHouse credentials are read from CH_USER and CH_PASSWORD.`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			o.table = args[0]
			return logged(cmd, &o.clickhouse, o.table, o.dryRun, func() error { return runUpload(cmd.Context(), &o) })
		},
	}
	f := cmd.Flags()
	f.StringVar(&o.fiesDir, "fies-dir", "", "directory containing fies2a-*.parquet files (required)")
	f.StringSliceVar(&o.pdids, "pdids", nil, "only these PD IDs, comma-separated")
	f.StringVar(&o.pdidsFile, "pdids-file", "", "only the PD IDs in this file, one per line")
	f.Float64Var(&o.percent, "pdid-percent", 0, "only a deterministic sample of this percentage of the PDs, in (0, 100]")
	f.Uint64Var(&o.seed, "seed", 0, "seed of the PD sample")
	f.StringVar(&o.start, "start", "", "only FIEs captured at or after this time (RFC 3339, e.g. 2026-10-03T11:00:00Z)")
	f.StringVar(&o.end, "end", "", "only FIEs captured before this time (RFC 3339)")
	f.Int64Var(&o.offset, "offset", 0, "skip this many filtered rows")
	f.Int64Var(&o.limit, "limit", -1, "upload at most this many rows (-1 for all)")
	f.IntVar(&o.batchSize, "batch-size", 1_000_000, "rows per insert")
	f.BoolVar(&o.dropOnFail, "drop-on-fail", true, "drop the table if the upload fails")
	f.BoolVar(&o.dryRun, "dry-run", false, "count what would be uploaded without touching ClickHouse")
	addClickHouseFlags(f, &o.clickhouse)
	_ = cmd.MarkFlagRequired("fies-dir")
	cmd.MarkFlagsMutuallyExclusive("pdids", "pdid-percent")
	cmd.MarkFlagsMutuallyExclusive("pdids-file", "pdid-percent")
	return cmd
}

// filter builds the row filter from the options.
func (o *uploadOptions) filter() (fies.Filter, error) {
	var f fies.Filter
	ids := o.pdids
	if o.pdidsFile != "" {
		fromFile, err := readLines(o.pdidsFile)
		if err != nil {
			return f, err
		}
		ids = append(ids, fromFile...)
	}
	for _, s := range ids {
		id, err := strconv.ParseUint(strings.TrimSpace(s), 10, 32)
		if err != nil {
			return f, fmt.Errorf("invalid PD ID %q: %w", s, err)
		}
		f.PDIDs = append(f.PDIDs, uint32(id))
	}
	if (len(o.pdids) > 0 || o.pdidsFile != "") && len(f.PDIDs) == 0 {
		return f, errors.New("the PD ID list is empty")
	}
	if o.percent != 0 && (o.percent <= 0 || o.percent > 100) {
		return f, fmt.Errorf("--pdid-percent must be in (0, 100]: got %v", o.percent)
	}
	f.Percent, f.Seed = o.percent, o.seed
	var err error
	if f.Start, err = parseTime("start", o.start); err != nil {
		return f, err
	}
	if f.End, err = parseTime("end", o.end); err != nil {
		return f, err
	}
	return f, f.Validate()
}

func parseTime(name, s string) (time.Time, error) {
	if s == "" {
		return time.Time{}, nil
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return t, fmt.Errorf("invalid --%s %q: want RFC 3339, e.g. 2026-10-03T11:00:00Z", name, s)
	}
	return t.UTC(), nil
}

func readLines(path string) ([]string, error) {
	f, err := os.Open(path) //nolint:gosec // the path comes from the operator
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	var lines []string
	s := bufio.NewScanner(f)
	for s.Scan() {
		if line := strings.TrimSpace(s.Text()); line != "" && !strings.HasPrefix(line, "#") {
			lines = append(lines, line)
		}
	}
	return lines, s.Err()
}

// describe summarizes the filters for the header line.
func (o *uploadOptions) describe(f *fies.Filter) string {
	var parts []string
	switch {
	case len(f.PDIDs) > 0:
		parts = append(parts, fmt.Sprintf("%d PD IDs", len(f.PDIDs)))
	case f.Percent > 0:
		parts = append(parts, fmt.Sprintf("PD sample %g%% (seed %d)", f.Percent, f.Seed))
	}
	if !f.Start.IsZero() || !f.End.IsZero() {
		parts = append(parts, fmt.Sprintf("time %s → %s", timeOrDots(f.Start), timeOrDots(f.End)))
	}
	if o.offset > 0 {
		parts = append(parts, fmt.Sprintf("offset %d", o.offset))
	}
	if o.limit >= 0 {
		parts = append(parts, fmt.Sprintf("limit %d", o.limit))
	}
	if len(parts) == 0 {
		return "no filters"
	}
	return strings.Join(parts, " · ")
}

func timeOrDots(t time.Time) string {
	if t.IsZero() {
		return "…"
	}
	return t.Format("2006-01-02 15:04:05Z")
}

func runUpload(ctx context.Context, o *uploadOptions) error {
	if err := clickhouse.ValidateTableName(o.table); err != nil {
		return err
	}
	if o.offset < 0 {
		return fmt.Errorf("--offset cannot be negative: got %d", o.offset)
	}
	if o.batchSize < 1 {
		return fmt.Errorf("--batch-size must be at least 1: got %d", o.batchSize)
	}
	filter, err := o.filter()
	if err != nil {
		return err
	}
	disp := progress.New(os.Stderr)
	target := o.clickhouse.Database + "." + o.table
	if o.dryRun {
		target += " (dry run)"
	}
	disp.Header("upload-fies → "+target, o.describe(&filter))
	disp.Run()
	defer disp.Stop()

	disp.Phase("reading file metadata in " + o.fiesDir)
	src, err := fies.Open(ctx, o.fiesDir, &filter)
	if err != nil {
		return err
	}
	defer func() { _ = src.Close() }()
	disp.Phase(fmt.Sprintf("counting matching rows in %d files", len(src.Files())))
	if err := src.Plan(ctx); err != nil {
		return err
	}
	reads := fies.Window(src.Files(), o.offset, o.limit)
	var total int64
	for _, r := range reads {
		total += r.Limit
	}

	if o.dryRun {
		return dryRun(ctx, disp, src, reads, total)
	}
	return upload(ctx, disp, src, o, reads, total)
}

func dryRun(ctx context.Context, disp *progress.Display, src *fies.Source, reads []fies.Read, total int64) error {
	disp.Phase("counting distinct PDs")
	pds, err := src.DistinctPDs(ctx)
	if err != nil {
		return err
	}
	disp.Phase("")
	disp.Stop()
	var matching int64
	for _, f := range src.Files() {
		matching += f.Rows
	}
	fmt.Fprintf(os.Stderr, "%s\n", disp.Success(fmt.Sprintf("%s rows would be uploaded from %d files", group(total), len(reads))))
	fmt.Fprintf(os.Stderr, "  %s\n", disp.Dim(fmt.Sprintf("%s rows match the filters in %d files, %s distinct PDs (before offset and limit)",
		group(matching), len(src.Files()), group(pds))))
	return nil
}

func upload(ctx context.Context, disp *progress.Display, src *fies.Source, o *uploadOptions, reads []fies.Read, total int64) (err error) {
	ch, err := newTable(ctx, disp, &o.clickhouse, o.table, (*clickhouse.Client).CreateFIETable)
	if err != nil {
		return err
	}
	defer func() { _ = ch.Close() }()
	defer dropOnFailure(ch, o.table, o.dropOnFail, &err)

	disp.Start(total, len(reads))
	started := time.Now()
	if err := transfer(ctx, disp, src, ch, o, reads); err != nil {
		disp.Stop()
		fmt.Fprintln(os.Stderr, disp.Failure("upload failed"))
		return err
	}
	disp.Stop()

	summary, err := ch.Summarize(ctx, o.table)
	if err != nil {
		return err
	}
	if int64(summary.Rows) != total { //nolint:gosec // row counts fit in int64
		return fmt.Errorf("ClickHouse has %d rows, %d were sent", summary.Rows, total)
	}
	took := time.Since(started)
	fmt.Fprintln(os.Stderr, disp.Success(fmt.Sprintf("uploaded %s rows into %s.%s in %s",
		group(total), o.clickhouse.Database, o.table, took.Round(100*time.Millisecond))))
	fmt.Fprintf(os.Stderr, "  %s\n", disp.Dim(fmt.Sprintf("%s distinct PDs · captured %s → %s · %d files · %s rows/s · verified in ClickHouse",
		group(summary.PDs), summary.FirstTime, summary.LastTime, len(reads),
		group(int64(float64(total)/max(took.Seconds(), 1e-3))))))
	return nil
}

// transfer reads the files and inserts their rows, reading the next batch
// while the previous one is being sent.
func transfer(ctx context.Context, disp *progress.Display, src *fies.Source, ch *clickhouse.Client, o *uploadOptions, reads []fies.Read) error {
	group, ctx := errgroup.WithContext(ctx)
	batches := make(chan []fies.Row, 2)
	group.Go(func() error {
		defer close(batches)
		batch := make([]fies.Row, 0, o.batchSize)
		for i, r := range reads {
			disp.File(i+1, filepath.Base(r.File.Path), r.Limit)
			start := r.File.IntervalStart
			var read int64
			err := src.Rows(ctx, r, func(raw *fies.Raw2a) error {
				batch = append(batch, fies.Convert2a(raw, start))
				if read++; read%8192 == 0 {
					disp.Read(8192)
				}
				if len(batch) < o.batchSize {
					return nil
				}
				select {
				case batches <- batch:
				case <-ctx.Done():
					return ctx.Err()
				}
				batch = make([]fies.Row, 0, o.batchSize)
				return nil
			})
			if err != nil {
				return err
			}
			disp.Read(read % 8192)
		}
		if len(batch) > 0 {
			select {
			case batches <- batch:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	})
	group.Go(func() error {
		for batch := range batches {
			if err := ch.Insert(ctx, o.table, batch); err != nil {
				return err
			}
			disp.Sent(int64(len(batch)), int64(len(batch))*approxRowBytes)
		}
		return nil
	})
	return group.Wait()
}
