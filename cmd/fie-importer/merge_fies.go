package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/spf13/cobra"

	"fie-importer/internal/fies"
	"fie-importer/internal/progress"
)

type mergeOptions struct {
	fiesDir      string
	output       string
	overwrite    bool
	dryRun       bool
	verify       bool
	memoryLimit  string
	tempDir      string
	threads      int
	rowGroupSize int
}

func newMergeFIEsCommand() *cobra.Command {
	var o mergeOptions
	cmd := &cobra.Command{
		Use:   "merge-fies",
		Short: "Merge fies2a capture files into one Parquet file",
		Long: `Merge the fies2a-*.parquet files of a directory into one Parquet file.

Nothing is sorted. The rows of the files are written one file after the other
in interval order, so each 15-minute (or other) segment keeps its order of
(pd_id, capture_time) and the file as a whole is not sorted by pd_id.

capture_second is relative to its file, so the merged file stores absolute
Unix times in seconds instead: capture_time, fie_build_time (capture_time
minus fie_transit_s), near_reply_time and far_reply_time (fie_build_time minus
the reply's age). A time is NULL when what it derives from is NULL. Ages and
transit of 255 s or more were saturated in the inputs, so such a time is only a
bound.

The command only reads and writes local files; it does not use ClickHouse.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error { return runMerge(cmd.Context(), &o) },
	}
	f := cmd.Flags()
	f.StringVar(&o.fiesDir, "fies-dir", "", "directory containing fies2a-*.parquet files (required)")
	f.StringVar(&o.output, "output", "", "merged Parquet file to write (required)")
	f.BoolVar(&o.overwrite, "overwrite", false, "replace the output file if it exists")
	f.BoolVar(&o.dryRun, "dry-run", false, "check the files and count the rows without writing anything")
	f.BoolVar(&o.verify, "verify", false, "read the output back and check its order and row counts per segment")
	f.StringVar(&o.memoryLimit, "memory-limit", "16GB", "DuckDB memory limit (empty for DuckDB's own default)")
	f.StringVar(&o.tempDir, "temp-dir", "", "DuckDB spill directory (default: DuckDB's)")
	f.IntVar(&o.threads, "threads", 0, "DuckDB threads (0 for all cores)")
	f.IntVar(&o.rowGroupSize, "row-group-size", 1_000_000, "rows per Parquet row group")
	_ = cmd.MarkFlagRequired("fies-dir")
	_ = cmd.MarkFlagRequired("output")
	return cmd
}

func runMerge(ctx context.Context, o *mergeOptions) error {
	if o.threads < 0 {
		return fmt.Errorf("--threads cannot be negative: got %d", o.threads)
	}
	if err := checkOutputPlace(o); err != nil {
		return err
	}
	if !o.dryRun {
		if err := fies.CheckOutput(o.output, o.overwrite); err != nil {
			return err
		}
	}
	disp := progress.New(os.Stderr)
	title := "merge-fies → " + o.output
	if o.dryRun {
		title += " (dry run)"
	}
	disp.Header(title, "all rows, in file order, no sorting · progress is estimated from the file size")
	disp.SetSink("disk")
	disp.Run()
	defer disp.Stop()

	disp.Phase("reading file metadata in " + o.fiesDir)
	src, err := fies.Open(ctx, o.fiesDir, &fies.Filter{})
	if err != nil {
		return err
	}
	defer func() { _ = src.Close() }()
	disp.Phase(fmt.Sprintf("counting rows in %d files", len(src.Files())))
	if err := src.Plan(ctx); err != nil {
		return err
	}
	gaps, err := src.CheckLayout()
	if err != nil {
		return err
	}
	var total int64
	for _, f := range src.Files() {
		total += f.Rows
	}
	if o.dryRun {
		disp.Stop()
		reportDryRun(disp, src, gaps, total)
		return nil
	}
	return merge(ctx, disp, src, o, total, gaps)
}

// checkOutputPlace refuses an output that a later run would take for an input.
func checkOutputPlace(o *mergeOptions) error {
	dir, err := filepath.Abs(o.fiesDir)
	if err != nil {
		return err
	}
	out, err := filepath.Abs(o.output)
	if err != nil {
		return err
	}
	if match, _ := filepath.Match("fies2a-*.parquet", filepath.Base(out)); match && filepath.Dir(out) == dir {
		return errors.New("the output would be taken for an input file: name it differently or put it outside --fies-dir")
	}
	return nil
}

func reportDryRun(disp *progress.Display, src *fies.Source, gaps []string, total int64) {
	files := src.Files()
	first, last := files[0], files[len(files)-1]
	fmt.Fprintln(os.Stderr, disp.Success(fmt.Sprintf("%s rows in %d files would be merged", group(total), len(files))))
	fmt.Fprintf(os.Stderr, "  %s\n", disp.Dim(fmt.Sprintf("%s → %s",
		first.IntervalStart.Format(time.RFC3339), last.IntervalStart.Add(last.Interval).Format(time.RFC3339))))
	for _, g := range gaps {
		fmt.Fprintf(os.Stderr, "  warning: %s\n", g)
	}
}

func merge(ctx context.Context, disp *progress.Display, src *fies.Source, o *mergeOptions, total int64, gaps []string) error {
	opts := &fies.MergeOptions{
		Output:       o.output,
		Overwrite:    o.overwrite,
		MemoryLimit:  o.memoryLimit,
		TempDir:      o.tempDir,
		Threads:      o.threads,
		RowGroupSize: o.rowGroupSize,
	}
	started := time.Now()
	disp.Start(total, len(src.Files()))
	stop := watchSize(disp, o.output+".tmp", src.Files(), total)
	res, err := src.Merge(ctx, opts)
	stop()
	if err != nil {
		disp.Stop()
		fmt.Fprintln(os.Stderr, disp.Failure("merge failed"))
		return err
	}
	took := time.Since(started)
	disp.SetSent(res.Rows, res.Bytes)
	disp.Stop()
	fmt.Fprintln(os.Stderr, disp.Success(fmt.Sprintf("merged %s rows from %d files into %s in %s",
		group(res.Rows), res.Files, o.output, took.Round(100*time.Millisecond))))
	fmt.Fprintf(os.Stderr, "  %s\n", disp.Dim(fmt.Sprintf("%.1f GB · %.2f B/FIE · %s rows/s", float64(res.Bytes)/1e9,
		float64(res.Bytes)/float64(max(res.Rows, 1)), group(int64(float64(res.Rows)/max(took.Seconds(), 1e-3))))))
	for _, g := range gaps {
		fmt.Fprintf(os.Stderr, "  warning: %s\n", g)
	}
	if !o.verify {
		return nil
	}
	fmt.Fprintf(os.Stderr, "  %s\n", disp.Dim("verifying the output (reads all of it)…"))
	verifyStart := time.Now()
	if err := src.Verify(ctx, o.output); err != nil {
		fmt.Fprintln(os.Stderr, disp.Failure("verification failed"))
		return err
	}
	fmt.Fprintln(os.Stderr, disp.Success(fmt.Sprintf("verified in %s", time.Since(verifyStart).Round(100*time.Millisecond))))
	return nil
}

// approxMergedRowBytes is the measured size of one merged row in the Parquet
// file, used to turn the bytes written so far into a rough row count.
const approxMergedRowBytes = 3.5

// watchSize shows roughly how much of the temporary output has been written
// and which input file that corresponds to. DuckDB reports no progress of its
// own through database/sql, so the rows are estimated from the size of the
// file, which lags a little behind the writer.
func watchSize(disp *progress.Display, tmp string, files []fies.File, total int64) (stop func()) {
	done := make(chan struct{})
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		tick := time.NewTicker(250 * time.Millisecond)
		defer tick.Stop()
		current := 0
		for {
			select {
			case <-done:
				return
			case <-tick.C:
				var size int64
				if info, err := os.Stat(tmp); err == nil {
					size = info.Size()
				}
				rows := min(int64(float64(size)/approxMergedRowBytes), total)
				disp.SetSent(rows, size)
				before := int64(0)
				idx := 0
				for idx < len(files)-1 && before+files[idx].Rows <= rows {
					before += files[idx].Rows
					idx++
				}
				if idx+1 != current {
					current = idx + 1
					disp.File(current, filepath.Base(files[idx].Path), files[idx].Rows)
				}
				disp.SetRead(rows - before)
			}
		}
	}()
	return func() {
		close(done)
		<-finished
	}
}
