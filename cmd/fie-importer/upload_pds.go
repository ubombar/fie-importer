package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"time"

	"github.com/spf13/cobra"
	"golang.org/x/sync/errgroup"

	"fie-importer/internal/clickhouse"
	"fie-importer/internal/pds"
	"fie-importer/internal/progress"
)

// approxPDRowBytes is the approximate size of one PD row in ClickHouse's
// native format before compression, used for the transfer rate shown.
const approxPDRowBytes = 30

type uploadPDsOptions struct {
	table      string
	pdsFile    string
	firstID    uint32
	batchSize  int
	dropOnFail bool
	dryRun     bool
	clickhouse clickhouse.Config
}

func newUploadPDsCommand() *cobra.Command {
	var o uploadPDsOptions
	cmd := &cobra.Command{
		Use:   "upload-pds <table>",
		Short: "Upload a PD file into a new ClickHouse PD table",
		Long: `Upload a PD file into a new ClickHouse PD table.

The file is JSON Lines of retina-commons ProbingDirectives, plain or gzip,
or - for stdin. Each non-blank line gets the next PD ID, starting at
--first-id, as the orchestrator assigns them; the file's own
probing_directive_id is ignored. A PD the orchestrator would refuse stops the
upload. The table is created and must not exist; if the upload fails it is
dropped unless --drop-on-fail=false.

ClickHouse credentials are read from CH_USER and CH_PASSWORD.`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			o.table = args[0]
			return logged(cmd, &o.clickhouse, o.table, o.dryRun, func() error { return runUploadPDs(cmd.Context(), &o) })
		},
	}
	f := cmd.Flags()
	f.StringVar(&o.pdsFile, "pds-file", "", "JSONL PD file, plain or gzip; - reads stdin (required)")
	f.Uint32Var(&o.firstID, "first-id", 0, "PD ID of the first line")
	f.IntVar(&o.batchSize, "batch-size", 1_000_000, "rows per insert")
	f.BoolVar(&o.dropOnFail, "drop-on-fail", true, "drop the table if the upload fails")
	f.BoolVar(&o.dryRun, "dry-run", false, "parse and check the whole file without touching ClickHouse")
	addClickHouseFlags(f, &o.clickhouse)
	_ = cmd.MarkFlagRequired("pds-file")
	return cmd
}

// openPDs opens the PD file and returns its reader, its size (0 if unknown)
// and a function closing both.
func openPDs(path string, firstID uint32) (*pds.Reader, int64, func(), error) {
	var in *os.File
	if path == "-" {
		in = os.Stdin
	} else {
		f, err := os.Open(path) //nolint:gosec // the path comes from the operator
		if err != nil {
			return nil, 0, nil, err
		}
		in = f
	}
	var size int64
	if info, err := in.Stat(); err == nil && info.Mode().IsRegular() {
		size = info.Size()
	}
	r, err := pds.NewReader(in, firstID)
	if err != nil {
		_ = in.Close()
		return nil, 0, nil, err
	}
	return r, size, func() { _ = r.Close(); _ = in.Close() }, nil
}

func runUploadPDs(ctx context.Context, o *uploadPDsOptions) error {
	if err := clickhouse.ValidateTableName(o.table); err != nil {
		return err
	}
	if o.batchSize < 1 {
		return fmt.Errorf("--batch-size must be at least 1: got %d", o.batchSize)
	}
	reader, size, closeFn, err := openPDs(o.pdsFile, o.firstID)
	if err != nil {
		return err
	}
	defer closeFn()

	disp := progress.New(os.Stderr)
	target := o.clickhouse.Database + "." + o.table
	if o.dryRun {
		target += " (dry run)"
	}
	name := o.pdsFile
	if name != "-" {
		name = filepath.Base(name)
	}
	disp.Header("upload-pds → "+target, fmt.Sprintf("%s · IDs from %d", name, o.firstID))
	disp.Run()
	defer disp.Stop()

	if o.dryRun {
		return dryRunPDs(disp, reader, size)
	}
	return uploadPDs(ctx, disp, reader, size, o)
}

// pdStats counts PDs by agent, IP version and protocol.
type pdStats struct {
	rows, ipv4, ipv6 int64
	agents           map[string]int64
	protocols        map[uint8]int64
	firstID, lastID  uint32
}

func (s *pdStats) add(r *pds.Row) {
	if s.rows == 0 {
		s.firstID = r.PDID
		s.agents, s.protocols = map[string]int64{}, map[uint8]int64{}
	}
	s.rows++
	s.lastID = r.PDID
	s.agents[r.AgentID]++
	s.protocols[r.Protocol]++
	if r.IPVersion == 4 {
		s.ipv4++
	} else {
		s.ipv6++
	}
}

func dryRunPDs(disp *progress.Display, reader *pds.Reader, size int64) error {
	disp.StartStream(size, reader.BytesRead)
	var stats pdStats
	for {
		row, err := reader.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			disp.Stop()
			fmt.Fprintln(os.Stderr, disp.Failure("invalid PD file"))
			return err
		}
		stats.add(&row)
		disp.Sent(1, 0)
	}
	disp.Stop()
	if stats.rows == 0 {
		return errors.New("the PD file has no PDs")
	}
	fmt.Fprintln(os.Stderr, disp.Success(fmt.Sprintf("%s valid PDs would be uploaded, IDs %d to %d", group(stats.rows), stats.firstID, stats.lastID)))
	fmt.Fprintf(os.Stderr, "  %s\n", disp.Dim(fmt.Sprintf("IPv4 %s · IPv6 %s · ICMP %s · UDP %s · ICMPv6 %s · %d agents",
		group(stats.ipv4), group(stats.ipv6), group(stats.protocols[pds.ICMP]), group(stats.protocols[pds.UDP]),
		group(stats.protocols[pds.ICMPv6]), len(stats.agents))))
	for _, agent := range slices.Sorted(maps.Keys(stats.agents)) {
		fmt.Fprintf(os.Stderr, "  %s\n", disp.Dim(fmt.Sprintf("  %-40s %12s", agent, group(stats.agents[agent]))))
	}
	return nil
}

func uploadPDs(ctx context.Context, disp *progress.Display, reader *pds.Reader, size int64, o *uploadPDsOptions) (err error) {
	ch, err := newTable(ctx, disp, &o.clickhouse, o.table, (*clickhouse.Client).CreatePDTable)
	if err != nil {
		return err
	}
	defer func() { _ = ch.Close() }()
	defer dropOnFailure(ch, o.table, o.dropOnFail, &err)

	disp.StartStream(size, reader.BytesRead)
	started := time.Now()
	sent, err := transferPDs(ctx, disp, reader, ch, o)
	disp.Stop()
	if err == nil && sent == 0 {
		err = errors.New("the PD file has no PDs")
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, disp.Failure("upload failed"))
		return err
	}

	s, err := ch.SummarizePDs(ctx, o.table)
	if err != nil {
		return err
	}
	if int64(s.Rows) != sent { //nolint:gosec // row counts fit in int64
		return fmt.Errorf("ClickHouse has %d rows, %d were sent", s.Rows, sent)
	}
	took := time.Since(started)
	fmt.Fprintln(os.Stderr, disp.Success(fmt.Sprintf("uploaded %s PDs into %s.%s in %s",
		group(sent), o.clickhouse.Database, o.table, took.Round(100*time.Millisecond))))
	fmt.Fprintf(os.Stderr, "  %s\n", disp.Dim(fmt.Sprintf("IDs %d to %d · %d agents · IPv4 %s · IPv6 %s · ICMP %s · UDP %s · ICMPv6 %s · verified in ClickHouse",
		s.FirstID, s.LastID, s.Agents, group(s.IPv4), group(s.IPv6), group(s.ICMP), group(s.UDP), group(s.ICMPv6))))
	return nil
}

// transferPDs reads the file and inserts its PDs, reading the next batch
// while the previous one is being sent. It returns the number of rows sent.
func transferPDs(ctx context.Context, disp *progress.Display, reader *pds.Reader, ch *clickhouse.Client, o *uploadPDsOptions) (int64, error) {
	group, ctx := errgroup.WithContext(ctx)
	batches := make(chan []pds.Row, 2)
	var sent int64
	group.Go(func() error {
		defer close(batches)
		batch := make([]pds.Row, 0, o.batchSize)
		flush := func() error {
			select {
			case batches <- batch:
			case <-ctx.Done():
				return ctx.Err()
			}
			batch = make([]pds.Row, 0, o.batchSize)
			return nil
		}
		for {
			row, err := reader.Next()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				return err
			}
			if batch = append(batch, row); len(batch) == o.batchSize {
				if err := flush(); err != nil {
					return err
				}
			}
		}
		if len(batch) > 0 {
			return flush()
		}
		return nil
	})
	group.Go(func() error {
		for batch := range batches {
			if err := ch.InsertPDs(ctx, o.table, batch); err != nil {
				return err
			}
			sent += int64(len(batch))
			disp.Sent(int64(len(batch)), int64(len(batch))*approxPDRowBytes)
		}
		return nil
	})
	err := group.Wait()
	return sent, err
}
