package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"fie-importer/internal/clickhouse"
	"fie-importer/internal/progress"
)

type computeFDHsOptions struct {
	table      string
	fiesTable  string
	pdsTable   string
	dropOnFail bool
	dryRun     bool
	clickhouse clickhouse.Config
}

func newComputeFDHsCommand() *cobra.Command {
	var o computeFDHsOptions
	cmd := &cobra.Command{
		Use:   "compute-fdhs <table>",
		Short: "Compute a new ClickHouse FDH table from an FIE and a PD table",
		Long: `Compute a new ClickHouse FDH table from an FIE and a PD table.

Each FIE with a near reply becomes one row, joined with its PD on pd_id. An
FDH is (agent_id, ip_version, near_addr, destination_addr) and its rows are
sorted by capture time. FIEs without a near reply are left out; a missing far
reply is kept as a NULL far_addr. The all-zero address counts as missing.

The query runs on the server: no data passes through fie-importer. The table
is created and must not exist; if the run fails it is dropped unless
--drop-on-fail=false.

ClickHouse credentials are read from CH_USER and CH_PASSWORD.`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			o.table = args[0]
			return runComputeFDHs(cmd.Context(), &o)
		},
	}
	f := cmd.Flags()
	f.StringVar(&o.fiesTable, "fies-table", "", "FIE table, from upload-fies (required)")
	f.StringVar(&o.pdsTable, "pds-table", "", "PD table, from upload-pds (required)")
	f.BoolVar(&o.dropOnFail, "drop-on-fail", true, "drop the table if the run fails")
	f.BoolVar(&o.dryRun, "dry-run", false, "print the statements without touching ClickHouse")
	addClickHouseFlags(f, &o.clickhouse)
	_ = cmd.MarkFlagRequired("fies-table")
	_ = cmd.MarkFlagRequired("pds-table")
	return cmd
}

func checkFDHTables(o *computeFDHsOptions) error {
	for _, name := range []string{o.table, o.fiesTable, o.pdsTable} {
		if err := clickhouse.ValidateTableName(name); err != nil {
			return err
		}
	}
	if o.table == o.fiesTable || o.table == o.pdsTable {
		return fmt.Errorf("the FDH table %s must differ from the input tables", o.table)
	}
	return nil
}

func runComputeFDHs(ctx context.Context, o *computeFDHsOptions) error {
	if err := checkFDHTables(o); err != nil {
		return err
	}
	if o.dryRun {
		fmt.Printf(clickhouse.FDHTableDDL+";\n\n%s;\n", "`"+o.table+"`",
			clickhouse.FDHInsertSQL(o.table, o.fiesTable, o.pdsTable))
		return nil
	}

	disp := progress.New(os.Stderr)
	disp.Header("compute-fdhs → "+o.clickhouse.Database+"."+o.table, o.fiesTable+" ⋈ "+o.pdsTable)
	disp.Run()
	defer disp.Stop()
	return computeFDHs(ctx, disp, o)
}

// prepareFDHs returns the create function for newTable: it checks that the
// input tables exist and counts the FIEs to expect into src before creating
// the table, so that nothing is created if either fails.
func prepareFDHs(disp *progress.Display, o *computeFDHsOptions, src *clickhouse.FDHSource) func(*clickhouse.Client, context.Context, string) error {
	return func(ch *clickhouse.Client, ctx context.Context, table string) error {
		if err := checkFDHInput(ctx, ch, o.fiesTable, "upload-fies", clickhouse.FDHFIEColumns); err != nil {
			return err
		}
		if err := checkFDHInput(ctx, ch, o.pdsTable, "upload-pds", clickhouse.FDHPDColumns); err != nil {
			return err
		}
		disp.Phase("counting the FIEs with a near reply in " + o.fiesTable)
		s, err := ch.CountFDHSource(ctx, o.fiesTable, o.pdsTable)
		if err != nil {
			return err
		}
		if s.NearFIEs == s.MissingPD {
			return fmt.Errorf("no FIE of %s has both a near reply and a PD in %s", o.fiesTable, o.pdsTable)
		}
		*src = s
		disp.Phase("creating table " + table)
		return ch.CreateFDHTable(ctx, table)
	}
}

// checkFDHInput checks that an input table exists and has the columns
// compute-fdhs reads, as the command that creates it would make them.
func checkFDHInput(ctx context.Context, ch *clickhouse.Client, table, command string, columns []string) error {
	exists, err := ch.TableExists(ctx, table)
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("table %s does not exist", table)
	}
	missing, err := ch.MissingColumns(ctx, table, columns)
	if err != nil {
		return err
	}
	if len(missing) > 0 {
		return fmt.Errorf("table %s lacks the columns %s: is it a table made by %s v2?", table, strings.Join(missing, ", "), command)
	}
	return nil
}

func computeFDHs(ctx context.Context, disp *progress.Display, o *computeFDHsOptions) (err error) {
	var src clickhouse.FDHSource
	ch, err := newTable(ctx, disp, &o.clickhouse, o.table, prepareFDHs(disp, o, &src))
	if err != nil {
		disp.Stop()
		return err
	}
	defer func() { _ = ch.Close() }()
	defer dropOnFailure(ch, o.table, o.dropOnFail, &err)

	started := time.Now()
	err = insertFDHs(ctx, disp, ch, o)
	disp.Stop()
	if err != nil {
		fmt.Fprintln(os.Stderr, disp.Failure("computing the FDHs failed"))
		return err
	}

	s, err := ch.SummarizeFDHs(ctx, o.table)
	if err != nil {
		return err
	}
	if want := src.NearFIEs - src.MissingPD; s.Rows != want {
		return fmt.Errorf("ClickHouse has %d rows, %d FIEs with a near reply and a PD were expected", s.Rows, want)
	}
	fmt.Fprintln(os.Stderr, disp.Success(fmt.Sprintf("computed %s FDH rows into %s.%s in %s",
		group(s.Rows), o.clickhouse.Database, o.table, time.Since(started).Round(100*time.Millisecond))))
	fmt.Fprintf(os.Stderr, "  %s\n", disp.Dim(fmt.Sprintf("≈%s FDHs · %d agents · %s rows without a far address · verified in ClickHouse",
		group(s.FDHs), s.Agents, group(s.FarNull))))
	if src.MissingPD > 0 {
		fmt.Fprintf(os.Stderr, "  %s\n", disp.Dim(fmt.Sprintf("%s FIEs with a near reply left out: their PD is not in %s",
			group(src.MissingPD), o.pdsTable)))
	}
	return nil
}

// insertFDHs runs the insert on the server and shows its progress. If it
// fails, the query is killed so that the table can be dropped.
func insertFDHs(ctx context.Context, disp *progress.Display, ch *clickhouse.Client, o *computeFDHsOptions) error {
	queryID, err := newQueryID()
	if err != nil {
		return err
	}
	disp.StartQuery()
	done := make(chan struct{})
	go pollQuery(ctx, disp, ch, queryID, done)
	err = ch.InsertFDHs(ctx, queryID, o.table, o.fiesTable, o.pdsTable)
	close(done)
	if err == nil {
		return nil
	}
	kctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if kerr := ch.KillQuery(kctx, queryID); kerr != nil {
		err = errors.Join(err, kerr)
	}
	return err
}

// pollQuery feeds the query's progress to the display until done is closed.
func pollQuery(ctx context.Context, disp *progress.Display, ch *clickhouse.Client, queryID string, done <-chan struct{}) {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-done:
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
			read, total, ok, err := ch.QueryProgress(ctx, queryID)
			if err == nil && ok {
				disp.Rows(int64(read), int64(total)) //nolint:gosec // row counts fit in int64
			}
		}
	}
}

func newQueryID() (string, error) {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return "fie-importer-compute-fdhs-" + hex.EncodeToString(b), nil
}
