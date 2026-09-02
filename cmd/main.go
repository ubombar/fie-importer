package main

import (
	"context"
	"errors"
	"fie-importer/importer"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

const defaultBatchSize = 100_000

type config struct {
	eventsDir          string
	fiesDir            string
	clickhouseAddress  string
	clickhouseDatabase string
	name               string
	batchSize          int
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func run() error { //nolint
	cfg := parseConfig()
	ctx := context.Background()

	conn, err := openClickHouse(ctx, cfg)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

	extractor, err := importer.NewCaptureExtractor(importer.CaptureExtractorConfig{
		EventsDir: cfg.eventsDir,
		FIEsDir:   cfg.fiesDir,
	})
	if err != nil {
		return fmt.Errorf("create capture extractor: %w", err)
	}
	defer func() { _ = extractor.Close() }()

	if err := extractor.Load(); err != nil {
		return fmt.Errorf("load capture: %w", err)
	}

	pdsTable := importer.NewPDsTable(conn, cfg.name+"__pds", cfg.batchSize)
	agentTermsTable := importer.NewAgentTermsTable(conn, cfg.name+"__agent_terms", cfg.batchSize)
	statusTable := importer.NewCurrentStatusTable(conn, cfg.name+"__current_status", cfg.batchSize)
	fiesTable := importer.NewFIELiteTable(conn, cfg.name+"__fies_lite", cfg.batchSize)

	if err := pdsTable.Create(ctx); err != nil {
		return err
	}
	if err := agentTermsTable.Create(ctx); err != nil {
		return err
	}
	if err := statusTable.Create(ctx); err != nil {
		return err
	}
	if err := fiesTable.Create(ctx); err != nil {
		return err
	}

	pds, err := extractor.PDs()
	if err != nil {
		return fmt.Errorf("get PDs: %w", err)
	}

	for _, pd := range pds {
		if err := pdsTable.Insert(ctx, &importer.ExtendedPD{ProbingDirective: *pd}); err != nil {
			return fmt.Errorf("insert PD %d: %w", pd.ProbingDirectiveID, err)
		}
	}

	if err := pdsTable.Flush(); err != nil {
		return fmt.Errorf("flush PDs: %w", err)
	}

	terms, err := extractor.AgentTerms()
	if err != nil {
		return fmt.Errorf("get agent terms: %w", err)
	}

	for _, term := range terms {
		if err := agentTermsTable.Insert(ctx, term); err != nil {
			return fmt.Errorf("insert agent term %q: %w", term.AgentID, err)
		}
	}

	if err := agentTermsTable.Flush(); err != nil {
		return fmt.Errorf("flush agent terms: %w", err)
	}

	statuses, err := extractor.StatusEvents()
	if err != nil {
		return fmt.Errorf("get status events: %w", err)
	}

	for _, status := range statuses {
		if err := statusTable.Insert(ctx, status); err != nil {
			return fmt.Errorf("insert status event: %w", err)
		}
	}

	if err := statusTable.Flush(); err != nil {
		return fmt.Errorf("flush status events: %w", err)
	}

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	var fieCount uint64

	for {
		fie, err := extractor.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("read FIE: %w", err)
		}

		if err := fiesTable.Insert(ctx, fie); err != nil {
			return fmt.Errorf("insert FIE %d: %w", fie.SequenceNumber, err)
		}

		fieCount++
		select {
		case <-ticker.C:
			fmt.Printf("Inserted total of %d FIEs\n", fieCount)
		default:
		}
	}

	if err := fiesTable.Flush(); err != nil {
		return fmt.Errorf("flush FIEs: %w", err)
	}

	return nil
}

func parseConfig() *config {
	cfg := &config{}

	flag.StringVar(&cfg.eventsDir, "events-dir", "", "directory containing event files")
	flag.StringVar(&cfg.fiesDir, "fies-dir", "", "directory containing FIE capture files")
	flag.StringVar(&cfg.clickhouseAddress, "clickhouse-address", "localhost:9000", "ClickHouse address")
	flag.StringVar(&cfg.clickhouseDatabase, "clickhouse-database", "default", "ClickHouse database")
	flag.StringVar(&cfg.name, "name", "", "import name used as table prefix")
	flag.IntVar(&cfg.batchSize, "batch-size", defaultBatchSize, "ClickHouse insertion batch size")
	flag.Parse()

	return cfg
}

func openClickHouse(ctx context.Context, cfg *config) (driver.Conn, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{cfg.clickhouseAddress},
		Auth: clickhouse.Auth{
			Database: cfg.clickhouseDatabase,
			Username: os.Getenv("CH_USER"),
			Password: os.Getenv("CH_PASSWORD"),
		},
	})
	if err != nil {
		return nil, fmt.Errorf("open ClickHouse: %w", err)
	}

	if err := conn.Ping(ctx); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("ping ClickHouse: %w", err)
	}

	return conn, nil
}
