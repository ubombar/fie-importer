package streams

import (
	"context"
	"crypto/tls"
	"fie-importer/internal/api"
	"fmt"
	"regexp"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

type ClickHouseCredentials struct {
	Addresses []string
	Database  string
	Username  string
	Password  string
	Secure    bool
}

type ClickhouseStreamerConfig struct {
	Credentials ClickHouseCredentials
	Workers     int
	BufferSize  int
	BatchSize   int
	Stream      FullFIEStream
}

type ClikchouseStreamer struct {
	conn driver.Conn

	stream FullFIEStream

	n         int
	k         int
	batchSize int
}

func NewClickhouseStreamer(cfg ClickhouseStreamerConfig) (*ClikchouseStreamer, error) {
	if cfg.Stream == nil {
		return nil, fmt.Errorf("FullFIE stream cannot be nil")
	}

	if cfg.Workers <= 0 {
		return nil, fmt.Errorf("parallel workers must be at least 1")
	}

	if cfg.BufferSize < 0 {
		return nil, fmt.Errorf("buffer size cannot be negative")
	}

	if cfg.BatchSize <= 0 {
		return nil, fmt.Errorf("batch size must be at least 1")
	}

	if len(cfg.Credentials.Addresses) == 0 {
		return nil, fmt.Errorf("at least one ClickHouse address is required")
	}

	options := &clickhouse.Options{
		Addr: cfg.Credentials.Addresses,
		Auth: clickhouse.Auth{
			Database: cfg.Credentials.Database,
			Username: cfg.Credentials.Username,
			Password: cfg.Credentials.Password,
		},
	}

	if cfg.Credentials.Secure {
		options.TLS = &tls.Config{
			MinVersion: tls.VersionTLS12,
		}
	}

	conn, err := clickhouse.Open(options)
	if err != nil {
		return nil, fmt.Errorf("open ClickHouse connection: %w", err)
	}

	return &ClikchouseStreamer{
		conn:      conn,
		stream:    cfg.Stream,
		n:         cfg.Workers,
		k:         cfg.BufferSize,
		batchSize: cfg.BatchSize,
	}, nil
}

// Close closes the ClickHouse connection pool.
func (s *ClikchouseStreamer) Close() error {
	if s == nil || s.conn == nil {
		return nil
	}

	return s.conn.Close()
}

// Ping verifies that ClickHouse is reachable.
func (s *ClikchouseStreamer) Ping(ctx context.Context) error {
	if err := s.conn.Ping(ctx); err != nil {
		return fmt.Errorf("ping ClickHouse: %w", err)
	}

	return nil
}

func (s *ClikchouseStreamer) Create(ctx context.Context, name string) error {
	if err := validateClickHouseIdentifier(name); err != nil {
		return err
	}

	ddl := `CREATE TABLE %s` // TODO: not implemented.

	if err := s.conn.Exec(ctx, fmt.Sprintf(ddl, fmt.Sprintf("`%v`", name))); err != nil {
		return fmt.Errorf("create ClickHouse table %q: %w", name, err)
	}

	return nil
}

func (s *ClikchouseStreamer) Drop(ctx context.Context, name string) error {
	if err := validateClickHouseIdentifier(name); err != nil {
		return err
	}

	ddl := `DROP TABLE %s`

	if err := s.conn.Exec(ctx, fmt.Sprintf(ddl, fmt.Sprintf("`%v`", name))); err != nil {
		return fmt.Errorf("drop ClickHouse table %q: %w", name, err)
	}

	return nil
}

func (s *ClikchouseStreamer) Ingest(ctx context.Context, name string) error {
	if err := validateClickHouseIdentifier(name); err != nil {
		return err
	}

	workers := make([]chWorkerHandle, s.n)
	for i := range workers {
		workers[i] = chWorkerHandle{
			conn:      s.conn,
			tableName: name,
			batchSize: s.batchSize,
			appendFIE: s.appendFullFIE,
		}
	}

	if err := ParallelForEach2(ctx, s.n, s.k, s.stream.FIEs(), func(winfo WorkerInfo, fie *api.FullFIE, err error) error {
		if err != nil {
			return err
		}
		return workers[winfo.Index()].Append(winfo.Context(), fie)
	}); err != nil {
		return fmt.Errorf("parallel ClickHouse ingestion: %w", err)
	}

	for i := range workers {
		if err := workers[i].Flush(); err != nil {
			return err
		}
	}

	return nil
}

func validateClickHouseIdentifier(name string) error {
	valid := regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
	if !valid.MatchString(name) {
		return fmt.Errorf("invalid ClickHouse identifier %q", name)
	}
	return nil
}
