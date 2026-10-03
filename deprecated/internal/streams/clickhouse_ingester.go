package streams

import (
	"context"
	"crypto/tls"
	"fmt"
	"iter"
	"regexp"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

type ClickHouseIngester[T any] interface {
	Create(ctx context.Context, name string) error
	Drop(ctx context.Context, name string) error
	Ingest(ctx context.Context, name string, stream iter.Seq2[T, error]) error
	Close() error
}

type ClickHouseAppender func(values ...any) error

type ClickHouseRowAppender[T any] func(append ClickHouseAppender, value T) error

type ClickHouseCredentials struct {
	Addresses []string
	Database  string
	Username  string
	Password  string
	Secure    bool
}

type ClickHouseIngesterConfig[T any] struct {
	Credentials ClickHouseCredentials
	Workers     int
	BufferSize  int
	BatchSize   int
	DDL         string
	Append      ClickHouseRowAppender[T]
}

type clickHouseIngester[T any] struct {
	conn driver.Conn

	n         int
	k         int
	batchSize int
	ddl       string
	append    ClickHouseRowAppender[T]
}

func NewClickHouseIngester[T any](cfg ClickHouseIngesterConfig[T]) (ClickHouseIngester[T], error) {
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
	if cfg.DDL == "" {
		return nil, fmt.Errorf("DDL cannot be empty")
	}
	if cfg.Append == nil {
		return nil, fmt.Errorf("append function cannot be nil")
	}

	options := &clickhouse.Options{
		Addr: cfg.Credentials.Addresses,
		Auth: clickhouse.Auth{
			Database: cfg.Credentials.Database,
			Username: cfg.Credentials.Username,
			Password: cfg.Credentials.Password,
		},
		MaxOpenConns: cfg.Workers + 2,
		MaxIdleConns: cfg.Workers,
	}

	if cfg.Credentials.Secure {
		options.TLS = &tls.Config{MinVersion: tls.VersionTLS12}
	}

	conn, err := clickhouse.Open(options)
	if err != nil {
		return nil, fmt.Errorf("open ClickHouse connection: %w", err)
	}

	return &clickHouseIngester[T]{
		conn:      conn,
		n:         cfg.Workers,
		k:         cfg.BufferSize,
		batchSize: cfg.BatchSize,
		ddl:       cfg.DDL,
		append:    cfg.Append,
	}, nil
}

func (s *clickHouseIngester[T]) Create(ctx context.Context, name string) error {
	if err := validateClickHouseIdentifier(name); err != nil {
		return err
	}

	if err := s.conn.Exec(ctx, fmt.Sprintf(s.ddl, fmt.Sprintf("`%s`", name))); err != nil {
		return fmt.Errorf("create ClickHouse table %q: %w", name, err)
	}
	return nil
}

func (s *clickHouseIngester[T]) Drop(ctx context.Context, name string) error {
	if err := validateClickHouseIdentifier(name); err != nil {
		return err
	}

	if err := s.conn.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS `%s`", name)); err != nil {
		return fmt.Errorf("drop ClickHouse table %q: %w", name, err)
	}
	return nil
}

func (s *clickHouseIngester[T]) Ingest(ctx context.Context, name string, stream iter.Seq2[T, error]) error {
	if err := validateClickHouseIdentifier(name); err != nil {
		return err
	}

	workers := make([]*clickhouseBatchWriter, s.n)
	for i := range workers {
		workers[i] = newClickHouseBatchWriter(s.conn, name, s.batchSize)
	}

	parallelForEachFunc := func(winfo WorkerInfo, value T, streamErr error) error {
		if streamErr != nil {
			return fmt.Errorf("stream error: %w", streamErr)
		}

		worker := workers[winfo.Index()]
		rowAppenderFunc := func(values ...any) error {
			return worker.Append(ctx, values...) // use the parent context.
		}
		if err := s.append(rowAppenderFunc, value); err != nil {
			return fmt.Errorf("append error: %w", err)
		}

		return nil
	}

	if err := ParallelForEach2(ctx, s.n, s.k, stream, parallelForEachFunc); err != nil {
		return fmt.Errorf("parallel ClickHouse ingestion: %w", err)
	}

	for i, worker := range workers {
		if err := worker.Flush(); err != nil {
			return fmt.Errorf("flush worker %d: %w", i, err)
		}
	}
	return nil
}

func (s *clickHouseIngester[T]) Close() error {
	return s.conn.Close()
}

func validateClickHouseIdentifier(name string) error {
	valid := regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
	if !valid.MatchString(name) {
		return fmt.Errorf("invalid ClickHouse identifier %q", name)
	}
	return nil
}
