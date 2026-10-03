// Package clickhouse creates and fills the ClickHouse FIE table.
package clickhouse

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"regexp"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"fie-importer/internal/fies"
)

// FIETableDDL creates the FIE table. It is general across capture formats:
// every time is absolute, at microsecond precision, and source_format tells a
// field the format lacks from a missing value.
const FIETableDDL = `CREATE TABLE %s (
    source_format        LowCardinality(String),
    pd_id                UInt32,
    capture_time         DateTime64(6, 'UTC'),
    fie_transit_s        Nullable(UInt8),
    near_reply_addr      Nullable(IPv6),
    far_reply_addr       Nullable(IPv6),
    near_probe_sent_time Nullable(DateTime64(6, 'UTC')),
    near_reply_recv_time Nullable(DateTime64(6, 'UTC')),
    far_probe_sent_time  Nullable(DateTime64(6, 'UTC')),
    far_reply_recv_time  Nullable(DateTime64(6, 'UTC'))
) ENGINE = MergeTree
ORDER BY (pd_id, capture_time)`

// Config says how to reach ClickHouse.
type Config struct {
	Address  string
	Database string
	Username string
	Password string
	Secure   bool
}

// Client is a connection to ClickHouse.
type Client struct {
	conn     driver.Conn
	database string
}

var identifier = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// ValidateTableName rejects names that would need quoting.
func ValidateTableName(name string) error {
	if !identifier.MatchString(name) {
		return fmt.Errorf("invalid table name %q: use letters, digits and underscores, not starting with a digit", name)
	}
	return nil
}

// Connect opens a connection and checks that the server answers.
func Connect(ctx context.Context, config *Config) (*Client, error) {
	options := &clickhouse.Options{
		Addr: []string{config.Address},
		Auth: clickhouse.Auth{
			Database: config.Database,
			Username: config.Username,
			Password: config.Password,
		},
		Compression: &clickhouse.Compression{Method: clickhouse.CompressionLZ4},
	}
	if config.Secure {
		options.TLS = &tls.Config{MinVersion: tls.VersionTLS12}
	}
	conn, err := clickhouse.Open(options)
	if err != nil {
		return nil, fmt.Errorf("cannot connect to ClickHouse at %s: %w", config.Address, err)
	}
	if err := conn.Ping(ctx); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("cannot reach ClickHouse at %s: %w", config.Address, err)
	}
	return &Client{conn: conn, database: config.Database}, nil
}

// TableExists reports whether the table exists in the database.
func (c *Client) TableExists(ctx context.Context, table string) (bool, error) {
	var n uint64
	err := c.conn.QueryRow(ctx, "SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = ?", table).Scan(&n)
	if err != nil {
		return false, fmt.Errorf("cannot look up table %s: %w", table, err)
	}
	return n > 0, nil
}

// CreateFIETable creates the FIE table. It fails if the table exists.
func (c *Client) CreateFIETable(ctx context.Context, table string) error {
	if err := ValidateTableName(table); err != nil {
		return err
	}
	if err := c.conn.Exec(ctx, fmt.Sprintf(FIETableDDL, quote(table))); err != nil {
		return fmt.Errorf("cannot create table %s: %w", table, err)
	}
	return nil
}

// DropTable drops the table.
func (c *Client) DropTable(ctx context.Context, table string) error {
	if err := ValidateTableName(table); err != nil {
		return err
	}
	if err := c.conn.Exec(ctx, "DROP TABLE "+quote(table)); err != nil {
		return fmt.Errorf("cannot drop table %s: %w", table, err)
	}
	return nil
}

// Insert sends rows to the table as one batch.
func (c *Client) Insert(ctx context.Context, table string, rows []fies.Row) error {
	batch, err := c.conn.PrepareBatch(ctx, "INSERT INTO "+quote(table))
	if err != nil {
		return fmt.Errorf("cannot prepare insert: %w", err)
	}
	for i := range rows {
		r := &rows[i]
		if err := batch.Append(r.SourceFormat, r.PDID, r.CaptureTime, r.FIETransitS,
			r.NearReplyAddr, r.FarReplyAddr,
			r.NearProbeSentTime, r.NearReplyRecvTime, r.FarProbeSentTime, r.FarReplyRecvTime); err != nil {
			_ = batch.Abort()
			return fmt.Errorf("cannot append row: %w", err)
		}
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("cannot send batch of %d rows: %w", len(rows), err)
	}
	return nil
}

// Summary describes the rows of an uploaded table.
type Summary struct {
	Rows      uint64
	PDs       uint64
	FirstTime string
	LastTime  string
}

// Summarize counts the table's rows and PDs and its capture time range.
func (c *Client) Summarize(ctx context.Context, table string) (Summary, error) {
	var s Summary
	q := "SELECT count(), uniqExact(pd_id), toString(min(capture_time)), toString(max(capture_time)) FROM " + quote(table)
	if err := c.conn.QueryRow(ctx, q).Scan(&s.Rows, &s.PDs, &s.FirstTime, &s.LastTime); err != nil {
		return s, fmt.Errorf("cannot summarize table %s: %w", table, err)
	}
	return s, nil
}

// Close closes the connection.
func (c *Client) Close() error {
	if c == nil || c.conn == nil {
		return errors.New("not connected")
	}
	return c.conn.Close()
}

func quote(table string) string { return "`" + table + "`" }
