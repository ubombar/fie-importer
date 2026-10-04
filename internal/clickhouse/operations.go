package clickhouse

import (
	"context"
	"fmt"
	"time"
)

// OperationsTable records every fie-importer command run against the
// database, one row per run. Every command creates it if it does not exist.
const OperationsTable = "fie_importer_operations"

// OperationsTableDDL creates the operations table if it does not exist: a
// shared log that every run appends to, so an existing table is expected.
const OperationsTableDDL = `CREATE TABLE IF NOT EXISTS %s (
    time       DateTime64(6, 'UTC'),
    operation  LowCardinality(String),
    table_name String,
    command    String,
    status     LowCardinality(String),
    error      Nullable(String),
    duration_s Float64,
    version    LowCardinality(String),
    os_user    LowCardinality(String),
    host       LowCardinality(String)
) ENGINE = MergeTree
ORDER BY time`

// Operation is one row of the operations table.
type Operation struct {
	Time      time.Time
	Operation string // the subcommand, e.g. compute-fdhs
	Table     string // the table the command created or changed
	Command   string // the command line, shell-quoted
	Status    string // ok or failed
	Error     *string
	Duration  time.Duration
	Version   string
	OSUser    string
	Host      string
}

// LogOperation creates the operations table if needed and appends op.
func (c *Client) LogOperation(ctx context.Context, op *Operation) error {
	if err := c.conn.Exec(ctx, fmt.Sprintf(OperationsTableDDL, quote(OperationsTable))); err != nil {
		return fmt.Errorf("cannot create table %s: %w", OperationsTable, err)
	}
	rows := []Operation{*op}
	return insert(ctx, c, OperationsTable, rows, func(o *Operation) []any {
		return []any{o.Time, o.Operation, o.Table, o.Command, o.Status, o.Error, o.Duration.Seconds(), o.Version, o.OSUser, o.Host}
	})
}
