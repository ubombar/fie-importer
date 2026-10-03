package streams

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// clickhouseBatchWriter batches rows for an existing ClickHouse table.
// It borrows conn from the caller and does not manage or close the connection.
type clickhouseBatchWriter struct {
	conn      driver.Conn
	tableName string
	batchSize int
	batch     driver.Batch
	rows      int
	total     int
}

func newClickHouseBatchWriter(conn driver.Conn, tableName string, batchSize int) *clickhouseBatchWriter {
	return &clickhouseBatchWriter{
		conn:      conn,
		tableName: tableName,
		batchSize: batchSize,
	}
}

func (w *clickhouseBatchWriter) Append(ctx context.Context, values ...any) error {
	if w.batch == nil {
		batch, err := w.conn.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO %s", w.tableName))
		if err != nil {
			return err
		}
		w.batch = batch
	}

	if err := w.batch.Append(values...); err != nil {
		return err
	}

	w.rows++
	w.total++

	if w.rows >= w.batchSize {
		return w.Flush()
	}

	return nil
}

func (w *clickhouseBatchWriter) Flush() error {
	if w.batch == nil || w.rows == 0 {
		return nil
	}

	if err := w.batch.Send(); err != nil {
		return err
	}

	w.batch = nil
	w.rows = 0
	return nil
}

func (w *clickhouseBatchWriter) Total() int {
	return w.total
}

func (w *clickhouseBatchWriter) Pending() int {
	return w.rows
}

func (w *clickhouseBatchWriter) BatchSize() int {
	return w.batchSize
}
