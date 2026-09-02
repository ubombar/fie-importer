package importer

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// ========================================================================== //
// baseTable
// ========================================================================== //

type appendFunc[T any] func(f func(a ...any) error, value T) error

type baseTable[T any] struct {
	tableName string
	ddl       string
	batchSize int
	pending   int

	conn   driver.Conn
	batch  driver.Batch
	append appendFunc[T]
}

var _ Table[any] = (*baseTable[any])(nil)

func newBaseTable[T any](conn driver.Conn, tableName, ddl string, batchSize int, appendFn appendFunc[T]) *baseTable[T] {
	return &baseTable[T]{
		conn:      conn,
		tableName: tableName,
		ddl:       ddl,
		batchSize: batchSize,
		append:    appendFn,
	}
}

func (t *baseTable[T]) Create(ctx context.Context) error {
	if err := t.conn.Exec(ctx, fmt.Sprintf(t.ddl, fmt.Sprintf("`%s`", t.tableName))); err != nil {
		return fmt.Errorf("create table %s: %w", t.tableName, err)
	}
	return nil
}

func (t *baseTable[T]) Insert(ctx context.Context, value T) error {
	if t.batch == nil {
		batch, err := t.conn.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO `%s`", t.tableName))
		if err != nil {
			return fmt.Errorf("prepare batch: %w", err)
		}
		t.batch = batch
	}

	if err := t.append(t.batch.Append, value); err != nil {
		return err
	}

	t.pending++

	if t.batchSize > 0 && t.pending >= t.batchSize {
		return t.Flush()
	}

	return nil
}

func (t *baseTable[T]) Drop(ctx context.Context) error {
	if err := t.Flush(); err != nil {
		return err
	}
	if err := t.conn.Exec(ctx, fmt.Sprintf("DROP TABLE `%s`", t.tableName)); err != nil {
		return fmt.Errorf("drop table %s: %w", t.tableName, err)
	}
	return nil
}

func (t *baseTable[T]) Flush() error {
	if t.batch == nil {
		return nil
	}

	if err := t.batch.Send(); err != nil {
		return fmt.Errorf("send batch for %s: %w", t.tableName, err)
	}

	t.batch = nil
	t.pending = 0
	return nil
}

// ========================================================================== //
// Other utility functions
// ========================================================================== //

func timestampAge(captureTime, timestamp time.Time) uint8 {
	if timestamp.IsZero() {
		return 63
	}

	age := int64(captureTime.Sub(timestamp) / time.Second)
	if age < 0 || age > 62 {
		return 63
	}

	return uint8(age)
}

func nullableIPv6(ip net.IP) net.IP {
	if ip == nil || ip.IsUnspecified() {
		return nil
	}

	ip = ip.To16()
	if ip == nil {
		return nil
	}

	return ip
}

func toIPv6(ip net.IP) net.IP {
	if ip == nil {
		return net.IPv6zero
	}

	v6 := ip.To16()
	if v6 == nil {
		return net.IPv6zero
	}

	out := make(net.IP, net.IPv6len)
	copy(out, v6)

	return out
}
