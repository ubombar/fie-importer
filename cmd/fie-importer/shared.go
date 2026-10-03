package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/pflag"

	"fie-importer/internal/clickhouse"
	"fie-importer/internal/progress"
)

// addClickHouseFlags adds the flags every upload command shares.
func addClickHouseFlags(f *pflag.FlagSet, c *clickhouse.Config) {
	f.StringVar(&c.Address, "clickhouse-address", envOr("CLICKHOUSE_ADDRESS", "localhost:9000"), "ClickHouse native address (default from CLICKHOUSE_ADDRESS)")
	f.StringVar(&c.Database, "clickhouse-database", envOr("CLICKHOUSE_DATABASE", "pam_campaign"), "ClickHouse database (default from CLICKHOUSE_DATABASE)")
	f.BoolVar(&c.Secure, "clickhouse-secure", false, "connect to ClickHouse with TLS")
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// newTable connects to ClickHouse with the credentials of CH_USER and
// CH_PASSWORD and creates the table, which must not exist.
func newTable(ctx context.Context, disp *progress.Display, c *clickhouse.Config, table string,
	create func(*clickhouse.Client, context.Context, string) error) (*clickhouse.Client, error) {
	c.Username, c.Password = os.Getenv("CH_USER"), os.Getenv("CH_PASSWORD")
	if c.Username == "" {
		return nil, errors.New("CH_USER is not set")
	}
	disp.Phase("connecting to ClickHouse at " + c.Address)
	ch, err := clickhouse.Connect(ctx, c)
	if err != nil {
		return nil, err
	}
	exists, err := ch.TableExists(ctx, table)
	if err == nil && exists {
		err = fmt.Errorf("table %s.%s already exists", c.Database, table)
	}
	if err == nil {
		err = create(ch, ctx, table)
	}
	if err != nil {
		_ = ch.Close()
		return nil, err
	}
	return ch, nil
}

// dropOnFailure drops the table when *err is set and drop is true. The
// context may already be canceled, so it uses its own.
func dropOnFailure(ch *clickhouse.Client, table string, drop bool, err *error) {
	if *err == nil || !drop {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if derr := ch.DropTable(ctx, table); derr != nil {
		*err = errors.Join(*err, derr)
		return
	}
	*err = fmt.Errorf("%w (table %s dropped)", *err, table)
}

// group formats n with thousands separators: 20,486,494.
func group[T int64 | uint64](n T) string {
	s := strconv.FormatUint(uint64(max(n, 0)), 10) //nolint:gosec // negative clamped
	var b strings.Builder
	for i, c := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			b.WriteByte(',')
		}
		b.WriteRune(c)
	}
	return b.String()
}
