// Package clickhouse creates and fills the ClickHouse FIE, PD, agent and FDH
// tables.
package clickhouse

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"regexp"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"fie-importer/internal/agents"
	"fie-importer/internal/fies"
	"fie-importer/internal/pds"
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

// PDTableDDL creates the PD table. pd_id joins it with the FIE table.
// IPv4 destinations are IPv4-mapped. The half-words are what the agent probes
// with: UDP source and destination ports, or the ICMP/ICMPv6 first half-word
// and zero.
const PDTableDDL = `CREATE TABLE %s (
    pd_id            UInt32,
    agent_id         LowCardinality(String),
    ip_version       UInt8,
    protocol         UInt8,
    destination_addr IPv6,
    near_ttl         UInt8,
    first_half_word  UInt16,
    second_half_word UInt16
) ENGINE = MergeTree
ORDER BY pd_id`

// AgentTableDDL creates the agent table: one row per agent VM and snapshot.
// agent_id joins it with the PD table. Prefixes are CIDR strings.
const AgentTableDDL = `CREATE TABLE %s (
    snapshot_time        DateTime64(6, 'UTC'),
    agent_id             LowCardinality(String),
    region               LowCardinality(String),
    zone                 LowCardinality(String),
    network              LowCardinality(String),
    subnetwork           LowCardinality(String),
    machine_type         LowCardinality(String),
    vm_status            LowCardinality(String),
    vm_created_time      DateTime64(6, 'UTC'),
    vm_last_start_time   Nullable(DateTime64(6, 'UTC')),
    internal_ipv4        Nullable(IPv4),
    internal_ipv4_prefix Nullable(String),
    external_ipv4        Nullable(IPv4),
    internal_ipv6        Nullable(IPv6),
    external_ipv6        Nullable(IPv6),
    external_ipv6_prefix Nullable(String),
    subnet_ipv6_prefix   Nullable(String),
    agent_image                  Nullable(String),
    agent_image_digest           Nullable(String),
    agent_container_state        Nullable(String),
    agent_container_status       Nullable(String),
    agent_container_started_time Nullable(DateTime64(6, 'UTC')),
    agent_service_state          Nullable(String),
    agent_service_restarts       Nullable(UInt32),
    version_error                Nullable(String)
) ENGINE = MergeTree
ORDER BY (agent_id, snapshot_time)`

// Config says how to reach ClickHouse.
type Config struct {
	Address  string
	Database string
	Username string
	Password string
	Secure   bool
	// ReadTimeout is how long to wait for a reply from the server; zero
	// keeps the driver's default of 5 minutes.
	ReadTimeout time.Duration
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
	if config.ReadTimeout > 0 {
		options.ReadTimeout = config.ReadTimeout
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
	return c.createTable(ctx, FIETableDDL, table)
}

// CreatePDTable creates the PD table. It fails if the table exists.
func (c *Client) CreatePDTable(ctx context.Context, table string) error {
	return c.createTable(ctx, PDTableDDL, table)
}

// CreateAgentTable creates the agent table. It fails if the table exists.
func (c *Client) CreateAgentTable(ctx context.Context, table string) error {
	return c.createTable(ctx, AgentTableDDL, table)
}

func (c *Client) createTable(ctx context.Context, ddl, table string) error {
	if err := ValidateTableName(table); err != nil {
		return err
	}
	if err := c.conn.Exec(ctx, fmt.Sprintf(ddl, quote(table))); err != nil {
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

// Insert sends FIE rows to the table as one batch.
func (c *Client) Insert(ctx context.Context, table string, rows []fies.Row) error {
	return insert(ctx, c, table, rows, func(r *fies.Row) []any {
		return []any{r.SourceFormat, r.PDID, r.CaptureTime, r.FIETransitS,
			r.NearReplyAddr, r.FarReplyAddr,
			r.NearProbeSentTime, r.NearReplyRecvTime, r.FarProbeSentTime, r.FarReplyRecvTime}
	})
}

// InsertPDs sends PD rows to the table as one batch.
func (c *Client) InsertPDs(ctx context.Context, table string, rows []pds.Row) error {
	return insert(ctx, c, table, rows, func(r *pds.Row) []any {
		return []any{r.PDID, r.AgentID, r.IPVersion, r.Protocol, r.Destination, r.NearTTL, r.FirstHalfWord, r.SecondHalfWord}
	})
}

// InsertAgents sends agent rows to the table as one batch.
func (c *Client) InsertAgents(ctx context.Context, table string, rows []agents.Agent) error {
	return insert(ctx, c, table, rows, func(a *agents.Agent) []any {
		return []any{a.SnapshotTime, a.AgentID, a.Region, a.Zone, a.Network, a.Subnetwork, a.MachineType, a.VMStatus,
			a.VMCreatedTime, a.VMLastStartTime, ipv4(a.InternalIPv4), a.InternalIPv4Prefix, ipv4(a.ExternalIPv4),
			a.InternalIPv6, a.ExternalIPv6, a.ExternalIPv6Prefix, a.SubnetIPv6Prefix,
			a.AgentImage, a.AgentImageDigest, a.AgentContainerState, a.AgentContainerStatus, a.AgentContainerStartedTime,
			a.AgentServiceState, a.AgentServiceRestarts, a.VersionError}
	})
}

// CountSnapshot counts the table's rows of one snapshot.
func (c *Client) CountSnapshot(ctx context.Context, table string, snapshot time.Time) (uint64, error) {
	var n uint64
	// A time.Time parameter is bound at second precision: pass the
	// microseconds explicitly.
	q := "SELECT count() FROM " + quote(table) + " WHERE snapshot_time = toDateTime64(?, 6, 'UTC')"
	if err := c.conn.QueryRow(ctx, q, snapshot.UTC().Format("2006-01-02 15:04:05.000000")).Scan(&n); err != nil {
		return 0, fmt.Errorf("cannot count rows of table %s: %w", table, err)
	}
	return n, nil
}

// ipv4 returns the 4-byte form ClickHouse's IPv4 type takes, nil for NULL.
func ipv4(ip *net.IP) *net.IP {
	if ip == nil {
		return nil
	}
	v4 := ip.To4()
	if v4 == nil {
		return nil
	}
	return &v4
}

func insert[T any](ctx context.Context, c *Client, table string, rows []T, values func(*T) []any) error {
	batch, err := c.conn.PrepareBatch(ctx, "INSERT INTO "+quote(table))
	if err != nil {
		return fmt.Errorf("cannot prepare insert: %w", err)
	}
	for i := range rows {
		if err := batch.Append(values(&rows[i])...); err != nil {
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

// PDSummary describes the rows of an uploaded PD table.
type PDSummary struct {
	Rows, Agents, IPv4, IPv6, ICMP, UDP, ICMPv6 uint64
	FirstID, LastID                             uint32
}

// SummarizePDs counts the PD table's rows by agent, IP version and protocol.
func (c *Client) SummarizePDs(ctx context.Context, table string) (PDSummary, error) {
	var s PDSummary
	q := `SELECT count(), uniqExact(agent_id), countIf(ip_version = 4), countIf(ip_version = 6),
		countIf(protocol = 1), countIf(protocol = 17), countIf(protocol = 58), min(pd_id), max(pd_id) FROM ` + quote(table)
	if err := c.conn.QueryRow(ctx, q).Scan(&s.Rows, &s.Agents, &s.IPv4, &s.IPv6, &s.ICMP, &s.UDP, &s.ICMPv6, &s.FirstID, &s.LastID); err != nil {
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
