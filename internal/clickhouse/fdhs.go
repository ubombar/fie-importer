package clickhouse

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
)

// FDHTableDDL creates the FDH table: one row per FIE with a near reply,
// joined with its PD. An FDH is (agent_id, ip_version, near_addr,
// destination_addr); its rows are sorted by capture time, with pd_id breaking
// ties. far_addr is NULL when the FIE has no far reply.
const FDHTableDDL = `CREATE TABLE %s (
    agent_id         LowCardinality(String),
    ip_version       UInt8,
    near_addr        IPv6,
    destination_addr IPv6,
    capture_time     DateTime64(6, 'UTC'),
    pd_id            UInt32,
    near_ttl         UInt8,
    far_addr         Nullable(IPv6)
) ENGINE = MergeTree
ORDER BY (agent_id, ip_version, near_addr, destination_addr, capture_time, pd_id)
PRIMARY KEY (agent_id, ip_version, near_addr, destination_addr, capture_time)`

// zeroAddrs are the all-zero addresses, IPv6 and IPv4 mapped, which mark a
// missing reply address.
const zeroAddrs = "(toIPv6('::'), toIPv6('::ffff:0.0.0.0'))"

// nearPresent selects the FIEs of FIE table f with a usable near address.
const nearPresent = "f.near_reply_addr IS NOT NULL AND f.near_reply_addr NOT IN " + zeroAddrs

// FDHFIEColumns and FDHPDColumns are the columns compute-fdhs reads from the
// FIE and PD tables, as upload-fies and upload-pds create them.
var (
	FDHFIEColumns = []string{"pd_id", "capture_time", "near_reply_addr", "far_reply_addr"}
	FDHPDColumns  = []string{"pd_id", "agent_id", "ip_version", "destination_addr", "near_ttl"}
)

// MissingColumns returns the columns of want that the table lacks.
func (c *Client) MissingColumns(ctx context.Context, table string, want []string) ([]string, error) {
	rows, err := c.conn.Query(ctx, "SELECT name FROM system.columns WHERE database = currentDatabase() AND table = ?", table)
	if err != nil {
		return nil, fmt.Errorf("cannot list the columns of table %s: %w", table, err)
	}
	defer func() { _ = rows.Close() }()
	have := map[string]bool{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		have[name] = true
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	var missing []string
	for _, name := range want {
		if !have[name] {
			missing = append(missing, name)
		}
	}
	return missing, nil
}

// CreateFDHTable creates the FDH table. It fails if the table exists.
func (c *Client) CreateFDHTable(ctx context.Context, table string) error {
	return c.createTable(ctx, FDHTableDDL, table)
}

// FDHInsertSQL returns the statement that fills the FDH table from the FIE
// and PD tables. FIEs whose PD is missing are left out by the join.
func FDHInsertSQL(fdhTable, fieTable, pdTable string) string {
	return fmt.Sprintf(`INSERT INTO %s (agent_id, ip_version, near_addr, destination_addr, capture_time, pd_id, near_ttl, far_addr)
SELECT
    p.agent_id,
    p.ip_version,
    assumeNotNull(f.near_reply_addr),
    p.destination_addr,
    f.capture_time,
    f.pd_id,
    p.near_ttl,
    if(f.far_reply_addr IN %s, NULL, f.far_reply_addr)
FROM %s AS f
INNER JOIN %s AS p ON f.pd_id = p.pd_id
WHERE %s`, quote(fdhTable), zeroAddrs, quote(fieTable), quote(pdTable), nearPresent)
}

// FDHSource counts the FIEs that compute-fdhs reads.
type FDHSource struct {
	// NearFIEs is the number of FIEs with a near reply.
	NearFIEs uint64
	// MissingPD is the number of those whose PD is not in the PD table.
	MissingPD uint64
}

// CountFDHSource counts the FIEs with a near reply and those without a PD.
// It uses ANY JOIN rather than NOT IN (SELECT …), which the analyzer of
// newer servers rejects as a correlated subquery; ANY keeps a duplicated PD
// from counting an FIE twice.
func (c *Client) CountFDHSource(ctx context.Context, fieTable, pdTable string) (FDHSource, error) {
	var s FDHSource
	//nolint:gosec // G201: table names are checked by ValidateTableName
	q := fmt.Sprintf(`SELECT count(), countIf(ifNull(p.found, 0) = 0) FROM %s AS f
LEFT ANY JOIN (SELECT pd_id, toUInt8(1) AS found FROM %s) AS p ON f.pd_id = p.pd_id
WHERE %s`, quote(fieTable), quote(pdTable), nearPresent)
	if err := c.conn.QueryRow(noTimeout(ctx), q).Scan(&s.NearFIEs, &s.MissingPD); err != nil {
		return s, fmt.Errorf("cannot count the FIEs of table %s: %w", fieTable, err)
	}
	return s, nil
}

// InsertFDHs fills the FDH table on the server under queryID, so that
// QueryProgress and KillQuery can find it.
func (c *Client) InsertFDHs(ctx context.Context, queryID, fdhTable, fieTable, pdTable string) error {
	ctx = clickhouse.Context(noTimeout(ctx), clickhouse.WithQueryID(queryID))
	if err := c.conn.Exec(ctx, FDHInsertSQL(fdhTable, fieTable, pdTable)); err != nil {
		return fmt.Errorf("cannot fill table %s: %w", fdhTable, err)
	}
	return nil
}

// QueryProgress returns the rows a running query has read and the server's
// estimate of the rows it will read. ok is false when the query is not
// running.
func (c *Client) QueryProgress(ctx context.Context, queryID string) (read, total uint64, ok bool, err error) {
	rows, err := c.conn.Query(ctx, "SELECT read_rows, total_rows_approx FROM system.processes WHERE query_id = ?", queryID)
	if err != nil {
		return 0, 0, false, err
	}
	defer func() { _ = rows.Close() }()
	if !rows.Next() {
		return 0, 0, false, rows.Err()
	}
	if err := rows.Scan(&read, &total); err != nil {
		return 0, 0, false, err
	}
	return read, total, true, nil
}

// KillQuery stops a running query and waits for it to end.
func (c *Client) KillQuery(ctx context.Context, queryID string) error {
	if err := c.conn.Exec(ctx, "KILL QUERY WHERE query_id = ? SYNC", queryID); err != nil {
		return fmt.Errorf("cannot kill query %s: %w", queryID, err)
	}
	return nil
}

// FDHSummary describes the rows of an FDH table.
type FDHSummary struct {
	Rows, FarNull, FDHs, Agents uint64
}

// SummarizeFDHs counts the FDH table's rows, the rows without a far address,
// the FDHs (approximately) and the agents.
func (c *Client) SummarizeFDHs(ctx context.Context, table string) (FDHSummary, error) {
	var s FDHSummary
	q := `SELECT count(), countIf(far_addr IS NULL), uniq(agent_id, ip_version, near_addr, destination_addr), uniqExact(agent_id)
		FROM ` + quote(table)
	if err := c.conn.QueryRow(noTimeout(ctx), q).Scan(&s.Rows, &s.FarNull, &s.FDHs, &s.Agents); err != nil {
		return s, fmt.Errorf("cannot summarize table %s: %w", table, err)
	}
	return s, nil
}

// noTimeout lifts the server's execution time limit for long queries.
func noTimeout(ctx context.Context) context.Context {
	return clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{"max_execution_time": 0}))
}
