package importer

import (
	"net"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// ========================================================================== //
// PDsTable
// ========================================================================== //

type PDsTable struct {
	*baseTable[*ExtendedPD]
}

var _ Table[*ExtendedPD] = (*PDsTable)(nil)

func NewPDsTable(conn driver.Conn, tableName string, batchSize int) *PDsTable {
	ddl := `
	CREATE TABLE %s
	(
		probing_directive_id UInt64,
		agent_id             LowCardinality(String),
		ip_version           UInt8,
		protocol             UInt8,
		destination_address  IPv6,
		near_probe_ttl       UInt8
	)
	ENGINE = MergeTree
	ORDER BY probing_directive_id
	`
	appendFunc := func(f func(...any) error, pd *ExtendedPD) error {
		return f(
			pd.ProbingDirectiveID,
			pd.AgentID,
			uint8(pd.IPVersion),
			uint8(pd.Protocol),
			toIPv6(pd.DestinationAddress),
			pd.NearTTL,
		)
	}
	return &PDsTable{baseTable: newBaseTable(conn, tableName, ddl, batchSize, false, false, appendFunc)}
}

// ========================================================================== //
// FIELiteTable
// ========================================================================== //

type FIELiteTable struct {
	*baseTable[*ExtendedFIE]
}

var _ Table[*ExtendedFIE] = (*FIELiteTable)(nil)

func NewFIELiteTable(conn driver.Conn, tableName string, batchSize int) *FIELiteTable { //nolint:funlen
	const ddl = `
	CREATE TABLE %s
	(
		-- Identification columns
		probing_directive_id UInt64,
		sequence_number      UInt64,
		agent_id             LowCardinality(String),
		ip_version           UInt8,
		protocol             UInt8,

		-- Critical columns.
		source_address      Nullable(IPv6),
		destination_address IPv6,
		near_reply_address  Nullable(IPv6),
		far_reply_address   Nullable(IPv6),
		near_probe_ttl      Nullable(UInt8),

		-- Time columns.
		capture_timestamp DateTime,
		near_sent_age     UInt8,
		near_received_age UInt8,
		far_sent_age      UInt8,
		far_received_age  UInt8,
		production_age    UInt8,

		-- Time related aliases.
		near_sent_timestamp Nullable(DateTime) ALIAS 
			if(near_sent_age = 63, NULL, subtractSeconds(capture_timestamp, near_sent_age)),
		near_received_timestamp Nullable(DateTime) ALIAS 
			if(near_received_age = 63, NULL, subtractSeconds(capture_timestamp, near_received_age)),
		far_sent_timestamp Nullable(DateTime) ALIAS 
			if(far_sent_age = 63, NULL, subtractSeconds(capture_timestamp, far_sent_age)),
		far_received_timestamp Nullable(DateTime) ALIAS 
			if(far_received_age = 63, NULL, subtractSeconds(capture_timestamp, far_received_age)),
		production_timestamp Nullable(DateTime) ALIAS 
			if(production_age = 63, NULL, subtractSeconds(capture_timestamp, production_age))

		-- Critical aliases.
		far_probe_ttl Nullable(UInt8) ALIAS 
			if(isNull(near_probe_ttl), NULL, near_probe_ttl + 1),
	)
	ENGINE = MergeTree
	ORDER BY
	(
		capture_timestamp,
		sequence_number,
		agent_id,
		destination_address
	)
	SETTINGS index_granularity = 8192
	`
	appendFunc := func(f func(...any) error, fie *ExtendedFIE) error {
		var (
			nearProbeTTL     *uint8
			nearReplyAddress net.IP
			farReplyAddress  net.IP
			nearSentAge      uint8 = 63
			nearReceivedAge  uint8 = 63
			farSentAge       uint8 = 63
			farReceivedAge   uint8 = 63
		)

		if fie.NearInfo != nil {
			ttl := fie.NearInfo.ProbeTTL
			nearProbeTTL = &ttl
			nearReplyAddress = nullableIPv6(fie.NearInfo.ReplyAddress)
			nearSentAge = timestampAge(fie.CaptureTime, fie.NearInfo.SentTimestamp)
			nearReceivedAge = timestampAge(fie.CaptureTime, fie.NearInfo.ReceivedTimestamp)
		}

		if fie.FarInfo != nil {
			farReplyAddress = nullableIPv6(fie.FarInfo.ReplyAddress)
			farSentAge = timestampAge(fie.CaptureTime, fie.FarInfo.SentTimestamp)
			farReceivedAge = timestampAge(fie.CaptureTime, fie.FarInfo.ReceivedTimestamp)
		}

		return f(
			fie.ProbingDirectiveID,
			fie.SequenceNumber,
			fie.Agent.AgentID,
			uint8(fie.IPVersion),
			uint8(fie.Protocol),
			nullableIPv6(fie.SourceAddress),
			toIPv6(fie.DestinationAddress),
			nearReplyAddress,
			farReplyAddress,
			nearProbeTTL,
			fie.CaptureTime,
			nearSentAge,
			nearReceivedAge,
			farSentAge,
			farReceivedAge,
			timestampAge(fie.CaptureTime, fie.ProductionTimestamp),
		)
	}
	return &FIELiteTable{baseTable: newBaseTable(conn, tableName, ddl, batchSize, false, false, appendFunc)}
}

// ========================================================================== //
// AgentTermsTable
// ========================================================================== //

type AgentTermsTable struct {
	*baseTable[*AgentTerm]
}

var _ Table[*AgentTerm] = (*AgentTermsTable)(nil)

func NewAgentTermsTable(conn driver.Conn, tableName string, batchSize int) *AgentTermsTable {
	const ddl = `
	CREATE TABLE %s
	(
		agent_id       LowCardinality(String),
		agent_ip       IPv6,
		agent_port     UInt16,
		beginning_time DateTime64(9, 'UTC'),
		end_time       DateTime64(9, 'UTC')
	)
	ENGINE = MergeTree
	ORDER BY
	(
		agent_id,
		beginning_time
	)
	`
	appendFunc := func(f func(...any) error, term *AgentTerm) error {
		return f(
			term.AgentID,
			toIPv6(term.AgentIP),
			uint16(term.AgentPort), //nolint:gosec
			term.BeginningTime,
			term.EndTime,
		)
	}
	return &AgentTermsTable{baseTable: newBaseTable(conn, tableName, ddl, batchSize, false, false, appendFunc)}
}

// ========================================================================== //
// CurrentStatusTable
// ========================================================================== //

type CurrentStatusTable struct {
	*baseTable[*CurrentStatus]
}

var _ Table[*CurrentStatus] = (*CurrentStatusTable)(nil)

func NewCurrentStatusTable(conn driver.Conn, tableName string, batchSize int) *CurrentStatusTable {
	const ddl = `
	CREATE TABLE %s
	(
		event_time                         DateTime,
		current_pd_count                   Int64,
		cumulative_insertions              UInt64,
		cumulative_issuances               UInt64,
		cumulative_updates                 UInt64,
		aggregate_requested_rate           Float64,
		aggregate_period_between_issuances Float64,
		realized_issuance_rate              Float64,
		realized_update_rate                Float64,
		distinct_impacted_addrs             Int64,
		period_min                          Float64,
		period_max                          Float64,
		pds_clamped_at_min                  Int64,
		pds_clamped_at_max                  Int64,
		pds_with_full_history               Int64,
		update_channel_occupancy            Int64,
		insert_channel_occupancy            Int64,
		cumulative_late_occurrences         UInt64
	)
	ENGINE = MergeTree
	ORDER BY event_time
	`
	appendFunc := func(f func(a ...any) error, status *CurrentStatus) error {
		return f(
			status.EventTime,
			status.CurrentPDCount,
			status.CumulativeInsertions,
			status.CumulativeIssuances,
			status.CumulativeUpdates,
			status.AggregateRequestedRate,
			status.AggregatePeriodBetweenIssuances,
			status.RealizedIssuanceRate,
			status.RealizedUpdateRate,
			status.DistinctImpactedAddrs,
			status.PeriodMin,
			status.PeriodMax,
			status.PDsClampedAtMin,
			status.PDsClampedAtMax,
			status.PDsWithFullHistory,
			status.UpdateChannelOccupancy,
			status.InsertChannelOccupancy,
			status.CumulativeLateOccurrences,
		)
	}
	return &CurrentStatusTable{baseTable: newBaseTable(conn, tableName, ddl, batchSize, false, false, appendFunc)}
}
