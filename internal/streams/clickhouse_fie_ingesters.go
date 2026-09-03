package streams

import (
	"fie-importer/internal/api"
	"net"
	"time"
)

// NewClickHouseLiteFIEIngester creates a ClickHouseIngester with the type
// api.FullFIE. It converts the FullFIE into a LiteFIE schema and ingests it to
// Clickhouse.
func NewClickHouseLiteFIEIngester(credentials ClickHouseCredentials, workers, bufferSize, batchSize int) (ClickHouseIngester[*api.FullFIE], error) {
	liteFIEDDL := `
CREATE TABLE %s
(
	probing_directive_id UInt64,
	sequence_number UInt64,
	-- agent_id LowCardinality(String),
	-- protocol UInt8,
	-- source_address Nullable(IPv6),
	destination_address IPv6,
	near_reply_address Nullable(IPv6),
	far_reply_address Nullable(IPv6),
	near_probe_ttl Nullable(UInt8),
	capture_timestamp DateTime
)
ENGINE = MergeTree
ORDER BY (
	sequence_number, 
	probing_directive_id
	-- agent_id
)
SETTINGS index_granularity = 8192
`
	return NewClickHouseIngester(ClickHouseIngesterConfig[*api.FullFIE]{
		Credentials: credentials,
		Workers:     workers,
		BufferSize:  bufferSize,
		BatchSize:   batchSize,
		DDL:         liteFIEDDL,
		Append: func(append ClickHouseAppender, fie *api.FullFIE) error {
			var nearReplyAddress, farReplyAddress net.IP
			var nearProbeTTL *uint8
			if fie.NearInfo != nil {
				nearReplyAddress = fie.NearInfo.ReplyAddress
				ttl := fie.NearInfo.ProbeTTL
				nearProbeTTL = &ttl
			}
			if fie.FarInfo != nil {
				farReplyAddress = fie.FarInfo.ReplyAddress
				ttl := fie.FarInfo.ProbeTTL - 1
				nearProbeTTL = &ttl
			}
			return append(
				fie.ProbingDirectiveID,
				fie.SequenceNumber,
				// fie.Agent.AgentID,
				// uint8(fie.Protocol),
				// fie.SourceAddress,
				fie.DestinationAddress,
				nearReplyAddress,
				farReplyAddress,
				nearProbeTTL,
				fie.CaptureTime,
			)
		},
	})
}

func NewClickHouseLiteFIEIngester2(credentials ClickHouseCredentials, workers, bufferSize, batchSize int) (ClickHouseIngester[*api.CompressedFIE], error) {
	liteFIEDDL := `
CREATE TABLE %s
(
	probing_directive_id UInt64,
	sequence_number      UInt64,
	near_reply_address   IPv6,
	far_reply_address    IPv6,
	capture_timestamp    DateTime
)
ENGINE = MergeTree
ORDER BY (
	sequence_number, 
	probing_directive_id
)
SETTINGS index_granularity = 8192
`
	return NewClickHouseIngester(ClickHouseIngesterConfig[*api.CompressedFIE]{
		Credentials: credentials,
		Workers:     workers,
		BufferSize:  bufferSize,
		BatchSize:   batchSize,
		DDL:         liteFIEDDL,
		Append: func(append ClickHouseAppender, fie *api.CompressedFIE) error {
			return append(
				uint64(fie.ProbingDirectiveID),
				fie.SequenceNumber,
				net.IP(fie.NearReplyAddress),
				net.IP(fie.FarReplyAddress),
				fie.CaptureBaseTime.Add(time.Duration(fie.CaptureSecond)*time.Second),
			)
		},
	})
}
