package api

import (
	"net"
	"time"

	"github.com/dioptra-io/retina-commons/api/v1"
)

const MissingTimestampAge uint8 = 63

type AgentConnectionEvent struct {
	AgentID      string    `json:"agent_id"`
	Time         time.Time `json:"time"`
	AgentAddress net.IP    `json:"agent_address"`
}

type CurrentStatusEvent struct {
	Timestamp                       time.Time `json:"timestamp"`
	CurrentPDCount                  int       `json:"current_pd_count"`
	CumulativeInsertions            uint64    `json:"cumulative_insertions"`
	CumulativeIssuances             uint64    `json:"cumulative_issuances"`
	CumulativeUpdates               uint64    `json:"cumulative_updates"`
	AggregateRequestedRate          float64   `json:"aggregate_requested_rate"`
	AggregatePeriodBetweenIssuances float64   `json:"aggregate_period_between_issuances"`
	RealizedIssuanceRate            float64   `json:"realized_issuance_rate"`
	RealizedUpdateRate              float64   `json:"realized_update_rate"`
	DistinctImpactedAddrs           int       `json:"distinct_impacted_addrs"`
	PeriodMin                       float64   `json:"period_min"`
	PeriodMax                       float64   `json:"period_max"`
	PDsClampedAtMin                 int       `json:"pds_clamped_at_min"`
	PDsClampedAtMax                 int       `json:"pds_clamped_at_max"`
	PDsWithFullHistory              int       `json:"pds_with_full_history"`
	UpdateChannelOccupancy          int       `json:"update_channel_occupancy"`
	InsertChannelOccupancy          int       `json:"insert_channel_occupancy"`
	CumulativeLateOccurrences       uint64    `json:"cumulative_late_occurrences"`
}

type CompressedFIE struct {
	SequenceNumber     uint64    `json:"sequence_number"`
	ProbingDirectiveID uint32    `json:"probing_directive_id"`
	NearReplyAddress   []byte    `json:"near_reply_address"`
	FarReplyAddress    []byte    `json:"far_reply_address"`
	CaptureSecond      uint16    `json:"capture_second"`
	TimeDeltas         uint32    `json:"time_deltas"`
	CaptureBaseTime    time.Time `json:"-"`
}

type FullFIE struct {
	api.ForwardingInfoElement

	SequenceNumber uint64    `json:"sequence_number"`
	CaptureTime    time.Time `json:"capture_time"`
}

type ParquetLiteFIE struct {
	SequenceNumber     uint64 `parquet:"sequence_number"`
	ProbingDirectiveID uint32 `parquet:"probing_directive_id"`
	NearReplyAddress   []byte `parquet:"near_reply_address,optional"`
	FarReplyAddress    []byte `parquet:"far_reply_address,optional"`
	CaptureTimestamp   uint32 `parquet:"capture_timestamp"`
}
