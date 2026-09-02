package importer

import (
	"net"
	"time"

	api "github.com/dioptra-io/retina-commons/api/v1"
)

const MissingTimestampAge uint8 = 63

// ExtendedFIE augments a ForwardingInfoElement with metadata
// reconstructed by the capture extractor.
//
// CaptureTime represents when the FIE was captured by the orchestrator.
// SequenceNumber represents the global insertion order across all capture files.
type ExtendedFIE struct {
	api.ForwardingInfoElement
	CaptureTime    time.Time `json:"capture_time"`
	SequenceNumber uint64    `json:"sequence_number"`
}

// ExtendedPD augments a ProbingDirective with importer-specific
// metadata. Additional reconstructed or derived fields can be added here later.
type ExtendedPD struct {
	api.ProbingDirective
}

// AgentTerm represents a period during which an agent is connected and active.
//
// BeginningTime should always be valid. A zero EndTime means that the
// disconnection time is unknown or the agent was still connected when the
// captured event history ended.
type AgentTerm struct {
	BeginningTime time.Time  `json:"beginning_time"`
	EndTime       *time.Time `json:"end_time"`
	AgentID       string     `json:"agent_id"`
	AgentIP       net.IP     `json:"agent_ip"`
	AgentPort     int        `json:"agent_port"`
}

// CurrentStatus is the exact copy of the current status event from
// retina-orchestrator.
type CurrentStatus struct {
	EventTime                       time.Time `json:"timestamp"`
	CurrentPDCount                  int       `json:"current_pd_count"`
	CumulativeInsertions            uint64    `json:"cumulative_insertions"`
	CumulativeIssuances             uint64    `json:"cumulative_issuances"`
	CumulativeUpdates               uint64    `json:"cumulative_updates"`
	AggregateRequestedRate          float64   `json:"aggregate_requested_rate"`           // Σ rᵢ = Σ 1/μᵢ, per second
	AggregatePeriodBetweenIssuances float64   `json:"aggregate_period_between_issuances"` // 1 / Σ rᵢ
	RealizedIssuanceRate            float64   `json:"realized_issuance_rate"`             // issuances over the last interval, per second
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

// capturerFIERecord represents the compact physical representation of one FIE
// stored in a DuckDB capture file.
//
// captureSecond is relative to the capture interval encoded in the file name.
// timeDeltas packs the production, near-sent, near-received, far-sent, and
// far-received timestamp ages into a single UInt32 value.
type capturerFIERecord struct {
	probingDirectiveID uint32
	nearReplyAddress   []byte
	farReplyAddress    []byte
	captureSecond      uint16
	timeDeltas         uint32
}
