package streams

import (
	"fmt"
	"iter"
	"net"
	"time"

	retinaapi "github.com/dioptra-io/retina-commons/api/v1"

	"fie-importer/internal/api"
)

type FullFIEStream interface {
	FIEs() iter.Seq2[*api.FullFIE, error]
	Len() int
}

type fullFIEStream struct {
	compressed CompressedFIEStream
	agents     AgentConnectionStream
	pdByID     map[uint64]*retinaapi.ProbingDirective
}

var _ FullFIEStream = (*fullFIEStream)(nil)

func NewFullFIEStream(compressed CompressedFIEStream, pds ProbingDirectiveStream, agents AgentConnectionStream) (*fullFIEStream, error) {
	s := &fullFIEStream{
		compressed: compressed,
		agents:     agents,
		pdByID:     make(map[uint64]*retinaapi.ProbingDirective, pds.Len()),
	}

	for pd, err := range pds.Events() {
		if err != nil {
			return nil, fmt.Errorf("read probing directive: %w", err)
		}

		if _, ok := s.pdByID[pd.ProbingDirectiveID]; ok {
			return nil, fmt.Errorf("probing directive %d already exists", pd.ProbingDirectiveID)
		}

		s.pdByID[pd.ProbingDirectiveID] = pd
	}

	return s, nil
}

func (s *fullFIEStream) Len() int {
	return s.compressed.Len()
}

func (s *fullFIEStream) FIEs() iter.Seq2[*api.FullFIE, error] {
	return func(yield func(*api.FullFIE, error) bool) {
		var sequenceNumber uint64

		for compressed, err := range s.compressed.Events() {
			if err != nil {
				yield(nil, err)
				return
			}

			fie, err := s.construct(compressed, sequenceNumber)
			if err != nil {
				yield(nil, err)
				return
			}

			if !yield(fie, nil) {
				return
			}

			sequenceNumber++
		}
	}
}

func (s *fullFIEStream) construct(row *api.CompressedFIE, sequenceNumber uint64) (*api.FullFIE, error) {
	pd, ok := s.pdByID[uint64(row.ProbingDirectiveID)]
	if !ok {
		return nil, fmt.Errorf("probing directive %d not found", row.ProbingDirectiveID)
	}

	captureTime := row.CaptureBaseTime.Add(
		time.Duration(row.CaptureSecond) * time.Second,
	)

	productionDelta := uint8(row.TimeDeltas & 0x3f)
	nearSentDelta := uint8((row.TimeDeltas >> 6) & 0x3f)
	nearRecvDelta := uint8((row.TimeDeltas >> 12) & 0x3f)
	farSentDelta := uint8((row.TimeDeltas >> 18) & 0x3f)
	farRecvDelta := uint8((row.TimeDeltas >> 24) & 0x3f)

	decodeTimestamp := func(delta uint8) time.Time {
		if delta == 63 {
			return time.Time{}
		}
		return captureTime.Add(-time.Duration(delta) * time.Second)
	}

	agentIP, _, err := s.agents.AddressAt(pd.AgentID, captureTime)
	if err != nil {
		return nil, fmt.Errorf("resolve address for agent %q at %s: %w", pd.AgentID, captureTime, err)
	}

	fie := retinaapi.ForwardingInfoElement{
		Agent: retinaapi.Agent{
			AgentID: pd.AgentID,
		},
		ProbingDirectiveID:  uint64(row.ProbingDirectiveID),
		IPVersion:           pd.IPVersion,
		Protocol:            pd.Protocol,
		SourceAddress:       agentIP,
		DestinationAddress:  pd.DestinationAddress,
		ProductionTimestamp: decodeTimestamp(productionDelta),
	}

	if len(row.NearReplyAddress) > 0 {
		fie.NearInfo = &retinaapi.Info{
			ProbeTTL:          pd.NearTTL,
			ReplyAddress:      net.IP(row.NearReplyAddress),
			SentTimestamp:     decodeTimestamp(nearSentDelta),
			ReceivedTimestamp: decodeTimestamp(nearRecvDelta),
		}
	}

	if len(row.FarReplyAddress) > 0 {
		fie.FarInfo = &retinaapi.Info{
			ProbeTTL:          pd.NearTTL + 1,
			ReplyAddress:      net.IP(row.FarReplyAddress),
			SentTimestamp:     decodeTimestamp(farSentDelta),
			ReceivedTimestamp: decodeTimestamp(farRecvDelta),
		}
	}

	return &api.FullFIE{
		ForwardingInfoElement: fie,
		SequenceNumber:        sequenceNumber,
		CaptureTime:           captureTime,
	}, nil
}
