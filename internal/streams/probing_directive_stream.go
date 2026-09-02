package streams

import (
	"encoding/json"
	"fmt"
	"iter"

	"github.com/dioptra-io/retina-commons/api/v1"
)

type ProbingDirectiveStream interface {
	Events() iter.Seq2[*api.ProbingDirective, error]
	Len() int
}

type probingDirectiveStream struct {
	events []*api.ProbingDirective
}

var _ ProbingDirectiveStream = (*probingDirectiveStream)(nil)

func NewProbingDirectiveStream(raw RawEventStream) (*probingDirectiveStream, error) {
	type eventEnvelope struct {
		Type string `json:"type"`
	}

	type pdBulkInsertionEvent struct {
		Type string                  `json:"type"`
		PDs  []*api.ProbingDirective `json:"pds"`
	}

	s := &probingDirectiveStream{}

	for data, err := range raw.Events() {
		if err != nil {
			return nil, err
		}

		var envelope eventEnvelope
		if err := json.Unmarshal(data, &envelope); err != nil {
			return nil, fmt.Errorf("decode event envelope: %w", err)
		}

		if envelope.Type != "PDBulkInsertionEvent" {
			continue
		}

		var event pdBulkInsertionEvent
		if err := json.Unmarshal(data, &event); err != nil {
			return nil, fmt.Errorf("decode PD bulk insertion event: %w", err)
		}

		s.events = append(s.events, event.PDs...)
	}

	return s, nil
}

func (s *probingDirectiveStream) Events() iter.Seq2[*api.ProbingDirective, error] {
	return func(yield func(*api.ProbingDirective, error) bool) {
		for _, event := range s.events {
			if !yield(event, nil) {
				return
			}
		}
	}
}

func (s *probingDirectiveStream) Len() int {
	return len(s.events)
}
