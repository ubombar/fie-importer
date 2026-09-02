package streams

import (
	"encoding/json"
	"fmt"
	"iter"

	"fie-importer/internal/api"
)

type CurrentStatusStream interface {
	Events() iter.Seq2[*api.CurrentStatusEvent, error]
	Len() int
}

type currentStatusStream struct {
	events []*api.CurrentStatusEvent
}

var _ CurrentStatusStream = (*currentStatusStream)(nil)

func NewCurrentStatusStream(raw RawEventStream) (*currentStatusStream, error) {
	type eventEnvelope struct {
		Type string `json:"type"`
	}

	s := &currentStatusStream{}

	for data, err := range raw.Events() {
		if err != nil {
			return nil, err
		}

		var envelope eventEnvelope
		if err := json.Unmarshal(data, &envelope); err != nil {
			return nil, fmt.Errorf("decode event envelope: %w", err)
		}

		if envelope.Type != "CurrentStatusEvent" {
			continue
		}

		var status api.CurrentStatusEvent
		if err := json.Unmarshal(data, &status); err != nil {
			return nil, fmt.Errorf("decode current status event: %w", err)
		}

		s.events = append(s.events, &status)
	}

	return s, nil
}

func (s *currentStatusStream) Events() iter.Seq2[*api.CurrentStatusEvent, error] {
	return func(yield func(*api.CurrentStatusEvent, error) bool) {
		for _, event := range s.events {
			if !yield(event, nil) {
				return
			}
		}
	}
}

func (s *currentStatusStream) Len() int {
	return len(s.events)
}
