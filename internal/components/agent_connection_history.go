package components

import (
	"fmt"
	"iter"
	"net"
	"sort"
	"time"

	"fie-importer/internal/api"
)

type AgentConnectionHistory interface {
	Events() iter.Seq[*api.AgentConnectionEvent]
	AddressAt(agentID string, t time.Time) (net.IP, bool, error)
}

type agentConnectionHistory struct {
	eventsByAgent map[string][]*api.AgentConnectionEvent
	events        []*api.AgentConnectionEvent
}

var _ AgentConnectionHistory = (*agentConnectionHistory)(nil)

func NewAgentConnectionHistory(events []*api.AgentConnectionEvent) AgentConnectionHistory {
	h := &agentConnectionHistory{
		eventsByAgent: make(map[string][]*api.AgentConnectionEvent),
		events:        append([]*api.AgentConnectionEvent(nil), events...),
	}

	sort.Slice(h.events, func(i, j int) bool {
		return h.events[i].Time.Before(h.events[j].Time)
	})

	for _, event := range h.events {
		h.eventsByAgent[event.AgentID] = append(h.eventsByAgent[event.AgentID], event)
	}

	return h
}

func (h *agentConnectionHistory) Events() iter.Seq[*api.AgentConnectionEvent] {
	return func(yield func(*api.AgentConnectionEvent) bool) {
		for _, event := range h.events {
			if !yield(event) {
				return
			}
		}
	}
}

func (h *agentConnectionHistory) AddressAt(agentID string, t time.Time) (net.IP, bool, error) {
	events, ok := h.eventsByAgent[agentID]
	if !ok || len(events) == 0 {
		return nil, false, fmt.Errorf("no connection events for agent %q", agentID)
	}

	i := sort.Search(len(events), func(i int) bool {
		return !events[i].Time.Before(t)
	})

	if i < len(events) && events[i].Time.Equal(t) {
		if events[i].AgentAddress == nil {
			return nil, true, fmt.Errorf("agent %q is disconnected at %s", agentID, t)
		}
		return events[i].AgentAddress, true, nil
	}

	if i == 0 {
		return nil, false, fmt.Errorf("no connection event for agent %q before %s", agentID, t)
	}

	event := events[i-1]
	if event.AgentAddress == nil {
		return nil, false, fmt.Errorf("agent %q is disconnected at %s", agentID, t)
	}

	return event.AgentAddress, false, nil
}
