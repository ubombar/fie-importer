package components

import (
	"encoding/json"
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
	AgentIDs() []string
	Len() int
}

type agentConnectionHistory struct {
	eventsByAgent map[string][]*api.AgentConnectionEvent
	events        []*api.AgentConnectionEvent
}

var _ AgentConnectionHistory = (*agentConnectionHistory)(nil)

func NewAgentConnectionHistory(stream RawEventStream) (*agentConnectionHistory, error) {
	type retinaBaseEvent struct {
		Type      string    `json:"type"`
		Timestamp time.Time `json:"timestamp"`
	}

	type agentConnectedEvent struct {
		retinaBaseEvent
		AgentID       string `json:"agent_id"`
		RemoteAddress string `json:"remote_address"`
	}

	type agentDisconnectedEvent struct {
		retinaBaseEvent
		AgentID       string `json:"agent_id"`
		RemoteAddress string `json:"remote_address"`
	}

	h := &agentConnectionHistory{
		eventsByAgent: make(map[string][]*api.AgentConnectionEvent),
	}

	for raw, err := range stream.Events() {
		if err != nil {
			return nil, err
		}

		var base retinaBaseEvent
		if err := json.Unmarshal(raw, &base); err != nil {
			return nil, fmt.Errorf("decode event envelope: %w", err)
		}

		var event *api.AgentConnectionEvent

		switch base.Type {
		case "AgentConnectedEvent":
			var connected agentConnectedEvent
			if err := json.Unmarshal(raw, &connected); err != nil {
				return nil, fmt.Errorf("decode agent connected event: %w", err)
			}

			host, _, err := net.SplitHostPort(connected.RemoteAddress)
			if err != nil {
				host = connected.RemoteAddress
			}

			ip := net.ParseIP(host)
			if ip == nil {
				return nil, fmt.Errorf("invalid agent address %q", connected.RemoteAddress)
			}

			event = &api.AgentConnectionEvent{
				AgentID:      connected.AgentID,
				Time:         connected.Timestamp,
				AgentAddress: ip,
			}

		case "AgentDisconnectedEvent":
			var disconnected agentDisconnectedEvent
			if err := json.Unmarshal(raw, &disconnected); err != nil {
				return nil, fmt.Errorf("decode agent disconnected event: %w", err)
			}

			event = &api.AgentConnectionEvent{
				AgentID:      disconnected.AgentID,
				Time:         disconnected.Timestamp,
				AgentAddress: nil,
			}

		default:
			continue
		}

		h.events = append(h.events, event)
	}

	sort.Slice(h.events, func(i, j int) bool {
		return h.events[i].Time.Before(h.events[j].Time)
	})

	for _, event := range h.events {
		h.eventsByAgent[event.AgentID] = append(h.eventsByAgent[event.AgentID], event)
	}

	return h, nil
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

func (h *agentConnectionHistory) Len() int {
	return len(h.eventsByAgent)
}

func (h *agentConnectionHistory) AgentIDs() []string {
	ids := make([]string, 0, len(h.eventsByAgent))
	for agentID := range h.eventsByAgent {
		ids = append(ids, agentID)
	}
	sort.Strings(ids)
	return ids
}
