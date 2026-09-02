package main

import (
	"fmt"

	"fie-importer/internal/streams"
)

func main() {
	rawEvents, err := streams.NewRawEventStream("../campaign4_snapshots/20260829_134155_s1/events/")
	// rawEvents, err := streams.NewRawEventStream("./test_capture/events/")
	if err != nil {
		panic(err)
	}

	pds, err := streams.NewProbingDirectiveStream(rawEvents)
	if err != nil {
		panic(err)
	}

	agents, err := streams.NewAgentConnectionHistory(rawEvents)
	if err != nil {
		panic(err)
	}

	compressed, err := streams.NewCompressedFIEStream("../campaign4_snapshots/20260829_134155_s1/fies/")
	// compressed, err := streams.NewCompressedFIEStream("./test_capture/fies/")
	if err != nil {
		panic(err)
	}

	full, err := streams.NewFullFIEStream(compressed, pds, agents)
	if err != nil {
		panic(err)
	}

	fmt.Printf("PDs: %d\n", pds.Len())
	fmt.Printf("agents: %d\n", agents.Len())
	fmt.Printf("compressed FIEs: %d\n", compressed.Len())
	fmt.Printf("full FIEs: %d\n", full.Len())

	var count int

	for fie, err := range full.FIEs() {
		if err != nil {
			panic(err)
		}

		if count < 10 {
			fmt.Printf(
				"seq=%d capture=%s pd=%d agent=%s src=%v dst=%v\n",
				fie.SequenceNumber,
				fie.CaptureTime,
				fie.ProbingDirectiveID,
				fie.Agent.AgentID,
				fie.SourceAddress,
				fie.DestinationAddress,
			)
		} else {
			break
		}

		count++
	}

	fmt.Printf("iterated FIEs: %d\n", count)
}
