package main

import (
	"fmt"
	"time"

	"fie-importer/internal/streams"
)

func main() {
	stream, err := streams.NewRawEventStream("../campaign4_snapshots/20260829_134155_s1/events/")
	if err != nil {
		panic(err)
	}

	statusStream, err := streams.NewCurrentStatusStream(stream)
	if err != nil {
		panic(err)
	}

	for status, err := range statusStream.Events() {
		if err != nil {
			panic(err)
		}

		fmt.Printf("status time: %v\n", status.Timestamp)
	}

	fmt.Printf("raw events: %d %v\n", stream.Len(), time.Now())
	fmt.Printf("current statuses: %d %v\n", statusStream.Len(), time.Now())
}

// func main() {
// 	stream, err := streams.NewRawEventStream("../campaign4_snapshots/20260829_134155_s1/events/")
// 	if err != nil {
// 		panic(err)
// 	}
//
// 	stream2, err := streams.NewProbingDirectiveStream(stream)
// 	if err != nil {
// 		panic(err)
// 	}
//
// 	for _, err := range stream2.Events() {
// 		if err != nil {
// 			panic(err)
// 		}
// 	}
// 	fmt.Printf("raw events: %d %v\n", stream.Len(), time.Now())
// 	fmt.Printf("agents: %d %v\n", stream2.Len(), time.Now())
//
// }

// func main() {
// 	stream, err := components.NewCompressedFIEStream("./test_capture/fies/")
// 	if err != nil {
// 		panic(err)
// 	}
//
// 	fmt.Printf("stream.Len(): %v\n", stream.Len())
//
// 	i := 1
// 	for fie, err := range stream.Events() {
// 		if err != nil {
// 			panic(err)
// 		}
// 		if i >= 10 {
// 			break
// 		}
//
// 		fmt.Printf(
// 			"pd=%d capture_second=%d near_len=%d far_len=%d time_deltas=%d\n",
// 			fie.ProbingDirectiveID,
// 			fie.CaptureSecond,
// 			len(fie.NearReplyAddress),
// 			len(fie.FarReplyAddress),
// 			fie.TimeDeltas,
// 		)
// 		i++
// 	}
//
// }
