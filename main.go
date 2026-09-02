package main

import (
	"fie-importer/internal/components"
	"fmt"
)

func main() {
	stream, err := components.NewRawEventStream("../campaign4_snapshots/20260829_134155_s1/events/")
	if err != nil {
		panic(err)
	}

	for line, err := range stream.Events() {
		if err != nil {
			panic(err)
		}
		fmt.Printf("len(line): %v\n", len(line))
	}

	fmt.Printf("stream.Len(): %v\n", stream.Len())

}

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
