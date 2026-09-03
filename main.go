package main

import (
	"encoding/json"
	"fmt"

	"fie-importer/internal/streams"
)

func main() {
	full, err := streams.NewFullFIEStreamFromDirs("./test_capture/events/", "./test_capture/fies/")
	if err != nil {
		panic(err)
	}

	for fie, err := range full.FIEs() {
		if err != nil {
			panic(err)
		}

		b, err := json.Marshal(fie)
		if err != nil {
			panic(err)
		}

		fmt.Printf("%v\n", string(b))
		break
	}

	// before := time.Now()
	// var count atomic.Uint64
	// streams.ParallelForEach2(context.Background(), 16, 16*1024*1024, full.FIEs(), func(winfo streams.WorkerInfo, t *api.FullFIE, e error) error {
	// 	count.Add(1)
	// 	return nil
	// })
	// fmt.Printf("time.Since(before).Seconds(): %v\n", time.Since(before).Seconds())

	// var count int
	// lastCount := 0
	// lastTime := time.Now()
	//
	// for _, err := range full.FIEs() {
	// 	if err != nil {
	// 		panic(err)
	// 	}
	//
	// 	count++
	//
	// 	now := time.Now()
	// 	if now.Sub(lastTime) >= time.Second {
	// 		elapsed := now.Sub(lastTime).Seconds()
	// 		rate := float64(count-lastCount) / elapsed
	//
	// 		fmt.Printf("total=%d rate=%.0f rows/sec\n", count, rate)
	//
	// 		lastTime = now
	// 		lastCount = count
	// 	}
	// }
}
