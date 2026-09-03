package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"fie-importer/internal/streams"
)

func main() {
	ctx := context.Background()

	compressedFIEStream, err := streams.NewCompressedFIEStream("./test_capture/fies/")
	if err != nil {
		log.Fatal(err)
	}

	ingester, err := streams.NewClickHouseLiteFIEIngester2(
		streams.ClickHouseCredentials{
			Addresses: []string{"localhost:9000"},
			Database:  "test_database",
			Username:  "admin",
			Password:  "admin123",
		},
		16,
		500_000,
		500_000,
	)
	if err != nil {
		log.Fatal(err)
	}
	defer ingester.Close()

	const table = "test_fies_lite2"

	if err := ingester.Drop(ctx, table); err != nil {
		log.Fatal(err)
	}

	if err := ingester.Create(ctx, table); err != nil {
		log.Fatal(err)
	}

	fmt.Printf("ingestion started with %v FIEs total.", compressedFIEStream.Len())
	start := time.Now()

	if err := ingester.Ingest(ctx, table, compressedFIEStream.Events()); err != nil {
		panic(err)
	}

	elapsed := time.Since(start)
	fmt.Printf("ingestion complete in %v\n", elapsed)
}
