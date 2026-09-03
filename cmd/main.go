package main

import (
	"fie-importer/internal/streams"
	"fmt"
	"log"
	"time"

	"github.com/spf13/cobra"
	"golang.org/x/sync/errgroup"
)

func main() {
	rootCmd := &cobra.Command{
		Use: "fie-importer",
	}

	rootCmd.AddCommand(newParquetCommand())

	if err := rootCmd.Execute(); err != nil {
		log.Fatal(err)
	}
}

func newParquetCommand() *cobra.Command { //nolint
	var (
		fiesDir   string
		output    string
		batchSize int
	)

	cmd := &cobra.Command{
		Use:   "parquet",
		Short: "Export compressed FIEs to Parquet",
		RunE: func(cmd *cobra.Command, args []string) error {
			compressedFIEStream, err := streams.NewCompressedFIEStream(fiesDir)
			if err != nil {
				return err
			}

			ingester, err := streams.NewLiteFIEParquetIngester(output, batchSize)
			if err != nil {
				return err
			}
			defer ingester.Close() //nolint

			total := uint64(compressedFIEStream.Len()) //nolint
			start := time.Now()

			fmt.Printf("parquet export started with %d FIEs total.\n", total)

			group, ctx := errgroup.WithContext(cmd.Context())
			done := make(chan struct{})

			group.Go(func() error {
				defer close(done)
				return ingester.Ingest(compressedFIEStream.Events())
			})

			group.Go(func() error {
				ticker := time.NewTicker(time.Second)
				defer ticker.Stop()

				for {
					select {
					case <-ticker.C:
						ingested := ingester.Count()
						elapsed := time.Since(start)

						var percentage float64
						var eta time.Duration

						if total > 0 {
							percentage = float64(ingested) / float64(total) * 100
						}

						if ingested > 0 && ingested < total {
							rate := float64(ingested) / elapsed.Seconds()
							eta = time.Duration(float64(total-ingested)/rate) * time.Second
						}

						fmt.Printf("\rtotal=%d ingested=%d since=%s ETA=%s completed=%.2f%%", total, ingested, elapsed.Round(time.Second), eta.Round(time.Second), percentage)

					case <-done:
						return nil

					case <-ctx.Done():
						return nil
					}
				}
			})

			if err := group.Wait(); err != nil {
				return err
			}

			fmt.Printf("\rtotal=%d ingested=%d since=%s ETA=0s completed=100.00%%\n", total, ingester.Count(), time.Since(start).Round(time.Second))
			fmt.Printf("parquet export complete in %v\n", time.Since(start))

			return nil
		},
	}

	cmd.Flags().StringVar(&fiesDir, "fies-dir", "", "directory containing compressed FIE files")
	cmd.Flags().StringVarP(&output, "output", "o", "", "output Parquet file")
	cmd.Flags().IntVar(&batchSize, "batch-size", 100_000, "Parquet ingestion batch size")

	_ = cmd.MarkFlagRequired("fies-dir")
	_ = cmd.MarkFlagRequired("output")

	return cmd
}
