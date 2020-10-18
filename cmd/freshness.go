package cmd

import (
	"context"
	"fmt"
	"strings"
	"time"

	bq "cloud.google.com/go/bigquery"
	"github.com/rs/zerolog/log"

	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(newFreshness())
}

func newFreshness() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "freshness",
		Short: "Check freshness for each table",
		Long: `Check freshness for each table.
The target tables and time thresholds should be listed on config file.
`,
		RunE: runFreshnessCmd,
	}
	return cmd
}

func runFreshnessCmd(cmd *cobra.Command, args []string) error {
	for _, p := range config.Project {
		oldTables, err := checkFreshness(p)
		if err != nil {
			return fmt.Errorf("failed to check freshness: project: %s, error: %w", p.Name, err)
		}

		// TODO: format outputs
		if len(oldTables) != 0 {
			fmt.Println(p.Name)
			for _, t := range oldTables {
				fmt.Printf("\t%s\n", t)
			}
		} else {
			log.Info().Msgf("All tables are fresh enough in %s", p.Name)
		}
	}
	return nil
}

func checkFreshness(project Project) ([]string, error) {
	ctx := context.Background()
	client, err := bq.NewClient(ctx, project.Name)
	if err != nil {
		return nil, fmt.Errorf("failed to create client: %w", err)
	}

	var oldTables []string
	for _, tc := range project.TableConfig {
		datasetID := strings.Split(tc.Table, ".")[0]
		tableID := getTableID(tc)

		md, err := client.Dataset(datasetID).Table(tableID).Metadata(ctx)
		if err != nil { // this means the table is not created
			log.Info().Msgf("failed to fetch metadata: table: %s.%s:", datasetID, tableID)
			oldTables = append(oldTables, tc.Table)
		}

		if isOld(tc.Timethreshold, md.LastModifiedTime) {
			oldTables = append(oldTables, tc.Table)
		}
	}
	return oldTables, nil
}

func getTableID(tc TableConfig) string {
	datefmt := "20060102"
	location, _ := time.LoadLocation("Asia/Tokyo")

	tablePrefix := strings.Split(tc.Table, ".")[1]
	switch tc.DateForShards {
	case "TODAY":
		{
			return tablePrefix + time.Now().In(location).Format(datefmt)
		}
	case "ONE_DAY_AGO":
		{
			return tablePrefix + time.Now().In(location).AddDate(0, 0, -1).Format(datefmt)
		}
	case "FIRST_DAY_OF_THE_MONTH":
		{
			now := time.Now().In(location)
			firstDayOfMonth := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, location).Format(datefmt)
			return tablePrefix + firstDayOfMonth
		}
	case "": // non-sharded table
		{
			return tablePrefix
		}
	default: // TODO: handle error
		{
			log.Info().Msgf("invalid format DateForShards: %s", tc.DateForShards)
			return tablePrefix
		}
	}
}

func isOld(timeThreshold string, lastModifiedTime time.Time) bool {
	const timefmt = "15:04:05"
	threshold, err := time.Parse(timefmt, timeThreshold)
	if err != nil {
		log.Info().Msgf("failed to parse threshold: timethreshold: %s", timeThreshold)
		return true // always launch alerts if failed to parse threshold
	}

	location, _ := time.LoadLocation("Asia/Tokyo")
	lastModifiedJST := lastModifiedTime.In(location)

	return lastModifiedJST.Format(timefmt) > threshold.Format(timefmt) // TOOD: ugly comparison
}
