package config

import (
	"context"
	"time"

	bq "cloud.google.com/go/bigquery"
	"github.com/rs/zerolog/log"
	"golang.org/x/xerrors"
)

type Config struct {
	Project []Project
}

type Project struct {
	ID      string
	Dataset []Dataset
}

type Dataset struct {
	ID          string
	TableConfig []TableConfig
}

type TableConfig struct {
	Table             string
	DateForShards     string
	TimeThreshold     timeThreshold
	DurationThreshold durationThreshold
}

type timeThreshold struct {
	time.Time
}

type durationThreshold struct {
	time.Duration
}

const timefmt = "15:04:05"

func (t *timeThreshold) UnmarshalText(text []byte) error {
	tmp, err := time.Parse(timefmt, string(text))
	t.Time = getTodaysClockObject(tmp)
	return err
}

func (d *durationThreshold) UnmarshalText(text []byte) error {
	var err error
	d.Duration, err = time.ParseDuration(string(text))
	return err
}

func getTodaysClockObject(clock time.Time) time.Time {
	location, _ := time.LoadLocation("Asia/Tokyo")
	y, m, d := time.Now().In(location).Date()
	hh, mm, ss := clock.Clock()
	return time.Date(y, m, d, hh, mm, ss, 0, location)
}

// CheckFreshness returns old tables whose last modified time is oldeer than time threshold on the config file.
func CheckFreshness(config Config, current time.Time) (oldTables []string, err error) {
	ctx := context.Background()

	for _, pj := range config.Project {
		client, err := bq.NewClient(ctx, pj.ID)
		if err != nil {
			return nil, xerrors.Errorf("failed to create client: %w", err)
		}

		for _, ds := range pj.Dataset {
			for _, tc := range ds.TableConfig {
				tableID := getSuitableTableID(tc)

				md, err := client.Dataset(ds.ID).Table(tableID).Metadata(ctx)
				if err != nil { // table is not created
					log.Warn().Msgf("failed to fetch metadata: table: %s.%s:", ds.ID, tableID)
					oldTables = append(oldTables, md.FullID)
				} else if tc.isOld(current, md.LastModifiedTime) { // should not reach here when `if err != nil`
					oldTables = append(oldTables, md.FullID)
				}
			}
		}
	}
	return oldTables, nil
}

func getSuitableTableID(tc TableConfig) string {
	datefmt := "20060102"
	location, _ := time.LoadLocation("Asia/Tokyo")

	tableIDPrefix := tc.Table

	switch tc.DateForShards {
	case "TODAY":
		{
			return tableIDPrefix + time.Now().In(location).Format(datefmt)
		}
	case "ONE_DAY_AGO":
		{
			return tableIDPrefix + time.Now().In(location).AddDate(0, 0, -1).Format(datefmt)
		}
	case "FIRST_DAY_OF_THE_MONTH":
		{
			now := time.Now().In(location)
			firstDayOfMonth := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, location).Format(datefmt)
			return tableIDPrefix + firstDayOfMonth
		}
	case "": // non-sharded table
		{
			return tableIDPrefix
		}
	default: // TODO: handle error
		{
			log.Info().Msgf("invalid format DateForShards: %s", tc.DateForShards)
			return tableIDPrefix
		}
	}

}

func (t *TableConfig) isOld(current, lastModified time.Time) bool {
	location, _ := time.LoadLocation("Asia/Tokyo")
	return current.After(t.TimeThreshold.Time) && current.In(location).Sub(lastModified.In(location)) > t.DurationThreshold.Duration
}
