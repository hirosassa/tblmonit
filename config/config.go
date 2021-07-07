package config

import (
	"context"
	"fmt"
	"time"

	bq "cloud.google.com/go/bigquery"
	"github.com/rs/zerolog/log"
	"golang.org/x/xerrors"
	"google.golang.org/api/option"
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
	TimeThreshold     *TimeThreshold
	DurationThreshold *DurationThreshold
}

type TimeThreshold struct {
	time.Time
}

type DurationThreshold struct {
	time.Duration
}

const timefmt = "15:04:05"

func (t *TimeThreshold) UnmarshalText(text []byte) error {
	tmp, err := time.Parse(timefmt, string(text))
	current := time.Now().In(time.Local)
	t.Time = getTodaysClockObject(tmp, current)
	return err
}

func (t TimeThreshold) MarshalText() (text []byte, err error) {
	text = []byte(t.Format(timefmt))
	return text, nil
}

func (d *DurationThreshold) UnmarshalText(text []byte) error {
	var err error
	d.Duration, err = time.ParseDuration(string(text))
	return err
}

func (d DurationThreshold) MarshalText() (text []byte, err error) {
	return []byte(d.Duration.String()), nil
}

func getTodaysClockObject(clock, current time.Time) time.Time {
	y, m, d := current.Date()
	hh, mm, ss := clock.Clock()
	return time.Date(y, m, d, hh, mm, ss, 0, time.Local)
}

type FreshnessResult struct {
	Table  string
	Reason []string
}

// CheckFreshness returns old tables whose last modified time is oldeer than time threshold on the config file.
func CheckFreshness(config Config, current time.Time, opts ...option.ClientOption) (oldTables []FreshnessResult, err error) {
	ctx := context.Background()

	for _, pj := range config.Project {
		client, err := bq.NewClient(ctx, pj.ID, opts...)
		if err != nil {
			return nil, xerrors.Errorf("failed to create client: %w", err)
		}

		for _, ds := range pj.Dataset {
			for _, tc := range ds.TableConfig {
				tableID := getSuitableTableID(tc)

				md, err := client.Dataset(ds.ID).Table(tableID).Metadata(ctx)
				if err != nil { // table is not created
					log.Warn().Msgf("failed to fetch metadata: table: %s.%s", ds.ID, tableID)

					// Before time threshold, table may not exist.
					if tc.TimeThreshold == nil || current.After(tc.TimeThreshold.Time) {
						oldTables = append(oldTables, FreshnessResult{
							Table:  fmt.Sprintf("%s.%s.%s", pj.ID, ds.ID, tableID),
							Reason: []string{"Table doesn't exist"},
						})
					}
					continue
				}

				if old, reason := tc.isOld(current, md.LastModifiedTime); old {
					oldTables = append(oldTables, FreshnessResult{
						Table:  md.FullID,
						Reason: reason,
					})
				}
			}
		}
	}
	return oldTables, nil
}

func getSuitableTableID(tc TableConfig) string {
	datefmt := "20060102"
	tableIDPrefix := tc.Table
	switch tc.DateForShards {
	case "TODAY":
		{
			return tableIDPrefix + time.Now().In(time.Local).Format(datefmt)
		}
	case "ONE_DAY_AGO":
		{
			return tableIDPrefix + time.Now().In(time.Local).AddDate(0, 0, -1).Format(datefmt)
		}
	case "FIRST_DAY_OF_THE_MONTH":
		{
			now := time.Now().In(time.Local)
			firstDayOfMonth := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.Local).Format(datefmt)
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

func (t *TableConfig) isOld(current, lastModified time.Time) (isOld bool, reason []string) {
	isOld, timeReason := t.isOldForTimeThreshold(lastModified)
	if isOld {
		reason = append(reason, timeReason)
	}

	isOld, durationReason := t.isOldForDurationThreshold(current, lastModified)
	if isOld {
		reason = append(reason, durationReason)
	}

	return len(reason) > 0, reason
}

func (t *TableConfig) isOldForTimeThreshold(lastModified time.Time) (isOld bool, reason string) {
	if t.TimeThreshold == nil {
		return false, ""
	}

	if !lastModified.After(t.TimeThreshold.Time) {
		return false, ""
	}
	return true, fmt.Sprintf("The table should be created by %s, but last modified time is %s", t.TimeThreshold.Time.Format("15:04"), lastModified.Format("15:04"))
}

func (t *TableConfig) isOldForDurationThreshold(current, lastModified time.Time) (isOld bool, reason string) {
	if t.DurationThreshold == nil {
		return false, ""
	}

	if current.In(time.Local).Sub(lastModified.In(time.Local)) < t.DurationThreshold.Duration {
		return false, ""
	}
	return true, fmt.Sprintf("The table should be modified in %s, but not modified in %s", t.DurationThreshold.Duration, current.In(time.Local).Sub(lastModified.In(time.Local)))
}
