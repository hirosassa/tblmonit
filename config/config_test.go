package config

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestGetTodaysClockObject(t *testing.T) {
	tests := []struct {
		clock   time.Time
		current time.Time
		wantRes time.Time
	}{

		{
			clock:   time.Date(0, 0, 0, 1, 2, 3, 0, time.Local),
			current: time.Date(2020, 1, 2, 0, 0, 0, 0, time.Local),
			wantRes: time.Date(2020, 1, 2, 1, 2, 3, 0, time.Local),
		},
		{
			clock:   time.Date(2020, 1, 2, 3, 4, 5, 6, time.Local),
			current: time.Date(2020, 1, 5, 0, 0, 0, 0, time.Local),
			wantRes: time.Date(2020, 1, 5, 3, 4, 5, 0, time.Local),
		},
	}
	for _, tt := range tests {
		actual := getTodaysClockObject(tt.clock, tt.current)
		expected := tt.wantRes
		assert.Equal(t, expected, actual)
	}
}

func TestGetSuitableTableID(t *testing.T) {
	datefmt := "20060102"
	now := time.Now()

	tests := []struct {
		ds      string
		tc      TableConfig
		wantRes string
	}{
		{
			tc: TableConfig{
				Table:         "sample_table_on_",
				DateForShards: "TODAY",
			},
			wantRes: "sample_table_on_" + now.In(time.Local).Format(datefmt),
		},
		{
			tc: TableConfig{
				Table:         "sample_table_on_",
				DateForShards: "ONE_DAY_AGO",
			},
			wantRes: "sample_table_on_" + now.In(time.Local).AddDate(0, 0, -1).Format(datefmt),
		},
		{
			tc: TableConfig{
				Table:         "sample_table_on_",
				DateForShards: "FIRST_DAY_OF_THE_MONTH",
			},
			wantRes: "sample_table_on_" + time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.Local).Format(datefmt),
		},
		{
			tc: TableConfig{
				Table:         "non_sharded_table",
				DateForShards: "",
			},
			wantRes: "non_sharded_table",
		},
	}

	for _, tt := range tests {
		actual := getSuitableTableID(tt.tc)
		expected := tt.wantRes
		assert.Equal(t, expected, actual)
	}
}

func createTableConfig(t, d string) TableConfig {
	du, _ := time.ParseDuration(d)
	ti, _ := time.Parse(timefmt, t)
	return TableConfig{
		DurationThreshold: DurationThreshold{du},
		TimeThreshold:     TimeThreshold{ti},
	}
}

func TestIsOld(t *testing.T) {
	location, _ := time.LoadLocation("Asia/Tokyo")

	tests := []struct {
		tc           TableConfig
		current      time.Time
		lastModified time.Time
		wantRes      bool
	}{
		{
			tc:           createTableConfig("03:00", "24h"),
			current:      time.Date(2020, 1, 1, 12, 0, 0, 0, location),
			lastModified: time.Date(2020, 1, 1, 3, 0, 0, 0, location),
			wantRes:      false,
		},
		{
			tc:           createTableConfig("03:00", "24h"),
			current:      time.Date(2020, 1, 2, 3, 0, 0, 0, location),
			lastModified: time.Date(2020, 1, 1, 3, 0, 0, 0, location),
			wantRes:      false,
		},
		{
			tc:           createTableConfig("03:00", "24h"),
			current:      time.Date(2020, 1, 2, 20, 1, 0, 0, location),
			lastModified: time.Date(2020, 1, 1, 20, 0, 0, 0, location),
			wantRes:      true,
		},
		{
			tc:           createTableConfig("03:00", "24h"),
			current:      time.Date(2020, 1, 2, 23, 0, 0, 0, location),
			lastModified: time.Date(2020, 1, 1, 20, 0, 0, 0, location),
			wantRes:      true,
		},
	}
	for _, tt := range tests {
		actual := tt.tc.isOld(tt.current, tt.lastModified)
		expected := tt.wantRes
		assert.Equal(t, expected, actual)
	}
}
