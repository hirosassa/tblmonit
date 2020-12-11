package config

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestGetSuitableTableID(t *testing.T) {
	datefmt := "20060102"
	location, _ := time.LoadLocation("Asia/Tokyo")
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
			wantRes: "sample_table_on_" + now.In(location).Format(datefmt),
		},
		{
			tc: TableConfig{
				Table:         "sample_table_on_",
				DateForShards: "ONE_DAY_AGO",
			},
			wantRes: "sample_table_on_" + now.In(location).AddDate(0, 0, -1).Format(datefmt),
		},
		{
			tc: TableConfig{
				Table:         "sample_table_on_",
				DateForShards: "FIRST_DAY_OF_THE_MONTH",
			},
			wantRes: "sample_table_on_" + time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, location).Format(datefmt),
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
		DurationThreshold: durationThreshold{du},
		TimeThreshold:     timeThreshold{ti},
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
