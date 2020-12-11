package cmd

import (
	"testing"
	"time"

	"github.com/hirosassa/tblmonit/config"
	"github.com/stretchr/testify/assert"
)

func TestGetTableID(t *testing.T) {
	datefmt := "20060102"
	location, _ := time.LoadLocation("Asia/Tokyo")
	now := time.Now()

	tests := []struct {
		tc      config.TableConfig
		wantRes string
	}{
		{
			tc: config.TableConfig{
				Table:         "sample_dataset.sample_table_on_",
				DateForShards: "TODAY",
				Timethreshold: "08:00:00",
			},
			wantRes: "sample_table_on_" + now.In(location).Format(datefmt),
		},
		{
			tc: config.TableConfig{
				Table:         "sample_dataset.sample_table_on_",
				DateForShards: "ONE_DAY_AGO",
				Timethreshold: "08:00:00",
			},
			wantRes: "sample_table_on_" + now.In(location).AddDate(0, 0, -1).Format(datefmt),
		},
		{
			tc: config.TableConfig{
				Table:         "sample_dataset.sample_table_on_",
				DateForShards: "FIRST_DAY_OF_THE_MONTH",
				Timethreshold: "08:00:00",
			},
			wantRes: "sample_table_on_" + time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, location).Format(datefmt),
		},
		{
			tc: config.TableConfig{
				Table:         "sample_dataset.non_sharded_table",
				DateForShards: "",
				Timethreshold: "08:00:00",
			},
			wantRes: "non_sharded_table",
		},
	}

	for _, tt := range tests {
		actual := getTableID(tt.tc)
		expected := tt.wantRes
		assert.Equal(t, expected, actual)
	}
}

func TestIsOld(t *testing.T) {
	location, _ := time.LoadLocation("Asia/Tokyo")

	tests := []struct {
		timeThreshold    string
		lastModifiedTime time.Time
		wantRes          bool
	}{
		{
			timeThreshold:    "08:00:00",
			lastModifiedTime: time.Date(2020, 1, 1, 20, 0, 0, 0, location),
			wantRes:          true,
		},
		{
			timeThreshold:    "08:00:00",
			lastModifiedTime: time.Date(2020, 1, 1, 7, 0, 0, 0, location),
			wantRes:          false,
		},
		{
			timeThreshold:    "28:97", // failed to parse time
			lastModifiedTime: time.Date(2020, 1, 1, 7, 0, 0, 0, location),
			wantRes:          true,
		},
	}
	for _, tt := range tests {
		actual := isOld(tt.timeThreshold, tt.lastModifiedTime)
		expected := tt.wantRes
		assert.Equal(t, expected, actual)
	}
}
