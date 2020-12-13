package flexconfig

import (
	"testing"
	"time"

	bq "cloud.google.com/go/bigquery"
	"github.com/hirosassa/tblmonit/config"
	"github.com/stretchr/testify/assert"
)

func TestTablePrefix(t *testing.T) {
	tests := []struct {
		table   bq.Table
		wantRes string
	}{
		{
			table:   bq.Table{TableID: "sample"},
			wantRes: "sample",
		},
		{
			table:   bq.Table{TableID: "sample_on_20200101"},
			wantRes: "sample_on_",
		},
		{
			table:   bq.Table{TableID: "sample_on_12345678"},
			wantRes: "sample_on_",
		},
		{
			table:   bq.Table{TableID: "sample_on_12345678_abc"},
			wantRes: "sample_on_12345678_abc",
		},
	}
	for _, tt := range tests {
		actual := tablePrefix(&tt.table)
		expected := tt.wantRes
		assert.Equal(t, expected, actual)
	}
}

func TestIsRequiredFieldFilled(t *testing.T) {
	tests := []struct {
		ft      FlexTableConfig
		wantRes bool
		desc    string
	}{
		{
			ft:      FlexTableConfig{},
			wantRes: false,
			desc:    "empty FlexTableConfig",
		},
		{
			ft: FlexTableConfig{
				TimeThreshold: config.TimeThreshold{Time: time.Date(2020, 1, 2, 3, 4, 5, 6, time.Local)},
			},
			wantRes: false,
			desc:    "only TimeThreshold is filled",
		},
		{
			ft: FlexTableConfig{
				DurationThreshold: config.DurationThreshold{Duration: 12345},
			},
			wantRes: false,
			desc:    "only Duration is filled",
		},
		{
			ft: FlexTableConfig{
				TimeThreshold:     config.TimeThreshold{Time: time.Date(2020, 1, 2, 3, 4, 5, 6, time.Local)},
				DurationThreshold: config.DurationThreshold{Duration: 12345},
			},
			wantRes: true,
			desc:    "both required fields are filled",
		},
	}
	for _, tt := range tests {
		actual := tt.ft.isRequiredFieldFilled()
		expect := tt.wantRes
		assert.Equal(t, expect, actual, tt.desc)
	}
}
