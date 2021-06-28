package flexconfig

import (
	"context"
	"fmt"
	"regexp"
	"time"

	bq "cloud.google.com/go/bigquery"
	"github.com/hirosassa/tblmonit/config"
	"github.com/rs/zerolog/log"
	"golang.org/x/xerrors"
	"google.golang.org/api/iterator"
)

type FlexConfig struct {
	FlexProject []FlexProject
}

type FlexProject struct {
	ID          string // project ID, DO NOT support regular expression
	Dataset     []config.Dataset
	FlexDataset []FlexDataset
}

type FlexDataset struct {
	ID              string // specify dataset id, supports regular expression
	TableConfig     []config.TableConfig
	FlexTableConfig []FlexTableConfig
}

type FlexTableConfig struct {
	Table             string
	FlexTable         string
	DateForShards     string
	TimeThreshold     *config.TimeThreshold
	DurationThreshold *config.DurationThreshold
}

// Expand returns config.Config defined by given FlexConfig
func (c *FlexConfig) Expand() (cfg config.Config, err error) {
	fmt.Println(c)

	ctx := context.Background()
	pjs := make([]config.Project, 0, len(c.FlexProject))
	for _, p := range c.FlexProject {
		pj, err := p.expand(ctx)
		if err != nil {
			return config.Config{}, xerrors.Errorf("failed to expand flex project: %w", err)
		}
		pjs = append(pjs, pj)
	}

	log.Info().Msgf("size of pjs: %d", len(pjs))

	return config.Config{
		Project: pjs,
	}, nil
}

func (p *FlexProject) expand(ctx context.Context) (pj config.Project, err error) {
	client, err := bq.NewClient(ctx, p.ID)
	if err != nil {
		return config.Project{}, xerrors.Errorf("failed to create client: %w", err)
	}
	defer client.Close()

	dss := make([]config.Dataset, 0)

	for _, d := range p.FlexDataset {
		ds, err := d.expand(ctx, client)
		if err != nil {
			return config.Project{}, xerrors.Errorf("failed to expand project: %w", err)
		}
		dss = append(dss, ds...)
	}

	dss = append(dss, p.Dataset...)

	log.Info().Msgf("p.ID: %s", p.ID)

	return config.Project{
		ID:      p.ID,
		Dataset: dss,
	}, nil
}

func (d *FlexDataset) expand(ctx context.Context, c *bq.Client) (ds []config.Dataset, err error) {
	dsiter := c.Datasets(ctx)
	datasets, err := d.filterDataset(dsiter)
	if err != nil {
		return []config.Dataset{}, xerrors.Errorf("failed to fetch datasets: %w", err)
	}

	ds = make([]config.Dataset, 0, len(datasets))
	for _, dataset := range datasets {
		tss := make([]config.TableConfig, 0)
		for _, tc := range d.FlexTableConfig {
			ts, err := tc.expand(ctx, dataset)
			if err != nil {
				return []config.Dataset{}, xerrors.Errorf("failed to expand table config: %w", err)
			}
			tss = append(tss, ts...)
		}

		tss = append(tss, d.TableConfig...)

		ds = append(ds, config.Dataset{
			ID:          dataset.DatasetID,
			TableConfig: tss,
		})
	}

	return ds, nil
}

func (t *FlexTableConfig) expand(ctx context.Context, ds *bq.Dataset) (tc []config.TableConfig, err error) {
	titer := ds.Tables(ctx)
	tables, err := t.filterTable(titer)
	if err != nil {
		return []config.TableConfig{}, xerrors.Errorf("failed to fetch tables: %w", err)
	}

	ts := make([]config.TableConfig, 0)
	processed := make(map[string]struct{})
	for _, tb := range tables {
		table := tablePrefix(tb)
		if _, ok := processed[table]; !ok {
			processed[table] = struct{}{}
			if table == tb.TableID { // non-sharded table (without DateForShards)
				ts = append(ts, config.TableConfig{
					Table:             table,
					TimeThreshold:     t.TimeThreshold,
					DurationThreshold: t.DurationThreshold,
				})
			} else { // sharded table
				ts = append(ts, config.TableConfig{
					Table:             table,
					DateForShards:     t.DateForShards,
					TimeThreshold:     t.TimeThreshold,
					DurationThreshold: t.DurationThreshold,
				})
			}
		}
	}
	return ts, nil
}

// tablePrefix returns table prefix if the table is sharded, table ID otherwise
func tablePrefix(t *bq.Table) string {
	r := regexp.MustCompile(`\d{8}$`) // matches suffix of YYYYMMDD
	if r.MatchString(t.TableID) {
		return string(r.ReplaceAllString(t.TableID, ""))
	}
	return t.TableID
}

// filterDataset returns datasets whose ID has match of a regular expression FlexDataset.ID
func (d *FlexDataset) filterDataset(dsiter *bq.DatasetIterator) (datasets []*bq.Dataset, err error) {
	r := regexp.MustCompile(d.ID)
	datasets = make([]*bq.Dataset, 0)
	for {
		ds, err := dsiter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, xerrors.Errorf("failed to fech datasets: %w", err)
		}

		if r.MatchString(ds.DatasetID) {
			datasets = append(datasets, ds)
		}
	}
	return datasets, nil
}

// filterTable returns tables whose ID has match of a regular expression FlexTableConfig.Table
func (t *FlexTableConfig) filterTable(titer *bq.TableIterator) (tables []*bq.Table, err error) {
	if !t.isValid() {
		return nil, xerrors.Errorf("required field, TimeThreshold or DurationThreshold, is not filled")
	}

	r := regexp.MustCompile(t.Table)
	tables = make([]*bq.Table, 0)
	for {
		tb, err := titer.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, xerrors.Errorf("failed to fech tables: %w", err)
		}

		if r.MatchString(tb.TableID) {
			tables = append(tables, tb)
		}
	}
	return tables, nil
}

// isValid returns false if both TimeThreshold and DurationThreshold is not configured
func (t *FlexTableConfig) isValid() bool {
	timeThresholdIsNil := t.TimeThreshold == nil || t.TimeThreshold.Time == time.Time{}
	durationThresholdIsNil := t.DurationThreshold == nil || t.DurationThreshold.Duration == 0
	return !timeThresholdIsNil || !durationThresholdIsNil
}
