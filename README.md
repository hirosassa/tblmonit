# tblmonit

Monitoring tool for BigQuery table's metadata

## Usage

### Set config file

You can set timezone in `$HOME/.tblmonit.yaml`, or set `--config` option.

The default timezone is Local.
And, the name is taken to be a location name corresponding to a file in the IANA Time Zone database, such as `America/New_York`.

Example:

```yaml
timeZone: Asia/Tokyo
```

### Check freshness of tables

First of all, you need to prepare configuration file for listing target tables to monitor in TOML format like below:

```
[[Project]]
    ID = "bigquery-project-id-1"
    [[Project.Dataset]]
        ID = "dataset1"
        [[Project.Dataset.TableConfig]]
            Table = "table1"
            DateForShards = ""
            Timethreshold = "09:00:00"
            DurationThreshold = "24h"
    [[Project.Dataset]]
        ID = "dataset2"
        [[Project.Dataset.TableConfig]]
            Table = "sharded_table2_on_"
            DateForShards = "ONE_DAY_AGO"
            Timethreshold = "12:00:00"
            DurationThreshold = "1h"
[[Project]]
    Name = "bigquery-project-id-2"
    [[Project.Dataset]]
        ID = "dataset3"
        [[Project.Dataset.TableConfig]]
            Table = "table1"
            DateForShards = ""
            Timethreshold = "09:00:00"
            DurationThreshold = "24h"
```

Then, run command as follows:
```
tblmonit freshness [target config file]
```

If current time is passed `TimeThreshold` and the target table's last modified date is older than `DurationThreshold`(or the table is not found), then `tblmonit` outputs a list of such tables in following format

```
bigquery-project-id-1.dataset1.sharded_table2_on_20200101
bigquery-project-id-2.dataset3.table2
```

`DateForShards` is for sharded table partitioned by date (tables' suffix should be YYYYMMDD format).

`DateForShards` should be one of `ONE_DAY_AGO`, `TODAY`, `FIRST_DAY_OF_THE_MONTH`.

### Flexible configuration (experimental)

**This feature is under experimental**

Editing configuration file manually usually cause errors.
To specify list of tables and thresholds easily, you can use FlexConfig DSL and expand command.

First of all, you should prepare toml file like below:

```
[[FlexProject]]
    ID = "bigquery-project-id-1"
    [[FlexProject.FlexDataset]]
        ID = "dataset1" # you can use regular expression
        [[FlexProject.FlexDataset.FlexTableConfig]]
            FlexTable = "*"  # you can use regular expression to specify tables
            DateForShards = "ONE_DAY_AGO"
            Timethreshold = "09:00:00" # must specify
            DurationThreshold = "24h" # must specify
[[FlexProject]]
    ID = "bigquery-project-id-2"
    [[FlexProject.Dataset]] # not FlexDataset for exact
        ID = "dataset1"
        [[FlexProject.Dataset.TableConfig]]
            FlexTable = "*"
            DateForShards = "ONE_DAY_AGO"
            Timethreshold = "09:00:00" # must specify
            DurationThreshold = "24h" # must specify
```

And then, run following command

```
tblmonit config expand [target config file]
```

As a result, the command outputs following config file which is acceptable by `tblmonit freshness` command

```
[[Project]]
  ID = "bigquery-project-id-1"

  [[Project.Dataset]]
    ID = "dataset1"

    [[Project.Dataset.TableConfig]]
      Table = "table1"
      DateForShards = ""
      TimeThreshold = "2020-12-13T09:00:00+09:00"
      DurationThreshold = "24h0m0s
    [[Project.Dataset.TableConfig]]
      Table = "sharded_table2_on_"
      DateForShards = "ONE_DAY_AGO"
      TimeThreshold = "2020-12-13T09:00:00+09:00"
      DurationThreshold = "24h0m0s
```
