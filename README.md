# tblmonit

Monitoring tool for BigQuery table's metadata

## Usage

### Check freshness of tables

First of all, you need to prepare configuration file (currently the file name should be `.tblmonit.toml` at your home directory) for listing target tables to monitor in TOML format like below:

```
[[Project]]
    Name = "bigquery-project-id-1"
    [[Project.TableConfig]]
        Table = "dataset1.table1"
        DateForShards = ""
        Timethreshold = "09:00:00"
    [[Project.TableConfig]]
        Table = "dataset2.sharded_table2_on_"
        DateForShards = "ONE_DAY_AGO"
        Timethreshold = "09:00:00"
[[Project]]
    Name = "bigquery-project-id-2"
    [[Project.TableConfig]]
        Table = "dataset3.table2"
        Timethreshold = "08:00:00"
```

Then, run command as follows:
```
tblmonit freshness
```

If the target tables is older than each `Timethreshold`(or the table is not found), then `tblmonit` outputs a list of such tables like

```
bigquery-project-id-1
    dataset1.sharded_table2_on_20200101
bigquery-project-id-2
    dataset3.table2
```

`DateForShards` is for sharded table partitioned by date (tables' suffix should be YYYYMMDD format).

`DateForShards` should be one of `ONE_DAY_AGO`, `TODAY`, `FIRST_DAY_OF_THE_MONTH`.

