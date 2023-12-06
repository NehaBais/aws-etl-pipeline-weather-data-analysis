import polars as pl
import sys
import os

datatype = sys.argv[1].upper()

pl.scan_csv(os.path.join("staging_data", f"raw{datatype}.csv")).join(
    pl.scan_csv("staging_data/dates.csv")
    .drop(columns=["month_name", "day_name", "date", "weekday", "day"])
    .with_columns(pl.col("year").cast(pl.UInt16))
    .with_columns(pl.col("quarter").cast(pl.UInt8))
    .with_columns(pl.col("month").cast(pl.UInt8))
    .with_columns(pl.col("week").cast(pl.UInt8))
    .with_columns(pl.col("day_of_year").cast(pl.UInt16))
    .with_columns(pl.col("is_leap_year").cast(pl.UInt8)),
    on="dateid",
    how="left",
).drop(columns=["dateid"]).join(
    pl.scan_csv("staging_data/stations.csv")
    .drop(columns=["name"])
    .with_columns(pl.col("latitude").cast(pl.Float32))
    .with_columns(pl.col("longitude").cast(pl.Float32))
    .with_columns(pl.col("elevation").cast(pl.Float32)),
    left_on="stationid",
    right_on="id",
    how="left",
).drop(
    columns=["stationid"]
).collect(
    streaming=True
).write_csv(
    os.path.join("staging_data", f"clean{datatype}.csv")
)
