import pandas as pd
import polars as pl
import sys
import os


stations = pd.read_csv(os.path.join("staging_data", "stations.csv"))
station_ids = stations["id"].unique().tolist()
datatype_ids = ["PRCP", "SNOW", "SNWD", "TMAX", "TMIN"]

start_date = pd.to_datetime(sys.argv[1])
end_date = pd.to_datetime(sys.argv[2])

start_date = int(
    f"{start_date.year}{str(start_date.month).zfill(2)}{str(start_date.day).zfill(2)}"
)
end_date = int(
    f"{end_date.year}{str(end_date.month).zfill(2)}{str(end_date.day).zfill(2)}"
)

for i in range(2010, 2024):
    table_name = f"{i}.csv"
    print(f"start processing {table_name}")
    _ = (
        pl.scan_csv(os.path.join(os.path.join("raw_data", "archive_data"), table_name))
        .drop(columns=["M_FLAG", "Q_FLAG", "S_FLAG", "OBS_TIME"])
        .filter(pl.col("ID").is_in(station_ids))
        .filter(pl.col("ELEMENT").is_in(datatype_ids))
        .filter(pl.col("DATE").ge(start_date))
        .filter(pl.col("DATE").le(end_date))
        .rename(
            {
                "ID": "stationid",
                "DATE": "dateid",
                "ELEMENT": "datatypeid",
                "DATA_VALUE": "value",
            }
        )
        .collect()
        .write_csv(os.path.join("staging_data", table_name))
    )
    print(f"stop processing {table_name}")
