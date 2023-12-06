import pandas as pd
import sys
import os


start_date = pd.to_datetime(sys.argv[1])
end_date = pd.to_datetime(sys.argv[2])

stations = pd.read_csv(os.path.join("staging_data", "stations.csv")).drop(
    columns=["name", "latitude", "longitude", "elevation"]
)

date_index = pd.date_range(start_date, end_date, freq="d")
dates = pd.DataFrame(
    [
        {
            "dateid": int(
                f"{date_index[i].year}{str(date_index[i].month).zfill(2)}{str(date_index[i].day).zfill(2)}"
            )
        }
        for i in range(len(date_index))
    ]
)

stations_dates = stations.join(dates, how="cross")
stations_dates.to_csv(os.path.join("staging_data", "stations_dates.csv"), index=False)
