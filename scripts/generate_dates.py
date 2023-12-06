import pandas as pd
import sys
import os


start_date = pd.to_datetime(sys.argv[1])
end_date = pd.to_datetime(sys.argv[2])

date_index = pd.date_range(start_date, end_date, freq="d")
dates = []

for i in range(len(date_index)):
    dates.append(
        {
            "dateid": int(
                f"{date_index[i].year}{str(date_index[i].month).zfill(2)}{str(date_index[i].day).zfill(2)}"
            ),
            "date": date_index[i].__str__().split(" ")[0],
            "year": date_index[i].year,
            "quarter": date_index[i].quarter,
            "month": date_index[i].month,
            "week": date_index[i].week,
            "weekday": date_index[i].weekday() + 1,
            "day_of_year": date_index[i].day_of_year,
            "day": date_index[i].day,
            "month_name": date_index[i].month_name(),
            "day_name": date_index[i].day_name(),
            "is_leap_year": date_index[i].is_leap_year,
        }
    )

pd.DataFrame(dates).to_csv(os.path.join("staging_data", "dates.csv"), index=False)
