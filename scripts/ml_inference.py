import xgboost as xgb
import pandas as pd
import sys
import os

tmax = xgb.XGBRegressor()
tmax.load_model(os.path.join("staging_data", "tmax_model_xgboost.json"))
tmin = xgb.XGBRegressor()
tmin.load_model(os.path.join("staging_data", "tmin_model_xgboost.json"))
prcp = xgb.XGBRegressor()
prcp.load_model(os.path.join("staging_data", "prcp_model_xgboost.json"))
snow = xgb.XGBRegressor()
snow.load_model(os.path.join("staging_data", "snow_model_xgboost.json"))
snwd = xgb.XGBRegressor()
snwd.load_model(os.path.join("staging_data", "snwd_model_xgboost.json"))

stations = pd.read_csv(os.path.join("staging_data", "stations.csv")).rename(
    columns={"id": "stationid", "name": "station_name"}
)

locations = pd.read_csv(os.path.join("staging_data", "locations.csv")).rename(
    columns={"id": "locationid", "name": "location_name"}
)

locationcategories = pd.read_csv(
    os.path.join("staging_data", "locationcategories.csv")
).rename(columns={"id": "locationcategoryid", "name": "locationcategory_name"})

station_relations = pd.read_csv(os.path.join("staging_data", "station_relations.csv"))

station_relations = (
    station_relations.merge(stations, on="stationid", how="inner")
    .merge(locations, on="locationid", how="inner")
    .merge(locationcategories, on="locationcategoryid", how="inner")
)

locationcategory_name = sys.argv[1].lower()
location_name = sys.argv[2].lower()
start_date = pd.to_datetime(sys.argv[3])
duration = int(sys.argv[4])
date_index = pd.date_range(
    start_date, start_date + pd.Timedelta(duration, "d"), freq="d"
)

filtered_stations = station_relations.loc[
    (
        station_relations["locationcategory_name"]
        .str.lower()
        .str.contains(locationcategory_name)
    )
    & (station_relations["location_name"].str.lower().str.contains(location_name)),
    ["stationid", "latitude", "longitude", "elevation", "location_name"],
].drop_duplicates()

dates = pd.DataFrame(
    [
        {
            "dateid": int(
                f"{date_index[i].year}{str(date_index[i].month).zfill(2)}{str(date_index[i].day).zfill(2)}"
            ),
            "date": date_index[i].__str__().split(" ")[0],
            "year": date_index[i].year - 2010,
            "quarter": date_index[i].quarter,
            "month": date_index[i].month,
            "week": date_index[i].week,
            "day_of_year": date_index[i].day_of_year,
            "is_leap_year": int(date_index[i].is_leap_year),
        }
        for i in range(len(date_index))
    ]
)

ref = dates.join(filtered_stations, how="cross")
X_test = ref.drop(columns=["dateid", "date", "stationid", "location_name"])
Y_tmin = pd.DataFrame((tmin.predict(X_test) / 10).reshape(-1, 1), columns=["pred_tmin"])
Y_tmax = pd.DataFrame((tmax.predict(X_test) / 10).reshape(-1, 1), columns=["pred_tmax"])
Y_prcp = pd.DataFrame((prcp.predict(X_test) / 10).reshape(-1, 1), columns=["pred_prcp"])
Y_snow = pd.DataFrame((snow.predict(X_test) / 10).reshape(-1, 1), columns=["pred_snow"])
Y_snwd = pd.DataFrame((snwd.predict(X_test) / 10).reshape(-1, 1), columns=["pred_snwd"])

out = pd.concat(
    [
        ref.drop(
            columns=[
                "dateid",
                "year",
                "quarter",
                "month",
                "week",
                "day_of_year",
                "is_leap_year",
                "stationid",
                "latitude",
                "longitude",
                "elevation",
            ]
        ),
        Y_tmin,
        Y_tmax,
        Y_prcp,
        Y_snow,
        Y_snwd,
    ],
    axis=1,
    ignore_index=True,
)
out.columns = ["Date", "Location", "TMIN", "TMAX", "PRCP", "SNOW", "SNWD"]
out = out.groupby(["Location", "Date"]).mean().reset_index()
print(out)
