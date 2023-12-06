import pandas as pd
import sys
import os


locations = pd.read_csv(os.path.join("raw_data", "locations.csv"))
locations["mindate"] = pd.to_datetime(locations["mindate"])
locations["maxdate"] = pd.to_datetime(locations["maxdate"])

stations = pd.read_csv(os.path.join("raw_data", "stations.csv"))
stations["mindate"] = pd.to_datetime(stations["mindate"])
stations["maxdate"] = pd.to_datetime(stations["maxdate"])

datatypes = pd.read_csv(os.path.join("raw_data", "datatypes.csv"))
datatypes["mindate"] = pd.to_datetime(datatypes["mindate"])
datatypes["maxdate"] = pd.to_datetime(datatypes["maxdate"])

locationcategories = pd.read_csv(os.path.join("raw_data", "locationcategories.csv"))

loc_stats = pd.read_csv(os.path.join("raw_data", "location_stations.csv"))
loccat_locs = pd.read_csv(os.path.join("raw_data", "locationcategories_locations.csv"))

start_date = pd.to_datetime(sys.argv[1])
end_date = pd.to_datetime(sys.argv[2])


print(f"Number of rows in locations before date filter - {len(locations)}")
locations = locations[
    (locations["mindate"] <= end_date) & (locations["maxdate"] >= start_date)
]
locations = locations[
    (locations["mindate"] <= start_date) & (locations["maxdate"] >= end_date)
]
print(f"Number of rows in locations after date filter - {len(locations)}")


print(f"Number of rows in stations before date filter - {len(stations)}")
stations = stations[
    (stations["mindate"] <= end_date) & (stations["maxdate"] >= start_date)
]
stations = stations[
    (stations["mindate"] <= start_date) & (stations["maxdate"] >= end_date)
]
print(f"Number of rows in stations after date filter - {len(stations)}")


print(f"Number of rows in datatypes before date filter - {len(datatypes)}")
datatypes = datatypes[
    (datatypes["mindate"] <= end_date) & (datatypes["maxdate"] >= start_date)
]
datatypes = datatypes[
    (datatypes["mindate"] <= start_date) & (datatypes["maxdate"] >= end_date)
]
print(f"Number of rows in datatypes after date filter - {len(datatypes)}")


stationids = stations["id"].unique().tolist()
locationids = locations["id"].unique().tolist()
locationcategoriesids = locationcategories["id"].unique().tolist()


print(f"Number of rows in loc_stats before date filter - {len(loc_stats)}")
loc_stats = loc_stats[
    (loc_stats["locationid"].isin(locationids))
    & (loc_stats["stationid"].isin(stationids))
]
print(f"Number of rows in loc_stats after date filter - {len(loc_stats)}")


print(f"Number of rows in loccat_locs before date filter - {len(loccat_locs)}")
loccat_locs = loccat_locs[
    (loccat_locs["locationcategoryid"].isin(locationcategoriesids))
    & (loccat_locs["locationid"].isin(locationids))
]
print(f"Number of rows in loccat_locs after date filter - {len(loccat_locs)}")


# locations.to_csv(os.path.join("staging_data", "locations.csv"), index=False)
# stations.to_csv(os.path.join("staging_data", "stations.csv"), index=False)
# datatypes.to_csv(os.path.join("staging_data", "datatypes.csv"), index=False)

locations.drop(columns=["mindate", "maxdate", "datacoverage"]).to_csv(
    os.path.join("staging_data", "locations.csv"), index=False
)
stations.drop(columns=["mindate", "maxdate", "datacoverage", "elevationUnit"]).to_csv(
    os.path.join("staging_data", "stations.csv"), index=False
)
datatypes.drop(columns=["mindate", "maxdate", "datacoverage"]).to_csv(
    os.path.join("staging_data", "datatypes.csv"), index=False
)
locationcategories.to_csv(
    os.path.join("staging_data", "locationcategories.csv"), index=False
)
loc_stats.to_csv(os.path.join("staging_data", "location_stations.csv"), index=False)
loccat_locs.to_csv(
    os.path.join("staging_data", "locationcategories_locations.csv"), index=False
)
