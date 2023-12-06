import pandas as pd
import os

loc_stats = pd.read_csv(os.path.join("staging_data", "location_stations.csv"))
loccat_locs = pd.read_csv(
    os.path.join("staging_data", "locationcategories_locations.csv")
)

station_relations = loc_stats.merge(right=loccat_locs, how="inner", on="locationid")

station_relations.to_csv(
    os.path.join("staging_data", "station_relations.csv"), index=False
)
