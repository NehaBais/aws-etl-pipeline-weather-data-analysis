import pandas as pd
import requests
import time
import sys
import os


BASE_URL = "https://www.ncei.noaa.gov/cdo-web/api/v2/stations"
API_KEY = open("API_KEY", "r").readlines()[0].strip()
HEADERS = {"token": API_KEY}
METADATA_PARAMS = {"datasetid": "GHCND", "limit": 1, "offset": 0}
BASE_PARAMS = {"datasetid": "GHCND", "limit": 1000}
CSV_PATH = os.path.join("raw_data", "location_stations.csv")

locations = pd.read_csv(os.path.join("raw_data", "locations.csv"))

location_stations = []

skip = int(sys.argv[1]) if len(sys.argv) > 1 else 0
for i, locationid in enumerate(sorted(locations["id"].tolist())):
    if i < skip:
        continue
    retry = True
    last_offset = None
    while retry:
        metadata_params = {"locationid": locationid, **METADATA_PARAMS}
        try:
            metadata = requests.get(
                url=BASE_URL, params=metadata_params, headers=HEADERS
            ).json()
        except:
            time.sleep(1)
            continue
        metadata = metadata["metadata"]["resultset"]
        total_count = metadata["count"]

        row = {"locationid": locationid}

        stations = []

        err = False
        begin = ((last_offset // 1000) + 1) * 1000 if last_offset else 0
        for j in range(begin // 1000, (total_count // 1000) + 1):
            offset = j * 1000
            limit = min(1000, total_count - j * 1000)
            params = {**BASE_PARAMS, "locationid": locationid, "offset": offset}
            try:
                data = requests.get(url=BASE_URL, params=params, headers=HEADERS).json()
                data = data["results"]
            except:
                err = True
                break
            for x in data:
                if x["id"] not in stations:
                    stations.append(x["id"])
                    location_stations.append({"stationid": x["id"], **row})
            print(
                f"location={i}\toffset={offset}\tlimit={limit}\tcount={len(location_stations)}"
            )
            last_offset = offset

        if not err:
            retry = False

    if i % 100 == 0:
        df = pd.DataFrame(location_stations)
        if os.path.exists(CSV_PATH):
            df1 = pd.read_csv(CSV_PATH)
            df = pd.concat([df1, df], ignore_index=True)
            df.drop_duplicates(inplace=True)
        df.to_csv(CSV_PATH, index=False)
        location_stations = []

df = pd.DataFrame(location_stations)
if os.path.exists(CSV_PATH):
    df1 = pd.read_csv(CSV_PATH)
    df = pd.concat([df1, df], ignore_index=True)
    df.drop_duplicates(inplace=True)
df["stationid"] = df["stationid"].str.split(":").str[1]
df.to_csv(CSV_PATH, index=False)
