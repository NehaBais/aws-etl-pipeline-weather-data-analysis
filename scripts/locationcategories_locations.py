import pandas as pd
import requests
import time
import sys
import os


BASE_URL = "https://www.ncei.noaa.gov/cdo-web/api/v2/locations"
API_KEY = open(os.path.join("api_keys", "API_KEY"), "r").readlines()[0].strip()
HEADERS = {"token": API_KEY}
METADATA_PARAMS = {"datasetid": "GHCND", "limit": 1, "offset": 0}
BASE_PARAMS = {"datasetid": "GHCND", "limit": 1000}
CSV_PATH = os.path.join("raw_data", "locationcategories_locations.csv")

locationcategories = pd.read_csv(os.path.join("raw_data", "locationcategories.csv"))

locationcategories_locations = []

skip = int(sys.argv[1]) if len(sys.argv) > 1 else 0
for i, locationcategoriesid in enumerate(sorted(locationcategories["id"].tolist())):
    if i < skip:
        continue
    retry = True
    last_offset = None
    while retry:
        metadata_params = {
            "locationcategoryid": locationcategoriesid,
            **METADATA_PARAMS,
        }
        try:
            metadata = requests.get(
                url=BASE_URL, params=metadata_params, headers=HEADERS
            ).json()
        except:
            time.sleep(1)
            continue
        metadata = metadata["metadata"]["resultset"]
        total_count = metadata["count"]

        row = {"locationcategoryid": locationcategoriesid}

        locations = []

        err = False
        begin = ((last_offset // 1000) + 1) * 1000 if last_offset else 0
        for j in range(begin // 1000, (total_count // 1000) + 1):
            offset = j * 1000
            limit = min(1000, total_count - j * 1000)
            params = {
                **BASE_PARAMS,
                "locationcategoryid": locationcategoriesid,
                "offset": offset,
            }
            try:
                data = requests.get(url=BASE_URL, params=params, headers=HEADERS).json()
                data = data["results"]
            except:
                err = True
                break
            for x in data:
                if x["id"] not in locations:
                    locations.append(x["id"])
                    locationcategories_locations.append({"locationid": x["id"], **row})
            print(
                f"locationcategory={i}\toffset={offset}\tlimit={limit}\tcount={len(locationcategories_locations)}"
            )
            last_offset = offset

        if not err:
            retry = False

    if i % 100 == 0:
        df = pd.DataFrame(locationcategories_locations)
        if os.path.exists(CSV_PATH):
            df1 = pd.read_csv(CSV_PATH)
            df = pd.concat([df1, df], ignore_index=True)
            df.drop_duplicates(inplace=True)
        df.to_csv(CSV_PATH, index=False)
        locationcategories_locations = []

df = pd.DataFrame(locationcategories_locations)
if os.path.exists(CSV_PATH):
    df1 = pd.read_csv(CSV_PATH)
    df = pd.concat([df1, df], ignore_index=True)
    df.drop_duplicates(inplace=True)
df.to_csv(CSV_PATH, index=False)
