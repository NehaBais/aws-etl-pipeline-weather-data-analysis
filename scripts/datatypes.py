import pandas as pd
import requests
import time
import os

BASE_URL = "https://www.ncei.noaa.gov/cdo-web/api/v2/datatypes"
API_KEY = open(os.path.join("api_keys", "API_KEY"), "r").readlines()[0].strip()
HEADERS = {"token": API_KEY}
METADATA_PARAMS = {"datasetid": "GHCND", "limit": 1, "offset": 0}
BASE_PARAMS = {"datasetid": "GHCND", "limit": 1000}
CSV_PATH = os.path.join("raw_data", "datatypes.csv")

datatypes = []

retry = True
last_offset = None
while retry:
    try:
        metadata = requests.get(
            url=BASE_URL, params=METADATA_PARAMS, headers=HEADERS
        ).json()
    except:
        time.sleep(1)
        continue
    metadata = metadata["metadata"]["resultset"]
    total_count = metadata["count"]

    err = False
    begin = ((last_offset // 1000) + 1) * 1000 if last_offset else 0
    for i in range(begin // 1000, (total_count // 1000) + 1):
        offset = i * 1000
        limit = min(1000, total_count - i * 1000)
        params = {"offset": offset, **BASE_PARAMS}
        try:
            data = requests.get(url=BASE_URL, params=params, headers=HEADERS).json()
            data = data["results"]
        except Exception as error:
            err = True
            break
        for x in data:
            if x not in datatypes:
                datatypes.append(x)
        print(f"offset={offset}\tlimit={limit}\tcount={len(datatypes)}")
        last_offset = offset

    if not err:
        retry = False

df = pd.DataFrame(datatypes)
if os.path.exists(CSV_PATH):
    df1 = pd.read_csv(CSV_PATH)
    df = pd.concat([df1, df], ignore_index=True)
    df.drop_duplicates(inplace=True)
df.to_csv(
    CSV_PATH,
    columns=["id", "name", "mindate", "maxdate", "datacoverage"],
    index=False,
)