import xgboost as xgb
import polars as pl
import sys
import os

datatype = sys.argv[1].lower()

dataset = (
    pl.scan_csv(os.path.join("staging_data", f"clean{datatype.upper()}.csv"), low_memory=True)
    .with_columns(pl.col("value").cast(pl.Int16).clip(-892, 567))
    .with_columns((pl.col("year") - 2010).cast(pl.UInt8))
    .with_columns(pl.col("quarter").cast(pl.UInt8))
    .with_columns(pl.col("month").cast(pl.UInt8))
    .with_columns(pl.col("week").cast(pl.UInt8))
    .with_columns(pl.col("day_of_year").cast(pl.UInt16))
    .with_columns(pl.col("is_leap_year").cast(pl.UInt8))
    .with_columns(pl.col("latitude").cast(pl.Float32))
    .with_columns(pl.col("longitude").cast(pl.Float32))
    .with_columns(pl.col("elevation").cast(pl.Float32).fill_null(0))
)

X = dataset.select(pl.all().exclude("value")).collect()
Y = dataset.select(pl.col("value")).collect()
m = xgb.XGBRegressor(random_state=42).fit(X, Y)

print(f"R2 Score for {datatype} - ", m.score(X, Y))

m.save_model(os.path.join("staging_data", f"{datatype}_model_xgboost.json"))
