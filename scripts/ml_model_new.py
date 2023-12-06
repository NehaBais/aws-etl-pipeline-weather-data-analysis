import xgboost as xgb
import polars as pl
import os

dataset = (
    pl.scan_csv(os.path.join("staging_data", f"training_data.csv"), low_memory=True)
    .select(pl.all().exclude(["quarter", "month", "week"]))
    .with_columns(pl.col("TMAX").cast(pl.Int16).clip(-892, 567))
    .with_columns(pl.col("TMIN").cast(pl.Int16).clip(-892, 567))
    .with_columns(pl.col("PRCP").cast(pl.Int16).clip(0, 6000))
    .with_columns(pl.col("SNOW").cast(pl.Int16).clip(0, 1000))
    .with_columns(pl.col("SNWD").cast(pl.Int16).clip(0, 7000))
    .with_columns((pl.col("year")).cast(pl.UInt8))
    .with_columns(pl.col("day_of_year").cast(pl.UInt16))
    .with_columns(pl.col("is_leap_year").cast(pl.UInt8))
    .with_columns(pl.col("latitude").cast(pl.Float32))
    .with_columns(pl.col("longitude").cast(pl.Float32))
    .with_columns(pl.col("elevation").cast(pl.Float32).fill_null(0))
)

print(dataset.columns)

X = dataset.select(pl.all().exclude(["TMAX", "TMIN", "PRCP", "SNOW", "SNWD"])).collect()
Y = dataset.select(["TMAX", "TMIN", "PRCP", "SNOW", "SNWD"]).collect()
m = xgb.XGBRegressor(
    random_state=42, tree_method="hist", multi_strategy="multi_output_tree"
).fit(X, Y)

print(f"R2 Score for model - ", m.score(X, Y))

m.save_model(os.path.join("staging_data", f"model_xgboost.json"))
