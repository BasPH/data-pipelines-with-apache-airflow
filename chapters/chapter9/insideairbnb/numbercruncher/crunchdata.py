import io
import os

import pandas as pd
from minio import Minio

S3_ENDPOINT = os.environ["S3_ENDPOINT"]
S3_ACCESS_KEY = os.environ["S3_ACCESS_KEY"]
S3_SECRET_KEY = os.environ["S3_SECRET_KEY"]

client = Minio(
    S3_ENDPOINT, access_key=S3_ACCESS_KEY, secret_key=S3_SECRET_KEY, secure=False
)

# Get list of all objects
objects = [
    obj.object_name
    for obj in client.list_objects(bucket_name="inside-airbnb", prefix="listing")
]
df = pd.DataFrame()
for obj in objects:
    response = client.get_object(bucket_name="inside-airbnb", object_name=obj)
    temp_df = pd.read_csv(
        io.BytesIO(response.read()),
        usecols=["id", "price", "download_date"],
        parse_dates=["download_date"],
    )
    df = df.append(temp_df)

# Per id, get the price increase/decrease
# There's probably a nicer way to do this
min_max_per_id = (
    df.groupby(["id"])
    .agg(
        download_date_min=("download_date", "min"),
        download_date_max=("download_date", "max"),
    )
    .reset_index()
)
df_with_min = (
    pd.merge(
        min_max_per_id,
        df,
        how="left",
        left_on=["id", "download_date_min"],
        right_on=["id", "download_date"],
    )
    .rename(columns={"price": "oldest_price"})
    .drop("download_date", axis=1)
)
df_with_max = (
    pd.merge(
        df_with_min,
        df,
        how="left",
        left_on=["id", "download_date_max"],
        right_on=["id", "download_date"],
    )
    .rename(columns={"price": "latest_price"})
    .drop("download_date", axis=1)
)

df_with_max = df_with_max[
    df_with_max["download_date_max"] != df_with_max["download_date_min"]
]
df_with_max["price_diff_per_day"] = (
    df_with_max["latest_price"] - df_with_max["oldest_price"]
) / ((df_with_max["download_date_max"] - df_with_max["download_date_min"]).dt.days)
df_with_max[["price_diff_per_day"]] = df_with_max[["price_diff_per_day"]].apply(
    pd.to_numeric
)
biggest_increase = df_with_max.nlargest(5, "price_diff_per_day")
biggest_decrease = df_with_max.nsmallest(5, "price_diff_per_day")

# We found the top 5, write back the results.
biggest_increase_json = biggest_increase.to_json(orient="records")
print(f"Biggest increases: {biggest_increase_json}")
biggest_increase_bytes = biggest_increase_json.encode("utf-8")
client.put_object(
    bucket_name="inside-airbnb",
    object_name="results/biggest_increase.json",
    data=io.BytesIO(biggest_increase_bytes),
    length=len(biggest_increase_bytes),
)

biggest_decrease_json = biggest_decrease.to_json(orient="records")
print(f"Biggest decreases: {biggest_decrease_json}")
biggest_decrease_bytes = biggest_decrease_json.encode("utf-8")
client.put_object(
    bucket_name="inside-airbnb",
    object_name="results/biggest_decrease.json",
    data=io.BytesIO(biggest_decrease_bytes),
    length=len(biggest_decrease_bytes),
)
