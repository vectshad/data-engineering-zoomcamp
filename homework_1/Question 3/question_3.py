import pandas as pd

# Load the parquet file into a DataFrame
df = pd.read_parquet('green_tripdata_2025-11.parquet')

# Ensure the pickup datetime column is of datetime type
df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)

# Filter for trips in November 2025
november_trips_df = df[(df.lpep_pickup_datetime >= '2025-11-01') & (df.lpep_pickup_datetime < '2025-12-01')]

# Filter for trips with a trip_distance of less than or equal to 1 mile
short_trips_df = november_trips_df[november_trips_df.trip_distance <= 1]

# Get the count
trip_count = len(short_trips_df)

print(f"Question 3: How many trips had a trip_distance of less than or equal to 1 mile? \nAnswer: {trip_count}")
