import pandas as pd

# Load the dataset
df = pd.read_parquet('green_tripdata_2025-11.parquet')

# Ensure the pickup datetime column is of datetime type
df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)

# Filter for trips in November 2025
november_trips_df = df[(df.lpep_pickup_datetime >= '2025-11-01') & (df.lpep_pickup_datetime < '2025-12-01')]

# Filter for trips with a trip_distance of less than 100 miles
valid_trips_df = november_trips_df[november_trips_df.trip_distance < 100]

# Group by pickup date and calculate the sum of trip_distance for each day
daily_distance = valid_trips_df.groupby(valid_trips_df['lpep_pickup_datetime'].dt.date)['trip_distance'].sum()

# Find the date with the maximum total trip distance
longest_trip_date = daily_distance.idxmax()

# Print the date with the longest trip distance
print(f"Question 4: Which was the pick up day with the longest trip distance? \nAnswer: {longest_trip_date}")