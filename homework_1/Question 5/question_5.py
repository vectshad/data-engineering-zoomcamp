import pandas as pd

# Load the dataset
df = pd.read_parquet('green_tripdata_2025-11.parquet')

# Load the zones dataset
df_zones = pd.read_csv('taxi_zone_lookup.csv')

# Ensure the pickup datetime column is of datetime type
df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)

# Filter for trips on 2025-11-18
target_date = '2025-11-18'
daily_trips = df[df.lpep_pickup_datetime.dt.date.astype(str) == target_date]

# Group by PULocationID (Pickup Location) and sum total_amount
zone_totals = daily_trips.groupby('PULocationID')['total_amount'].sum()

# Find the LocationID with the maximum total_amount
max_zone_id = zone_totals.idxmax()

# Find the Zone name corresponding to that LocationID
max_zone_name = df_zones.loc[df_zones['LocationID'] == max_zone_id, 'Zone'].values[0]

print(f"Question 5: Which was the pickup zone with the largest total_amount on {target_date}? \nAnswer: {max_zone_name}")