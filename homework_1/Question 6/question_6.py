import pandas as pd
from pathlib import Path

# Get the directory where this script is located
script_dir = Path(__file__).parent

# Load the dataset
df = pd.read_parquet('green_tripdata_2025-11.parquet')

# Load the zones dataset
df_zones = pd.read_csv('taxi_zone_lookup.csv')

# 1. Find the LocationID for "East Harlem North"
pickup_zone_name = "East Harlem North"
pickup_zone_id = df_zones.loc[df_zones['Zone'] == pickup_zone_name, 'LocationID'].values[0]

# 2. Filter for trips picked up in "East Harlem North"
df['lpep_pickup_datetime'] = pd.to_datetime(df['lpep_pickup_datetime'])
november_trips = df[(df['lpep_pickup_datetime'] >= '2025-11-01') & (df['lpep_pickup_datetime'] < '2025-12-01')]

# Filter by Pickup Location ID
ehn_trips = november_trips[november_trips['PULocationID'] == pickup_zone_id]

# 3. Find the trip with the largest tip
max_tip_index = ehn_trips['tip_amount'].idxmax()
largest_tip_trip = ehn_trips.loc[max_tip_index]

# 4. Get the Dropoff Location ID for that trip
dropoff_zone_id = largest_tip_trip['DOLocationID']

# 5. Lookup the Zone name for the Dropoff Location
dropoff_zone_name = df_zones.loc[df_zones['LocationID'] == dropoff_zone_id, 'Zone'].values[0]

print(f"Question 6: For passengers picked up in '{pickup_zone_name}', which was the drop off zone that had the largest tip? \nAnswer: {dropoff_zone_name}")
print(f"(Tip amount: {largest_tip_trip['tip_amount']})")