import duckdb
import os

# Connect to the DuckDB database created by dlt
db_path = os.path.join('..', 'taxi-pipeline', 'taxi_pipeline.duckdb')
conn = duckdb.connect(db_path)

# Query the date range
query = """
    SELECT 
        MIN(trip_pickup_date_time) as start_date,
        MAX(trip_pickup_date_time) as end_date
    FROM nyc_taxi.taxi_trips
"""

result = conn.execute(query).fetchone()
start_date = result[0]
end_date = result[1]

print("=" * 70)
print("QUESTION 1: What is the start date and end date of the dataset?")
print("=" * 70)
print(f"Start Date: {start_date}")
print(f"End Date: {end_date}")

print("\nOptions:")
print("  a) 2009-01-01 to 2009-01-31")
print("  b) 2009-06-01 to 2009-07-01")
print("  c) 2024-01-01 to 2024-02-01")
print("  d) 2024-06-01 to 2024-07-01")

print("\nAnswer: b) 2009-06-01 to 2009-07-01")

# Close connection
conn.close()