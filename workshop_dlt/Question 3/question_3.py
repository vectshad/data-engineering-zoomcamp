import duckdb
import os

# Connect to the DuckDB database created by dlt
db_path = os.path.join('..', 'taxi-pipeline', 'taxi_pipeline.duckdb')
conn = duckdb.connect(db_path)

print("=" * 70)
print("QUESTION 3: What is the total amount of money generated in tips?")
print("=" * 70)

# Show tip statistics
print("\nTip statistics:")
tip_stats = conn.execute("""
    SELECT 
        COUNT(*) as total_trips,
        COUNT(tip_amt) as trips_with_tip,
        ROUND(MIN(tip_amt), 2) as min_tip,
        ROUND(MAX(tip_amt), 2) as max_tip,
        ROUND(AVG(tip_amt), 2) as avg_tip
    FROM nyc_taxi.taxi_trips
""").fetchone()

print(f"  Total trips: {tip_stats[0]:,}")
print(f"  Trips with tip data: {tip_stats[1]:,}")
print(f"  Min tip: ${tip_stats[2]}")
print(f"  Max tip: ${tip_stats[3]}")
print(f"  Average tip: ${tip_stats[4]}")

# Calculate total tips
query = """
    SELECT 
        ROUND(SUM(tip_amt), 2) as total_tips
    FROM nyc_taxi.taxi_trips
"""

result = conn.execute(query).fetchone()
total_tips = result[0]

print(f"\nTotal Tips: ${total_tips:,.2f}")

print("\nOptions:")
print("  a) $4,063.41")
print("  b) $6,063.41")
print("  c) $8,063.41")
print("  d) $10,063.41")

print(f"\nAnswer: b) $6,063.41")

# Close connection
conn.close()
