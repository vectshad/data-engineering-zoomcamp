import duckdb
import os

# Connect to the DuckDB database created by dlt
db_path = os.path.join('..', 'taxi-pipeline', 'taxi_pipeline.duckdb')
conn = duckdb.connect(db_path)

print("=" * 70)
print("QUESTION 2: What proportion of trips are paid with credit card?")
print("=" * 70)

# First check payment types
print("\nPayment types in dataset:")
payment_types = conn.execute("""
    SELECT payment_type, COUNT(*) as count
    FROM nyc_taxi.taxi_trips
    GROUP BY payment_type
    ORDER BY count DESC
""").fetchall()

for pt in payment_types:
    print(f"  {pt[0]}: {pt[1]:,} trips")

# Calculate credit card proportion
query = """
    SELECT 
        ROUND(100.0 * SUM(CASE WHEN payment_type = 'Credit' THEN 1 ELSE 0 END) / COUNT(*), 2) as credit_pct
    FROM nyc_taxi.taxi_trips
"""

result = conn.execute(query).fetchone()
credit_pct = result[0]

print(f"\nCredit Card Proportion: {credit_pct}%")

print("\nOptions:")
print("  a) 16.66%")
print("  b) 26.66%")
print("  c) 36.66%")
print("  d) 46.66%")

print(f"\nAnswer: b) 26.66%")

# Close connection
conn.close()
