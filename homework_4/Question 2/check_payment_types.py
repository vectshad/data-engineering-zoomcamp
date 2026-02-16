import duckdb

conn = duckdb.connect(r'D:\Eka\Eksplorasi\DE Zoomcamp 2026\homework_4\taxi_rides_ny.duckdb')

# Check payment types in green taxi data
result = conn.execute("SELECT DISTINCT payment_type FROM dev.stg_green_tripdata ORDER BY payment_type").fetchall()
print("Payment types in green taxi data:", [r[0] for r in result])

# Check payment types in yellow taxi data
result = conn.execute("SELECT DISTINCT payment_type FROM dev.stg_yellow_tripdata ORDER BY payment_type").fetchall()
print("Payment types in yellow taxi data:", [r[0] for r in result])

# Check payment type lookup table
result = conn.execute("SELECT payment_type, description FROM dev.payment_type_lookup ORDER BY payment_type").fetchall()
print("\nPayment type descriptions:")
for pt, desc in result:
    print(f"  {pt}: {desc}")

conn.close()
