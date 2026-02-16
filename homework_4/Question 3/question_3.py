import duckdb

conn = duckdb.connect(r'D:\Eka\Eksplorasi\DE Zoomcamp 2026\homework_4\taxi_rides_ny.duckdb')

# Query the count of records
result = conn.execute("SELECT COUNT(*) FROM prod.fct_monthly_zone_revenue").fetchone()

print(f"Count of records in fct_monthly_zone_revenue: {result[0]:,}")

conn.close()
