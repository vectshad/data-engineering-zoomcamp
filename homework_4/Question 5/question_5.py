import duckdb

conn = duckdb.connect(r'D:\Eka\Eksplorasi\DE Zoomcamp 2026\homework_4\taxi_rides_ny.duckdb')

# Query total trips for Green taxis in October 2019
result = conn.execute("""
SELECT 
    SUM(total_monthly_trips) as total_trips
FROM prod.fct_monthly_zone_revenue
WHERE service_type = 'Green' 
    AND EXTRACT(YEAR FROM revenue_month) = 2019
    AND EXTRACT(MONTH FROM revenue_month) = 10
""").fetchone()

print(f"Total Green taxi trips in October 2019: {result[0]:,}")

conn.close()
