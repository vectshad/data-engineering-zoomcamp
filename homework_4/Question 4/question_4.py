import duckdb

conn = duckdb.connect(r'D:\Eka\Eksplorasi\DE Zoomcamp 2026\homework_4\taxi_rides_ny.duckdb')

# Query to find the pickup zone with highest revenue for Green taxis in 2020
result = conn.execute("""
SELECT 
    pickup_zone,
    SUM(revenue_monthly_total_amount) as total_revenue
FROM prod.fct_monthly_zone_revenue
WHERE service_type = 'Green' 
    AND EXTRACT(YEAR FROM revenue_month) = 2020
GROUP BY pickup_zone
ORDER BY total_revenue DESC
LIMIT 10
""").fetchall()

print("Top 10 pickup zones by revenue for Green taxis in 2020:\n")
for i, (zone, revenue) in enumerate(result, 1):
    print(f"{i}. {zone}: ${revenue:,.2f}")

conn.close()
