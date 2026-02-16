import duckdb

conn = duckdb.connect(r'D:\Eka\Eksplorasi\DE Zoomcamp 2026\homework_4\taxi_rides_ny.duckdb')

# Count records in stg_fhv_tripdata  
result = conn.execute("SELECT COUNT(*) FROM dev.stg_fhv_tripdata").fetchone()

print(f"Count of records in stg_fhv_tripdata: {result[0]:,}")

conn.close()
