import requests
import duckdb
import gzip
import shutil
from pathlib import Path

# Create data directory
data_dir = Path(r'D:\Eka\Eksplorasi\DE Zoomcamp 2026\homework_4\data')
data_dir.mkdir(exist_ok=True)

print("Downloading FHV 2019 data...\n")

# Download all 12 months of FHV 2019 data
base_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv"
months = range(1, 13)

for month in months:
    filename = f"fhv_tripdata_2019-{month:02d}.csv.gz"
    url = f"{base_url}/{filename}"
    filepath = data_dir / filename
    
    if filepath.exists():
        print(f"✓ {filename} already exists, skipping...")
        continue
    
    print(f"Downloading {filename}... ", end="", flush=True)
    response = requests.get(url, stream=True)
    
    if response.status_code == 200:
        with open(filepath, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        print(f"Done ({filepath.stat().st_size / 1024 / 1024:.1f} MB)")
    else:
        print(f"Failed (status {response.status_code})")

print("\nLoading FHV data into DuckDB...")

# Connect to DuckDB
conn = duckdb.connect(r'D:\Eka\Eksplorasi\DE Zoomcamp 2026\homework_4\taxi_rides_ny.duckdb')

# Create table from all CSV files
conn.execute("CREATE SCHEMA IF NOT EXISTS prod")
conn.execute("DROP TABLE IF EXISTS prod.fhv_tripdata")

conn.execute(f"""
CREATE TABLE prod.fhv_tripdata AS
SELECT * FROM read_csv(
    '{str(data_dir)}/fhv_tripdata_2019-*.csv.gz',
    auto_detect=true,
    union_by_name=true
)
""")

# Check total count
total_count = conn.execute("SELECT COUNT(*) FROM prod.fhv_tripdata").fetchone()[0]
print(f"Total FHV records loaded: {total_count:,}")

# Check count with dispatching_base_num IS NOT NULL
filtered_count = conn.execute("""
    SELECT COUNT(*) 
    FROM prod.fhv_tripdata 
    WHERE dispatching_base_num IS NOT NULL
""").fetchone()[0]
print(f"Records where dispatching_base_num IS NOT NULL: {filtered_count:,}")

conn.close()

print("\nDone! Now create the staging model stg_fhv_tripdata.sql")
