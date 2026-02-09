"""
Download Yellow Taxi data locally (no GCP credentials needed)
Then you'll upload manually via GCP Console
"""
import requests
import os

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
FILES = [
    "yellow_tripdata_2024-01.parquet",
    "yellow_tripdata_2024-02.parquet",
    "yellow_tripdata_2024-03.parquet",
    "yellow_tripdata_2024-04.parquet",
    "yellow_tripdata_2024-05.parquet",
    "yellow_tripdata_2024-06.parquet",
]

def download_file(url, filename):
    """Download file from URL"""
    print(f"Downloading {filename}...")
    response = requests.get(url, stream=True)
    response.raise_for_status()
    
    total_size = int(response.headers.get('content-length', 0))
    downloaded = 0
    
    with open(filename, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
            downloaded += len(chunk)
            if total_size > 0:
                percent = (downloaded / total_size) * 100
                print(f"\rProgress: {percent:.1f}%", end='')
    
    print(f"\n✓ Downloaded {filename}")

def main():
    # Create directory
    os.makedirs("yellow_taxi_2024", exist_ok=True)
    
    for file in FILES:
        url = BASE_URL + file
        local_path = os.path.join("yellow_taxi_2024", file)
        
        if os.path.exists(local_path):
            print(f"✓ {file} already exists, skipping...")
            continue
            
        download_file(url, local_path)
    
    print("\n" + "="*50)
    print("All files downloaded to yellow_taxi_2024/")
    print("="*50)
    print("\nNext steps:")
    print("1. Go to https://console.cloud.google.com/storage")
    print("2. Click on bucket: de-zoomcamp-taxi-data")
    print("3. Click 'UPLOAD FILES'")
    print("4. Select all 6 parquet files from yellow_taxi_2024/ folder")
    print("5. Wait for upload to complete")

if __name__ == "__main__":
    main()
