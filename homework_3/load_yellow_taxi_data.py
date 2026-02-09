"""
Script to download and upload Yellow Taxi data to GCS bucket
Handles January 2024 - June 2024 parquet files
"""
import requests
from google.cloud import storage
import os

# Configuration
BUCKET_NAME = "de-zoomcamp-taxi-data"
PROJECT_ID = "de-zoomcamp-2026-486908"

# Yellow taxi data URLs (Jan-Jun 2024)
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
    
    with open(filename, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
    print(f"Downloaded {filename}")

def upload_to_gcs(bucket_name, source_file, destination_blob):
    """Upload file to GCS bucket"""
    storage_client = storage.Client(project=PROJECT_ID)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob)
    
    print(f"Uploading {source_file} to gs://{bucket_name}/{destination_blob}...")
    blob.upload_from_filename(source_file)
    print(f"Uploaded {source_file}")

def main():
    # Create temp directory if it doesn't exist
    os.makedirs("temp_data", exist_ok=True)
    
    for file in FILES:
        url = BASE_URL + file
        local_path = os.path.join("temp_data", file)
        
        # Download file
        download_file(url, local_path)
        
        # Upload to GCS
        upload_to_gcs(BUCKET_NAME, local_path, f"yellow_taxi_2024/{file}")
        
        # Clean up local file
        os.remove(local_path)
        print(f"Cleaned up {local_path}\n")
    
    print("All files uploaded successfully!")

if __name__ == "__main__":
    main()
