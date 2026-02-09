# BigQuery Setup Guide for Homework 3

## Prerequisites
1. GCP Free Account created ✓
2. Project created in GCP Console

## Step-by-Step Setup

### 1. Install Google Cloud SDK (if not already installed)
```powershell
# Download and install from: https://cloud.google.com/sdk/docs/install
# Or use PowerShell:
(New-Object Net.WebClient).DownloadFile("https://dl.google.com/dl/cloudsdk/channels/rapid/GoogleCloudSDKInstaller.exe", "$env:Temp\GoogleCloudSDKInstaller.exe")
& $env:Temp\GoogleCloudSDKInstaller.exe
```

### 2. Authenticate with GCP
```powershell
gcloud auth login
gcloud config set project YOUR_PROJECT_ID
```

### 3. Create GCS Bucket
```powershell
# Replace YOUR_BUCKET_NAME with a globally unique name
gsutil mb -l us-central1 gs://YOUR_BUCKET_NAME
```

### 4. Enable Required APIs
```powershell
gcloud services enable bigquery.googleapis.com
gcloud services enable storage.googleapis.com
```

### 5. Create Service Account and Download Key
```powershell
# Create service account
gcloud iam service-accounts create de-zoomcamp-sa --display-name "DE Zoomcamp Service Account"

# Grant permissions
gcloud projects add-iam-policy-binding YOUR_PROJECT_ID --member="serviceAccount:de-zoomcamp-sa@YOUR_PROJECT_ID.iam.gserviceaccount.com" --role="roles/storage.admin"

gcloud projects add-iam-policy-binding YOUR_PROJECT_ID --member="serviceAccount:de-zoomcamp-sa@YOUR_PROJECT_ID.iam.gserviceaccount.com" --role="roles/bigquery.admin"

# Download key
gcloud iam service-accounts keys create de-zoomcamp-key.json --iam-account=de-zoomcamp-sa@YOUR_PROJECT_ID.iam.gserviceaccount.com
```

### 6. Set Environment Variable
```powershell
$env:GOOGLE_APPLICATION_CREDENTIALS = "D:\Eka\Eksplorasi\DE Zoomcamp 2026\homework_3\de-zoomcamp-key.json"
```

### 7. Install Python Dependencies
```powershell
pip install google-cloud-storage google-cloud-bigquery requests
```

### 8. Update and Run Upload Script
1. Open `load_yellow_taxi_data.py`
2. Update `BUCKET_NAME` and `PROJECT_ID`
3. Run: `python load_yellow_taxi_data.py`

### 9. Create BigQuery Dataset
```powershell
bq mk --dataset YOUR_PROJECT_ID:taxi_data
```

### 10. Create External Table in BigQuery
```sql
CREATE OR REPLACE EXTERNAL TABLE `YOUR_PROJECT_ID.taxi_data.yellow_tripdata_external`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://YOUR_BUCKET_NAME/yellow_taxi_2024/*.parquet']
);
```

### 11. Create Materialized Table
```sql
CREATE OR REPLACE TABLE `YOUR_PROJECT_ID.taxi_data.yellow_tripdata` AS
SELECT * FROM `YOUR_PROJECT_ID.taxi_data.yellow_tripdata_external`;
```

## Next Steps
You're now ready to answer the homework questions!
