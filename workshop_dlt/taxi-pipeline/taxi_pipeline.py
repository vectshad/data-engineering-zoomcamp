import dlt
from dlt.sources.helpers import requests

def fetch_taxi_data():
    """Generator that fetches paginated data from the API"""
    base_url = "https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api"
    page = 1
    
    while True:
        # Fetch current page
        url = f"{base_url}?page={page}"
        print(f"Fetching page {page} from: {url}")
        response = requests.get(url)
        print(f"Response status: {response.status_code}")
        response.raise_for_status()
        data = response.json()
        
        print(f"Response type: {type(data)}")
        print(f"Data length: {len(data) if isinstance(data, list) else 'N/A'}")
        
        # Stop if empty page
        if not data or (isinstance(data, list) and len(data) == 0):
            print(f"Page {page} is empty. Stopping.")
            break
        
        print(f"Page {page}: Got {len(data)} records")
        yield data
        page += 1
        
        # Safety limit to prevent infinite loops
        if page > 100:
            print("Reached page limit (100). Stopping.")
            break

@dlt.resource(name="taxi_trips", write_disposition="replace")
def taxi_trips_resource():
    """dlt resource that yields taxi trip data"""
    return fetch_taxi_data()

if __name__ == "__main__":
    # Create pipeline
    pipeline = dlt.pipeline(
        pipeline_name="taxi_pipeline",
        destination="duckdb",
        dataset_name="nyc_taxi"
    )
    
    # Run pipeline
    print("Starting pipeline run...")
    load_info = pipeline.run(taxi_trips_resource())
    print("\nPipeline completed!")
    print(load_info)