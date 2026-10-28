# Data Engineering Zoomcamp – Homework and Workshop

My coursework for the free [Data Engineering Zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp) by DataTalksClub (January – March 2026). Each module has a folder with the homework instructions (`homework.md`, from the course) and my work: code, configs, and one `Question N/` folder per question with my answer as text or a screenshot.

All modules use the public NYC taxi trip data (yellow and green taxis).

| Folder | Module | What I used |
|---|---|---|
| [homework_1](homework_1) | Docker and SQL | Docker, docker-compose, Postgres, pgAdmin, Python and SQL |
| [homework_2](homework_2) | Workflow orchestration | [Kestra](https://kestra.io/) flows (YAML) that load taxi data into Postgres |
| [homework_3](homework_3) | Data warehouse | Google Cloud Storage and BigQuery; Python scripts that download Jan–Jun 2024 yellow taxi Parquet files and upload them to a bucket |
| [homework_4](homework_4) | Analytics engineering | A dbt project (`taxi_rides_ny`: staging, intermediate and marts models); taxi data loaded into DuckDB with Python |
| [homework_5](homework_5) | Data platforms | Written answers on building a pipeline with Bruin. The pipeline itself isn't in this repo. |
| [homework_6](homework_6) | Batch processing | PySpark in a notebook (`Homework_6.ipynb`) |
| [workshop_dlt](workshop_dlt) | dlt workshop | A [dlt](https://dlthub.com/) pipeline that loads a paginated REST API into DuckDB |

## Running things

Each folder is separate, so there's one setup per module. The setup steps are in the module's own files:

- `homework_3/SETUP_GUIDE.md` – GCP project, bucket and BigQuery setup
- `homework_4/local_setup.md` and `homework_4/cloud_setup.md` – dbt setup (local DuckDB, or cloud)
- `workshop_dlt/taxi-pipeline/` – `pip install -r requirements.txt`, then `python taxi_pipeline.py`

## Notes

- Cloud steps (GCP) need your own project and credentials. None are included in this repo; the service-account JSON in `cloud_setup.md` is a placeholder.
- Some small data files (Parquet, CSV, a `.duckdb` file) are committed so the answers can be checked. Larger files are downloaded by the scripts.
- This is course practice work, not a production project.
