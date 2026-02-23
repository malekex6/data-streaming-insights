
# Data Streaming Insights: Real-Time Sales & Transactions Analytics

This project is a real-time streaming analytics platform focused on **sales data** and **transaction events**. It ingests, processes, and analyzes sales transactions as they happen, providing instant business insights and dashboards. Built on Google Cloud Platform, it shows modern data engineering for retail, e-commerce, or any domain with high-velocity sales data.




## Project Overview

- **Sales & Transaction Focus**: Simulates and analyzes real-world sales transactions (e.g., purchases, revenue, product sales, regions)
- **FastAPI**: REST API for generating and ingesting mock sales transaction events
- **Google Pub/Sub**: Real-time event distribution and decoupling
- **Apache Beam (Dataflow)**: Streaming ETL pipeline (parse → validate → deduplicate → enrich → aggregate)
- **Medallion Architecture**: Bronze (raw), Silver (clean), and Gold (aggregated) BigQuery tables
- **BigQuery**: Scalable storage for all layers
- **Looker Studio**: Real-time business dashboards (sales trends, top products, revenue by region, etc.)
- **Pydantic**: Schema validation and error handling
- **Dead letter handling**: For malformed records
- **Auto-scaling**: 1-5 workers (DirectRunner for local, DataflowRunner for cloud)


## Architecture

```
FastAPI → Pub/Sub → Dataflow (Beam) → BigQuery (Bronze → Silver → Gold) → Looker Studio
  |         |            |                |         |         |
  |         |            |                |         |         +-- Dashboards & Analytics
  |         |            |                |         +------------ Gold: Aggregated business metrics
  |         |            |                +--------------------- Silver: Clean, deduplicated
  |         |            +-------------------------------------- Bronze: Raw ingestion
  |         +----------------------------------------------- Streaming pipeline
  +------------------------------------------------------ Mock data/API ingestion
```

## Quick Start

### 1. Setup
```bash
python -m venv .venv
.\.venv\Scripts\activate  # Windows
source .venv/bin/activate  # macOS/Linux
pip install -r requirements.txt
```

### 2. Google Credentials
```bash

Create a service account and key for Pub/Sub publishing:
gcloud iam service-accounts create <SERVICE_ACCOUNT_NAME> --display-name="Publisher"
gcloud projects add-iam-policy-binding <YOUR_PROJECT_ID> \
  --member="serviceAccount:<SERVICE_ACCOUNT_NAME>@<YOUR_PROJECT_ID>.iam.gserviceaccount.com" \
  --role="roles/pubsub.publisher"
gcloud iam service-accounts keys create <PATH_TO_KEY_JSON> \
  --iam-account=<SERVICE_ACCOUNT_NAME>@<YOUR_PROJECT_ID>.iam.gserviceaccount.com
```

### 3. Environment Variables
Copy `.env.example` to `.env`:
```
GCP_PROJECT_ID=<YOUR_PROJECT_ID>
PUBSUB_TOPIC=projects/<YOUR_PROJECT_ID>/topics/<TOPIC_NAME>
GOOGLE_APPLICATION_CREDENTIALS=<PATH_TO_KEY_JSON>
```

### 4. Run Producer
```bash
uvicorn app.main:app --reload
# Visit http://127.0.0.1:8000/docs
```

### 5. Generate Test Events
```bash
curl -X POST http://127.0.0.1:8000/generate \
  -H 'Content-Type: application/json' \
  -d '{"num_events": 10, "region": "EU"}'
```


### 6. Run Pipeline Locally (DirectRunner)
```bash
python dataflow/pipeline.py \
  --runner=DirectRunner \
  --project=<YOUR_PROJECT_ID> \
  --input_topic=projects/<YOUR_PROJECT_ID>/topics/<TOPIC_NAME> \
  --output_dataset=<YOUR_DATASET>
```

### 7. Verify Results
```bash
bq query --use_legacy_sql=false \
  'SELECT COUNT(*), COUNT(DISTINCT transaction_id) FROM insightstream_analytics.transactions_clean'
```

### 8. Explore Analytics in Looker Studio
- Connect Looker Studio to the Gold table (`insightstream_analytics.transactions_gold`)
- Build dashboards: sales trends, top products, revenue by region, etc.

## Project Structure

```
insightstream/
├── app/           # Mock Producer (FastAPI)
├── dataflow/      # Streaming Pipeline (Apache Beam)
├── tests/         # Unit tests
└── requirements.txt
```

## Key Features

| Feature | Details |
|---------|---------|
| **Validation** | Pydantic schemas, type hints, business logic checks |
| **Deduplication** | By transaction_id using Beam's Distinct.PerKey() |
| **Error Handling** | Dead letter side outputs, structured logging |
| **Scalability** | Auto-scaling workers 1-5 based on throughput |
| **Storage** | BigQuery Bronze (raw), Silver (clean), Gold (aggregated) tables |


---

