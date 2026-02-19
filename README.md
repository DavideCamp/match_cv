# match-cv

CV ingestion and matching service built with Django, pgvector, Datapizza pipelines, and Celery.

## Requirements

- Python `3.13+`
- `uv`
- Docker (recommended for PostgreSQL + Redis)

## Local setup (uv + venv)

1. Create and activate virtualenv:

```bash
uv venv
source .venv/bin/activate
```

2. Install dependencies:

```bash
uv sync
```

3. Create local env file from template:

```bash
cp .env.example .env
```

4. Configure environment variables (`.env`):

```bash
OPENAI_API_KEY=your_key
EMBEDDING_MODEL_NAME=text-embedding-3-small
```

5. Start infrastructure (PostgreSQL + Redis):

```bash
docker compose up -d
```

6. Run migrations:

```bash
python manage.py migrate
```

7. Start Django API:

```bash
python manage.py runserver
```

8. Start Celery worker (separate terminal):

```bash
celery -A src.config.celery worker -l info
```

## Run tests

```bash
pytest
```

With coverage:

```bash
pytest --cov --cov-report=html
```

## API endpoints

Base prefix: `/api/`

### 1) Upload single CV

- `POST /api/cv-documents/`
- Content-Type: `multipart/form-data`
- Field: `source_file` (file)

Example:

```bash
curl -X POST http://127.0.0.1:8000/api/cv-documents/ \
  -F "source_file=@/absolute/path/cv.pdf"
```

Response:
- `201`: document created and ingested synchronously
- `400`: validation error

### 2) Bulk upload CVs (async via Celery)

- `POST /api/cv-documents/bulk/`
- Content-Type: `multipart/form-data`
- Field: repeated `files` keys

Example:

```bash
curl -X POST http://127.0.0.1:8000/api/cv-documents/bulk/ \
  -F "files=@/absolute/path/cv1.pdf" \
  -F "files=@/absolute/path/cv2.pdf"
```

Response:
- `202`: returns `batch_id` and `upload_item_id`s

### 3) Bulk upload batch status

- `GET /api/cv-documents/bulk/<batch_id>/status/`

Response includes:
- batch status (`PENDING|RUNNING|SUCCESS|FAILED|PARTIAL`)
- counters (`total_files`, `processed_files`, `failed_files`)
- per-item status and `error_message` if failed

### 4) Run matching pipeline

- `POST /api/search-runs/`
- Content-Type: `application/json`

Example payload:

```json
{
  "job_offer_text": "Looking for backend engineer with Python and 5+ years",
  "weights": {
    "skill": 0.1,
    "experience": 0.7,
    "education": 0.2
  },
  "top_k": 10
}
```

Response:
- `200`: ranked candidate list
- `400`: request validation error
- `500`: pipeline/runtime error

## Architecture overview
# Search
- split job offer (`skill`, `experience`, `education`) -> parallel category retrieval -> merge by document -> weighted scoring.

![Search Pipeline](docs/search_pipeline.drawio.png)

# Upload
- **Single upload**: API -> serializer -> metadata extraction -> embedding -> vector store write.
- **Bulk upload**: API creates batch/items -> Celery task per item -> status polling endpoint.

![upload flow](docs/upload_flow.drawio.png)

## Notes

- Use multipart file upload for CV endpoints. JSON file paths are not accepted.
- If Celery worker is down, bulk upload items stay `PENDING`.
- pgvector metadata should be JSON-serializable; UUIDs are converted safely in the vector store layer.
