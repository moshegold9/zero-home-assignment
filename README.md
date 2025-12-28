# Running the pipeline with Docker

## Prerequisites
- Docker installed
- The datalake services running (Trino + Iceberg REST + MinIO):

```bash
docker compose -f datalake/docker-compose.yaml up -d
```

## Build the pipeline image

```bash
docker build -t noaa-pipeline:latest .
```

## Run the pipeline container

Create a local `.env` (or reuse `example.env`) and set at least `NOAA_API_TOKEN`.

When running the pipeline container alongside the docker-compose datalake, use these hostnames:
- `TRINO_HOST=trino-coordinator`
- `ICEBERG_REST_URI=http://iceberg-rest:8181`
- `MINIO_ENDPOINT_URL=http://minio:9000`

Run:

```bash
docker run --rm \
  --env-file .env \
  --network datalake_default \
  -e TRINO_HOST=trino-coordinator \
  -e ICEBERG_REST_URI=http://iceberg-rest:8181 \
  -e MINIO_ENDPOINT_URL=http://minio:9000 \
  noaa-pipeline:latest
```
