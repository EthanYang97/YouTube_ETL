# YouTube ETL Pipeline

An end-to-end ELT pipeline that ingests YouTube channel data daily, lands it in a PostgreSQL warehouse with a staging/core layered model, and validates quality with Soda — all orchestrated by Apache Airflow and containerized with Docker.

![CI](https://github.com/EthanYang97/YouTube_ETL/actions/workflows/ci-cd_yt-etl.yaml/badge.svg)
![Python](https://img.shields.io/badge/python-3.10-blue)
![Airflow](https://img.shields.io/badge/airflow-2.9.2-017cee)
![Postgres](https://img.shields.io/badge/postgres-warehouse-336791)
![Docker](https://img.shields.io/badge/docker-compose-2496ed)
![Soda](https://img.shields.io/badge/soda-data%20quality-success)

---

## Overview

This pipeline pulls the full upload history and engagement stats for a configured YouTube channel (default: `MrBeast`), stores the raw response as dated JSON, loads it into a staging layer in Postgres, promotes cleaned records to a core layer, and then runs Soda data-quality checks against both layers.

The work is split across three chained Airflow DAGs so each concern fails independently and can be re-run in isolation:

```
┌─────────────────┐   trigger   ┌──────────────┐   trigger   ┌───────────────┐
│  produce_json   │ ──────────► │  update_db   │ ──────────► │  data_quality │
│  (extract)      │             │  (load)      │             │  (validate)   │
└─────────────────┘             └──────────────┘             └───────────────┘
   YouTube API                    Postgres                      Soda Checks
      │                          staging → core                staging + core
      ▼
  /data/*.json
```

## Architecture

| Stage | Component | Purpose |
|------|-----------|---------|
| Extract | `dags/api/video_stats.py` | Calls YouTube Data API v3 — fetches channel's upload playlist, paginates video IDs, batches stats requests (50 per call to respect quota) |
| Land | Local `./data` volume | Daily JSON snapshot: `YT_data_YYYY-MM-DD.json` |
| Load | `dags/datawarehouse/` | Inserts raw records into `staging.*`, transforms and promotes to `core.*` |
| Quality | `dags/dataquality/soda.py` + `include/soda/` | Runs Soda scans against both schemas; fails the DAG on violations |
| Orchestrate | `dags/main.py` | Three DAGs wired via `TriggerDagRunOperator` |

## Tech Stack

- **Python 3.10** — extraction and transformation logic
- **Apache Airflow 2.9.2** — orchestration using the TaskFlow API (`@task` decorators)
- **PostgreSQL** — data warehouse with staging/core schema separation
- **Soda Core** — declarative data quality checks
- **Docker Compose** — local reproducibility for the full stack
- **GitHub Actions** — CI pipeline (lint, format check, Docker build, image publish)
- **YouTube Data API v3** — source

## Repository Structure

```
YouTube_ETL/
├── .github/workflows/
│   └── ci-cd_yt-etl.yaml         # CI pipeline
├── dags/
│   ├── main.py                   # DAG definitions and dependencies
│   ├── api/
│   │   └── video_stats.py        # Extract tasks
│   ├── datawarehouse/
│   │   ├── dwh.py                # Staging + core load tasks
│   │   ├── data_loading.py
│   │   ├── data_modification.py
│   │   ├── data_transformation.py
│   │   └── data_utils.py
│   └── dataquality/
│       └── soda.py               # Soda scan task
├── include/
│   └── soda/
│       ├── checks.yml            # Soda data quality rules
│       └── configurations.yml    # Soda Postgres data source config
├── tests/
│   ├── conftest.py               # Mock Airflow fixtures
│   ├── unit_test.py              # DAG integrity tests
│   └── integration_test.py       # End-to-end tests (run locally)
├── data/                         # Daily JSON snapshots (gitignored)
├── docker-compose.yaml
├── Dockerfile
├── requirements.txt
└── README.md
```

## Quickstart

### Prerequisites

- Docker + Docker Compose
- A YouTube Data API v3 key ([get one here](https://console.cloud.google.com/apis/library/youtube.googleapis.com))

### 1. Clone and configure

```bash
git clone https://github.com/EthanYang97/YouTube_ETL.git
cd YouTube_ETL
```

### 2. Start the stack

```bash
docker compose up -d
```

Airflow UI: [http://localhost:8080](http://localhost:8080)

### 3. Set Airflow Variables

In the Airflow UI under **Admin → Variables**, add:

| Key | Value |
|-----|-------|
| `API_KEY` | your YouTube Data API key |
| `CHANNEL_HANDLE` | channel handle without `@` (e.g., `MrBeast`) |

### 4. Trigger the pipeline

Unpause and run `produce_json`. The two downstream DAGs (`update_db`, `data_quality`) will chain automatically via `TriggerDagRunOperator`.

## Design Notes

**Why three DAGs instead of one?** Extraction, loading, and validation have different failure modes and different reasonable retry policies. Splitting them means a transient API quota error doesn't force you to re-run data-quality checks, and a failed Soda scan doesn't re-download every video. The `TriggerDagRunOperator` chain keeps the end-to-end flow intact while preserving that isolation.

**Why staging + core?** The staging layer mirrors the raw API shape so historical snapshots can always be reconstructed. The core layer is where types are enforced, deduplication happens, and the schema is stable for downstream consumers.

**Pure functions vs. Airflow tasks.** Extraction logic is split into plain Python functions (`_get_playlist_id`, `_get_video_ids`, etc.) with thin `@task` wrappers on top. This keeps business logic decoupled from orchestration — the pure functions are testable in isolation, runnable outside Airflow, and portable to other contexts.

**API quota handling.** The YouTube API charges per call, not per video. Batching stats requests 50 IDs at a time (`_extract_video_data`) keeps quota cost predictable regardless of channel size.

## CI Pipeline

GitHub Actions runs on every push and pull request:

| Job | Purpose |
|-----|---------|
| `lint-and-validate` | Ruff lint + Black format check on `dags/` |
| `docker-build` | Verifies the Docker image builds successfully |
| `publish-image` | Pushes tagged image to DockerHub (on `main` pushes only) |

## Testing

The `tests/` folder contains pytest-based tests that mock Airflow Variables and Connections via `unittest.mock`:

- **`unit_test.py`** — validates DAG integrity (import errors, expected DAG IDs, task counts per DAG), mock Airflow Variables, and Postgres connection configuration.
- **`integration_test.py`** — end-to-end tests requiring a live Postgres instance; run locally against the Docker Compose stack.
- **`conftest.py`** — shared fixtures including mock Airflow Variables, mock Postgres connections, and a `DagBag` loader.

Run tests locally with:
```bash
docker exec -t airflow-worker pytest tests/ -v
```

## Data Quality Checks

Soda validates the `yt_api` table in both staging and core schemas after each load. Configuration lives in `include/soda/`:

- **`configurations.yml`** — defines the Postgres data source, with credentials injected from environment variables.
- **`checks.yml`** — declarative rules covering:

| Check | Purpose |
|-------|---------|
| `missing_count("Video_ID") = 0` | No null primary keys |
| `duplicate_count("Video_ID") = 0` | No duplicate videos |
| Custom SQL: `Likes_Count > Video_Views` | Catches logically impossible engagement values (likes should never exceed views) |
| Custom SQL: `Comments_Count > Video_Views` | Same logic applied to comments |

The last two checks are the interesting ones — they catch real data integrity issues that schema-level constraints can't express. A video with more likes than views signals either a parsing bug or an API change worth investigating. Scan failures surface in Airflow logs and block downstream consumers from seeing bad data.

## Roadmap

- [x] Refactor extraction into pure functions for unit testability
- [x] Add CI workflow (lint, format check, Docker build, image publish)
- [ ] Incremental loads (`publishedAt` watermark) instead of full refresh
- [ ] Parameterize channel list to support multi-channel ingestion
- [ ] Swap local JSON landing for object storage (S3 / GCS)
- [ ] Add a small dbt layer between core and analytics marts

## What I Learned

Building this taught me the difference between *writing* an Airflow DAG and *designing* a pipeline: choosing where to draw DAG boundaries, how to handle upstream API quirks without coupling them to load logic, and why a staging layer exists even when it feels redundant. Wiring Soda in last was a deliberate choice — quality checks are only useful when they can block downstream consumers, and Airflow's DAG-level failure gives that enforcement cleanly.

Setting up CI from scratch also surfaced a lot of subtle issues: Python version mismatches between local and CI, Black formatting differences across versions, Windows vs. Linux path separators in Git, and the importance of pinning tool versions to keep environments reproducible.

---

**Author:** [Ethan Yang](https://github.com/EthanYang97)