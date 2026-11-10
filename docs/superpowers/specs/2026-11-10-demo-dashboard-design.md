# Demo dashboard: design

## Intent
A single page for the project demo. It shows that the pipeline ran end to end, then lets
the presenter explore the fraud alerts it produced. It must look clean and minimal and be
readable on a projector.

## Goals
- Pipeline strip: CSV, Kafka, Bronze, Silver, Gold, GBT model, Postgres, API, with the status
  and row count of the last load run.
- Headline numbers: transactions scored, transactions flagged, flag rate, model F1, precision and recall.
- Flagged transactions by type (bar chart).
- Alerts table with a probability slider and a transaction ID lookup.
- Links to Airflow, Kafka UI and the API docs.
- Light and dark theme from the OS setting.

## Non-goals
- Login, editing data, live streaming updates, mobile layout polish beyond "does not break".

## Constraints
- 6.36M rows in `fraud_predictions`. The group-by-type query takes 0.4 s, so no cache.
- The API container mounts only `./src`, so the model metrics file needs a read-only mount.

## Approaches
1. **Static HTML page served by the existing FastAPI app (chosen).** No new container,
   dependency or build step. Plain JS fetches the API on the same origin.
2. Streamlit: a new dependency and service, and a generic look.
3. React or Vite: a build toolchain for one screen.

## Design
- `GET /stats`: one group-by-type scan over `fraud_predictions`, the last 5 `pipeline_runs`,
  model metrics from `models/member3/model_config.json` (null if missing) and an
  `airflow_url` (env `AIRFLOW_UI_URL`, default `http://localhost:8081`). Returns 503 before
  the first load, as the other endpoints do.
- `GET /`: serves `src/serving/static/index.html` (no extra files, no CDN).
- Compose: mount `./models:/app/models:ro` and set `AIRFLOW_UI_URL` from `AIRFLOW_WEB_PORT`.
- Test: `/stats` totals, per-type counts and last-run status in `tests/test_serving.py`.
