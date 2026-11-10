# Demo Dashboard Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** One minimal dashboard page at `http://localhost:8000/` that shows the pipeline ran and lets the presenter explore fraud alerts.

**Architecture:** A new `GET /stats` endpoint in the existing FastAPI app, plus `GET /` serving one self-contained HTML file. The page uses plain JS `fetch` against `/stats`, `/alerts` and `/transactions/{id}` on the same origin.

**Tech Stack:** FastAPI, psycopg2, plain HTML/CSS/JS (no CDN, no build).

**Spec:** `docs/superpowers/specs/2026-11-10-demo-dashboard-design.md`

## Global Constraints
- No new Python dependency, container or build step. The API image stays `fastapi`, `uvicorn`, `psycopg2-binary`.
- No external scripts, fonts or CDNs: the demo must work offline.
- `/stats` returns 503 `{"detail": "No predictions loaded yet"}` before the first load, as `/alerts` does.
- Default Airflow URL `http://localhost:8081`, overridden by env `AIRFLOW_UI_URL`.

## Review Focus
- Model config file missing (API run outside compose): `/stats` still returns 200 with `"model": null`, and the page hides the metrics. Tested in Task 1.
- `pipeline_runs` exists but the last run `failed`: the strip shows failed state and the error text. Tested in Task 1 (status in payload).
- API down or 503 while the page is open: the page shows a clear "No data yet / API unreachable" message, not a blank screen. Checked by hand in Task 2.
- Transaction ID not found: the lookup shows "Not found", not an exception. Checked by hand in Task 2.
- Untrusted text (transaction IDs, error text) is rendered with `textContent`, never `innerHTML`. Checked in review of Task 2.

---

### Task 1: `/stats` endpoint

**Files:**
- Modify: `src/serving/api.py`
- Modify: `docker-compose.yml` (api service: volume `./models:/app/models:ro`, env `AIRFLOW_UI_URL: http://localhost:${AIRFLOW_WEB_PORT:-8081}`)
- Test: `tests/test_serving.py`

**Interfaces:**
- Produces: `GET /stats` returning
  `{"total": int, "flagged": int, "by_type": [{"type": str, "total": int, "flagged": int}], "runs": [{"run_id", "dag_run_id", "status", "rows_loaded", "error", "started_at", "finished_at"}], "model": {"model_type", "threshold", "test_precision", "test_recall", "test_f1"} | null, "airflow_url": str}`.
  `by_type` sorted by `flagged` desc then `type`; `runs` is the last 5 by `run_id` desc.

- [ ] **Step 1: Write the failing test** `test_stats` (load "low" 0.1, "edge" 0.42 at threshold 0.4, "high" 0.95; all type TRANSFER):
  `total == 3`, `flagged == 2`, `by_type == [{"type": "TRANSFER", "total": 3, "flagged": 2}]`, `runs[0]["status"] == "success"`, `runs[0]["rows_loaded"] == 3`, `"airflow_url" in body`. Add to `test_api_returns_503_before_first_load`: `/stats` is 503.
- [ ] **Step 2: Run** `pytest tests/test_serving.py -v` in the Airflow image on the compose network. Expected: `test_stats` FAIL with 404.
- [ ] **Step 3: Implement `get_stats(conn=Depends(get_conn)) -> dict`.** One `GROUP BY type` query; totals summed in Python. Model config read from `models/member3/model_config.json` relative to cwd; `None` on `OSError`/`ValueError`.
- [ ] **Step 4: Run tests.** Expected: all pass.
- [ ] **Step 5: Commit** `feat(member5): /stats endpoint for the dashboard`.

### Task 2: Dashboard page

**Files:**
- Create: `src/serving/static/index.html`
- Modify: `src/serving/api.py` (`GET /` returns `FileResponse` of that file, `include_in_schema=False`)
- Test: `tests/test_serving.py`

**Interfaces:**
- Consumes: `/stats` (Task 1), `/alerts?min_probability=&limit=50`, `/transactions/{id}`.

- [ ] **Step 1: Write the failing test** `test_dashboard_page`: `GET /` is 200, `content-type` starts with `text/html`, body contains `Fraud Detection`. No DB needed.
- [ ] **Step 2: Run it.** Expected: FAIL with 404.
- [ ] **Step 3: Add the route and the page.** Sections, top to bottom: header (title, API status dot, last run time), pipeline strip (8 stages, last run status and rows), 4 KPI tiles (scored, flagged, flag rate, F1 with precision/recall), flagged-by-type bars, alerts table with probability slider (default 0.9) and ID search, footer links (Airflow, Kafka UI `http://localhost:8080`, API docs `/docs`). Light/dark via `prefers-color-scheme`.
- [ ] **Step 4: Run tests and open the page** against the full PaySim data in light and dark mode. Expected: tests pass; numbers match `/stats`; slider and lookup work; stopping Postgres shows the error state.
- [ ] **Step 5: Commit** `feat(member5): demo dashboard page`.
