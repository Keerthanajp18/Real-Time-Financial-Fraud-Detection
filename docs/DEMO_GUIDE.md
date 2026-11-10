# Demo Guide

How to present the Real-Time Financial Fraud Detection project in about 12 minutes.
The story: **raw transactions go in, the pipeline runs on its own, and fraud alerts come out.**
Follow the data from left to right, and let each member present their own stage.

## The day before

1. Pull `main` and start everything:
   ```bash
   git pull
   AIRFLOW_WEB_PORT=8082 docker compose up -d
   ```
   Use any free port. 8081 is the default.
2. Check `dataset/paysim.csv` is the full file (493 MB, 6,362,620 rows).
3. Run the full pipeline once, so the demo shows finished results. In Airflow, trigger
   `fraud_detection_pipeline`. It takes about 25 minutes. **Do not run it live in front of the audience.**
4. Open these tabs, in this order, and leave them open:
   - Dashboard: http://localhost:8000/
   - Airflow: http://localhost:8082 (login `admin` / `admin`), Graph view of the last run
   - Kafka UI: http://localhost:8080
   - API docs: http://localhost:8000/docs
5. Take a screenshot of each tab. They are the backup if Docker fails on the day.

Docker Desktop needs about 11 GB of memory for the full run. See `%USERPROFILE%\.wslconfig`
(`memory=11GB`, `swap=8GB`).

## 10 minutes before

- Check http://localhost:8000/. The pill top right must say **Live** in green.
- Browser full screen (F11), zoom 110-125 % so the back row can read the numbers.
- Close other tabs and turn off notifications.

## Running order

| # | Time | Who | Screen | What to say |
|---|------|-----|--------|-------------|
| 1 | 1 min | Any | Dashboard, top | The problem: mobile-money fraud is rare (0.13 % of PaySim transactions) and costly. We built a pipeline that scores every transaction and serves alerts to analysts. |
| 2 | 1 min | Any | Dashboard, pipeline strip | Walk the strip left to right: CSV, Kafka, Bronze/Silver/Gold, model, Postgres, API. Each stage names its owner. Every step is green, and the last run loaded 6,362,620 rows. |
| 3 | 1.5 min | Member 1 | Kafka UI | Topic `financial-transactions`: show messages arriving as JSON. Explain the producer, batching and rate limiting. |
| 4 | 2 min | Member 2 | Dashboard, then explain | Bronze keeps the raw data, Silver cleans and validates it (bad rows go to quarantine), Gold adds features. Spark, Parquet, partitioned by `step`. |
| 5 | 2 min | Member 3 | Dashboard, F1 tile | GBT classifier on deployment-safe features (no post-transaction balances, which would leak the label). Held-out test: precision 0.773, recall 0.726, F1 0.749 at threshold 0.4. |
| 6 | 1.5 min | Member 4 | Airflow, Graph view | 7 tasks in order, all green: ingest, ETL, quality checks, inference, load, health check, success. Runs every 6 hours, with retries. |
| 7 | 2 min | Member 5 | Dashboard, alerts and lookup | Drag the slider from 0.90 down to 0.50 and watch the table fill. Click a row: the lookup shows the verdict. Open `/docs` and run `GET /alerts` live. Postgres upsert, about 3.5 minutes for 6.3M rows; alert queries use an index. |
| 8 | 1 min | Any | Dashboard, bars | Results: 7,598 flagged. TRANSFER and CASH_OUT dominate, matching where PaySim fraud actually happens. Close with what you would do next. |

**Tips**
- Point at the screen before you speak about it. Let numbers sit for a second.
- Say each number once, the same way each time: "6.36 million transactions", "F1 0.75".
- If something breaks, say what should happen and move to the screenshot. Do not debug live.

## Questions to expect

**Why do the top alerts show an amount of 0.00?**
They are real rows from PaySim. All 16 zero-amount transactions in the dataset are labelled
fraud, and the model learned that pattern.

**Why does CASH_IN have 8 flags?**
They are false positives. In PaySim, real fraud only occurs in TRANSFER and CASH_OUT. A
production system could add a rule to skip CASH_IN, or retrain with the type weighted.

**Why recall 0.73 and not higher?**
It is a trade-off set by the threshold. Lowering 0.4 catches more fraud but flags more good
customers. We chose 0.4 to keep precision near 0.77, so most alerts are worth an analyst's time.

**How did you handle the class imbalance (0.13 % fraud)?**
Member 3: the decision threshold (0.4, not the default 0.5) was tuned on a separate split, and
the model is judged on precision, recall and F1, because accuracy would be 99.9 % for a model
that never flags anything.

**Is it really real-time?**
Ingestion streams through Kafka, and there is a Spark Structured Streaming ETL. The Airflow DAG
runs the batch path every 6 hours, which is what we show today.

**What happens if a step fails half way?**
Airflow retries it. The ETL overwrites each layer, so a retry never duplicates data, and the
Postgres load is one transaction keyed on `transaction_id`, so a re-run updates rows instead of
adding them. Every load is logged in `pipeline_runs`.

**Does it scale?**
The full 6.36M-row run takes about 25 minutes on one laptop. Spark runs the same code on a
cluster; Postgres alert queries use a probability index and return in milliseconds.

**The full-data F1 is 0.751, why quote 0.749?**
The full set includes the rows the model trained on. 0.749 is the held-out test score, the
honest number.

## If something breaks

| Problem | Fix |
|---------|-----|
| Dashboard says "API offline" | `docker compose up -d`, wait 20 s, refresh |
| Dashboard says "No data yet" | The DAG has not loaded predictions. Use the screenshots |
| Airflow page does not load | Wrong port. Check `AIRFLOW_WEB_PORT` you started with |
| Docker containers all stopped | Out of memory. Use the screenshots; restart after the demo |
