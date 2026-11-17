# Presentation Guide: Real-Time Financial Fraud Detection

Everything you need for tomorrow: what to show, what to say, and how to answer questions.

---

## 1. Visuals you can add to the slides

All of these are in the project and use the deck's black, grey and green look (no blue).

| File | What it shows | Best slide |
|---|---|---|
| `report/figures/ppt/data_daily.png` | Payments per day (bars) and fraud per day (green line) | 5 (data) or a new slide after it |
| `report/figures/ppt/data_types.png` | All payments vs fraud cases by payment type | 5 (data) |
| `report/figures/ppt/data_amounts.png` | Average genuine vs fraud amount (178k vs 1.47M) | 5 (data) |
| `report/figures/ppt/pipeline_layers.png` | Rows and size on disk at every stage | 12 (pipeline results) |
| `report/figures/ppt/model_confusion.png` | Fraud caught / missed / false alarms | 13 (model results) |
| `report/figures/ppt/model_accuracy.png` | Why 99.94% accuracy is misleading | 13 (model results) |
| `report/figures/ppt/api_docs.png` | The real API docs page with the 4 endpoints (greyscale) | 11 (serving) |
| `report/figures/dashboard_overview.png` | The dashboard with full results (already on slide 11) | 11 |
| `report/figures/dashboard_alerts.png` | The riskiest alerts table | 11 or 13 |

**Not available yet: Airflow and Kafka UI screenshots.** They need Docker running, and
Docker needs about 11 GB of memory for the full run. This laptop has about 0.8 GB free, so
take them on a 16 GB laptop (Sebin's). Capture these screens:

1. **Airflow, Graph view** of a finished `fraud_detection_pipeline` run, all 7 boxes green
   (http://localhost:8081, login `admin` / `admin`).
2. **Airflow, Grid view**, showing the run history and task durations.
3. **Airflow, a task log**, for example `member5_load_predictions` showing the row count.
4. **Kafka UI, Topics list** with `financial-transactions` and `financial-transactions-dlq`
   and their message counts (http://localhost:8080).
5. **Kafka UI, Messages tab** of `financial-transactions`, showing one JSON message.
6. **Docker Desktop, Containers**, showing all services running.

Windows shortcut for screenshots: `Win + Shift + S`.

---

## 2. Abstract (read this if asked to summarise)

> This project builds an automated data pipeline that spots suspicious mobile-money payments.
> We used the PaySim dataset: 6,362,620 transactions over one month, of which only 8,213
> (0.13%) are fraud. A Python program reads the data in batches, checks every record and
> sends valid records to Apache Kafka, while bad records go to a separate dead-letter topic.
> Apache Spark stores the data in three layers: Bronze keeps the raw copy, Silver keeps
> cleaned records without duplicates, and Gold adds features for prediction. A
> gradient-boosted tree model gives every transaction a fraud score. The scores are saved in
> PostgreSQL and shown through a FastAPI service and a dashboard. Apache Airflow runs every
> step in order every six hours, and Docker packages the whole system. On the full dataset the
> pipeline finished in about 24 minutes, and every stage held all 6,362,620 records with none
> lost or duplicated. The model caught 5,938 of the 8,213 fraud cases, and about 8 in 10 of its
> alerts were real fraud.

**30-second version (opening line):**
"We built a pipeline that takes millions of payment records, cleans them, scores each one for
fraud, and shows the risky ones to an analyst, automatically, every six hours."

---

## 3. The project from start to finish

### The problem
Payment apps handle millions of payments a day. Almost all are genuine; fraud is about 1 in
775. A model alone does not solve this. Someone has to bring the data in, check it, clean it,
score it, store the results and show them to people, and do it again every time new data
arrives. **That chain is the data engineering work, and it is what this project is about.**

### What kind of data we handle

| Question | Answer |
|---|---|
| What is it? | Financial **transaction** data: one row per payment |
| Structure | **Structured, tabular** data with a fixed set of 11 columns |
| Source | **PaySim**, a simulator tuned on one month of real mobile-money records from an African country. Real bank data is private. |
| Size | 6,362,620 rows, 494 MB as a CSV file |
| Time | Ordered by `step`: 1 step = 1 hour, 743 hours = 31 days |
| Labels | Every row says if it is fraud (`isFraud` = 1 or 0), so we can measure the model |
| Balance | Very **imbalanced**: only 0.13% fraud |
| Column types | Numbers (amounts, balances), categories (payment type), text IDs (account names), 0/1 flags |

The columns:

| Column | Type | Meaning |
|---|---|---|
| `step` | whole number | Hour of the simulation (1 to 743) |
| `type` | category | PAYMENT, TRANSFER, CASH_OUT, CASH_IN, DEBIT |
| `amount` | decimal | Money moved |
| `nameOrig` / `nameDest` | text | Sender and receiver IDs (C = customer, M = merchant) |
| `oldbalanceOrg` / `newbalanceOrig` | decimal | Sender's balance before and after |
| `oldbalanceDest` / `newbalanceDest` | decimal | Receiver's balance before and after |
| `isFraud` | 0 or 1 | The true label |
| `isFlaggedFraud` | 0 or 1 | The simulator's own simple rule (catches only 16 frauds) |

**What we found in the data:**
- Fraud only happens in **transfers (4,097) and cash-outs (4,116)**. The fraudster takes over an
  account, transfers the money out, then cashes it out.
- Fraud stays steady at **about 220 to 320 cases a day**, even when normal activity drops.
- Fraud payments are **about 8 times larger**: 1.47 million on average, against 178 thousand.

### How the data changes shape on its journey

| Stage | Format | Why this format |
|---|---|---|
| Input | CSV text file | How the data is delivered |
| Kafka | JSON messages, one per payment | Easy to read by any consumer |
| Bronze / Silver / Gold | **Parquet** files, compressed, split by hour | Column format, fast for analysis |
| Results | **PostgreSQL** table | Fast lookups for the API |
| API | JSON over HTTP | Any app can use it |
| Dashboard | Web page | For people |

### The steps, one by one

1. **Ingestion (Kafka).** A Python program reads the CSV **5,000 rows at a time** to keep
   memory low. Each row is checked with **Pydantic** rules (right types, known payment type, no
   negative amounts, label 0 or 1, valid account names). Valid rows go to the Kafka topic
   `financial-transactions`, using the **sender's account as the key** so each account's
   payments stay in order. Bad rows go to `financial-transactions-dlq` (the dead-letter queue)
   with the error and row number. Kafka confirms every write (`acks=all`) and failed sends are
   retried 3 times. Kafka runs in **KRaft mode** (no ZooKeeper needed).

2. **Lakehouse (Spark).** Spark builds three layers, the **medallion pattern**:
   - **Bronze**: the raw copy. Every record as it arrived, plus 4 extra columns: batch ID, time
     stored, source file, and a **SHA-256 fingerprint** of the record.
   - **Silver**: the clean copy. Correct data types and **7 rules** (valid hour, valid type,
     amount not missing or negative, valid account names, no missing or negative balances).
     Rows that break a rule go to a **quarantine** table with the reason. Duplicates are removed.
   - **Gold**: ready to use. A **permanent transaction ID** (a hash of the payment's key fields,
     so it is the same on every run), **10 features** for the model, and summary tables per
     customer and per hour.
   This is **ELT**: store raw first, clean later. If a rule changes, we rebuild Silver and Gold
   from Bronze without reading the CSV again.

3. **Quality gate.** Before scoring, a check makes sure every layer exists, can be read and is
   not empty. If not, the run stops, so the model never scores incomplete data.

4. **Scoring (model).** A **gradient-boosted tree** model (Spark MLlib) reads Gold and gives
   every payment a fraud score from 0 to 1. An alert is raised at **0.4 or more**. It uses only
   facts known *before* the payment: amount, starting balances, payment type, hour, and safe
   flags. Using the balances *after* the payment would be **data leakage**.

5. **Load (PostgreSQL).** All scores go into the table `fraud_predictions`, with
   `transaction_id` as the primary key. The load is an **upsert** (`ON CONFLICT ... DO UPDATE`):
   loading again updates rows instead of copying them. The whole load is one transaction (all
   rows or none), and every load is logged in `pipeline_runs`.

6. **Serving (FastAPI + dashboard).** Four endpoints: `/health`, `/alerts`,
   `/transactions/{id}`, `/stats`. The dashboard shows the main numbers, where the alerts are,
   and lets an analyst check any payment, with a plain-English explanation of its score.

7. **Orchestration (Airflow).** One workflow, `fraud_detection_pipeline`, with 7 tasks:
   `member1_ingestion` → `member2_etl` → `member4_quality_checks` → `member3_fraud_detection`
   → `member5_load_predictions` → `pipeline_health_check` → `pipeline_success`.
   Settings: runs **every 6 hours** (`0 */6 * * *`), **3 retries, 5 minutes apart**,
   **one run at a time** (`max_active_runs=1`), **no catch-up** of missed runs
   (`catchup=False`). Uses the **LocalExecutor**. Each task runs the same script a person could
   run by hand.

8. **Packaging (Docker Compose).** Kafka, Kafka UI, PostgreSQL, Airflow (webserver and
   scheduler) and the API all start with one command: `docker compose up -d`.

---

## 4. Numbers to remember

| What | Number |
|---|---|
| Transactions | 6,362,620 (6.36 million) |
| Fraud cases | 8,213 (0.13%) |
| Full run time | about 24 minutes, all 7 steps succeeded |
| Rows at every stage | 6,362,620 (nothing lost or duplicated) |
| Quarantined rows | 0 (the simulated data is clean) |
| PostgreSQL load | 3 min 29 s for 6.36M rows |
| Alerts raised | 7,598 (about 245 a day) |
| Fraud caught | 5,938 of 8,213 |
| False alarms | 1,660 |
| Precision (test) | 0.773, so about 8 in 10 alerts are real |
| Recall (test) | 0.726, so about 7 in 10 frauds are caught |
| F1 (test) | 0.749 |
| Accuracy | 99.94% (but "never flag" scores 99.87%) |
| Automated tests | 50 |
| Storage | CSV 494 MB, Bronze 758 MB, Silver 724 MB, Gold 1,260 MB |

---

## 5. Showing a live demo

**Plan the demo around the laptop you will use.**

### Option A: a 16 GB laptop with Docker (best)

Full steps are in `docs/DEMO_GUIDE.md`. In short:

**The day before**
1. `git pull`, then `docker compose up -d`.
2. Trigger `fraud_detection_pipeline` in Airflow and let it finish (about 25 minutes).
   **Do not run the full pipeline live.** It is too slow for a talk.
3. Take all the screenshots in section 1 as a backup.
4. Record a short screen video of the demo (`Win + Alt + R` starts Xbox Game Bar recording).
   If anything fails tomorrow, play the video.

**During the talk: show data streaming in, then the finished result**
1. **Stream:** in a terminal, send a few thousand payments to Kafka slowly, so people can
   watch them arrive:
   ```bash
   python scripts/run_ingestion.py --bootstrap-servers localhost:9092 --max-records 3000 --eps 50
   ```
   (`--eps 50` = 50 payments per second.) In Kafka UI, open Topics →
   `financial-transactions` → Messages and watch the count go up. Open one message to show
   the JSON. **Test this command the day before.**
2. **Pipeline:** switch to Airflow, open the finished run in Graph view. Walk the 7 green
   boxes left to right. Click one task and show its log. You can also press "Trigger DAG" to
   show a new run starting, but do not wait for it.
3. **Result:** switch to the dashboard (http://localhost:8000). Point at the pipeline strip,
   the numbers, and the alerts by type. Paste a transaction ID into "Check a payment".
4. **API:** open http://localhost:8000/docs, expand `GET /alerts`, press "Try it out" and
   "Execute" to show live JSON.

### Option B: this laptop (no Docker)

You cannot run the real pipeline here, but you can still show real results:

1. **Real dashboard with real scores** (reads the last full run's output from
   `data/gold/fraud_predictions`):
   ```bash
   python scripts/serve_dashboard_offline.py
   ```
   Wait about a minute, then open http://127.0.0.1:8766. Say clearly that this reads the saved
   results, because Docker is not running.
2. **Row-by-row checking at a set speed** (no Kafka needed, checks rows and prints a summary):
   ```bash
   python scripts/run_ingestion.py --dry-run --max-records 500 --eps 50
   ```
3. Use the Airflow and Kafka screenshots or the video from the 16 GB laptop.

**If something breaks:** say what should happen, then move to the screenshot or video. Do not
debug in front of the audience.

---

## 6. Questions and answers

### About the project

**Q: What does your project do, in one sentence?**
It moves payment data from a raw file to an analyst's screen automatically: checking,
cleaning, scoring each payment for fraud and showing the risky ones.

**Q: Why is this a data engineering project and not just machine learning?**
The model is one step out of seven. Most of the work is moving 6.36 million records
correctly: checking them, storing them in layers, scheduling the steps, making re-runs safe
and serving the results. The model only works because that pipeline feeds it clean data.

**Q: Why did you use simulated data?**
Real bank data is private and cannot be shared. PaySim is a well-known simulator built from
real mobile-money records, and every row is labelled, so we can measure results.

**Q: What are the 3 Vs of your data?**
Volume: 6.36 million rows, 494 MB. Velocity: streamed through Kafka, scored every 6 hours.
Variety: numbers, categories, text IDs and labels in one table.

### About Kafka (ingestion)

**Q: Why Kafka? Why not just read the file?**
Kafka separates the sender from the readers. Data is stored durably, can be read again
later, keeps order per account, and any number of consumers can read the same stream. In a
real bank, payments arrive one by one, so a message log is the natural entry point.

**Q: What is a dead-letter queue?**
A separate topic for records that fail the checks. They are kept with the error message
instead of being thrown away, so someone can look at them later.

**Q: Why use the sender's account as the message key?**
Kafka keeps messages with the same key in the same partition, in order. So all payments from
one account stay in the order they happened.

**Q: What does `acks=all` mean?**
Kafka only confirms a message once it is safely stored. Together with 3 retries, this means
a message is not lost silently if the broker has a short problem.

**Q: What is KRaft?**
Kafka's built-in way to manage itself, without the separate ZooKeeper service. Fewer moving
parts.

**Q: Does the scheduled run send all 6.36 million rows through Kafka?**
No, and it is good to be honest about this. In the Airflow run, the ingestion task sends the
first 100,000 rows to Kafka to keep the run short, and Spark's batch job reads the full CSV
directly. The full file can be streamed by removing that limit. The project also has a Spark
streaming job that reads from Kafka.

### About Spark and the lakehouse

**Q: What is the medallion architecture?**
Three quality levels. Bronze is the raw data, Silver is cleaned and checked, Gold is ready
for analysis and the model. Each layer is built from the one before it.

**Q: What is the difference between ETL and ELT? Which did you use?**
ETL cleans data before storing it; ELT stores the raw data first and cleans it later. We used
ELT inside the lakehouse: Bronze keeps the raw copy, so we can rebuild Silver and Gold at any
time.

**Q: Why Parquet and not CSV?**
Parquet stores data by column, is compressed and keeps the data types. Reading a few columns
from millions of rows is much faster.

**Q: Your Parquet layers are bigger than the CSV. Why?**
Two reasons. Each layer adds columns, including a 64-character fingerprint that barely
compresses. And splitting by hour makes thousands of small files (Silver has 2,868), which
compress less well. The fix is to merge them into fewer, larger files.

**Q: Why partition by hour (`step`)?**
So a job that needs only some hours reads only those folders. The cost is many small files,
which we noted as a limitation.

**Q: How do you remove duplicates?**
In Silver, two rows are the same payment if they have the same hour, type, amount, sender,
receiver and sender's balances. Only one is kept.

**Q: What happens to bad rows?**
They go to the quarantine table with the rule they broke. Nothing is deleted silently. In
the full run, 0 rows were quarantined because the simulated data is clean, so we also wrote
a test file with 3 bad rows to prove the rules work.

**Q: How is the transaction ID made?**
It is a hash of the payment's key fields. The same payment always gets the same ID, on every
run. That is what makes the database load safe to repeat.

### About Airflow (my part)

**Q: What is Airflow and why did you use it?**
A tool that runs a chain of tasks in order on a schedule. Compared with a simple cron job, it
knows the order of the steps, retries failed steps, stops the chain when something fails,
and has a web page with logs and history for every run.

**Q: What is a DAG?**
A Directed Acyclic Graph: a set of tasks with arrows showing which comes first, and no loops.
Ours is a straight line of 7 tasks.

**Q: Walk me through your DAG.**
Ingest to Kafka, then Spark ETL, then the quality check, then fraud scoring, then load to
PostgreSQL, then a health check of the outputs, then success. If any task fails, the tasks
after it do not start.

**Q: What are the main settings and why?**
- Every 6 hours (`0 */6 * * *`): regular batches without overloading the machine.
- 3 retries, 5 minutes apart: covers short failures, like Kafka still starting up.
- `max_active_runs=1`: two runs never write to the same files at the same time.
- `catchup=False`: if Airflow was off for a while, it runs once instead of replaying every
  missed run.

**Q: What does the quality check do?**
It checks that Bronze, Silver and Gold exist, can be read and are not empty. If any check
fails, the run stops before scoring.

**Q: What does "idempotent" mean, and is your pipeline idempotent?**
Running a step twice gives the same result as running it once. Yes: layers are replaced (not
added to) on each run, IDs are the same every run, and the database uses an upsert. So
Airflow can retry any step safely.

**Q: Which executor did you use?**
The LocalExecutor, which runs tasks as processes on the same machine. Fine for one machine;
for a cluster you would use the Celery or Kubernetes executor.

**Q: How does each Airflow task run the code?**
Each task calls the same project script you would run by hand (for example
`scripts/run_lakehouse_etl.py`). So Airflow and manual runs always behave the same.

**Q: What problems did you face with Airflow?**
Java would not install the usual way in the Airflow image, so we copied it from a ready-made
image. The workflow could not find the scripts inside Docker because it looked in the wrong
folder. And each run added another full copy of the data, so we switched the layers to
"overwrite".

### About the model

**Q: Which model and why?**
Gradient-boosted trees (Spark MLlib). We compared logistic regression, random forest and
gradient-boosted trees; gradient-boosted trees gave the best results on this table data.

**Q: What features does it use?**
Amount, the sender's and receiver's starting balances, payment type, hour, and flags such as
"is it a transfer or cash-out", "is the receiver a merchant", "was the sender's balance zero",
"amount compared with the sender's balance" and "is it a large payment".

**Q: What is data leakage? How did you avoid it?**
Using information the model would not have at decision time. The balances *after* the
payment make the model look excellent in tests, but a bank must decide before the money
moves. We left them out.

**Q: How did you handle the class imbalance?**
We judged the model on precision, recall and F1 instead of accuracy, and tuned the alert
cut-off (0.4 instead of the default 0.5) on a separate split of the training data.

**Q: Why is the cut-off 0.4?**
It gave the best balance between catching fraud and avoiding false alarms. Lower catches
more fraud but floods analysts with alerts; higher makes alerts more reliable but misses
more fraud.

**Q: Your accuracy is 99.94%. Isn't that great?**
It sounds great, but a model that never flags anything scores 99.87%, because almost every
payment is genuine. That is why we use precision and recall.

**Q: Why do the top alerts have an amount of 0?**
They are real rows. PaySim has 16 zero-amount payments and every one is fraud, so the model
learned that pattern.

**Q: Why are 8 cash-ins flagged?**
They are false alarms. No cash-in in the data is fraud. A simple rule could remove them.

### About serving and Docker

**Q: Why PostgreSQL if you already have Parquet files?**
Parquet is good for scanning lots of data; PostgreSQL is good for quick lookups of single
rows, like "show me this transaction" or "top 100 alerts". Alert queries take milliseconds.

**Q: What is an upsert?**
Insert a row, or update it if a row with the same key already exists. It means loading the
same results twice never creates duplicate rows.

**Q: What does Docker give you?**
Every service (Kafka, PostgreSQL, Airflow, the API) runs in a container with fixed versions,
so the system builds the same way on every machine and starts with one command.

### About results and limitations

**Q: How do you know no data was lost?**
We counted rows at every stage after the full run: CSV, Bronze, Silver, Gold, scores and
PostgreSQL all hold exactly 6,362,620 rows.

**Q: Is it really real-time?**
Not fully. Data enters through Kafka as a stream, but scoring runs in batches every 6 hours.
Scoring each payment as it arrives is our main next step.

**Q: Does it scale?**
The same Spark code runs on a cluster. Kafka can add partitions and brokers. Airflow can use
a distributed executor. Today it runs on one machine and needs about 11 GB of memory.

**Q: What would you improve?**
Score payments the moment they arrive; add automatic row-count and duplicate checks between
layers; merge small files; retrain the model when the data changes; move to the cloud with
logins and failure alerts.

**Q: What was the hardest part?**
Joining the parts. Each part worked on its own, but version mismatches, folder paths and
repeated data only appeared when everything ran together.

**Q: What did you learn?**
That data engineering is mostly about reliability: making every step safe to repeat, never
losing a record, and checking data before using it.

### If you do not know an answer
Say: "Good question. I'm not sure about that detail; the part I can explain is ..." and go
back to what you know. Do not guess numbers.

---

## 7. Timing (about 15 minutes for 17 slides)

| Slides | Time | Focus |
|---|---|---|
| 1 to 5 (intro, team, problem, aims, data) | 3 min | Keep it quick |
| 6 (how the data flows) | 2 min | **Most important slide.** Walk it left to right |
| 7 to 11 (the five steps) | 5 min | Spend longest on 10 (Airflow, your part) |
| 12 to 15 (results, safety, lessons) | 3 min | Say the key numbers once, clearly |
| 16 to 17 (limits, summary) | 1.5 min | End on "no record lost" |
| Demo | 3 to 5 min | Stream, Airflow, dashboard, API |
