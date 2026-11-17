"""Show the dashboard without Docker, using the scores already in data/gold/fraud_predictions.

Backup for demos on a laptop that cannot run Docker. It stands in for PostgreSQL only:
the numbers are the pipeline's real output from the last full run.

    python scripts/serve_dashboard_offline.py          # then open http://127.0.0.1:8766
"""

import json
import sys
from collections import Counter
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import pyarrow.compute as pc
import pyarrow.dataset as ds

REPO = Path(__file__).resolve().parent.parent
PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 8766
COLUMNS = ["transaction_id", "type", "amount", "step", "fraud_probability",
           "fraud_prediction", "model_threshold"]

print("Reading scores from data/gold/fraud_predictions (about a minute)...", flush=True)
total, flagged_by_type, flagged_rows = Counter(), Counter(), []
for batch in ds.dataset(REPO / "data/gold/fraud_predictions", format="parquet").to_batches(
        columns=COLUMNS, batch_size=200_000):
    for t, n in zip(*[c.to_pylist() for c in pc.value_counts(batch.column("type")).flatten()]):
        total[t] += n
    hits = batch.filter(pc.equal(batch.column("fraud_prediction"), 1)).to_pylist()
    for r in hits:
        flagged_by_type[r["type"]] += 1
    flagged_rows.extend(hits)

flagged_rows.sort(key=lambda r: (-r["fraud_probability"], r["transaction_id"]))
by_id = {r["transaction_id"]: r for r in flagged_rows}
cfg = json.loads((REPO / "models/member3/model_config.json").read_text(encoding="utf-8"))

STATS = {
    "total": sum(total.values()),
    "flagged": len(flagged_rows),
    "by_type": sorted(({"type": t, "total": total[t], "flagged": flagged_by_type[t]} for t in total),
                      key=lambda r: (-r["flagged"], r["type"])),
    "runs": [],
    "model": {k: cfg[k] for k in ("model_type", "threshold", "test_precision", "test_recall", "test_f1")},
    "airflow_url": "http://localhost:8081",
}

# The run-status line needs a Postgres load record, so hide it instead of showing an empty state
PAGE = (REPO / "src/serving/static/index.html").read_text(encoding="utf-8").replace(
    "</style>", "#runline{display:none}</style>", 1).encode()


class Handler(BaseHTTPRequestHandler):
    def send(self, code, body, kind="application/json"):
        data = body if isinstance(body, bytes) else json.dumps(body).encode()
        self.send_response(code)
        self.send_header("Content-Type", kind)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        url = urlparse(self.path)
        q = parse_qs(url.query)
        if url.path == "/":
            return self.send(200, PAGE, "text/html; charset=utf-8")
        if url.path == "/health":
            return self.send(200, {"status": "ok", "database": "ok"})
        if url.path == "/stats":
            return self.send(200, STATS)
        if url.path == "/alerts":
            minp = float(q.get("min_probability", ["0.4"])[0])
            limit = int(q.get("limit", ["100"])[0])
            return self.send(200, [r for r in flagged_rows if r["fraud_probability"] >= minp][:limit])
        if url.path.startswith("/transactions/"):
            row = by_id.get(url.path.rsplit("/", 1)[1])
            return self.send(200, row) if row else self.send(404, {"detail": "Transaction not found"})
        self.send(404, {"detail": "Not found"})

    def log_message(self, *args):
        pass


print(f"{STATS['total']:,} scored, {STATS['flagged']:,} flagged. Open http://127.0.0.1:{PORT}", flush=True)
HTTPServer(("127.0.0.1", PORT), Handler).serve_forever()
