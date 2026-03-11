# Complete Project Explanation — POC Real-Time SaaS Billing Pipeline

## Table of Contents

1. [Context and Objectives](#1-context-and-objectives)
2. [Overall Architecture](#2-overall-architecture)
3. [Phase 1 — Terraform Infrastructure](#3-phase-1--terraform-infrastructure)
4. [Phase 2 — MySQL on GKE](#4-phase-2--mysql-on-gke)
5. [Phase 3 — Go Simulator](#5-phase-3--go-simulator)
6. [Phase 4 — Debezium CDC](#6-phase-4--debezium-cdc)
7. [Phase 5 — Dataflow Pipeline (Beam streaming)](#7-phase-5--dataflow-pipeline-beam-streaming)
8. [Phase 6 — Dataproc batch bootstrap](#8-phase-6--dataproc-batch-bootstrap)
9. [Phase 7 — Flask Serving API](#9-phase-7--flask-serving-api)
10. [Phase 8 — BigQuery materialized views](#10-phase-8--bigquery-materialized-views)
11. [Phase 9 — DataHub lineage (code written, not deployed)](#11-phase-9--datahub-lineage-code-written-not-deployed)
12. [Phase 10 — Batch vs streaming reconciliation](#12-phase-10--batch-vs-streaming-reconciliation)
13. [End-to-end data flow](#13-end-to-end-data-flow)
14. [Issues encountered and solutions](#14-issues-encountered-and-solutions)
15. [Interview applications](#15-interview-applications)

---

## 1. Context and Objectives

### The Problem

Recurring billing SaaS platforms (like Stripe, Lightspeed, Chargebee) need two very different types of data access:

1. **Serving layer (real-time)** — "What is the current state of merchant MCH-000001's subscription?" Response in < 5ms, primary key lookup.
2. **Analytics layer** — "What is the total MRR by plan this month? What is the churn rate?" Response in seconds, aggregations over millions of rows.

These two needs cannot be efficiently served by the same database. A `SELECT SUM(amount) GROUP BY plan` on a transactional table blocks writes. An OLAP scan on Firestore would cost a fortune.

### The Solution: Hybrid Kappa/Lambda Architecture

This project implements an architecture inspired by Stripe:

- **Source of truth**: MySQL (transactional database)
- **CDC (Change Data Capture)**: Debezium captures every INSERT/UPDATE/DELETE from the MySQL binlog
- **Messaging**: Cloud Pub/Sub transports CDC events
- **Dual write**:
  - **Firestore** (serving): one document per merchant, O(1) lookup
  - **BigQuery** (analytics): tables with streaming insert for OLAP
- **Batch bootstrap**: Dataproc/Spark loads a MySQL snapshot into BigQuery for reconciliation
- **Reconciliation**: compares streaming MRR vs batch, alerts if discrepancy > 0.1%

### Why This Design?

| Need | Technology Chosen | Reason |
|---|---|---|
| Merchant state < 5ms | Firestore | Document store, O(1) lookup by merchant_id |
| MRR, churn, NRR | BigQuery | Serverless OLAP, materialized views |
| CDC without Kafka | Debezium Server + Pub/Sub | Reduces operational complexity (no Kafka cluster) |
| Batch oracle | Dataproc Spark | Ephemeral workload, processes CSVs in batch |
| Infrastructure as Code | Terraform | 10 modules, reproducible, destroyed after each session |

---

## 2. Overall Architecture

```
                      [Go Simulator — LOCAL]
                               |
                         INSERT/UPDATE
                               v
                    MySQL 8.0 (GKE pod)
                      binlog_format=ROW
                               |
                    Debezium Server 2.4
                     (GKE pod, no Kafka)
                      direct Pub/Sub sink
                               |
                      Cloud Pub/Sub (5 CDC topics)
                        lightspeed.merchants.cdc
                        lightspeed.subscriptions.cdc
                        lightspeed.invoices.cdc
                        lightspeed.payment_attempts.cdc
                        lightspeed.plan_changes.cdc
                               |
                +--------------+--------------+
                |                             |
         Cloud Dataflow                 Cloud Dataflow
        (Beam streaming)               (batch Spark via Dataproc)
                |                             |
       +--------+--------+                   |
       |                 |                    |
   Firestore          BigQuery            BigQuery
(serving layer)     (streaming)           (batch)
doc/merchant_id    lightspeed_analytics   lightspeed_batch
       |                 |                    |
  Flask API         Materialized views   Reconciliation log
  (GKE pod)         + regular views      (discrepancy < 0.1%)
       |
  GET /merchant/
  {id}/subscription
  < 25ms
```

### Data Flow Summary

1. The Go simulator generates business events (new merchants, payments, churns) by writing to MySQL
2. Debezium Server reads the MySQL binlog and sends CDC events to Pub/Sub (5 topics)
3. The Beam pipeline (Dataflow) reads the 5 topics in streaming and writes simultaneously to:
   - Firestore (serving): one document per merchant with the current state
   - BigQuery (analytics): one row per CDC event in the `*_stream` tables
4. In parallel, Dataproc loads a MySQL snapshot (CSV) into BigQuery `*_batch`
5. Reconciliation compares streaming MRR vs batch MRR
6. The Flask API reads Firestore to serve requests in < 25ms

---

## 3. Phase 1 — Terraform Infrastructure

### Why Terraform?

All GCP infrastructure is defined as code in 10 Terraform modules. This allows:
- Creating the complete infrastructure in a single command (`terraform apply`)
- Destroying everything after each 3-4h session (`terraform destroy`) to limit costs
- Ensuring reproducibility (same infrastructure at each deployment)

### The 10 Terraform Modules

```
terraform/
  main.tf                    # Orchestrator: calls all modules
  variables.tf               # All variables (no hardcoded secrets)
  outputs.tf                 # IPs, URLs, instructions
  terraform.tfvars.example   # Config template
  modules/
    gke/                     # GKE cluster (2 nodes e2-standard-4)
    pubsub/                  # 5 CDC topics + Dataflow subscriptions
    bigquery/                # 2 datasets, 7+ tables (stream + batch + reconciliation)
    firestore/               # Native mode database
    gcs/                     # Bucket for MySQL dump bootstrap
    iam/                     # 4 service accounts + Workload Identity
    k8s-mysql/               # MySQL Deployment + binlog ConfigMap + PVC + LoadBalancer
    k8s-debezium/            # Debezium Server + Workload Identity
    k8s-serving-api/         # Flask API (2 replicas) + LoadBalancer
    k8s-datahub/             # DataHub all-in-one (code written, NOT DEPLOYED)
```

### How It Works

The `main.tf` file orchestrates the creation order:

1. **GKE** first (the cluster must exist before deploying pods)
2. **Pub/Sub, BigQuery, Firestore, GCS, IAM** in parallel (managed services)
3. **k8s-mysql** (depends on GKE)
4. **k8s-debezium** (depends on MySQL + Pub/Sub)
5. **k8s-serving-api** (depends on GKE + Firestore)

The 4 IAM service accounts:
- `dataflow-sa`: for the Beam pipeline (Pub/Sub, BigQuery, Firestore)
- `dataproc-sa`: for the Spark batch (GCS, BigQuery)
- `serving-api-sa`: for the Flask API (Firestore)
- `debezium-sa`: for Debezium (Pub/Sub publisher)

### Commands

```bash
cd terraform
terraform init
terraform plan -out=tfplan
TF_VAR_mysql_root_password="$VOTRE_PASSWORD" terraform apply tfplan
```

---

## 4. Phase 2 — MySQL on GKE

### Why MySQL on GKE?

MySQL is the transactional source of truth. It is deployed on GKE (not Cloud SQL) to:
- Control the `binlog_format=ROW` configuration (required for Debezium)
- Have a LoadBalancer accessible from the local Go simulator
- Reduce costs (Cloud SQL is more expensive for a POC)

### Database Schema

The file `mysql/schema/lightspeed_db.sql` defines 5 tables:

| Table | Description | Primary Key |
|---|---|---|
| `merchants` | SaaS merchant account | `merchant_id` (MCH-000001) |
| `subscriptions` | Active/cancelled subscription | `subscription_id` (SUB-000002) |
| `invoices` | Monthly invoices | `invoice_id` (INV-000003) |
| `payment_attempts` | Payment attempts (retries) | `attempt_id` (ATT-000004) |
| `plan_changes` | Upgrades / Downgrades | `change_id` (CHG-000005) |

Each table has:
- Optimized indexes for CDC queries (`idx_*_updated`)
- Foreign keys for referential integrity
- An `updated_at` field with `ON UPDATE CURRENT_TIMESTAMP` (binlog trigger)

### Debezium User

```sql
CREATE USER 'debezium'@'%' IDENTIFIED BY 'dbz_cdc_2026';
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'debezium'@'%';
```

The `debezium` user has `REPLICATION SLAVE` and `REPLICATION CLIENT` privileges to read the MySQL binlog.

---

## 5. Phase 3 — Go Simulator

### Why Go?

The simulator generates realistic business events to feed MySQL. Go is chosen for:
- **Goroutines**: 5 concurrent simulators without thread overhead
- **Native MySQL connection pool**
- **Clean graceful shutdown** (SIGINT/SIGTERM)

### Simulator Structure

```
go-simulator/
  main.go                          # Entry point, CLI flags, graceful shutdown
  config/scenarios.yaml            # Calibrated SaaS benchmarks
  scenarios/loader.go              # YAML parser
  db/mysql.go                      # Connection pool with retry
  lifecycle/merchant_lifecycle.go  # Complete merchant lifecycle
  simulators/
    merchants.go                   # ~10 events/h
    subscriptions.go               # ~30 events/h
    invoices.go                    # ~200 events/h
    payment_attempts.go            # ~220 events/h
    plan_changes.go                # ~5 events/h
```

### Calibrated SaaS Benchmarks (`scenarios.yaml`)

Parameters are calibrated on real SaaS benchmarks:

| Metric | Value | Source |
|---|---|---|
| Monthly churn | 3.5% | B2B SaaS benchmark (vol. 2.5% + invol. 1%) |
| Payment failure rate | 6% | Stripe Radar average |
| Trial conversion | 65% | B2B SaaS with 14-day trial |
| Target NRR | 110% | Good B2B SaaS (expansion > churn) |
| Monthly upgrade | 1.5% | Natural expansion revenue |
| Reactivation | 2% | Win-back after 45-day median |

### Pricing Plans

| Plan | Amount CAD | Merchant Ratio |
|---|---|---|
| Starter | $99/month | 35% |
| Pro | $299/month | 45% |
| Enterprise | $799/month | 15% |
| Enterprise Plus | $1,499/month | 5% |

### Merchant Lifecycle

```
Sign-up -> Trial (14d) -> Conversion (65%)
                                |
                          Active + Monthly Invoice
                                |
                    +-----------+-----------+
                    |           |           |
                 Upgrade    Payment    Voluntary churn
                 (1.5%/m)   failed(6%)  (2.5%/m)
                    |           |           |
                    |     Retry 1 (70%)     |
                    |     Retry 2 (50%)     |
                    |           |           |
                    |     Involuntary churn |
                    |           |           |
                    |           +-----+-----+
                    |                 |
                    |           Reactivation (2%)
                    +-----------+-----+
```

### Execution Modes

- `--seed-only`: generates 1 year of history (500 merchants) then exits
- `--skip-seed`: skips the seed, launches real-time directly
- Normal mode: seed + real-time (permanent goroutines)

### Seasonality

The simulator simulates seasonality:
- **January**: +40% spike (New Year resolutions)
- **July-August**: -40% slowdown (vacations)

---

## 6. Phase 4 — Debezium CDC

### Why Debezium Server (and not Kafka Connect)?

The classic Debezium architecture uses Kafka Connect + a Kafka broker. Here, we use **Debezium Server** in standalone mode:

```
Classic architecture:   MySQL -> Debezium Connect -> Kafka -> Kafka Connect Pub/Sub Sink -> Pub/Sub
Our architecture:       MySQL -> Debezium Server -> Pub/Sub (direct)
```

Advantages:
- **No Kafka cluster** to manage (reduces complexity + costs)
- **Direct Pub/Sub sink**: Debezium Server writes directly to Pub/Sub
- A single GKE pod instead of 3+ (Kafka broker, ZooKeeper, Connect)

### Configuration

The file `debezium/connector-lightspeed.json` configures:
- Source: MySQL (`lightspeed_db`, 5 tables)
- Sink: Google Cloud Pub/Sub
- Routing: `lightspeed.lightspeed_db.{table}` -> `lightspeed.{table}.cdc`
- Schemas: `schemas.enable=false` (no schema envelope, just raw JSON)

### CDC Message Format

Each Pub/Sub message contains a JSON like:

```json
{
  "before": null,
  "after": {
    "subscription_id": "SUB-000002",
    "merchant_id": "MCH-000001",
    "plan": "pro",
    "statut": "active",
    "montant_mensuel_cad": "299.00",
    "date_debut": 20148,
    "updated_at": 1741305600000
  },
  "source": {
    "table": "subscriptions",
    "db": "lightspeed_db"
  },
  "op": "c",
  "ts_ms": 1741305600123
}
```

Important points about the Debezium format:
- **Dates** are integers (days since epoch 1970-01-01): `20148` = 2025-03-04
- **Decimals** are strings: `"299.00"` (with `decimal.handling.mode=string`)
- **Booleans** are integers: `0` or `1`
- The `source.table` field identifies the source table (routing to BigQuery)
- The `op` field identifies the operation: `c` (create), `u` (update), `d` (delete)

---

## 7. Phase 5 — Dataflow Pipeline (Beam streaming)

### Why Apache Beam on Dataflow?

Apache Beam is the unified framework for streaming and batch. Dataflow is GCP's managed runner:
- **Auto-scaling**: adjusts the number of workers based on volume
- **Exactly-once**: delivery guarantee with Pub/Sub
- **Serverless**: no infrastructure to manage

### Pipeline Architecture (`pipeline.py`)

```
Pub/Sub (5 subscriptions)
    |
    v
ParseCDCEvent (DoFn)
    |-- Decode Debezium JSON
    |-- Normalize dates (epoch-day -> YYYY-MM-DD)
    |-- Decode decimals (string/base64 -> float)
    |-- Convert booleans (0/1 -> true/false)
    |
    +---> WriteToFirestore (DoFn)
    |       |-- merchants -> merchants_state/{merchant_id} (merge)
    |       |-- subscriptions -> merchants_state/{merchant_id} (merge)
    |       |-- payment_attempts -> merchants_state/{merchant_id} (merge)
    |
    +---> WriteToBigQuery (DoFn)
            |-- subscriptions -> subscriptions_stream
            |-- invoices -> invoices_stream
            |-- payment_attempts -> payment_attempts_stream
            |-- plan_changes -> plan_changes_stream
```

### The 3 Main Transforms

**1. ParseCDCEvent** — Debezium Decoding

Decoding Debezium values is critical:

```python
# Dates: days since epoch -> "YYYY-MM-DD"
_EPOCH = date(1970, 1, 1)
def _decode_debezium_date(value):
    return (_EPOCH + timedelta(days=value)).isoformat()  # 20148 -> "2025-03-04"

# Decimals: string -> float
def _decode_debezium_decimal(value, scale):
    return float(value)  # "299.00" -> 299.0

# Booleans: 0/1 -> True/False
annulation_schedulee = bool(value)  # 0 -> False
```

**2. WriteToFirestore** — Serving Layer

Each CDC event is transformed into a Firestore merge:
- Collection: `merchants_state`
- Document ID: `merchant_id`
- `merge=True`: only updates the fields present (no overwrite)

The `merchants`, `subscriptions`, and `payment_attempts` tables merge into the same document.
The result: a single document per merchant with the complete state.

**3. WriteToBigQuery** — Analytics Layer

Each event is inserted via streaming into BigQuery:
- Destination table: `{table}_stream` in the `lightspeed_analytics` dataset
- Added metadata: `cdc_operation` (c/u/d) and `cdc_timestamp`

### Deployment

```bash
python pipeline.py \
  --project=$(gcloud config get-value project) \
  --runner=DataflowRunner \
  --region=us-east1 \
  --streaming
```

The Dataflow job `lightspeed-cdc-streaming-v5` runs continuously on GCP.

---

## 8. Phase 6 — Dataproc Batch Bootstrap

### Why a Batch in Parallel with Streaming?

Streaming is the primary data path. But a **batch oracle** is needed to:
1. **Reconciliation**: verify that streaming hasn't lost/duplicated data
2. **Backfill**: if streaming goes down for 1h, batch can fill the gap
3. **Audit**: an independent source for compliance

### The Batch Flow

```
MySQL (GKE pod)
    |
    v (kubectl exec + mysql --batch)
Local CSV (/tmp/subscriptions.csv, /tmp/invoices.csv)
    |
    v (gsutil cp)
GCS (gs://poc-stripe-bootstrap-$PROJECT_ID/mysql-dump/)
    |
    v (gcloud dataproc jobs submit pyspark)
Ephemeral Dataproc (Spark)
    |
    v
BigQuery (lightspeed_batch.subscriptions_batch, invoices_batch)
```

### The Spark Job (`bootstrap_job.py`)

The Spark job reads CSVs from GCS and writes to BigQuery.

The issue encountered: **Spark's `inferSchema=true` infers types incompatible** with BigQuery:

| Column | Spark Infers | BigQuery Expects |
|---|---|---|
| `annulation_schedulee` | `int` (0/1) | `BOOLEAN` |
| `montant_mensuel_cad` | `double` | `NUMERIC(10,2)` |
| `date_debut` | `string` | `DATE` |

The solution: **explicit Spark schemas** with post-read casts:

```python
SUBSCRIPTIONS_SCHEMA = StructType([
    StructField("subscription_id", StringType(), False),
    StructField("montant_mensuel_cad", StringType(), True),  # read as string, cast to DecimalType
    StructField("annulation_schedulee", StringType(), True),  # read as string, cast to boolean
    ...
])

df = spark.read.option("header", "true").schema(schema).csv(input_path)
df = df.withColumn("montant_mensuel_cad", F.col("montant_mensuel_cad").cast(DecimalType(10, 2)))
df = df.withColumn("annulation_schedulee", F.col("annulation_schedulee").cast("int").cast("boolean"))
```

### Missing Permission

The Dataproc service account (`dataproc-sa`) had `bigquery.dataEditor` but not `bigquery.jobUser`. Without this role, it cannot create BigQuery load jobs. Fixed with:

```bash
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:dataproc-sa@$PROJECT_ID.iam.gserviceaccount.com" \
  --role="roles/bigquery.jobUser"
```

### Ephemeral Cluster

```bash
gcloud dataproc clusters create poc-stripe-bootstrap \
  --single-node \
  --master-machine-type=e2-standard-2 \
  --max-idle=600s  # auto-delete after 10 min of inactivity
```

The `--max-idle=600s` is crucial: the cluster auto-destroys after 10 minutes without a job. No unnecessary costs.

### Result

- `subscriptions_batch`: 515 rows (313 active, MRR $122,787 CAD)
- `invoices_batch`: 1,936 rows

---

## 9. Phase 7 — Flask Serving API

### Why Firestore + Flask?

The serving API simulates the Stripe Subscriptions API. It responds in < 25ms thanks to Firestore:
- **O(1) lookup**: one document per `merchant_id`
- **No joins**: all merchant data is denormalized in a single document
- **Natural caching**: Firestore is geo-replicated and edge-cached

### The 3 Endpoints

| Endpoint | Description | Measured Latency |
|---|---|---|
| `GET /health` | Health check | < 1ms |
| `GET /merchant/{id}/subscription` | Merchant subscription state | ~25ms (median) |
| `GET /health/mrr` | Total MRR from Firestore | ~200ms |

### Example Response

```
GET /merchant/MCH-000001/subscription
```

```json
{
  "merchant_id": "MCH-000001",
  "nom": "Merchant 1",
  "plan_actuel": "pro",
  "statut_abonnement": "active",
  "montant_mensuel_cad": 299.0,
  "date_prochain_paiement": "2025-03-18",
  "annulation_schedulee": false,
  "_latency_ms": 24.7
}
```

### Deployment

- 2 replicas on GKE with LoadBalancer
- Docker image: Python 3.11 + Gunicorn
- Workload Identity for Firestore access (no JSON key)
- Readiness/liveness probes on `/health`

---

## 10. Phase 8 — BigQuery Materialized Views

### Why Materialized Views?

BigQuery supports **materialized views** (MV) that pre-compute aggregations and refresh automatically. This is the equivalent of Apache Pinot for sub-second queries on pre-aggregated data.

### The 5 Views Created

| View | Type | Description |
|---|---|---|
| `mrr_current` | MATERIALIZED VIEW | MRR by plan and status, 15 min refresh |
| `churn_monthly` | MATERIALIZED VIEW | Churn by month and status, 15 min refresh |
| `nrr_monthly` | VIEW (regular) | NRR with expansion/contraction (too complex for MV) |
| `mrr_waterfall` | VIEW (regular) | New + Expansion - Contraction - Churned |
| `failed_payment_rate` | VIEW (regular) | Payment failure rate, error codes, revenue at risk |

### BigQuery Materialized View Restrictions

BigQuery MVs have strict restrictions. Here is what is **NOT supported**:

| Restriction | Forbidden Example | Solution |
|---|---|---|
| Non-deterministic functions | `CURRENT_DATE()` | Use `DATE_TRUNC(DATE(updated_at), MONTH)` |
| Scalar transforms on aggregates | `SUM(amount) * 12` | Compute `* 12` in the query that reads the MV |
| `COUNTIF` | `COUNTIF(statut = 'failed')` | `GROUP BY statut` + `COUNT(*)` |
| `SAFE_DIVIDE` | `SAFE_DIVIDE(a, b)` | Compute in the query that reads the MV |
| CTEs (`WITH`) | `WITH cte AS (...)` | Regular view |
| `JOIN` | `FROM a JOIN b` | Regular view |
| Window functions | `LAG()`, `ROW_NUMBER()` | Regular view |

This is why `nrr_monthly`, `mrr_waterfall`, and `failed_payment_rate` are regular views: they use CTEs, JOINs, `LAG()`, `COUNTIF`, and `SAFE_DIVIDE`.

### Example: MRR Waterfall

```sql
-- New MRR: subscriptions created this month (CDC operation = 'c')
-- Expansion: plan_changes with new_amount > old_amount
-- Contraction: plan_changes with new_amount < old_amount
-- Churned: subscriptions that moved to 'cancelled' or 'churned'
-- MRR End = sum(active amounts)
-- ARR = MRR * 12
```

---

## 11. Phase 9 — DataHub Lineage (code written, not deployed)

> **Note**: The DataHub emitter code (`datahub/emitters/datahub_emitter.py`) and the Terraform module `k8s-datahub` were written, but DataHub **was never deployed or used** in this POC. This phase remains future work.

### What Was Coded (but not executed)

- `datahub_emitter.py`: Python script that will emit the complete lineage (MySQL -> Pub/Sub -> Dataflow -> Firestore/BigQuery) via the DataHub REST API
- Terraform module `k8s-datahub`: all-in-one deployment (GMS + Frontend + Elasticsearch + Kafka + internal MySQL)

### Why It Was Not Deployed

- DataHub all-in-one is resource-hungry (Kafka + ES + internal MySQL) — the 2-node GKE cluster was already loaded with MySQL, Debezium, and the Serving API
- It was not blocking for the POC: lineage is a bonus, not a prerequisite for demonstrating the CDC pipeline

---

## 12. Phase 10 — Batch vs Streaming Reconciliation

### Why Reconcile?

Streaming is the fast path, but it can:
- **Lose messages** (Pub/Sub ack without processing)
- **Duplicate messages** (Pub/Sub retry)
- **Have transient states** (intermediate CDC events)

The batch (MySQL snapshot) is the **source of truth**. Reconciliation compares them.

### The Reconciliation Job (`reconciliation_job.py`)

The job compares the MRR of active subscriptions from both sources:

```python
# 1. Streaming: dedup via ROW_NUMBER (take the latest state per subscription_id)
# 2. Batch: MySQL snapshot as-is
# 3. INNER JOIN on subscription_id (compare only common subs)
# 4. Discrepancy = |MRR_streaming - MRR_batch| / MRR_batch
# 5. If discrepancy > 0.1% -> Slack alert
# 6. Write to reconciliation_log BigQuery
```

### The 3 Corrections Needed to Reach Discrepancy < 0.1%

The reconciliation required 3 correction iterations:

**Iteration 1 — No deduplication (72% discrepancy)**

The streaming BigQuery contains **multiple rows per subscription** (each MySQL UPDATE generates a new CDC row). The raw sum of `montant_mensuel_cad` counted duplicates.

```sql
-- BEFORE (incorrect):
SELECT SUM(montant_mensuel_cad) FROM subscriptions_stream WHERE statut = 'active'
-- Result: $191,907 (inflated by duplicates)
```

Solution: **`ROW_NUMBER()` by subscription_id** to keep only the latest version:

```sql
ROW_NUMBER() OVER (PARTITION BY subscription_id ORDER BY updated_at DESC) AS rn
-- Then: WHERE rn = 1 AND statut = 'active'
```

**Iteration 2 — Different subscription IDs (17.9% discrepancy)**

The streaming data contained `subscription_id`s from old CDC sessions that no longer existed in MySQL (and therefore not in the batch). The raw comparison included these phantom subscriptions.

Solution: **INNER JOIN** on `subscription_id` — compare only subscriptions present in both sources:

```sql
FROM streaming_active s
INNER JOIN batch_active b ON s.subscription_id = b.subscription_id
```

**Iteration 3 — Non-deterministic tie-breaker (0.18% discrepancy)**

SUB-004687 had **two rows with the same `updated_at`**: a downgrade to $99 and an upgrade to $299. The `ROW_NUMBER()` randomly picked the downgrade, while MySQL had the upgrade.

Solution: **tie-breaker `montant_mensuel_cad DESC`** — in case of timestamp tie, take the highest amount (final state):

```sql
ROW_NUMBER() OVER (
    PARTITION BY subscription_id
    ORDER BY updated_at DESC, montant_mensuel_cad DESC
) AS rn
```

### Final Result

| Iteration | Discrepancy | Status | Correction |
|---|---|---|---|
| 1 | 72.08% | KO | Added `ROW_NUMBER()` dedup |
| 2 | 17.89% | KO | Added `INNER JOIN` on subscription_id |
| 3 | 0.18% | KO | Added tie-breaker `montant_mensuel_cad DESC` |
| 4 | **0.00%** | **OK** | Streaming $111,519 = Batch $111,519 |

### History in BigQuery

The `reconciliation_log` table contains the complete history:

```
+---------------------+------+-----------------+-------------+--------+
|     checked_at      | date | mrr_streaming   | mrr_batch   | status |
+---------------------+------+-----------------+-------------+--------+
| 2026-03-11 04:35:57 | ...  | 191,907         | 111,519     | KO     |  <- no dedup
| 2026-03-11 04:36:25 | ...  | 131,475         | 111,519     | KO     |  <- dedup but no INNER JOIN
| 2026-03-11 04:42:23 | ...  | 111,319         | 111,519     | KO     |  <- INNER JOIN but no tie-break
| 2026-03-11 04:46:34 | ...  | 111,319         | 111,519     | KO     |  <- re-export, same issue
| 2026-03-11 04:47:44 | ...  | 111,519         | 111,519     | OK     |  <- tie-breaker added
+---------------------+------+-----------------+-------------+--------+
```

---

## 13. End-to-End Data Flow

### Scenario: A Merchant Signs Up and Pays

1. **Go Simulator**: `INSERT INTO merchants VALUES ('MCH-000501', 'Boutique Laval', ...)`
2. **MySQL binlog**: records the INSERT in the binary log (ROW format)
3. **Debezium Server**: reads the binlog, generates a CDC JSON message
4. **Pub/Sub**: the message arrives in the `lightspeed.merchants.cdc` topic
5. **Dataflow**: the Beam pipeline reads the message:
   - `ParseCDCEvent`: decodes the JSON, normalizes dates/decimals
   - `WriteToFirestore`: merges the document `merchants_state/MCH-000501`
   - `WriteToBigQuery`: inserts a row into `merchants_stream`
6. **Flask API**: `GET /merchant/MCH-000501/subscription` returns the state in 25ms
7. **BigQuery**: the `mrr_current`, `churn_monthly` views refresh every 15 min

### Scenario: A Payment Fails and the Merchant Churns

1. Simulator: `INSERT INTO payment_attempts (statut='failed', code_erreur='card_expired')`
2. CDC -> Pub/Sub -> Dataflow -> Firestore (increment `nb_echecs_paiement_consecutifs`)
3. After 3 failures: Simulator `UPDATE subscriptions SET statut='churned'`
4. CDC -> Pub/Sub -> Dataflow -> Firestore (statut_abonnement = 'churned')
5. BigQuery: `churn_monthly` shows the involuntary churn
6. View `failed_payment_rate` shows the failure rate by error code

---

## 14. Issues Encountered and Solutions

### 1. Apache Beam Incompatible with Python 3.12

**Problem**: `apache-beam==2.53.0` required `grpcio-tools==1.53.0` which does not compile under Python 3.12 (`ModuleNotFoundError: No module named 'pkg_resources'`).

**Solution**: Upgrade to `apache-beam>=2.56.0` (2.71.0 installed, compatible with Python 3.12).

### 2. Debezium Format Without Schema Envelope

**Problem**: The pipeline expected `raw["payload"]["after"]` but with `schemas.enable=false`, Debezium sends `raw["after"]` directly.

**Solution**: `payload = raw.get("payload", raw)` — accepts both formats.

### 3. BigQuery Materialized View Restrictions

**Problem**: `CURRENT_DATE()`, `COUNTIF`, `SAFE_DIVIDE`, CTEs, JOINs, `LAG()` are forbidden in MVs.

**Solution**: 2 simple MVs (GROUP BY only) + 3 regular views for the rest.

### 4. Spark inferSchema Type Mismatch

**Problem**: Spark infers `int` for `annulation_schedulee` (BQ expects `BOOLEAN`), `double` for `montant` (BQ expects `NUMERIC`).

**Solution**: Explicit Spark schemas + casts `DecimalType(10,2)` and `string -> int -> boolean`.

### 5. Dataproc SA Without bigquery.jobUser

**Problem**: The SA had `bigquery.dataEditor` but not `bigquery.jobUser`, so it could not create load jobs.

**Solution**: `gcloud projects add-iam-policy-binding --role=roles/bigquery.jobUser`.

### 6. Non-Deterministic ROW_NUMBER

**Problem**: Two CDC rows with the same `updated_at` — the `ROW_NUMBER()` picked the wrong one.

**Solution**: Tie-breaker `montant_mensuel_cad DESC` to guarantee the latest state.

---

## 15. Interview Applications

### Lightspeed (Primary Use Case)

This POC directly simulates the Lightspeed recurring billing platform:
- Merchants with monthly subscriptions
- MRR, churn, NRR as key metrics
- Serving layer < 5ms for the merchant API
- Dual streaming + batch pipeline with reconciliation

### BNC (National Bank of Canada)

The architecture translates to the banking sector:
- **Serving layer** (Firestore) = client account state (balance, status)
- **Analytics layer** (BigQuery) = credit risk, compliance
- **Reconciliation** = verify that real-time balance matches the accounting batch
- Lineage (DataHub or equivalent) would be a natural addition for OSFI E-21 compliance

### Intact Financial (Insurance)

The architecture translates to insurance:
- **Serving layer** = active policy state
- **Analytics layer** = loss ratio metrics, actuarial analysis
- **CDC** = real-time capture of policy modifications
- **Reconciliation** = verify calculated premiums vs the central system

---

## Project Tree

```
poc-stripe-lightspeed/
  terraform/
    main.tf, variables.tf, outputs.tf
    modules/ (10 modules)
  mysql/
    schema/lightspeed_db.sql
  go-simulator/
    main.go, config/scenarios.yaml
    db/, lifecycle/, simulators/, scenarios/
  debezium/
    connector-lightspeed.json
  dataflow/beam_pipeline/
    pipeline.py              # Streaming CDC -> Firestore + BigQuery
    reconciliation_job.py    # Batch vs streaming comparison
    requirements.txt
  dataproc/
    bootstrap_job.py         # Spark CSV -> BigQuery batch
  serving-api/
    app.py, Dockerfile, requirements.txt
  bigquery/views/
    mrr_current.sql, churn_monthly.sql, nrr_monthly.sql
    mrr_waterfall.sql, failed_payment_rate.sql
  datahub/emitters/               # Code written but NOT DEPLOYED
    datahub_emitter.py
  scripts/
    deploy.sh, teardown.sh
```

---

## GCP Costs (3-4h session)

| Service | Configuration | Cost/hour |
|---|---|---|
| GKE cluster (2 nodes e2-standard-4) | 8 vCPU, 32 GB RAM total | ~$0.27 |
| Cloud Pub/Sub | ~100K messages/h | ~$0.00 |
| Cloud Dataflow | 1 worker n1-standard-1 | ~$0.05 |
| Dataproc (ephemeral) | 1 node e2-standard-2, max-idle 10min | ~$0.02 |
| Firestore | < 1 GB, < 50K reads/day | ~$0.00 |
| BigQuery | Free tier | ~$0.00 |
| **Total 4h session** | | **~$1.30** |

> Always run `./scripts/teardown.sh` after each session.
