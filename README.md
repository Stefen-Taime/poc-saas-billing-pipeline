# POC Real-Time SaaS Billing Pipeline

**Inspired by Stripe's architecture** — Dual streaming + batch pipeline with a serving layer separate from the analytics layer.

**Use case:** Recurring billing SaaS platform, Lightspeed-style.

---

## Architecture

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
                               |
                +--------------+--------------+
                |                             |
         Cloud Dataflow                 Dataproc Spark
        (Beam streaming)              (batch bootstrap)
                |                             |
       +--------+--------+                   |
       |                 |                    |
   Firestore          BigQuery            BigQuery
(serving layer)     (streaming)           (batch)
doc/merchant_id    lightspeed_analytics   lightspeed_batch
       |                 |                    |
  Flask API         Materialized views   Reconciliation log
  (GKE pod)         (MRR, churn, NRR)    (discrepancy < 0.1%)
       |
  GET /merchant/
  {id}/subscription
  < 25ms
```

---

## Tech Stack

| Layer | Technology | Deployment |
|---|---|---|
| Infrastructure | Terraform (10 modules) | GCP us-east1 |
| Orchestration | GKE (e2-standard-4 x 2 nodes) | Terraform |
| Source database | MySQL 8.0 (binlog ROW) | GKE pod |
| CDC | Debezium Server 2.4 (direct Pub/Sub) | GKE pod |
| Messaging | Cloud Pub/Sub (5 topics) | Terraform managed |
| Streaming | Cloud Dataflow (Apache Beam) | GCP managed |
| Batch bootstrap | Ephemeral Dataproc (Spark) | GCP managed |
| Serving layer | Firestore (doc/merchant_id) | Terraform managed |
| Serving API | Flask + Gunicorn | GKE pod (2 replicas) |
| Analytics | BigQuery (materialized views) | Terraform managed |
| Simulator | Go (goroutines) | Local |

---

## Prerequisites

- `gcloud` CLI authenticated (`gcloud auth login`)
- `terraform` >= 1.5
- `go` >= 1.21
- `python` >= 3.11
- `kubectl` installed
- `docker` installed (for building images)
- GCP project with billing enabled
- GCP APIs enabled: Compute, GKE, Pub/Sub, BigQuery, Firestore, Dataflow, Dataproc, GCR

---

## Reproduction Guide — Step by Step

### Step 1 — Deploy Terraform Infrastructure

```bash
cd terraform
cp terraform.tfvars.example terraform.tfvars
# Edit terraform.tfvars: fill in project_id, region, etc.

export TF_VAR_mysql_root_password="your-secure-password"
terraform init
terraform plan -out=tfplan
terraform apply tfplan
```

This creates:
- GKE cluster (2 nodes e2-standard-4)
- 5 CDC Pub/Sub topics + Dataflow subscriptions
- 2 BigQuery datasets (`lightspeed_analytics`, `lightspeed_batch`) + tables
- Firestore database (native mode)
- GCS bucket for MySQL dump
- 4 IAM service accounts (Dataflow, Dataproc, Serving API, Debezium) with Workload Identity

### Step 2 — Configure kubectl and Deploy MySQL

```bash
# Retrieve GKE credentials
gcloud container clusters get-credentials poc-stripe-cluster \
  --region=us-east1 \
  --project=$(gcloud config get-value project)

# Verify that MySQL is deployed by Terraform
kubectl get pods -n lightspeed

# Retrieve MySQL external IP (for the local simulator)
kubectl get svc mysql-lb -n lightspeed -o jsonpath='{.status.loadBalancer.ingress[0].ip}'
```

MySQL is automatically deployed by the Terraform `k8s-mysql` module with:
- `binlog_format=ROW` (required for Debezium)
- `lightspeed_db` schema (5 tables: merchants, subscriptions, invoices, payment_attempts, plan_changes)
- `debezium` user with REPLICATION privileges

### Step 3 — Launch the Go Simulator (data seeding)

```bash
cd go-simulator
go mod tidy

export MYSQL_PASSWORD=$TF_VAR_mysql_root_password
MYSQL_IP=$(kubectl get svc mysql-lb -n lightspeed -o jsonpath='{.status.loadBalancer.ingress[0].ip}')

# Seed-only mode: generates 1 year of history (500 merchants) then exits
go run main.go --mysql-host=$MYSQL_IP --seed-only

# OR full mode: seed + real-time streaming (permanent goroutines)
go run main.go --mysql-host=$MYSQL_IP
```

The simulator generates realistic data calibrated on SaaS benchmarks:
- Monthly churn 3.5%, payment failure rate 6%, trial conversion 65%
- Plans: Starter ($99), Pro ($299), Enterprise ($799), Enterprise Plus ($1,499)
- Lifecycle: trial -> conversion -> invoices -> upgrade/churn/reactivation

### Step 4 — Verify Debezium CDC is Working

Debezium Server is automatically deployed by Terraform (`k8s-debezium`). Verify:

```bash
# Check the Debezium pod
kubectl get pods -n lightspeed -l app=debezium

# Check CDC logs
kubectl logs -n lightspeed -l app=debezium --tail=50

# Verify messages are flowing in Pub/Sub
gcloud pubsub subscriptions pull lightspeed.subscriptions.cdc-sub \
  --project=$(gcloud config get-value project) \
  --limit=1 --auto-ack
```

Debezium reads the MySQL binlog and writes directly to Pub/Sub (no Kafka). The 5 topics:
- `lightspeed.merchants.cdc`
- `lightspeed.subscriptions.cdc`
- `lightspeed.invoices.cdc`
- `lightspeed.payment_attempts.cdc`
- `lightspeed.plan_changes.cdc`

### Step 5 — Launch the Dataflow Pipeline (Beam streaming)

```bash
cd dataflow/beam_pipeline
pip install -r requirements.txt

python pipeline.py \
  --project=$(gcloud config get-value project) \
  --runner=DataflowRunner \
  --region=us-east1 \
  --streaming
```

The pipeline reads the 5 Pub/Sub topics and writes simultaneously to:
- **Firestore** (serving): one document per merchant with the current state (`merchants_state/{merchant_id}`)
- **BigQuery** (analytics): one row per CDC event in `lightspeed_analytics.*_stream`

Verify the job on the Dataflow console or via:

```bash
gcloud dataflow jobs list --region=us-east1
```

### Step 6 — Build and Deploy the Serving API

```bash
cd serving-api

# Build the Docker image
docker build -t gcr.io/$(gcloud config get-value project)/serving-api:latest .
docker push gcr.io/$(gcloud config get-value project)/serving-api:latest

# GKE deployment is managed by Terraform (k8s-serving-api, 2 replicas)
# Retrieve the external IP
API_IP=$(kubectl get svc serving-api-lb -n lightspeed -o jsonpath='{.status.loadBalancer.ingress[0].ip}')

# Test the endpoints
curl http://$API_IP/health
curl http://$API_IP/merchant/MCH-000001/subscription
```

Available endpoints:

| Endpoint | Description |
|---|---|
| `GET /health` | Health check |
| `GET /merchant/{id}/subscription` | Merchant subscription state (< 25ms) |
| `GET /health/mrr` | Total MRR from Firestore |

### Step 7 — Create BigQuery Views

```bash
cd bigquery/views

# Execute each SQL file in BigQuery
bq query --use_legacy_sql=false < mrr_current.sql
bq query --use_legacy_sql=false < churn_monthly.sql
bq query --use_legacy_sql=false < nrr_monthly.sql
bq query --use_legacy_sql=false < mrr_waterfall.sql
bq query --use_legacy_sql=false < failed_payment_rate.sql
```

5 views created:
- `mrr_current` (materialized view) — MRR by plan and status, 15 min refresh
- `churn_monthly` (materialized view) — Churn by month and status
- `nrr_monthly` (view) — NRR with expansion/contraction
- `mrr_waterfall` (view) — New + Expansion - Contraction - Churned MRR
- `failed_payment_rate` (view) — Payment failure rate by error code

### Step 8 — Batch Bootstrap (Dataproc Spark)

Load a MySQL snapshot into BigQuery for reconciliation:

```bash
# 1. Export MySQL to CSV
MYSQL_POD=$(kubectl get pods -n lightspeed -l app=mysql -o jsonpath='{.items[0].metadata.name}')

kubectl exec -n lightspeed $MYSQL_POD -- mysql -u root -p$TF_VAR_mysql_root_password \
  --batch -e "SELECT * FROM lightspeed_db.subscriptions" > /tmp/subscriptions.csv

kubectl exec -n lightspeed $MYSQL_POD -- mysql -u root -p$TF_VAR_mysql_root_password \
  --batch -e "SELECT * FROM lightspeed_db.invoices" > /tmp/invoices.csv

# 2. Upload to GCS
BUCKET="gs://poc-stripe-bootstrap-$(gcloud config get-value project)/mysql-dump"
gsutil cp /tmp/subscriptions.csv $BUCKET/
gsutil cp /tmp/invoices.csv $BUCKET/

# 3. Create an ephemeral Dataproc cluster
gcloud dataproc clusters create poc-stripe-bootstrap \
  --project=$(gcloud config get-value project) \
  --region=us-east1 \
  --single-node \
  --master-machine-type=e2-standard-2 \
  --max-idle=600s \
  --service-account=dataproc-sa@$(gcloud config get-value project).iam.gserviceaccount.com

# 4. Submit the Spark job
gsutil cp dataproc/bootstrap_job.py $BUCKET/jobs/
gcloud dataproc jobs submit pyspark $BUCKET/jobs/bootstrap_job.py \
  --cluster=poc-stripe-bootstrap \
  --region=us-east1 \
  -- --project=$(gcloud config get-value project)
```

The cluster auto-destroys after 10 min of inactivity (`--max-idle=600s`).

### Step 9 — Batch vs Streaming Reconciliation

```bash
cd dataflow/beam_pipeline

python reconciliation_job.py \
  --project=$(gcloud config get-value project) \
  --date=$(date -u -v-1d +%Y-%m-%d)  # yesterday UTC
```

The job compares the MRR of active subscriptions between streaming (BigQuery `lightspeed_analytics`) and batch (BigQuery `lightspeed_batch`). It uses:
- `ROW_NUMBER()` to dedup streaming CDC events
- `INNER JOIN` on `subscription_id` to compare only common subscriptions
- Tie-breaker `montant_mensuel_cad DESC` for identical timestamps

Expected result: discrepancy < 0.1% (status OK). Results are written to `lightspeed_analytics.reconciliation_log`.

To add a Slack alert:

```bash
export SLACK_WEBHOOK_URL="https://hooks.slack.com/services/..."
python reconciliation_job.py --project=$(gcloud config get-value project)
```

---

## Tear Down Infrastructure (after session)

```bash
./scripts/teardown.sh
```

This destroys all GCP infrastructure (GKE, Pub/Sub, BigQuery, Firestore, GCS, IAM).

---

## GCP Cost Estimate

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

---

## Project Tree

```
poc-stripe-lightspeed/
  terraform/
    main.tf                    # Orchestrator for 10 modules
    variables.tf               # Variables (no hardcoded secrets)
    outputs.tf                 # IPs, URLs
    modules/
      gke/                     # GKE cluster + node pool
      pubsub/                  # 5 CDC topics + subscriptions
      bigquery/                # 2 datasets, 7+ tables
      firestore/               # Native mode database
      gcs/                     # Bootstrap bucket
      iam/                     # 4 service accounts + Workload Identity
      k8s-mysql/               # MySQL 8.0 + binlog ROW
      k8s-debezium/            # Debezium Server 2.4
      k8s-serving-api/         # Flask API (2 replicas)
  mysql/
    schema/lightspeed_db.sql   # 5 tables + indexes + debezium user
  go-simulator/
    main.go                    # Entry point, flags, graceful shutdown
    config/scenarios.yaml      # Calibrated SaaS benchmarks
    db/                        # MySQL connection pool
    lifecycle/                 # Merchant lifecycle
    simulators/                # 5 goroutines (merchants, subs, invoices, payments, plan_changes)
    scenarios/                 # YAML parser
  debezium/
    connector-lightspeed.json  # CDC config 5 tables -> Pub/Sub
  dataflow/beam_pipeline/
    pipeline.py                # Streaming CDC -> Firestore + BigQuery
    reconciliation_job.py      # Batch vs streaming comparison
    requirements.txt
  dataproc/
    bootstrap_job.py           # Spark CSV -> BigQuery batch
  serving-api/
    app.py                     # Flask API (3 endpoints)
    Dockerfile                 # Python 3.11 + Gunicorn
    requirements.txt
  bigquery/views/
    mrr_current.sql            # MRR by plan (materialized view)
    churn_monthly.sql          # Monthly churn (materialized view)
    nrr_monthly.sql            # Net Revenue Retention
    mrr_waterfall.sql          # MRR movement waterfall
    failed_payment_rate.sql    # Payment failure rate
  scripts/
    deploy.sh                  # Automated full deploy
    teardown.sh                # Complete teardown
```
