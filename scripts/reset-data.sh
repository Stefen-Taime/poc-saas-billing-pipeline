#!/usr/bin/env bash
# =============================================================================
# reset-data.sh — Wipe ALL data from every store and restart fresh
#
# Targets: MySQL, BigQuery, Firestore, Pub/Sub (drain), Debezium offsets
# Does NOT destroy infrastructure — only data.
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"
TF_DIR="$ROOT_DIR/terraform"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log()  { echo -e "${GREEN}[reset]${NC} $1"; }
warn() { echo -e "${YELLOW}[reset]${NC} $1"; }
err()  { echo -e "${RED}[reset]${NC} $1"; exit 1; }

# --- Pre-flight ---
command -v gcloud >/dev/null 2>&1  || err "gcloud not found"
command -v kubectl >/dev/null 2>&1 || err "kubectl not found"
command -v bq >/dev/null 2>&1      || err "bq not found"

PROJECT_ID=$(gcloud config get-value project 2>/dev/null)
[ -z "$PROJECT_ID" ] && err "No GCP project set. Run: gcloud config set project YOUR_PROJECT"

NAMESPACE="lightspeed"
MYSQL_DATABASE="lightspeed_db"
BQ_DATASET_ANALYTICS="lightspeed_analytics"
BQ_DATASET_BATCH="lightspeed_batch"
FIRESTORE_COLLECTION="merchants_state"

# --- Confirmation ---
echo ""
echo -e "${RED}WARNING: This will DELETE ALL DATA (not infrastructure):${NC}"
echo "  - MySQL: TRUNCATE all 5 tables in ${MYSQL_DATABASE}"
echo "  - BigQuery: DELETE all rows from 7 tables in ${BQ_DATASET_ANALYTICS} + ${BQ_DATASET_BATCH}"
echo "  - Firestore: DELETE all documents in ${FIRESTORE_COLLECTION}"
echo "  - Pub/Sub: SEEK subscriptions to now (discard pending messages)"
echo "  - Debezium: DELETE offset + schema history files (force re-snapshot)"
echo ""
read -p "Type 'reset' to confirm: " confirm
[ "$confirm" != "reset" ] && { log "Aborted."; exit 0; }

# =============================================================================
# 1. Stop Debezium (so it doesn't capture our TRUNCATE as CDC events)
# =============================================================================
log "1/6 — Scaling down Debezium to 0 replicas..."
kubectl -n "$NAMESPACE" scale deployment/debezium --replicas=0 2>/dev/null || warn "Debezium deployment not found — skipping"
sleep 5

# =============================================================================
# 2. Wipe MySQL — TRUNCATE all tables (order matters: FK constraints)
# =============================================================================
log "2/6 — Truncating MySQL tables..."
MYSQL_POD=$(kubectl -n "$NAMESPACE" get pod -l app=mysql -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")

if [ -n "$MYSQL_POD" ]; then
    kubectl -n "$NAMESPACE" exec "$MYSQL_POD" -- mysql -u root -p"${TF_VAR_mysql_root_password:?Set TF_VAR_mysql_root_password}" "$MYSQL_DATABASE" -e "
        SET FOREIGN_KEY_CHECKS = 0;
        TRUNCATE TABLE plan_changes;
        TRUNCATE TABLE payment_attempts;
        TRUNCATE TABLE invoices;
        TRUNCATE TABLE subscriptions;
        TRUNCATE TABLE merchants;
        SET FOREIGN_KEY_CHECKS = 1;
        RESET MASTER;
    " 2>/dev/null && log "  MySQL: all tables truncated + binlog reset" || warn "  MySQL truncate failed"
else
    warn "  MySQL pod not found — skipping"
fi

# =============================================================================
# 3. Wipe BigQuery — DELETE FROM all tables
# =============================================================================
log "3/6 — Deleting BigQuery data..."

BQ_TABLES_ANALYTICS=(
    "subscriptions_stream"
    "invoices_stream"
    "payment_attempts_stream"
    "plan_changes_stream"
    "reconciliation_log"
)

BQ_TABLES_BATCH=(
    "subscriptions_batch"
    "invoices_batch"
)

for table in "${BQ_TABLES_ANALYTICS[@]}"; do
    bq query --use_legacy_sql=false --project_id="$PROJECT_ID" \
        "DELETE FROM \`${PROJECT_ID}.${BQ_DATASET_ANALYTICS}.${table}\` WHERE TRUE" \
        2>/dev/null && log "  BQ: ${BQ_DATASET_ANALYTICS}.${table} cleared" || warn "  BQ: ${BQ_DATASET_ANALYTICS}.${table} — failed or empty"
done

for table in "${BQ_TABLES_BATCH[@]}"; do
    bq query --use_legacy_sql=false --project_id="$PROJECT_ID" \
        "DELETE FROM \`${PROJECT_ID}.${BQ_DATASET_BATCH}.${table}\` WHERE TRUE" \
        2>/dev/null && log "  BQ: ${BQ_DATASET_BATCH}.${table} cleared" || warn "  BQ: ${BQ_DATASET_BATCH}.${table} — failed or empty"
done

# =============================================================================
# 4. Wipe Firestore — delete all documents in merchants_state
# =============================================================================
log "4/6 — Deleting Firestore documents..."
gcloud firestore documents list \
    --project="$PROJECT_ID" \
    --database="(default)" \
    "projects/${PROJECT_ID}/databases/(default)/documents/${FIRESTORE_COLLECTION}" \
    --format="value(name)" 2>/dev/null | while read -r doc; do
    gcloud firestore documents delete "$doc" --project="$PROJECT_ID" --database="(default)" --quiet 2>/dev/null || true
done
log "  Firestore: ${FIRESTORE_COLLECTION} cleared"

# =============================================================================
# 5. Drain Pub/Sub — seek all subscriptions to now
# =============================================================================
log "5/6 — Draining Pub/Sub subscriptions..."

PUBSUB_SUBS=(
    "lightspeed.merchants.cdc-dataflow-sub"
    "lightspeed.subscriptions.cdc-dataflow-sub"
    "lightspeed.invoices.cdc-dataflow-sub"
    "lightspeed.payment_attempts.cdc-dataflow-sub"
    "lightspeed.plan_changes.cdc-dataflow-sub"
)

for sub in "${PUBSUB_SUBS[@]}"; do
    gcloud pubsub subscriptions seek "projects/${PROJECT_ID}/subscriptions/${sub}" \
        --time="$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
        --project="$PROJECT_ID" 2>/dev/null \
        && log "  Pub/Sub: ${sub} seeked to now" \
        || warn "  Pub/Sub: ${sub} — seek failed or not found"
done

# =============================================================================
# 6. Wipe Debezium offsets + schema history (force fresh snapshot)
# =============================================================================
log "6/6 — Wiping Debezium offset + schema history..."
DEBEZIUM_POD=$(kubectl -n "$NAMESPACE" get pod -l app=debezium -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")

if [ -n "$DEBEZIUM_POD" ]; then
    kubectl -n "$NAMESPACE" exec "$DEBEZIUM_POD" -c debezium-server -- \
        sh -c "rm -f /debezium/data/offsets.dat /debezium/data/schema-history.dat" 2>/dev/null \
        && log "  Debezium: offsets + schema history deleted" \
        || warn "  Debezium: could not delete files (pod may be down)"
else
    # Pod is scaled to 0 — delete PVC data by recreating PVC
    warn "  Debezium pod is down — deleting and recreating PVC..."
    kubectl -n "$NAMESPACE" delete pvc debezium-data 2>/dev/null || true
    log "  PVC deleted — Terraform will recreate it on next apply"
fi

# =============================================================================
# 7. Restart Debezium (scale back up)
# =============================================================================
log "Scaling Debezium back up..."
kubectl -n "$NAMESPACE" scale deployment/debezium --replicas=1 2>/dev/null || warn "Could not scale up Debezium — run terraform apply"

# =============================================================================
# Done
# =============================================================================
echo ""
log "=========================================="
log "All data wiped. Ready to re-seed from zero."
log "=========================================="
log ""
log "Next steps:"
log "  1. If PVC was deleted, run: cd terraform && terraform apply"
log "  2. Wait for Debezium pod to be ready:"
log "     kubectl -n ${NAMESPACE} wait --for=condition=ready pod -l app=debezium --timeout=120s"
log "  3. Re-seed MySQL with the Go simulator:"
log "     cd go-simulator && go run main.go --mysql-host=\$(terraform -chdir=${TF_DIR} output -raw mysql_external_ip)"
