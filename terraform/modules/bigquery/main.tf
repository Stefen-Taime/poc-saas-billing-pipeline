###############################################################################
# Module: bigquery — Analytics + Batch datasets with tables
###############################################################################

# ---------- Datasets ----------
resource "google_bigquery_dataset" "analytics" {
  dataset_id = var.dataset_analytics
  project    = var.project_id
  location   = var.region

  description                 = "Streaming analytics — CDC events from Dataflow"
  default_table_expiration_ms = null
  delete_contents_on_destroy  = true

  labels = {
    pipeline = "streaming"
  }
}

resource "google_bigquery_dataset" "batch" {
  dataset_id = var.dataset_batch
  project    = var.project_id
  location   = var.region

  description                 = "Batch bootstrap — historical data from Dataproc"
  default_table_expiration_ms = null
  delete_contents_on_destroy  = true

  labels = {
    pipeline = "batch"
  }
}

# ---------- Analytics tables (streaming) ----------
resource "google_bigquery_table" "subscriptions_stream" {
  dataset_id          = google_bigquery_dataset.analytics.dataset_id
  table_id            = "subscriptions_stream"
  project             = var.project_id
  deletion_protection = false

  schema = jsonencode([
    { name = "subscription_id",         type = "STRING",    mode = "REQUIRED" },
    { name = "merchant_id",             type = "STRING",    mode = "REQUIRED" },
    { name = "plan",                    type = "STRING",    mode = "NULLABLE" },
    { name = "statut",                  type = "STRING",    mode = "NULLABLE" },
    { name = "montant_mensuel_cad",     type = "NUMERIC",   mode = "NULLABLE" },
    { name = "devise",                  type = "STRING",    mode = "NULLABLE" },
    { name = "date_debut",              type = "DATE",      mode = "NULLABLE" },
    { name = "date_prochain_paiement",  type = "DATE",      mode = "NULLABLE" },
    { name = "periode_essai_fin",       type = "DATE",      mode = "NULLABLE" },
    { name = "annulation_schedulee",    type = "BOOLEAN",   mode = "NULLABLE" },
    { name = "updated_at",             type = "TIMESTAMP",  mode = "NULLABLE" },
    { name = "cdc_operation",          type = "STRING",    mode = "NULLABLE" },
    { name = "cdc_timestamp",          type = "TIMESTAMP",  mode = "NULLABLE" },
  ])
}

resource "google_bigquery_table" "invoices_stream" {
  dataset_id          = google_bigquery_dataset.analytics.dataset_id
  table_id            = "invoices_stream"
  project             = var.project_id
  deletion_protection = false

  schema = jsonencode([
    { name = "invoice_id",            type = "STRING",    mode = "REQUIRED" },
    { name = "merchant_id",           type = "STRING",    mode = "REQUIRED" },
    { name = "subscription_id",       type = "STRING",    mode = "NULLABLE" },
    { name = "montant_cad",           type = "NUMERIC",   mode = "NULLABLE" },
    { name = "statut",                type = "STRING",    mode = "NULLABLE" },
    { name = "date_emission",         type = "DATE",      mode = "NULLABLE" },
    { name = "date_echeance",         type = "DATE",      mode = "NULLABLE" },
    { name = "date_paiement",         type = "TIMESTAMP", mode = "NULLABLE" },
    { name = "tentatives_paiement",   type = "INTEGER",   mode = "NULLABLE" },
    { name = "updated_at",            type = "TIMESTAMP", mode = "NULLABLE" },
    { name = "cdc_operation",         type = "STRING",    mode = "NULLABLE" },
    { name = "cdc_timestamp",         type = "TIMESTAMP", mode = "NULLABLE" },
  ])
}

resource "google_bigquery_table" "payment_attempts_stream" {
  dataset_id          = google_bigquery_dataset.analytics.dataset_id
  table_id            = "payment_attempts_stream"
  project             = var.project_id
  deletion_protection = false

  schema = jsonencode([
    { name = "attempt_id",         type = "STRING",    mode = "REQUIRED" },
    { name = "invoice_id",         type = "STRING",    mode = "REQUIRED" },
    { name = "merchant_id",        type = "STRING",    mode = "NULLABLE" },
    { name = "montant_cad",        type = "NUMERIC",   mode = "NULLABLE" },
    { name = "statut",             type = "STRING",    mode = "NULLABLE" },
    { name = "code_erreur",        type = "STRING",    mode = "NULLABLE" },
    { name = "gateway",            type = "STRING",    mode = "NULLABLE" },
    { name = "tentative_numero",   type = "INTEGER",   mode = "NULLABLE" },
    { name = "created_at",         type = "TIMESTAMP", mode = "NULLABLE" },
    { name = "cdc_operation",      type = "STRING",    mode = "NULLABLE" },
    { name = "cdc_timestamp",      type = "TIMESTAMP", mode = "NULLABLE" },
  ])
}

resource "google_bigquery_table" "plan_changes_stream" {
  dataset_id          = google_bigquery_dataset.analytics.dataset_id
  table_id            = "plan_changes_stream"
  project             = var.project_id
  deletion_protection = false

  schema = jsonencode([
    { name = "change_id",            type = "STRING",    mode = "REQUIRED" },
    { name = "merchant_id",          type = "STRING",    mode = "REQUIRED" },
    { name = "subscription_id",      type = "STRING",    mode = "NULLABLE" },
    { name = "ancien_plan",          type = "STRING",    mode = "NULLABLE" },
    { name = "nouveau_plan",         type = "STRING",    mode = "NULLABLE" },
    { name = "ancien_montant_cad",   type = "NUMERIC",   mode = "NULLABLE" },
    { name = "nouveau_montant_cad",  type = "NUMERIC",   mode = "NULLABLE" },
    { name = "date_effet",           type = "DATE",      mode = "NULLABLE" },
    { name = "motif",                type = "STRING",    mode = "NULLABLE" },
    { name = "updated_at",           type = "TIMESTAMP", mode = "NULLABLE" },
    { name = "cdc_operation",        type = "STRING",    mode = "NULLABLE" },
    { name = "cdc_timestamp",        type = "TIMESTAMP", mode = "NULLABLE" },
  ])
}

# ---------- Reconciliation log ----------
resource "google_bigquery_table" "reconciliation_log" {
  dataset_id          = google_bigquery_dataset.analytics.dataset_id
  table_id            = "reconciliation_log"
  project             = var.project_id
  deletion_protection = false

  schema = jsonencode([
    { name = "date",              type = "DATE",      mode = "REQUIRED" },
    { name = "mrr_streaming_cad", type = "NUMERIC",   mode = "NULLABLE" },
    { name = "mrr_batch_cad",     type = "NUMERIC",   mode = "NULLABLE" },
    { name = "ecart_pct",         type = "FLOAT64",   mode = "NULLABLE" },
    { name = "status",            type = "STRING",    mode = "NULLABLE" },
    { name = "checked_at",        type = "TIMESTAMP", mode = "NULLABLE" },
  ])
}

# ---------- Batch tables (Dataproc bootstrap) ----------
resource "google_bigquery_table" "subscriptions_batch" {
  dataset_id          = google_bigquery_dataset.batch.dataset_id
  table_id            = "subscriptions_batch"
  project             = var.project_id
  deletion_protection = false

  schema = jsonencode([
    { name = "subscription_id",         type = "STRING",    mode = "REQUIRED" },
    { name = "merchant_id",             type = "STRING",    mode = "REQUIRED" },
    { name = "plan",                    type = "STRING",    mode = "NULLABLE" },
    { name = "statut",                  type = "STRING",    mode = "NULLABLE" },
    { name = "montant_mensuel_cad",     type = "NUMERIC",   mode = "NULLABLE" },
    { name = "devise",                  type = "STRING",    mode = "NULLABLE" },
    { name = "date_debut",              type = "DATE",      mode = "NULLABLE" },
    { name = "date_prochain_paiement",  type = "DATE",      mode = "NULLABLE" },
    { name = "periode_essai_fin",       type = "DATE",      mode = "NULLABLE" },
    { name = "annulation_schedulee",    type = "BOOLEAN",   mode = "NULLABLE" },
    { name = "updated_at",             type = "TIMESTAMP",  mode = "NULLABLE" },
  ])
}

resource "google_bigquery_table" "invoices_batch" {
  dataset_id          = google_bigquery_dataset.batch.dataset_id
  table_id            = "invoices_batch"
  project             = var.project_id
  deletion_protection = false

  schema = jsonencode([
    { name = "invoice_id",            type = "STRING",    mode = "REQUIRED" },
    { name = "merchant_id",           type = "STRING",    mode = "REQUIRED" },
    { name = "subscription_id",       type = "STRING",    mode = "NULLABLE" },
    { name = "montant_cad",           type = "NUMERIC",   mode = "NULLABLE" },
    { name = "statut",                type = "STRING",    mode = "NULLABLE" },
    { name = "date_emission",         type = "DATE",      mode = "NULLABLE" },
    { name = "date_echeance",         type = "DATE",      mode = "NULLABLE" },
    { name = "date_paiement",         type = "TIMESTAMP", mode = "NULLABLE" },
    { name = "tentatives_paiement",   type = "INTEGER",   mode = "NULLABLE" },
    { name = "updated_at",            type = "TIMESTAMP", mode = "NULLABLE" },
  ])
}
