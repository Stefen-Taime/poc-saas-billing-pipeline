###############################################################################
# Module: pubsub — CDC topics + subscriptions
###############################################################################

resource "google_pubsub_topic" "cdc" {
  for_each = toset(var.topics)

  name    = each.value
  project = var.project_id

  message_retention_duration = "86400s" # 24h

  labels = {
    component = "cdc"
    pipeline  = "poc-stripe-lightspeed"
  }
}

# Subscription pour Dataflow streaming
resource "google_pubsub_subscription" "dataflow" {
  for_each = toset(var.topics)

  name    = "${each.value}-dataflow-sub"
  topic   = google_pubsub_topic.cdc[each.key].id
  project = var.project_id

  ack_deadline_seconds       = 60
  message_retention_duration = "86400s"

  retry_policy {
    minimum_backoff = "10s"
    maximum_backoff = "600s"
  }

  labels = {
    consumer = "dataflow"
  }
}
