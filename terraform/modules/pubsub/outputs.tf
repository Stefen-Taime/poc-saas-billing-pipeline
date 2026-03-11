output "topic_names" {
  value = [for t in google_pubsub_topic.cdc : t.name]
}

output "topic_ids" {
  value = { for k, t in google_pubsub_topic.cdc : k => t.id }
}

output "subscription_names" {
  value = [for s in google_pubsub_subscription.dataflow : s.name]
}
