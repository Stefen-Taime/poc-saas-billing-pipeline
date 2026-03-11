output "bucket_name" {
  value = google_storage_bucket.bootstrap.name
}

output "bucket_url" {
  value = google_storage_bucket.bootstrap.url
}
