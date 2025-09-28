output "cloud_run_service_url" {
  description = "The URL of the deployed Cloud Run service."
  value       = google_cloud_run_v2_service.default.uri
}

output "cloud_run_service_name" {
  description = "The name of the Cloud Run service."
  value       = google_cloud_run_v2_service.default.name
}