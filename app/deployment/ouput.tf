# --- Frontend URL Output ---
output "frontend_url" {
  description = "The URL of the deployed frontend service."
  value = var.deploy_target == "cloud" ? (
    one(google_cloud_run_v2_service.frontend[*].uri)
    ) : (
    "http://localhost:${var.frontend_local_external_port}"
  )
}

# --- Backend URL Output ---
output "backend_url" {
  description = "The URL of the deployed backend service."
  value = var.deploy_target == "cloud" ? (
    "http://${one(google_compute_instance.backend_vm[*].network_interface[0].access_config[0].nat_ip)}:8000"
    ) : (
    "http://localhost:${var.backend_local_external_port}"
  )
}

# --- GCE Instance IP Output ---
output "backend_vm_ip" {
  description = "The public IP address of the GCE instance hosting the backend."
  value = var.deploy_target == "cloud" ? (
    one(google_compute_instance.backend_vm[*].network_interface[0].access_config[0].nat_ip)
    ) : (
    "Not applicable for local deployment."
  )
}

# --- FIX: MongoDB Connection URI ---
output "mongodb_connection_uri" {
  description = "The connection URI for the MongoDB database."
  value = var.deploy_target == "cloud" ? (
    # This is the internal connection string used by the backend container inside the VM.
    # The database is NOT exposed to the public internet for security.
    "mongodb://${var.mongo_local_container_name}:27017/mydatabase (internal to GCE VM)"
    ) : (
    "mongodb://localhost:${var.mongo_local_external_port}/mydatabase"
  )
  sensitive = true
}

# --- NEW: Static IP for the build script ---
output "backend_static_ip" {
  description = "The reserved static IP for the GCE backend. Required by the setup.sh script."
  value       = var.deploy_target == "cloud" ? one(google_compute_address.backend_static_ip[*].address) : "N/A"
}