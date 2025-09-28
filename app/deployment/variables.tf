# --- Environment Toggle ---
variable "deploy_target" {
  description = "The deployment target: 'cloud' for Google Cloud Run or 'local' for Docker."
  type        = string
  default     = "cloud"
  validation {
    condition     = contains(["cloud", "local"], var.deploy_target)
    error_message = "The deploy_target must be either 'cloud' or 'local'."
  }
}

# --- Shared Variables ---
variable "image_name" {
  description = "The name of the Docker image to build or deploy."
  type        = string
}

# --- Google Cloud Run Variables ---
variable "project_id" {
  description = "The GCP project ID. Required if deploy_target is 'cloud'."
  type        = string
  default     = null
}

variable "region" {
  description = "The GCP region for the Cloud Run service."
  type        = string
  default     = "us-central1"
}

variable "service_name" {
  description = "The name of the Cloud Run service."
  type        = string
  default     = "frontend"
}

# --- Local Docker Variables ---
variable "local_container_name" {
  description = "The name for the local Docker container."
  type        = string
  default     = "local-frontend-tf"
}

variable "local_external_port" {
  description = "The external port to map to the container's internal port."
  type        = number
  default     = 8080
}
