variable "deploy_target" {
  description = "The deployment target: 'cloud' or 'local'."
  type        = string
  default     = "cloud"
}

variable "project_id" {
  description = "The GCP project ID."
  type        = string
}

variable "region" {
  description = "The GCP region for resources."
  type        = string
}

variable "gce_instance_name" {
  description = "Name for the GCE instance hosting the backend and DB."
  type        = string
  default     = "backend-vm"
}

variable "machine_type" {
  description = "Machine type for the GCE instance."
  type        = string
  default     = "e2-medium"
}

variable "artifact_repo_id" {
  description = "ID for the Artifact Registry repository."
  type        = string
  default     = "app-images"
}

variable "frontend_service_name" {
  description = "Name for the frontend service."
  type        = string
  default     = "frontend-service"
}

variable "frontend_image_name" {
  description = "Name for the frontend image."
  type        = string
  default     = "my-app/frontend:latest"
}

variable "backend_image_name" {
  description = "Name for the backend image."
  type        = string
  default     = "my-app/backend:latest"
}

variable "mongo_image" {
  description = "Name of the mongoDB image to use."
  type        = string
  default     = "mongo:latest"
}

variable "backend_local_container_name" {
  description = "Name for the local backend Docker container."
  type        = string
  default     = "backend-app-local"
}

variable "frontend_local_container_name" {
  description = "Name for the local frontend Docker container."
  type        = string
  default     = "frontend-app-local"
}

variable "mongo_local_container_name" {
  description = "Name for the local MongoDB Docker container."
  type        = string
  default     = "mongo-db-local"
}

variable "frontend_local_external_port" {
  description = "External port for the local frontend container."
  type        = number
  default     = 8080
}

variable "backend_local_external_port" {
  description = "External port for the local backend container."
  type        = number
  default     = 8000
}

variable "mongo_local_external_port" {
  description = "External port for the local MongoDB container."
  type        = number
  default     = 27017
}