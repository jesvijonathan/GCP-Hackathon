terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
    docker = {
      source  = "kreuzwerker/docker"
      version = "~> 3.0"
    }
  }
}

#----------------------------------------------------#
#                    Providers                       #
#----------------------------------------------------#

provider "google" {
  project = var.project_id
  region  = var.region
}

provider "docker" {
  # Configures the Docker provider for local management.
}


#----------------------------------------------------#
#               Google Cloud Run Resources           #
#----------------------------------------------------#

# These resources will only be created if var.deploy_target is "cloud"

resource "google_project_service" "run_api" {
  count                = var.deploy_target == "cloud" ? 1 : 0
  project              = var.project_id
  service              = "run.googleapis.com"
  disable_on_destroy   = false
}

resource "google_cloud_run_v2_service" "default" {
  count    = var.deploy_target == "cloud" ? 1 : 0
  name     = var.service_name
  location = var.region
  project  = var.project_id

  template {
    containers {
      image = var.image_name
    }
  }

  depends_on = [google_project_service.run_api]
}

resource "google_cloud_run_v2_service_iam_member" "noauth" {
  count    = var.deploy_target == "cloud" ? 1 : 0
  project  = google_cloud_run_v2_service.default[0].project
  location = google_cloud_run_v2_service.default[0].location
  name     = google_cloud_run_v2_service.default[0].name
  role     = "roles/run.invoker"
  member   = "allUsers"
}


#----------------------------------------------------#
#                 Local Docker Resources             #
#----------------------------------------------------#

# These resources will only be created if var.deploy_target is "local"

resource "docker_image" "frontend" {
  count = var.deploy_target == "local" ? 1 : 0
  name  = var.image_name
  build {
    # Path to the Dockerfile context, relative to the terraform directory (app/deployment)
    context = "../frontend"
  }
}

resource "docker_container" "frontend_container" {
  count = var.deploy_target == "local" ? 1 : 0
  image = docker_image.frontend[0].image_id
  name  = var.local_container_name
  ports {
    internal = 8080 # The port inside the container, based on your Dockerfile/nginx.conf
    external = var.local_external_port
  }
}

#----------------------------------------------------#
#                       Outputs                      #
#----------------------------------------------------#

output "frontend_url" {
  description = "The URL of the deployed frontend service."
  value       = var.deploy_target == "cloud" ? (
    one(google_cloud_run_v2_service.default[*].uri) 
    ) : (
    "http://localhost:${var.local_external_port}"
  )
}
