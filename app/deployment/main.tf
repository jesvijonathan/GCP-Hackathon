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
    null = {
      source  = "hashicorp/null"
      version = "~> 3.2"
    }
  }
}

#----------------------------------------------------#
#                       Providers                      #
#----------------------------------------------------#

provider "google" {
  project = var.project_id
  region  = var.region
}

provider "docker" {}

#----------------------------------------------------#
#               Cloud Deployment ('cloud')             #
#----------------------------------------------------#

# --- Enable Necessary GCP APIs ---
resource "google_project_service" "gcp_apis" {
  project = var.project_id

  for_each = var.deploy_target == "cloud" ? toset([
    "run.googleapis.com",
    "compute.googleapis.com",
    "artifactregistry.googleapis.com",
  ]) : []

  service                  = each.key
  disable_on_destroy       = false
  disable_dependent_services = true
}

# --- Artifact Registry to store Docker Images ---
resource "google_artifact_registry_repository" "docker_repo" {
  count         = var.deploy_target == "cloud" ? 1 : 0
  project       = var.project_id
  location      = var.region
  repository_id = var.artifact_repo_id
  description   = "Docker repository for application images"
  format        = "DOCKER"
  depends_on    = [google_project_service.gcp_apis]
}

locals {
  # Define the full image URL for Artifact Registry
  backend_image_url = var.deploy_target == "cloud" ? "${var.region}-docker.pkg.dev/${var.project_id}/${one(google_artifact_registry_repository.docker_repo[*].repository_id)}/${var.backend_image_name}" : var.backend_image_name
}

# --- Networking for GCE Instance ---
resource "google_compute_network" "vpc_network" {
  count                   = var.deploy_target == "cloud" ? 1 : 0
  project                 = var.project_id
  name                    = "app-vpc-network"
  auto_create_subnetworks = true
}

# --- FIX: Reserve a static IP address for the backend VM ---
resource "google_compute_address" "backend_static_ip" {
  count   = var.deploy_target == "cloud" ? 1 : 0
  project = var.project_id
  name    = "${var.gce_instance_name}-static-ip"
  region  = var.region
}

# --- FIX: Updated Firewall to be more secure ---
resource "google_compute_firewall" "allow_http_ssh" {
  count   = var.deploy_target == "cloud" ? 1 : 0
  project = var.project_id
  name    = "allow-http-ssh"
  network = one(google_compute_network.vpc_network[*].name)

  allow {
    protocol = "tcp"
    # Allow SSH (22) and the backend app (8000).
    # DO NOT expose the database port (27017) to the public internet.
    ports    = ["22", "8000"]
  }

  source_ranges = ["0.0.0.0/0"]
  target_tags   = ["backend-vm"]
}

# --- GCE Instance for Backend + MongoDB ---
resource "google_compute_instance" "backend_vm" {
  count        = var.deploy_target == "cloud" ? 1 : 0
  project      = var.project_id
  zone         = "${var.region}-a"
  name         = var.gce_instance_name
  machine_type = var.machine_type
  tags         = ["backend-vm"]

  boot_disk {
    initialize_params {
      image = "debian-cloud/debian-11"
    }
  }

  network_interface {
    network = one(google_compute_network.vpc_network[*].id)
    access_config {
      # FIX: Assign the reserved static IP
      nat_ip = one(google_compute_address.backend_static_ip[*].address)
    }
  }

  service_account {
    email  = one(data.google_compute_default_service_account.default[*].email)
    scopes = ["cloud-platform"]
  }

  metadata_startup_script = <<-EOT
    #!/bin/bash
    set -e

    # 1. Install Docker
    apt-get update
    apt-get install -y apt-transport-https ca-certificates curl gnupg lsb-release
    curl -fsSL https://download.docker.com/linux/debian/gpg | gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg
    echo "deb [arch=amd64 signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/debian $(lsb_release -cs) stable" | tee /etc/apt/sources.list.d/docker.list > /dev/null
    apt-get update
    apt-get install -y docker-ce docker-ce-cli containerd.io

    # 2. Install Google Cloud SDK
    apt-get install -y google-cloud-sdk

    # 3. Create a docker network
    docker network create app-network || true

    # 4. Run MongoDB container
    docker run -d --name ${var.mongo_local_container_name} --network app-network -p 27017:27017 ${var.mongo_image}

    # 5. Authenticate Docker with Artifact Registry
    gcloud auth configure-docker ${var.region}-docker.pkg.dev -q

    # 6. Pull and Run the backend application container
    docker pull ${local.backend_image_url}
    docker run -d --name ${var.backend_local_container_name} --network app-network -p 8000:8000 \
    -e MONGO_URI="mongodb://${var.mongo_local_container_name}:27017/mydatabase" \
    ${local.backend_image_url}
  EOT

  depends_on = [
    google_project_service.gcp_apis,
  ]
}

data "google_compute_default_service_account" "default" {
  count   = var.deploy_target == "cloud" ? 1 : 0
  project = var.project_id
  depends_on = [
    google_project_service.gcp_apis["compute.googleapis.com"]
  ]
}


# --- Cloud Run Frontend Service (Unchanged) ---
resource "google_cloud_run_v2_service" "frontend" {
  count    = var.deploy_target == "cloud" ? 1 : 0
  name     = var.frontend_service_name
  location = var.region
  project  = var.project_id

  template {
    containers {
      image = var.frontend_image_name
      # This value will be populated by the setup.sh script during the build process
      # The runtime env var is still useful for other potential purposes
      env {
        name  = "VITE_API_BASE"
        value = "http://${one(google_compute_instance.backend_vm[*].network_interface[0].access_config[0].nat_ip)}:8000"
      }
    }
  }
  depends_on = [google_compute_instance.backend_vm]
}

resource "google_cloud_run_v2_service_iam_member" "frontend_noauth" {
  count    = var.deploy_target == "cloud" ? 1 : 0
  project  = one(google_cloud_run_v2_service.frontend[*].project)
  location = one(google_cloud_run_v2_service.frontend[*].location)
  name     = one(google_cloud_run_v2_service.frontend[*].name)
  role     = "roles/run.invoker"
  member   = "allUsers"
}


#----------------------------------------------------#
#        Local Docker Resources ('local' deploy)       #
#----------------------------------------------------#

# --- Docker Network ---
resource "docker_network" "private_network" {
  count = var.deploy_target == "local" ? 1 : 0
  name  = "app-network"
}

# --- Docker Images ---
resource "docker_image" "frontend" {
  count = var.deploy_target == "local" ? 1 : 0
  name  = var.frontend_image_name
  build { context = "../frontend" }
}

resource "docker_image" "backend" {
  count = var.deploy_target == "local" ? 1 : 0
  name  = var.backend_image_name
  build { context = "../backend" }
}

resource "docker_image" "mongo" {
  count         = var.deploy_target == "local" ? 1 : 0
  name          = var.mongo_image
  pull_triggers = [var.mongo_image]
}

# --- Docker Containers ---
resource "docker_container" "mongo_container" {
  count = var.deploy_target == "local" ? 1 : 0
  image = one(docker_image.mongo[*].image_id)
  name  = var.mongo_local_container_name
  networks_advanced {
    name = one(docker_network.private_network[*].name)
  }
  ports {
    internal = 27017
    external = var.mongo_local_external_port
  }
}

resource "docker_container" "backend_container" {
  count = var.deploy_target == "local" ? 1 : 0
  image = one(docker_image.backend[*].image_id)
  name  = var.backend_local_container_name
  networks_advanced {
    name = one(docker_network.private_network[*].name)
  }
  ports {
    internal = 8000
    external = var.backend_local_external_port
  }
  env = [
    "MONGO_URI=mongodb://${var.mongo_local_container_name}:27017/mydatabase"
  ]
  depends_on = [docker_container.mongo_container]
}

resource "docker_container" "frontend_container" {
  count = var.deploy_target == "local" ? 1 : 0
  image = one(docker_image.frontend[*].image_id)
  name  = var.frontend_local_container_name
  networks_advanced {
    name = one(docker_network.private_network[*].name)
  }
  ports {
    internal = 8080
    external = var.frontend_local_external_port
  }
  env = [
    "VITE_API_BASE=http://localhost:${var.backend_local_external_port}"
  ]
  depends_on = [docker_container.backend_container]
}