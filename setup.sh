#!/bin/bash

#
# setup.sh: A script to set up and manage a development environment.
#
# Usage:
#   ./setup.sh --dev            - Installs gcloud, Docker, and Terraform.
#   ./setup.sh --deploy-cloud   - Deploys the application to the cloud.
#   ./setup.sh --deploy-docker  - Deploys the application locally using Docker Compose.
#   ./setup.sh --undo           - Destroys the Terraform-managed infrastructure.
#   ./setup.sh --undo-docker    - Stops the local Docker Compose deployment.
#   ./setup.sh --help           - Displays the help menu.
#

# --- Configuration ---
set -e

print_help() {
  echo "Usage: $0 [OPTION]"
  echo "A script to set up and manage a development environment."
  echo ""
  echo "Options:"
  echo "  --dev             Install essential development tools: gcloud, Docker, and Terraform."
  echo "  --deploy-cloud    Deploys the application to the cloud using gcloud and Terraform."
  echo "  --deploy-docker   Deploys the application locally using Docker Compose."
  echo "  --undo            Destroys the Terraform-managed infrastructure."
  echo "  --undo-docker     Stops the local Docker Compose deployment."
  echo "  --help            Display this help menu."
  echo ""
  echo "Example:"
  echo "  ./setup.sh --dev"
  echo "  ./setup.sh --deploy-docker"
  echo "  ./setup.sh --undo-docker"
}


install_gcloud() {
  echo "--- ⚙️  Installing Google Cloud SDK (gcloud) ---"
  sudo apt-get update
  sudo apt-get install -y apt-transport-https ca-certificates gnupg curl
  echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | sudo tee /etc/apt/sources.list.d/google-cloud-sdk.list
  curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | sudo apt-key --keyring /usr/share/keyrings/cloud.google.gpg add -
  sudo apt-get update && sudo apt-get install -y google-cloud-sdk
  echo "✅ gcloud installed successfully."
}

install_docker() {
  echo "--- 🐳 Installing Docker ---"
  sudo apt-get update
  sudo apt-get install -y ca-certificates curl
  sudo install -m 0755 -d /etc/apt/keyrings
  sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
  sudo chmod a+r /etc/apt/keyrings/docker.asc
  echo \
    "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu \
    $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | \
    sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
  sudo apt-get update
  sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
  sudo usermod -aG docker $USER
  echo "✅ Docker installed successfully."
}

install_terraform() {
  echo "--- 🛠️  Installing Terraform ---"
  sudo apt-get update
  sudo apt-get install -y software-properties-common gnupg
  wget -O- https://apt.releases.hashicorp.com/gpg | \
    gpg --dearmor | \
    sudo tee /usr/share/keyrings/hashicorp-archive-keyring.gpg > /dev/null
  echo "deb [signed-by=/usr/share/keyrings/hashicorp-archive-keyring.gpg] \
    https://apt.releases.hashicorp.com $(lsb_release -cs) main" | \
    sudo tee /etc/apt/sources.list.d/hashicorp.list
  sudo apt-get update
  sudo apt-get install -y terraform
  echo "✅ Terraform installed successfully."
}

if [ "$1" == "--dev" ]; then
  echo "🚀 Starting development environment setup..."
  install_gcloud
  install_docker
  install_terraform
  echo ""
  echo "🎉 All tools have been installed successfully!"
  echo "💡 NOTE: Please log out and log back in for the Docker group changes to apply."

elif [ "$1" == "--deploy-cloud" ]; then
# Check whether logged in
if ! gcloud auth list --filter=status:ACTIVE --format="value(account)" | grep -q .; then
    gcloud auth application-default login
fi
if [ -z "$(gcloud config get-value project)" ]; then
    echo "Please enter your GCP project ID:"
    read PROJECT_ID
    gcloud config set project $PROJECT_ID
fi
echo "building... frontend"
echo "project: $(gcloud config get-value project)"
gcloud builds submit --tag gcr.io/$(gcloud config get-value project)/frontend ./app/frontend
# if previous command failed, exit
if [ $? -ne 0 ]; then
    echo "Failed to build and submit the frontend image."
    exit 1
fi
terraform -chdir=./app/deployment init
terraform -chdir=./app/deployment apply -auto-approve
URL=$(terraform -chdir=./app/deployment output -raw cloud_run_service_url)
# if previous command failed, exit
if [ $? -ne 0 ]; then
    echo "Failed to deploy the frontend service."
    exit 1
fi
echo "Deployed to cloud: $URL"

elif [ "$1" == "--deploy-docker" ]; then
  echo "🚀 Starting local production environment with Docker Compose..."
  docker compose -f ./app/docker-compose.yml up --build -d
  echo "✅ Application should be running at http://localhost:8080"

elif [ "$1" == "--undo-docker" ]; then
  echo "🔥 Stopping local Docker Compose environment..."
  docker compose -f ./app/docker-compose.yml down
  echo "✅ Environment stopped."

elif [ "$1" == "--undo" ]; then
  echo "🔥 Destroying Terraform infrastructure..."
  terraform -chdir=./app/deployment destroy -auto-approve
  echo "✅ Infrastructure destroyed."

else
  print_help
fi

