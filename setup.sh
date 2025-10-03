#!/bin/bash
#
# setup.sh: A script to set up and manage the development environment using Terraform.
#
# Usage:
#   ./setup.sh --dev                   - Installs gcloud, Docker, and Terraform.
#   ./setup.sh --deploy-cloud          - Deploys the application to Google Cloud Run.
#   ./setup.sh --deploy-cloud --skip-cloudbuild - Deploys to cloud without rebuilding images.
#   ./setup.sh --deploy-local          - Deploys the application locally using Terraform and Docker.
#   ./setup.sh --undo-cloud            - Destroys the cloud infrastructure.
#   ./setup.sh --undo-local            - Destroys the local Docker infrastructure.
#   ./setup.sh --build-local           - Rebuilds the local frontend and backend Docker images.
#   ./setup.sh --help                  - Displays this help menu.
#

# --- Configuration ---
TF_LOG=TRACE
set -e
TF_DIR="./app/deployment"
LOCAL_VARS_FILE="${TF_DIR}/terraform.tfvars"

# --- Helper Functions ---
print_help() {
  echo "Usage: $0 [OPTION]"
  echo "A script to set up and manage the development environment using Terraform."
  echo ""
  echo "Options:"
  echo "  --dev                          Install essential tools: gcloud, Docker, and Terraform."
  echo "  --deploy-cloud [--skip-cloudbuild]"
  echo "                                 Builds and deploys to GCP. Use the optional flag"
  echo "                                 to skip the image build step."
  echo "  --deploy-local                 Deploys the application locally using Terraform and Docker."
  echo "  --undo-cloud                   Destroys the Google Cloud Terraform infrastructure."
  echo "  --build-local                  Rebuilds the local frontend and backend Docker images."
  echo "  --undo-local                   Destroys the local Docker Terraform infrastructure."
  echo "  --help                         Display this help menu."
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
  echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
  sudo apt-get update
  sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
  sudo usermod -aG docker $USER
  echo "✅ Docker installed successfully."
}

install_terraform() {
  echo "--- 🛠️  Installing Terraform ---"
  sudo apt-get update && sudo apt-get install -y software-properties-common gnupg
  wget -O- https://apt.releases.hashicorp.com/gpg | gpg --dearmor | sudo tee /usr/share/keyrings/hashicorp-archive-keyring.gpg > /dev/null
  echo "deb [signed-by=/usr/share/keyrings/hashicorp-archive-keyring.gpg] https://apt.releases.hashicorp.com $(lsb_release -cs) main" | sudo tee /etc/apt/sources.list.d/hashicorp.list
  sudo apt-get update && sudo apt-get install -y terraform
  echo "✅ Terraform installed successfully."
}


# --- Main Logic ---

if [ "$1" == "--dev" ]; then
  echo "🚀 Starting development environment setup..."
  install_gcloud
  install_docker
  install_terraform
  echo ""
  echo "🎉 All tools have been installed successfully!"
  echo "💡 NOTE: Please log out and log back in for the Docker group changes to apply."

elif [ "$1" == "--deploy-cloud" ]; then
  echo "🚀 Deploying to Google Cloud..."
  # Check if logged in to gcloud
  if ! gcloud auth list --filter=status:ACTIVE --format="value(account)" | grep -q .; then
      gcloud auth application-default login
  fi
  
  # Get project ID if not set
  PROJECT_ID=$(gcloud config get-value project)
  if [ -z "$PROJECT_ID" ]; then
      echo "Please enter your GCP project ID:"
      read PROJECT_ID
      gcloud config set project $PROJECT_ID
  fi
  
  # --- START: MODIFIED DEPLOYMENT LOGIC ---
  
  echo "--- Terraform Init ---"
  terraform -chdir=$TF_DIR init

  echo "--- 💡 Step 1: Applying Terraform to get static IP for backend ---"
  # Note: This requires a 'google_compute_address' resource in your Terraform config
  terraform -chdir=$TF_DIR apply -auto-approve -target=google_compute_address.backend_static_ip
  BACKEND_IP=$(terraform -chdir=$TF_DIR output -raw backend_static_ip)

  if [ -z "$BACKEND_IP" ]; then
    echo "❌ Could not retrieve static IP. Make sure 'google_compute_address.backend_static_ip' and its output exist in your Terraform config. Halting."
    exit 1
  fi
  echo "✅ Using static IP for backend: $BACKEND_IP"

  if [ "$2" == "--skip-cloudbuild" ]; then
      echo "--- ⏭️  Skipping Cloud Build step as requested. Using existing images. ---"
  else
      echo "--- 📦 Step 2: Building container images with the correct backend IP ---"
      echo "Building frontend image..."
      # This requires a cloudbuild.yaml in your frontend directory to use the substitution
      gcloud builds submit --tag gcr.io/$PROJECT_ID/frontend ./app/frontend \
        --substitutions=_VITE_API_BASE="http://$BACKEND_IP:8000"
      
      echo "Building backend image..."
      gcloud builds submit --tag gcr.io/$PROJECT_ID/backend ./app/backend
  fi

  echo "--- 🚀 Step 3: Applying full Terraform configuration for Cloud ---"
  terraform -chdir=$TF_DIR apply -auto-approve \
    -var="project_id=$PROJECT_ID" \
    -var="deploy_target=cloud" \
    -var="frontend_image_name=gcr.io/$PROJECT_ID/frontend" \
    -var="backend_image_name=gcr.io/$PROJECT_ID/backend"

  # --- END: MODIFIED DEPLOYMENT LOGIC ---

  echo "--- ✅ Deployment Complete ---"
  FRONTEND_URL=$(terraform -chdir=$TF_DIR output -raw frontend_url)
  BACKEND_URL=$(terraform -chdir=$TF_DIR output -raw backend_url)
  echo "➡️  Frontend URL: $FRONTEND_URL"
  echo "➡️  Backend URL: $BACKEND_URL"

elif [ "$1" == "--deploy-local" ]; then
  echo "🚀 Deploying locally with Terraform and Docker..."
  
  if [ ! -f "$LOCAL_VARS_FILE" ]; then
    echo "--- 🔑 Configuring local environment (first time setup) ---"
    echo "Please provide credentials for the local MongoDB instance."
    read -p "Enter MongoDB Root User: " MONGO_USER
    read -sp "Enter MongoDB Root Password: " MONGO_PASSWORD
    echo ""

    cat > "$LOCAL_VARS_FILE" << EOL
# This file is for local development variables and should be in .gitignore
deploy_target       = "local"
mongo_root_user     = "$MONGO_USER"
mongo_root_password = "$MONGO_PASSWORD"
EOL
    echo "✅ Local configuration saved to $LOCAL_VARS_FILE"
    echo "ℹ️  Add this file to your .gitignore to keep secrets safe!"
  fi

  echo "--- 🚀 Applying Terraform configuration for Local Docker ---"
  terraform -chdir=$TF_DIR init
  terraform -chdir=$TF_DIR apply -auto-approve -var-file=terraform.tfvars

  echo "--- ✅ Local Deployment Complete ---"
  FRONTEND_URL=$(terraform -chdir=$TF_DIR output -raw frontend_url)
  BACKEND_URL=$(terraform -chdir=$TF_DIR output -raw backend_url)
  MONGO_CONN=$(terraform -chdir=$TF_DIR output -raw mongodb_connection_uri)
  echo "➡️  Frontend available at: $FRONTEND_URL"
  echo "➡️  Backend available at: $BACKEND_URL"
  echo "➡️  MongoDB connection: $MONGO_CONN"

elif [ "$1" == "--undo-local" ]; then
  echo "🔥 Destroying local Docker environment..."
  if [ ! -f "$LOCAL_VARS_FILE" ]; then
    echo "⚠️  Warning: Local variables file ($LOCAL_VARS_FILE) not found. Destruction might fail if state is missing."
  fi
  terraform -chdir=$TF_DIR destroy -auto-approve -var-file=terraform.tfvars
  echo "✅ Local environment destroyed."

elif [ "$1" == "--undo-cloud" ]; then
  echo "🔥 Destroying Google Cloud infrastructure..."
  PROJECT_ID=$(gcloud config get-value project)
  if [ -z "$PROJECT_ID" ]; then
    echo "❌ GCP Project ID not set. Please run 'gcloud config set project YOUR_PROJECT_ID' and try again."
    exit 1
  fi
  terraform -chdir=$TF_DIR destroy -auto-approve \
    -var="project_id=$PROJECT_ID" \
    -var="deploy_target=cloud" \
    -var="frontend_image_name=gcr.io/$PROJECT_ID/frontend" \
    -var="backend_image_name=gcr.io/$PROJECT_ID/backend"
  echo "✅ Cloud infrastructure destroyed."

elif [ "$1" == "--build-local" ]; then
  echo "🛠️  Building/Rebuilding local Docker images using Terraform..."
  if [ ! -f "$LOCAL_VARS_FILE" ]; then
    echo "❌ Local configuration file not found at '$LOCAL_VARS_FILE'."
    echo "💡 Please run './setup.sh --deploy-local' first to initialize the environment."
    exit 1
  fi
  
  terraform -chdir=$TF_DIR init
  
  terraform -chdir=$TF_DIR apply -auto-approve -var-file=terraform.tfvars \
    -target=docker_image.frontend \
    -target=docker_image.backend

  echo "✅ Local images for frontend and backend have been rebuilt successfully."

else
  print_help
fi