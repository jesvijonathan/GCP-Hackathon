# Use an official Python runtime as a parent image (slim version for smaller size)
FROM python:3.11-slim

# Set the working directory inside the container
WORKDIR /app

# --- DATA DOWNLOAD & PREPARATION ---
# These steps are placed first to leverage Docker's layer caching.
# The large data file will only be downloaded again if the URL changes,
# not every time your application code is modified.

# Install curl, needed for downloading the data
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

# Create the directory for the manual data
RUN mkdir -p /app/manual_data

# Use a build-time argument to specify the tar file URL.
ARG TAR_URL="https://storage.googleapis.com/nullbyte_merchant_data_bkt/output/output.tar.gz"

# Download and extract the specified tar file.
# The curl command is updated to be more resilient to network issues.
# -C - : Resumes the download if it gets interrupted.
# --retry 5 : Retries the command up to 5 times if it fails.
# --retry-delay 10 : Waits 10 seconds between retries.

# RUN echo "Downloading and extracting data from ${TAR_URL}" && \
#     curl -L -C - --retry 5 --retry-delay 10 "${TAR_URL}" -o /app/manual_data/data.tar.gz && \
#     tar -xzf /app/manual_data/data.tar.gz -C /app/manual_data/ && \
#     rm /app/manual_data/data.tar.gz

# --- DEPENDENCIES & APPLICATION CODE ---

# Copy the requirements file to leverage cache for dependencies
COPY requirements.txt .

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code into the container.
# Changes to your code from this point on will not trigger the data download again.
COPY . .

# --- DATA LOADING ---

# Find all manifest files from the extracted data and execute the loading script.
# Install cron
RUN apt-get update && apt-get install -y cron && rm -rf /var/lib/apt/lists/*

# Add a @reboot cron job to execute on container start/restart
RUN echo "@reboot for f in /app/manual_data/output/main_manifest*; do echo \"Processing manifest file: \$f\"; python3 /app/manual_data/load_to_mongo.py --manifest \"\$f\" --mongo \"mongodb://mongodb-container:27017\"; done" >> /etc/cron.d/load_manifests

# Give execution rights on the cron job
RUN chmod 0644 /etc/cron.d/load_manifests

# Apply cron job
RUN crontab /etc/cron.d/load_manifests

# Start cron in the background when container starts
CMD service cron start && python main.py

# --- RUN CONFIGURATION ---

# Make port 8000 available to the world outside this container
EXPOSE 8000

# Command to run the application
CMD ["python", "main.py"]

