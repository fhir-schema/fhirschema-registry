#!/bin/bash

# Configuration
REPO_DIR="./"
SERVICE_COMMAND="sudo clj -m fhirschema.registry.core"
SERVICE_PID_FILE="./service.pid"
INTERVAL=10

# Function to start the service
start_service() {
    echo "$(date): Starting service..."
    cd "$REPO_DIR" || exit 1
    nohup $SERVICE_COMMAND > service.log 2>&1 &
    SERVICE_PID=$!
    echo $SERVICE_PID > "$SERVICE_PID_FILE"
    echo "$(date): Service started with PID $SERVICE_PID."
}

# Function to stop the service
stop_service() {
    if [ -f "$SERVICE_PID_FILE" ]; then
        SERVICE_PID=$(cat "$SERVICE_PID_FILE")
        if kill -0 "$SERVICE_PID" 2>/dev/null; then
            echo "$(date): Stopping service with PID $SERVICE_PID..."
            kill "$SERVICE_PID"
            rm "$SERVICE_PID_FILE"
            echo "$(date): Service stopped."
        else
            echo "$(date): No running service found with PID $SERVICE_PID."
            rm "$SERVICE_PID_FILE"
        fi
    else
        echo "$(date): PID file not found. Service may not be running."
    fi
}

# Ensure the repository directory exists
if [ ! -d "$REPO_DIR" ]; then
    echo "Repository directory not found!"
    exit 1
fi

# Start the service initially
start_service

# Infinite loop to poll for changes
while true; do
    cd "$REPO_DIR" || exit 1

    # Fetch updates from the remote repository
    git fetch origin

    # Compare local and remote commits
    LOCAL_COMMIT=$(git rev-parse HEAD)
    REMOTE_COMMIT=$(git rev-parse @{u})

    if [ "$LOCAL_COMMIT" != "$REMOTE_COMMIT" ]; then
        echo "$(date): Changes detected. Pulling updates..."
        git pull

        echo "$(date): Restarting service..."
        stop_service
        start_service
    else
        echo "$(date): No changes detected."
    fi

    # Wait for the specified interval before polling again
    sleep "$INTERVAL"
done
