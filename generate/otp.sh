#!/bin/bash

# Define repository URL and image name
REPO_URL="https://github.com/rishavsen1/otp-carta.git"
IMAGE_NAME="otp"
CONTAINER_NAME="otp-carta-container"

# Clone the repository
git clone $REPO_URL repo-dir
cd repo-dir

# Build the Docker image
docker build -t $IMAGE_NAME .

# Check if the container already exists and remove it if it does
if [ $(docker ps -aq -f name=^/$CONTAINER_NAME$) ]; then
    echo "Removing existing container $CONTAINER_NAME"
    docker rm -f $CONTAINER_NAME
fi

# Run the Docker container
docker run -it --name $CONTAINER_NAME -p 8080:8080 $IMAGE_NAME java -Xmx3G -jar otp-1.5.0-shaded.jar --build /app/otp --inMemory
