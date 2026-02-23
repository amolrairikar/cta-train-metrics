#!/bin/bash
set -e

# Run this script from the root of the lambdas directory
DIRECTORY=$1
cd "$DIRECTORY"

# Clean up old deployment package
if [ -f "deployment_package.zip" ]; then
    rm deployment_package.zip
fi

# Copy the Dockerfile from the root of the lambdas directory
cp ../Dockerfile .

# Remove any stale containers/images
docker rm -f temp-step-fn-lambda-packager 2>/dev/null || true
docker rmi -f step-fn-lambda-packager:latest 2>/dev/null || true

# Build the Docker image for x86_64 Lambda
docker buildx build --platform linux/amd64 -t step-fn-lambda-packager .

# Create a container and copy the deployment package without running the Lambda entrypoint
docker create --name temp-step-fn-lambda-packager step-fn-lambda-packager
docker cp temp-step-fn-lambda-packager:/app/deployment_package.zip ./deployment_package.zip
docker rm temp-step-fn-lambda-packager

# Remove the copied Dockerfile
rm Dockerfile
