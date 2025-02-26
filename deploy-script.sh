#!/bin/bash
set -e  # Exit on any error

echo "Starting deployment script"

# The bucket name is passed as an environment variable
if [ -z "\$BUCKET_NAME" ]; then
  echo "Error: BUCKET_NAME environment variable is not set"
  exit 1
fi

echo "Deploying to bucket: \$BUCKET_NAME"

# Navigate to the project directory (we're already in the repo root)
/tmp/repo/mytagger/frontend

# Install dependencies
echo "Installing dependencies"
npm install

# Build the React app
echo "Building React application"
npm run build

# Deploy to S3
echo "Deploying to S3"
aws s3 sync build/ s3://\$BUCKET_NAME/ 

echo "Deployment completed successfully"