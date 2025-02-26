#!/bin/bash
set -e  # Exit on any error

echo "Starting deployment script"

# The bucket name is passed as an environment variable
if [ -z "\$BUCKET_NAME" ]; then
  echo "Error: BUCKET_NAME environment variable is not set"
  exit 1
fi

echo "Deploying to bucket: \$BUCKET_NAME"

# Navigate to the project directory (adjust if your React app is in a subdirectory)
# cd ./react-app  # Uncomment if needed
cd /tmp/mytagger/frontend

# Check if npm is available, if not install it
if ! command -v npm &> /dev/null; then
  echo "npm not found, installing Node.js"
  curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.39.3/install.sh | bash
  export NVM_DIR="\$HOME/.nvm"
  [ -s "\$NVM_DIR/nvm.sh" ] && \. "\$NVM_DIR/nvm.sh"  # Load nvm
  nvm install 16  # Install Node.js 16
fi

# Install dependencies
echo "Installing dependencies"
npm install

# Build the React app
echo "Building React application"
npm run build

# Install AWS CLI if not available
if ! command -v aws &> /dev/null; then
  echo "AWS CLI not found, installing"
  pip3 install awscli --upgrade
fi

# Deploy to S3
echo "Deploying to S3"
aws s3 sync build/ s3://\$BUCKET_NAME/ 

echo "Deployment completed successfully"