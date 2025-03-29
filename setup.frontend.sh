#!/bin/bash

#source variables.env

set -e

# Variables
IMAGE_TAG="$APP_ID"

echo "--## Copying build directory"
rm -rf build
mkdir build
cp -r $BUILD_PATH/*  build/


echo "--## Creating AWS ECR repository: $ECR_REPO_NAME"
aws ecr create-repository --repository-name $ECR_REPO_NAME --region $AWS_REGION || echo "Repository already exists"

AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
ECR_REPO_URI="${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/${ECR_REPO_NAME}"


echo "--## Logging into AWS ECR : $ECR_REPO_URI"
aws ecr get-login-password --region $AWS_REGION | docker login --username AWS --password-stdin $AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com


echo "--## Creating Dockerfile"
# Create a temporary Dockerfile
cat > Dockerfile << EOF

FROM public.ecr.aws/amazonlinux/amazonlinux:2023
RUN dnf update -y && \
    dnf install -y nginx procps shadow-utils && \
    dnf clean all

RUN rm -rf /usr/share/nginx/html/*
COPY ./build/ /usr/share/nginx/html/
EXPOSE 80
ENTRYPOINT ["nginx", "-g", "daemon off;"]
EOF

echo "--## Building Docker image"
docker build -t $ECR_REPO_NAME:$IMAGE_TAG .


echo "--## Tagging Docker image for ECR"
docker tag $ECR_REPO_NAME:$IMAGE_TAG $ECR_REPO_URI:$IMAGE_TAG


echo "--## Pushing Docker image to ECR"
docker push $ECR_REPO_URI:$IMAGE_TAG
DOCKER_IMAGE="$ECR_REPO_URI:$IMAGE_TAG"


echo "--## Docker image completed."
echo "----------------------------------------------------------------"
echo "New Docker image available at: $DOCKER_IMAGE"
echo "----------------------------------------------------------------"


echo "--## Creating AWS App Runner Service"

if [ $ACCESSIBILITY == "PUBLIC" ]
then
aws cloudformation create-stack \
  --stack-name "$STACK_ID-frontend" \
  --template-body file://template.frontend.yaml \
  --parameters ParameterKey=StackID,ParameterValue="$STACK_ID" ParameterKey=DockerImage,ParameterValue=$DOCKER_IMAGE ParameterKey=AllowedIPv4CIDR,ParameterValue=$IPV4_CIDR  ParameterKey=AllowedIPv6CIDR,ParameterValue=$IPV6_CIDR \
  --capabilities CAPABILITY_IAM \
  --region $AWS_REGION
fi



aws cloudformation wait stack-create-complete --stack-name "$STACK_ID-frontend" --region $AWS_REGION

export $(aws cloudformation describe-stacks --stack-name "$STACK_ID-frontend" --output text --query 'Stacks[0].Outputs[].join(`=`, [join(`_`, [`CF`, `OUT`, OutputKey]), OutputValue ])' --region us-east-1)

echo "----------------------------------------------------------------"
echo "AWS App Runner URL: $CF_OUT_AppRunnerServiceURL"
echo "----------------------------------------------------------------"
export AppRunnerServiceURL=$CF_OUT_AppRunnerServiceURL