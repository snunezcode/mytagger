#!/bin/bash 

#######
####### STEP 1 - Build - Testing
#######

export IDENTIFIER=$(date '+%H%M%S')
export APP_USER="snmatus@amazon.com"
export IPV4_CIDR="192.168.1.0/24"
export IPV6_CIDR="2605:59c8:731d:4810:415:bd81:f251:f260/128"
export GITHUB_REPO="https://github.com/snunezcode/mytagger.git"
export VPC_ID="vpc-07d80a425057895a3"
export SUBNET_ID="subnet-03bff4b2b43b0d393"


aws cloudformation create-stack \
  --stack-name "build-private-$IDENTIFIER" \
  --template-body file://cloudformation.private.yaml \
  --parameters ParameterKey=GitHubRepositoryUrl,ParameterValue=$GITHUB_REPO ParameterKey=AppUser,ParameterValue=$APP_USER ParameterKey=IPv4CIDR,ParameterValue=$IPV4_CIDR ParameterKey=IPv6CIDR,ParameterValue=$IPV6_CIDR ParameterKey=VPCId,ParameterValue=$VPC_ID ParameterKey=SubnetId,ParameterValue=$SUBNET_ID --capabilities CAPABILITY_IAM
 