#!/bin/bash 

#######
####### STEP 1 - Build - Testing
#######

export IDENTIFIER=$(date '+%H%M%S')
export APP_USER="snmatus@amazon.com"
export IPV4_CIDR="148.222.132.11/32"
export IPV6_CIDR="2605:59c8:731d:4810:5081:248b:9c32:bc69/128"
export GITHUB_REPO="https://github.com/snunezcode/mytagger.git"
export GITHUB_REPO="https://github.com/aws-samples/sample-tagger.git"
export WAF="false"

aws cloudformation create-stack \
  --stack-name "build-public-$IDENTIFIER" \
  --template-body file://cloudformation.public.yaml \
  --parameters ParameterKey=GitHubRepositoryUrl,ParameterValue=$GITHUB_REPO ParameterKey=AppUser,ParameterValue=$APP_USER ParameterKey=IPv4CIDR,ParameterValue=$IPV4_CIDR ParameterKey=IPv6CIDR,ParameterValue=$IPV6_CIDR ParameterKey=WAFRequired,ParameterValue=$WAF --capabilities CAPABILITY_IAM
 