#!/bin/bash 

#######
####### STEP 1 - Build - Testing
#######

export IDENTIFIER=$(date '+%H%M%S')
export APP_USER="snmatus@amazon.com"
export IPV4_CIDR="138.84.54.61/32"
export IPV6_CIDR="2605:59c8:731d:4810:2cd8:d9a:59f4:888c/128"
export GITHUB_REPO="https://github.com/snunezcode/mytagger.git"
export WAF="false"

aws cloudformation create-stack \
  --stack-name "build-public-$IDENTIFIER" \
  --template-body file://cloudformation.public.yaml \
  --parameters ParameterKey=GitHubRepositoryUrl,ParameterValue=$GITHUB_REPO ParameterKey=AppUser,ParameterValue=$APP_USER ParameterKey=IPv4CIDR,ParameterValue=$IPV4_CIDR ParameterKey=IPv6CIDR,ParameterValue=$IPV6_CIDR ParameterKey=WAFRequired,ParameterValue=$WAF --capabilities CAPABILITY_IAM
 