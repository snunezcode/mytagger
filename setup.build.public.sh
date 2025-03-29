#!/bin/bash 

#######
####### STEP 1 - Build - Testing
#######

export IDENTIFIER=$(date '+%H%M%S')
export APP_USER="snmatus@amazon.com"
export IPV4_CIDR="148.222.132.51/32"
export IPV6_CIDR="2605:59c8:731d:4810:415:bd81:f251:f260/128"
export GITHUB_REPO="https://github.com/snunezcode/mytagger.git"


aws cloudformation create-stack \
  --stack-name "build-$IDENTIFIER" \
  --template-body file://cloudformation.public.yaml \
  --parameters \
    ParameterKey=GitHubRepositoryUrl,ParameterValue=$GITHUB_REPO \
    ParameterKey=AppUser,ParameterValue=$APP_USER \
    ParameterKey=IPv4CIDR,ParameterValue=$IPV4_CIDR \
    ParameterKey=IPv6CIDR,ParameterValue=$IPV6_CIDR \    
  --capabilities CAPABILITY_IAM
 