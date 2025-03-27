#!/bin/bash 
username=$1
#cidr=$2
id=$(date '+%H%M%S')
start_time=$(date +%s)

stack_name="deployment-metadata-management-solution"
template_frontend="solution.template.yaml"
echo "`date '+%H:%M:%S'` -  ## Creating AWS Cloudformation StackID : $id "


#######
####### STEP 1 - Creating Amazon Aurora DSQL Cluster
#######

echo -e "\n|--#### (1/7) - Creating Amazon Aurora DSQL Cluster ... \n\n"
dsql_cluster_identifier=$(aws dsql create-cluster --tags Name=$stack_name-$id --no-deletion-protection-enabled --query 'identifier' --output text)
dsql_cluster_endpoint="$dsql_cluster_identifier.dsql.us-east-1.on.aws"


#######
####### STEP 2 - Creating artifacts
#######

echo -e "\n|--#### (2/7) - Creating artifacts ...\n\n"
mkdir -p artifacts/functions
cd artifacts/lambda.api/ && zip -r ../functions/lambda.api.zip lambda_function.py && cd ../../
cd artifacts/lambda.discovery/ && zip -r ../functions/lambda.discovery.zip lambda_function.py  && cd ../../
cd artifacts/lambda.tagger/ && zip -r ../functions/lambda.tagger.zip lambda_function.py  && cd ../../
cd artifacts/lambda.initdb/ && zip -r ../functions/lambda.initdb.zip lambda_function.py  && cd ../../


aws s3 mb s3://$stack_name-$id
aws s3 cp artifacts/functions/. s3://$stack_name-$id/functions/ --recursive
aws s3 cp artifacts/layers/. s3://$stack_name-$id/layers/ --recursive




#######
####### STEP 4 - Creating AWS Resources
#######

echo -e "\n|--#### (3/7) - Creating AWS Resources  ...\n\n"
#aws cloudformation create-stack --stack-name "$stack_name-$id" --parameters ParameterKey=Username,ParameterValue=$username ParameterKey=S3Artifacts,ParameterValue=$stack_name-$id ParameterKey=DSQLCluster,ParameterValue=$dsql_cluster_endpoint ParameterKey=DSQLClusterId,ParameterValue=$dsql_cluster_identifier ParameterKey=CIDRInboundAccess,ParameterValue=$cidr  --template-body file://$template_frontend --region us-east-1 --capabilities CAPABILITY_NAMED_IAM
aws cloudformation create-stack --stack-name "$stack_name-$id" --parameters ParameterKey=Username,ParameterValue=$username ParameterKey=S3Artifacts,ParameterValue=$stack_name-$id ParameterKey=DSQLCluster,ParameterValue=$dsql_cluster_endpoint ParameterKey=DSQLClusterId,ParameterValue=$dsql_cluster_identifier --template-body file://$template_frontend --region us-east-1 --capabilities CAPABILITY_NAMED_IAM
aws cloudformation wait stack-create-complete --stack-name "$stack_name-$id" --region us-east-1


echo -e "\n|--#### (4/7) -  Removing artifacts ...\n\n"
aws s3 rm s3://$stack_name-$id/ --recursive
aws s3 rb s3://$stack_name-$id

export $(aws cloudformation describe-stacks --stack-name "$stack_name-$id" --output text --query 'Stacks[0].Outputs[].join(`=`, [join(`_`, [`CF`, `OUT`, OutputKey]), OutputValue ])' --region us-east-1)





#######
####### STEP 5 - Building Frontend Application
#######

echo -e "\n|--#### (5/7) -  Building Frontend Application  ...\n\n"

cat << EOF > frontend/public/aws-exports.json
{ "aws_region": "us-east-1",  "aws_cognito_user_pool_id": "$CF_OUT_CognitoUserPool",  "aws_cognito_user_pool_web_client_id": "$CF_OUT_CognitoUserPoolClient",  "aws_api_port": 3000,  "aws_token_expiration": 24 }
EOF

cat << EOF > frontend/src/pages/Api.js
export const api = { "url" : "$CF_OUT_ApiURL" };
EOF

cd frontend/ && npm install && npm run build && aws s3 cp  build/. s3://$CF_OUT_S3BucketFrontendApp/ --recursive && cd .. 
aws s3 cp  modules/. s3://$CF_OUT_S3PluginBucket/ --recursive 




#######
####### STEP 6 - Configuring database store
#######

echo -e "\n|--#### (6/7) - Configuring database store  ...\n\n"

aws dsql wait cluster-active  --identifier $dsql_cluster_identifier
aws lambda invoke --function-name mtdt-mng-lambda-initdb --cli-binary-format raw-in-base64-out --region $CF_OUT_Region response.json




#######
####### STEP 7 - AWS Resources created
#######

echo -e "\n|--#### (7/7) -  AWS Resources created.\n\n"
echo " "
echo " PublicAppURL   : $CF_OUT_PublicAppURL"
echo " ApiURL   : $CF_OUT_ApiURL"
echo " CognitoUserPool   : $CF_OUT_CognitoUserPool"
echo " CognitoUserPoolClient   : $CF_OUT_CognitoUserPoolClient"
echo " S3BucketFrontendApp   : $CF_OUT_S3BucketFrontendApp"
echo " S3PluginBucket   : $CF_OUT_S3PluginBucket"
echo " IAMRoleTaggerSolution   : $CF_OUT_IAMRoleTaggerSolution"

echo -e "\n|--#### Clean up commands."
echo -e "\naws cloudformation delete-stack --stack-name $stack_name-frontend-$id --region us-east-1"
echo -e "\naws dsql delete-cluster --identifier $dsql_cluster_identifier \n"

#read wait
#aws cloudformation delete-stack --stack-name $stack_name-frontend-$id --region us-east-1
#aws dsql delete-cluster --identifier $dsql_cluster_identifier

end_time=$(date +%s)
elapsed=$(( end_time - start_time ))
eval "echo Elapsed time: $(date -ud "@$elapsed" +'$((%s/3600/24)) days %H hr %M min %S sec')"
