id=$(date '+%H%M%S')

aws cloudformation create-stack \
  --stack-name build-metadata-management-solution-$id \
  --template-body file://build.template.yaml \
  --parameters \
    ParameterKey=GitHubRepositoryUrl,ParameterValue=https://github.com/snunezcode/mytagger.git \
    ParameterKey=Username,ParameterValue=snmatus@amazon.com \
  --capabilities CAPABILITY_IAM
 