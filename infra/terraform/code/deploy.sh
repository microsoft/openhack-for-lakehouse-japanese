#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset
# set -o xtrace # For debugging

bash ./code/init.sh

start=$(date)

az account set --subscription $ARM_SUBSCRIPTION_ID

echo "use subscription ${ARM_SUBSCRIPTION_ID}" 

echo "terraform initializing..."

terraform init

# trainer

echo "terraform setup for trainer..."

terraform apply --auto-approve -var-file ./environments/dev/trainer.tfvars 
mv ./terraform.tfstate ./terraform_trainer.tfstate

# student

echo "terraform setup for students..."

for i in `cat ./code/student_rg_list.txt`;
do
    echo "terraform initializing..."
    terraform init
    echo "${i}..."
    terraform apply --auto-approve -var-file ./environments/dev/student.tfvars -var rg_name=$i
    mv ./terraform.tfstate ./terraform_${i}.tfstate
done

echo "Starting deployment at ${start}"
echo "finish deployment at "$(date)

