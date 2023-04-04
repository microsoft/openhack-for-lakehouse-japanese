#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset
# set -o xtrace # For debugging

mode="" #"plan"

##################################################

bash ./code/init.sh

start=$(date)
datetime=$(date "+%Y%m%d_%H%M%S")
mkdir -p .logs 

az account set --subscription $TRAINER_ARM_SUBSCRIPTION_ID
echo "use subscription ${TRAINER_ARM_SUBSCRIPTION_ID}" 

echo "terraform initializing..."
terraform init

# trainer
trainer_logfile=".logs/iac_setup_trainer_${datetime}.log"

if [ "${mode}" = "plan" ];then
    echo "terraform plan for trainer..."
    terraform plan -var-file ./environments/dev/trainer.tfvars -state=./terraform_trainer.tfstate >>${trainer_logfile} 2>&1
else
    echo "terraform apply for trainer..."
    terraform apply --auto-approve -var-file ./environments/dev/trainer.tfvars -state=./terraform_trainer.tfstate >>${trainer_logfile} 2>&1
fi

# student

echo "terraform setup for students..."

az account set --subscription $STUDENT_ARM_SUBSCRIPTION_ID
echo "use subscription ${STUDENT_ARM_SUBSCRIPTION_ID}" 

echo "terraform initializing..."
terraform init

for i in `cat ./code/student_rg_list.txt`;
do
    student_logfile=".logs/iac_setup_student_${i}_${datetime}.log"
    if [ "${mode}" = "plan" ];then
        echo "terraform plan for student_${i}..."
        terraform plan -var-file ./environments/dev/student.tfvars -var rg_name=$i -state=./terraform_${i}.tfstate >>${student_logfile} 2>&1 &
    else
        echo "terraform apply student_${i}..."
        terraform apply --auto-approve -var-file ./environments/dev/student.tfvars -var rg_name=$i -state=./terraform_${i}.tfstate >>${student_logfile} 2>&1 &
    fi
    sleep 10
done

end=$(date)

echo "Starting deployment at ${start}"
echo "finish deployment at ${end}"

