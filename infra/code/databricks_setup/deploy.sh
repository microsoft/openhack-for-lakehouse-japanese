#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset
set -o xtrace # For debugging


project='adbconfig' # CONSTANT - this is prefix for this sample

. ./init.sh
start=$(date)

az account set --subscription $AZURE_SUBSCRIPTION_ID1
echo "Setting up databricks 1"
databrickslist1=$(az databricks workspace list | jq -r '.[].id') 

for adb in $databrickslist1
do
    workspaceInfo=$(az databricks workspace show --ids $adb )
    workspaceUrl=$(echo $workspaceInfo | jq -r '.workspaceUrl')
    workspaceUrl="https://${workspaceUrl}"
    workspaceName=$(echo $workspaceInfo | jq -r '.name')
    DATABRICKSHOSTURL=$workspaceUrl \
    DATABRICKSAPPID=$DATABRICKS_APP_ID \
    DATABRICKSWORKSPACENAME=$workspaceName \
        bash -c "./deploy_databricks.sh"
done



az account set --subscription $AZURE_SUBSCRIPTION_ID2
echo "Setting up databricks 2"
databrickslist2=$(az databricks workspace list | jq -r '.[].id') 

for adb in $databrickslist2
do
    workspaceInfo=$(az databricks workspace show --ids $adb)
    workspaceUrl=$(echo $workspaceInfo | jq -r '.workspaceUrl')
    workspaceUrl="https://${workspaceUrl}"
    workspaceName=$(echo $workspaceInfo | jq -r '.name')
    DATABRICKSHOSTURL=$workspaceUrl \
    DATABRICKSAPPID=$DATABRICKS_APP_ID \
    DATABRICKSWORKSPACENAME=$workspaceName \
        bash -c "./deploy_databricks.sh"
done

echo "Starting deployment at ${start}"
echo "finish deployment at "$(date)