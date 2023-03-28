#!/bin/bash

# set -o errexit
set -o pipefail
set -o nounset
set -o xtrace # For debugging

####################################
# Variables
# DATABRICKSHOSTURL
# DATABRICKSWORKSPACENAME
# KEYVAULTRESOURCEID
# KEYVAULTDNSNAME
# DATABRICKSAPPID
# CONFIG
####################################

databricksHostUrl=$DATABRICKSHOSTURL
databricksWorkspaceName=$DATABRICKSWORKSPACENAME
databricksAppId=$DATABRICKSAPPID
ScopeName='SecretScope'

echo "Start deploying databricks "$databricksWorkspaceName

export DATABRICKS_AAD_TOKEN=$(az account get-access-token --resource "${databricksAppId}" | jq .accessToken --raw-output)
databricks configure --aad-token --host "${databricksHostUrl}"

# adbTmpDir=.tmp/databricks
# mkdir -p $adbTmpDir && cp -a ./iac/code/databricks .tmp/
# tmpfile=.tmpfile


# ################################################################
# CREATE CLUSTER
echo "Create Cluster"
# databricks clusters create --json-file cluster_DE.json
# databricks clusters create --json-file cluster_ML.json
databricks clusters create --json-file cluster_unity.json

# ################################################################


echo "Finish deploying databricks "$databricksWorkspaceName
