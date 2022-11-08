param datafactoryId string 
param databricksId string
param databricksWorkspaceUrl string

var databricksName = last(split(databricksId,'/'))
var databricksNameCleaned = toLower(replace(databricksName,'-','_'))

resource datafactory 'Microsoft.DataFactory/factories@2018-06-01' existing = {
  name: last(split(datafactoryId,'/'))
}

resource linkedServicesAzureDatabricks 'Microsoft.DataFactory/factories/linkedservices@2018-06-01' = {
  parent: datafactory
  name: databricksNameCleaned
  properties: {
    annotations: []
    type: 'AzureDatabricks'
    typeProperties: {
      domain: databricksWorkspaceUrl
      authentication: 'MSI'
      workspaceResourceId: databricksId
      newClusterNodeType: 'Standard_E4ds_v4'
      newClusterNumOfWorker: '1'
      newClusterSparkConf: { 'spark.databricks.dataLineage.enabled': 'true' }
      newClusterSparkEnvVars: {
        PYSPARK_PYTHON: '/databricks/python3/bin/python3'
      }
      newClusterVersion: '11.3.x-photon-scala2.12'
      clusterOption: 'Fixed'
    }
  }
}
