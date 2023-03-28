targetScope = 'subscription'

param location string
param tags object

param prefix string
param  rg_trainer_name string

var tags_joind = union(tags,{
  Group: 'Master'
})

var suffix = last(split(rg_trainer_name,'-'))
var databricksName = '${prefix}-adb-${suffix}'
var databricksConnectorName = '${prefix}-unitycon-${suffix}'
var storageName = '${prefix}-metastore-${suffix}'
var factoryName = '${prefix}-adf-${suffix}'

resource trainer 'Microsoft.Resources/resourceGroups@2021-04-01'={
  name: rg_trainer_name
  location: location
  tags:tags
}

module databricks 'services/databricks.bicep' = {
  scope: trainer
  name: 'databricks'
  params: {
    databricksName: databricksName
    location: location
    tags: tags_joind
  }
}
module databricksConnector 'services/databricksConnector.bicep' = {
  scope: trainer
  name: 'databricksConnector'
  params: {
    databricksConnectorName: databricksConnectorName
    location: location
    tags: tags_joind
  }
}

module metastorelake 'services/storage.bicep' = {
  scope: trainer
  name: 'metastorelake'
  params: {
    fileSystemNames: [
      'metastore-jpeast'
    ]
    location: location
    storageName: storageName
    tags: tags_joind
  }
}

module datafactory 'services/datafactory.bicep' = {
  scope: trainer
  name: 'datafactory'
  params: {
    factoryName: factoryName
    location:location
    tags: tags_joind
  }
}

module trainerRBAC 'auxiliary/trainerRBAC.bicep' = {
  scope: trainer
  name: 'trainerRBAC'
  params: {
    connectorMSI: databricksConnector.outputs.databricksConnectorMSI
    databricksId: databricks.outputs.databricksId
    factoryMSI: datafactory.outputs.datafactoryMSI
    storageId: metastorelake.outputs.storageId
  }
}



module linkedServices 'auxiliary/databricksLinkedService.bicep' = {
  scope: trainer
  name: 'linkedServices'
  params: {
    databricksId: databricks.outputs.databricksId
    databricksWorkspaceUrl: databricks.outputs.databricksWorkspaceUrl
    datafactoryId: datafactory.outputs.datafactoryId
  }
}
