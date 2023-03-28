// This template is used to create a datalake.
targetScope = 'resourceGroup'

// Parameters
param location string
param tags object

param factoryName string

resource datafactory 'Microsoft.DataFactory/factories@2018-06-01' = {
  name: factoryName
  location: location
  tags: tags  
  identity: {
    type:'SystemAssigned'
  }
}




output datafactoryMSI string = datafactory.identity.principalId
output datafactoryId string = datafactory.id
