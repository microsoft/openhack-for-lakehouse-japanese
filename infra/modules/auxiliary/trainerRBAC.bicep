

param connectorMSI string
param storageId string
param databricksId string
param factoryMSI string

var blobDataContributorId = 'ba92f5b4-2d11-453d-a403-e96b0029c9fe'
var contributorId = 'b24988ac-6180-42a0-ab88-20f7382dd24c'

resource storage 'Microsoft.Storage/storageAccounts@2022-05-01' existing = {
  name: last(split(storageId,'/'))
}

resource databricks 'Microsoft.Databricks/workspaces@2022-04-01-preview' existing = {
  name:  last(split(databricksId,'/'))
}

resource metastoreRoleAssignment 'Microsoft.Authorization/roleAssignments@2020-04-01-preview' = {
  name: guid(storageId,connectorMSI, blobDataContributorId)
  scope: storage
  properties: {
    roleDefinitionId: resourceId('Microsoft.Authorization/roleDefinitions', blobDataContributorId)
    principalId: connectorMSI
    principalType: 'ServicePrincipal'
  }
}

resource databricksRoleAssignment 'Microsoft.Authorization/roleAssignments@2020-04-01-preview' = {
  name: guid(databricksId,factoryMSI, contributorId)
  scope: databricks
  properties: {
    roleDefinitionId: resourceId('Microsoft.Authorization/roleDefinitions', contributorId)
    principalId: factoryMSI
    principalType: 'ServicePrincipal'
  }
}
