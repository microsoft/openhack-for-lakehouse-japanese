targetScope = 'resourceGroup'

// Parameters
param location string
param tags object
param databricksConnectorName string
// param vnetId string
// param privateSubnetName string
// param publicSubnetName string

// Variables

// Resources

resource databricksConnector 'Microsoft.Databricks/accessConnectors@2022-04-01-preview' = {
  name: databricksConnectorName
  location: location
  identity: {
     type: 'SystemAssigned'
  }
  tags : tags
}


// Outputs

output databricksConnectorMSI string = databricksConnector.identity.principalId
