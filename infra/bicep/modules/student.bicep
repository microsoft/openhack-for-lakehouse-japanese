targetScope = 'subscription'

param location string
param tags object

param prefix string
param  rg_student_name string

var tags_joind = union(tags,{
  Group: 'Student'
})

var suffix = 'team-${last(split(rg_student_name,'-'))}'
var databricksName = '${prefix}-adb-${suffix}'
var factoryName = '${prefix}-adf-${suffix}'

resource student 'Microsoft.Resources/resourceGroups@2021-04-01'={
  name: rg_student_name
  location: location
  tags:tags
}

module databricks 'services/databricks.bicep' = {
  scope: student
  name: 'databricks'
  params: {
    databricksName: databricksName
    location: location
    tags: tags_joind
  }
}


module datafactory 'services/datafactory.bicep' = {
  scope: student
  name: 'datafactory'
  params: {
    factoryName: factoryName
    location:location
    tags: tags_joind
  }
}


module linkedServices 'auxiliary/databricksLinkedService.bicep' = {
  scope: student
  name: 'linkedServices'
  params: {
    databricksId: databricks.outputs.databricksId
    databricksWorkspaceUrl: databricks.outputs.databricksWorkspaceUrl
    datafactoryId: datafactory.outputs.datafactoryId
  }
}

module studentRBAC 'auxiliary/studentRBAC.bicep' = {
  scope: student
  name: 'studentRBAC'
  params: {
    databricksId: databricks.outputs.databricksId
    factoryMSI: datafactory.outputs.datafactoryMSI
  }
}
