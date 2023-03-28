
targetScope = 'subscription'

// general params
param location string ='japaneast' 
param project string = 'openhack-lh'
param deployment_id string ='001'

param rg_trainer_name string = 'Hands-on-Master'
param rg_student_names array  = [
    'Hands-on-WorkGroup-A'
    'Hands-on-WorkGroup-B'
]
param isTrainerSetUp bool = true

var prefix = '${project}-${deployment_id}'

var tags = {
  Project : project
}

module trainer 'modules/trainer.bicep' = if (isTrainerSetUp) {
  name: 'trainer'
  params: {
    location: location
    prefix: prefix
    rg_trainer_name: rg_trainer_name
    tags: tags
  }
}


module student 'modules/student.bicep' = [for studentRg in rg_student_names : {
  name: studentRg
  params: {
    location: location
    prefix: prefix
    rg_student_name: studentRg
    tags: tags
  }
}]
