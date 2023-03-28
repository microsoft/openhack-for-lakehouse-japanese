data "databricks_spark_version" "unity" {
  photon = false
  depends_on = [
    azurerm_databricks_workspace.this
  ]
}

resource "databricks_cluster" "openhack_UnityCatalog" {
  autoscale {
    min_workers = 2
    max_workers = 4
  }
  cluster_name = "openhack_UnityCatalog"
  #   spark_version = "11.3.x-scala2.12"
  spark_version = data.databricks_spark_version.unity.id
  spark_conf = {
    "spark.databricks.dataLineage.enabled" : "true",
    "spark.databricks.delta.preview.enabled" : "true"
  }
  azure_attributes {
    first_on_demand    = 1
    availability       = "SPOT_WITH_FALLBACK_AZURE"
    spot_bid_max_price = -1
  }
  node_type_id            = "Standard_E4ds_v4"
  ssh_public_keys         = []
  custom_tags             = {}
  spark_env_vars          = {}
  autotermination_minutes = 30
  enable_elastic_disk     = true
  data_security_mode      = "USER_ISOLATION"
  runtime_engine          = "PHOTON"
  depends_on = [
    azurerm_databricks_workspace.this
  ]
}

data "databricks_spark_version" "photon" {
  photon = true
  depends_on = [
    azurerm_databricks_workspace.this
  ]
}

resource "databricks_cluster" "openhack" {
  autoscale {
    min_workers = 2
    max_workers = 4
  }
  cluster_name = "openhack"
  #   spark_version = "11.3.x-photon-scala2.12"
  spark_version = data.databricks_spark_version.photon.id
  spark_conf = {
    "spark.databricks.cluster.profile" : "serverless",
    "spark.databricks.repl.allowedLanguages" : "sql,python,r"
  }
  azure_attributes {
    first_on_demand    = 1
    availability       = "ON_DEMAND_AZURE"
    spot_bid_max_price = -1
  }
  node_type_id    = "Standard_E4ds_v4"
  ssh_public_keys = []
  custom_tags = {
    "ResourceClass" = "Serverless"
  }
  spark_env_vars          = {}
  autotermination_minutes = 30
  enable_elastic_disk     = true
  depends_on = [
    azurerm_databricks_workspace.this
  ]

}

data "databricks_spark_version" "ml" {
  ml = true
  depends_on = [
    azurerm_databricks_workspace.this
  ]
}

resource "databricks_cluster" "openhack_ML" {
  autoscale {
    min_workers = 2
    max_workers = 4
  }
  cluster_name = "openhack_ML"
  #   spark_version = "11.3.x-cpu-ml-scala2.12"
  spark_version = data.databricks_spark_version.ml.id
  spark_conf = {
    "spark.databricks.cluster.profile" : "serverless",
    "spark.databricks.repl.allowedLanguages" : "sql,python,r"
  }
  azure_attributes {
    first_on_demand    = 1
    availability       = "ON_DEMAND_AZURE"
    spot_bid_max_price = -1
  }
  node_type_id    = "Standard_E4ds_v4"
  ssh_public_keys = []
  custom_tags = {
    "ResourceClass" = "Serverless"
  }
  spark_env_vars          = {}
  autotermination_minutes = 30
  enable_elastic_disk     = true
  depends_on = [
    azurerm_databricks_workspace.this
  ]
}