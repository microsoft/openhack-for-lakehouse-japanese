resource "azurerm_data_factory_linked_service_azure_databricks" "databricks" {
  data_factory_id            = var.data_factory_id
  adb_domain                 = var.adb_domain
  name                       = "LS_databricks"
  msi_work_space_resource_id = var.msi_work_space_resource_id
  new_cluster_config {
    cluster_version       = "11.3.x-photon-scala2.12"
    node_type             = "Standard_E4ds_v4"
    min_number_of_workers = 1
    driver_node_type      = "Standard_E4ds_v4"
    spark_config          = { "spark.databricks.dataLineage.enabled" : "true" }
    spark_environment_variables = {
      "PYSPARK_PYTHON" : "/databricks/python3/bin/python3"
    }
  }
}