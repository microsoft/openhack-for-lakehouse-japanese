terraform {
  required_version = ">=0.14"
  required_providers {
    azurerm = {
      source = "hashicorp/azurerm"
    }
  }
}

provider "azurerm" {
  features {}
}

locals {
  suffix                        = element(split("-", var.rg_name), length(split("-", var.rg_name)) - 1)
  databricksName                = format("%s-adb-%s", var.prefix, local.suffix)
  databricksAccessConnectorName = format("%s-unitycon-%s", var.prefix, local.suffix)
  datafactoryName               = format("%s-adf-%s", var.prefix, local.suffix)
  datalakename                  = replace(lower(format("%smetastore%s", var.prefix, local.suffix)), "-", "")
}

# リソースグループが存在する場合はコメントアウト
# resource "azurerm_resource_group" "this" {
#   location = var.resource_group_location
#   name     = var.rg_name
#   tags     = var.tags
# }

module "datafactory" {
  source   = "./modules/services/datafactory"
  name     = local.datafactoryName
  location = var.resource_group_location
  rg_name  = var.rg_name
  tags     = var.tags
}
module "databricks" {
  source   = "./modules/services/databricks"
  name     = local.databricksName
  location = var.resource_group_location
  rg_name  = var.rg_name
}

module "assign_rbac_databricks" {
  source           = "./modules/auxiliary/assign_rbac_databricks"
  adf_principal_id = module.datafactory.adf_identity_id
  databricks_id    = module.databricks.adb_resource_id
}

module "linked_services" {
  source                     = "./modules/auxiliary/adf_linked_services"
  adb_domain                 = "https://${module.databricks.workspace_url}"
  data_factory_id            = module.datafactory.adf_id
  msi_work_space_resource_id = module.databricks.adb_resource_id

}

# trainer resources

module "datalake" {
  count = var.is_trainer

  source          = "./modules/services/datalake"
  name            = local.datalakename
  rg_name         = var.rg_name
  location        = var.resource_group_location
  filesystemNames = ["metastore-jpeast"]
}
module "databricks_accessconnector" {
  count = var.is_trainer

  source   = "./modules/services/databricks_accessconnector"
  name     = local.databricksAccessConnectorName
  location = var.resource_group_location
  rg_name  = var.rg_name

}
module "assign_rbac_datalake" {
  count = var.is_trainer

  source           = "./modules/auxiliary/assign_rbac_datalake"
  dbc_principal_id = module.databricks_accessconnector[0].databricks_accessconnector_identity_id
  storage_id       = module.datalake[0].storage_id
}