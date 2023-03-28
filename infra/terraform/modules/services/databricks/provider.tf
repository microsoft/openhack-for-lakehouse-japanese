terraform {
  required_version = ">=0.14"
  required_providers {
    azurerm = {
      source = "hashicorp/azurerm"
    }
    databricks = {
      source = "databricks/databricks"
    }
  }
}

provider "databricks" {
  azure_workspace_resource_id = azurerm_databricks_workspace.this.id
}
