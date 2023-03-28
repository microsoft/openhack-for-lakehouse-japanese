resource "azurerm_databricks_access_connector" "this" {
  name                = var.name
  resource_group_name = var.rg_name
  identity {
    type = "SystemAssigned"
  }
  location = var.location
}