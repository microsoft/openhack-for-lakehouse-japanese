

resource "azurerm_databricks_workspace" "this" {
  name                = var.name
  location            = var.location
  resource_group_name = var.rg_name
  sku                 = var.sku
  tags = {
    "iac" = "terraform"
  }
}

