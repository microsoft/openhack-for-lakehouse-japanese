resource "azurerm_data_factory" "this" {
  name                = var.name
  location            = var.location
  resource_group_name = var.rg_name

  identity {
    type = "SystemAssigned"
  }

  lifecycle {
    ignore_changes = [
      vsts_configuration,
    ]
  }
  tags = merge({ "iac" = "terraform" }, var.tags)
}
