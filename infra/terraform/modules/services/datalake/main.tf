resource "azurerm_storage_account" "this" {
  name                            = var.name
  resource_group_name             = var.rg_name
  location                        = var.location
  account_tier                    = "Standard"
  account_replication_type        = "LRS"
  account_kind                    = "StorageV2"
  is_hns_enabled                  = "true"
  min_tls_version                 = "TLS1_2"
  enable_https_traffic_only       = true
  allow_nested_items_to_be_public = false
  blob_properties {
    container_delete_retention_policy {
      days = 7
    }
  }
  tags = {
    "iac" = "terraform"
  }
}


resource "azurerm_storage_container" "filesystems" {
  for_each             = { for s, v in var.filesystemNames : s => v }
  name                 = each.value
  storage_account_name = azurerm_storage_account.this.name
}