resource "azurerm_role_assignment" "databrikcsConnector_BlobDataContributor" {
  role_definition_name = "Storage Blob Data Contributor"
  scope                = var.storage_id
  principal_id         = var.dbc_principal_id
  lifecycle {
    ignore_changes = [
      name
    ]
  }
}