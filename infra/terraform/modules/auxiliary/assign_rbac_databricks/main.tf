resource "azurerm_role_assignment" "databrikcsContributor" {
  role_definition_name = "contributor"
  scope                = var.databricks_id
  principal_id         = var.adf_principal_id
  lifecycle {
    ignore_changes = [
      name
    ]
  }
}
