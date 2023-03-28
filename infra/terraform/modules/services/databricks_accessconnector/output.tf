output "databricks_accessconnector_identity_id" {
  value       = azurerm_databricks_access_connector.this.identity[0].principal_id
  description = "ID of databricks access connector managed identity."
  sensitive   = true
}