output "adf_identity_id" {
  value       = azurerm_data_factory.this.identity[0].principal_id
  description = "ID of ADF managed identity."
  sensitive   = true
}

output "adf_name" {
  value       = azurerm_data_factory.this.name
  description = "Azure Data Factory name"
}

output "adf_id" {
  value     = azurerm_data_factory.this.id
  sensitive = true
}