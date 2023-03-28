output "adb_resource_id" {
  value       = azurerm_databricks_workspace.this.id
  description = "ID of adb"
  sensitive   = true
}

output "workspace_url" {
  value     = azurerm_databricks_workspace.this.workspace_url
  sensitive = true
}

