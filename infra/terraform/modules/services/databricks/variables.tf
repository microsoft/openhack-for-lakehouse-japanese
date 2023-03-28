variable "rg_name" {
  type        = string
  description = "Name of resource group manually created before terraform init"
}

variable "location" {
  type        = string
  description = "Region where databricks will be provisioned"
}

variable "name" {
  type = string
}

variable "sku" {
  type        = string
  description = "Unique identifier of the app"
  default     = "premium"
}
