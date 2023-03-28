variable "rg_name" {
  type        = string
  description = "Name of resource group manually created before terraform init"
}

variable "location" {
  type        = string
  description = "Region where data factory will be provisioned"
}

variable "name" {
  type = string
}