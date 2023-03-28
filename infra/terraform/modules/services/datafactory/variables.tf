# ---------------------------------------------------------------------------------------------------------------------
# REQUIRED PARAMETERS
# You must provide a value for each of these parameters.
# ---------------------------------------------------------------------------------------------------------------------
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

variable "tags" {
  type = object({
    project = string
  })
}