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
  description = "Region where datalake will be provisioned"
}

variable "name" {
  type = string
}

variable "filesystemNames" {
  type        = list(string)
  description = "Unique identifier of the app"
}

