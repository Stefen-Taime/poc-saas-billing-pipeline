variable "namespace" {
  type = string
}

variable "root_password" {
  type      = string
  sensitive = true
}

variable "database_name" {
  type = string
}
