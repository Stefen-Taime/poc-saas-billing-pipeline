variable "namespace" {
  type = string
}

variable "mysql_host" {
  type = string
}

variable "mysql_port" {
  type    = number
  default = 3306
}

variable "mysql_password" {
  type      = string
  sensitive = true
}

variable "mysql_database" {
  type = string
}

variable "project_id" {
  type = string
}

