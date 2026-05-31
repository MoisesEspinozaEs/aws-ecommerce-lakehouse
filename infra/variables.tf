variable "project" {
  description = "Short project name, used as a prefix for all resources"
  type        = string
  default     = "ecomlake"
}

variable "environment" {
  description = "Deployment environment"
  type        = string
  default     = "dev"
}

variable "region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}

variable "bronze_retention_days" {
  description = "How long to keep raw bronze files before lifecycle expiry"
  type        = number
  default     = 30
}

variable "alert_email" {
  description = "Email that receives the budget alert"
  type        = string
  default     = "espinoza.moises@outlook.com"
}

variable "monthly_budget_usd" {
  description = "Monthly spend cap that triggers the budget alert at 80%"
  type        = string
  default     = "5"
}

variable "enable_streaming" {
  description = <<-EOT
    Turn the live Kinesis + Firehose streaming path on or off.
    Set to false for the cheapest path, where sample events are written
    straight to S3 bronze (also useful while a new AWS account is still
    being activated and Kinesis is not yet available).
  EOT
  type        = bool
  default     = false
}
