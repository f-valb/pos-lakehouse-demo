variable "country" { type = string }
variable "aws_region" { type = string }
variable "databricks_account_id" { type = string }
variable "workspace_url" { type = string }
variable "workspace_id" { type = number }
variable "raw_bucket" {
  type        = string
  description = "Output raw_bucket of infra/terraform/aws for the same country"
}
variable "uc_access_role_arn" {
  type        = string
  description = "Output uc_access_role_arn of infra/terraform/aws"
}
variable "global_metastore_sharing_id" {
  type        = string
  description = "Sharing identifier of the global analytics metastore (cloud:region:metastore-uuid)"
}
variable "monthly_budget_usd" {
  type    = number
  default = 20000
}
variable "finops_email" { type = string }
