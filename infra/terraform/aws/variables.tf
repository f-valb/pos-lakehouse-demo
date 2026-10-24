variable "country" {
  type        = string
  description = "ISO country code of this data-residency domain (PH, SG, DE, US)"
}

variable "env" {
  type    = string
  default = "prod"
  validation {
    condition     = contains(["dev", "staging", "prod"], var.env)
    error_message = "env must be dev, staging or prod"
  }
}

variable "aws_region" {
  type        = string
  description = "In-country region, e.g. ap-southeast-1 (SG), eu-central-1 (DE)"
}

variable "pos_api_url" {
  type        = string
  description = "Legacy POS API base URL, reached over PrivateLink / VPN"
}

variable "poller_image_uri" {
  type        = string
  description = "ECR image with the ingestion package (same code as local mode)"
}

variable "max_api_concurrency" {
  type        = number
  default     = 4
  description = "Hard cap on concurrent calls to the legacy API from this country (Lambda reserved concurrency)"
}

variable "alarm_email" {
  type = string
}

variable "databricks_uc_master_role_arn" {
  type        = string
  description = "Databricks Unity Catalog master role ARN (trust for the storage credential)"
  default     = "arn:aws:iam::414351767826:role/unity-catalog-prod-UCMasterRole-14S5ZJVKOTYTL"
}

variable "databricks_storage_credential_external_id" {
  type        = string
  description = "External ID shown by Databricks when creating the storage credential"
}

variable "vpc_subnet_ids" {
  type        = list(string)
  description = "Private subnets with a route to the legacy API"
}

variable "vpc_security_group_ids" {
  type = list(string)
}
