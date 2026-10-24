# One instance of this stack per COUNTRY, applied in that country's AWS account + region
# (data residency): e.g. `terraform apply -var-file=env/prod-ph.tfvars`.
# NOT applied in the demo (no AWS account). The local equivalent is docker-compose (LocalStack)
# + ingestion/run_local.py setup, which creates the same buckets/queues/alarms.

terraform {
  required_version = ">= 1.6"
  required_providers {
    aws = { source = "hashicorp/aws", version = "~> 5.70" }
  }
  backend "s3" {} # bucket/key/region/dynamodb_table supplied per country at init time
}

provider "aws" {
  region = var.aws_region
  default_tags {
    tags = {
      cost_center  = "retail-data"
      env          = var.env
      data_country = upper(var.country)
      component    = "ingestion"
      managed_by   = "terraform"
    }
  }
}

locals {
  cc     = lower(var.country)
  prefix = "pos-${local.cc}-${var.env}"
}

data "aws_caller_identity" "me" {}
