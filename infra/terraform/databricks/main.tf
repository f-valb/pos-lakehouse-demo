# Databricks side of ONE country domain (prod): metastore in-region, workspace binding,
# storage credential + external location on the country bucket (managed file events for Auto
# Loader), the country catalog, SCIM-synced groups bound to the workspace, a serverless SQL
# warehouse for Power BI, policies for FinOps, and the Delta Share that publishes the global
# aggregates. NOT applied in the demo (Free Edition has no account console / AWS account).
# Demo equivalents: governance/*.sql, governance/setup_identities.py, databricks.yml.

terraform {
  required_version = ">= 1.6"
  required_providers {
    databricks = { source = "databricks/databricks", version = "~> 1.60" }
  }
  backend "s3" {}
}

provider "databricks" {
  alias      = "account"
  host       = "https://accounts.cloud.databricks.com"
  account_id = var.databricks_account_id
}

provider "databricks" {
  alias = "workspace"
  host  = var.workspace_url
}

locals {
  cc = lower(var.country)
}

# ---------------------------------------------------------------- metastore (one per region)
resource "databricks_metastore" "this" {
  provider      = databricks.account
  name          = "metastore-${var.aws_region}"
  region        = var.aws_region
  force_destroy = false
}

resource "databricks_metastore_assignment" "this" {
  provider     = databricks.account
  metastore_id = databricks_metastore.this.id
  workspace_id = var.workspace_id
}

# ---------------------------------------------------------------- storage
resource "databricks_storage_credential" "raw" {
  provider = databricks.workspace
  name     = "pos-${local.cc}-raw"
  aws_iam_role { role_arn = var.uc_access_role_arn }
  comment    = "Read/write the ${upper(var.country)} raw POS bucket"
  depends_on = [databricks_metastore_assignment.this]
}

resource "databricks_external_location" "raw" {
  provider           = databricks.workspace
  name               = "pos-${local.cc}-raw"
  url                = "s3://${var.raw_bucket}/"
  credential_name    = databricks_storage_credential.raw.name
  enable_file_events = true # Auto Loader file notification mode without per-stream queues
  comment            = "Landing zone written by the poller"
}

resource "databricks_catalog" "country" {
  provider     = databricks.workspace
  name         = "pos_${local.cc}"
  storage_root = "s3://${var.raw_bucket}/uc/"
  comment      = "POS data domain ${upper(var.country)} (stored and processed in-country)"
  properties   = { data_country = upper(var.country), cost_center = "retail-data" }
}

resource "databricks_volume" "landing" {
  provider         = databricks.workspace
  for_each         = toset(["dev", "staging", "prod"])
  catalog_name     = databricks_catalog.country.name
  schema_name      = "${each.key}_bronze"
  name             = "landing"
  volume_type      = "EXTERNAL"
  storage_location = "s3://${var.raw_bucket}/raw-${each.key}/"
}

# ---------------------------------------------------------------- identity (Entra ID -> SCIM)
# Groups are created by Entra ID SCIM provisioning at the account level; Terraform only looks
# them up and binds them to this workspace with the right entitlements.
data "databricks_group" "scim" {
  provider     = databricks.account
  for_each     = toset(["data_platform", "global_exec", "mgr_${local.cc}", "pii_reader_${local.cc}"])
  display_name = each.key
}

resource "databricks_mws_permission_assignment" "groups" {
  provider     = databricks.account
  for_each     = data.databricks_group.scim
  workspace_id = var.workspace_id
  principal_id = each.value.id
  permissions  = each.key == "data_platform" ? ["ADMIN"] : ["USER"]
}

# ---------------------------------------------------------------- BI serving
resource "databricks_sql_endpoint" "bi" {
  provider                  = databricks.workspace
  name                      = "pos-${local.cc}-bi"
  cluster_size              = "Small"
  min_num_clusters          = 1
  max_num_clusters          = 4 # scale out for ~5,000 Power BI users at peak; DirectQuery caching does the rest
  auto_stop_mins            = 10
  enable_serverless_compute = true
  warehouse_type            = "PRO"
  tags {
    custom_tags {
      key   = "cost_center"
      value = "retail-data"
    }
    custom_tags {
      key   = "component"
      value = "bi"
    }
  }
}

resource "databricks_permissions" "bi_warehouse" {
  provider        = databricks.workspace
  sql_endpoint_id = databricks_sql_endpoint.bi.id
  access_control {
    group_name       = "mgr_${local.cc}"
    permission_level = "CAN_USE"
  }
  access_control {
    group_name       = "global_exec"
    permission_level = "CAN_USE"
  }
}

# ---------------------------------------------------------------- global layer via Delta Sharing
resource "databricks_share" "global_aggregates" {
  provider = databricks.workspace
  name     = "pos_${local.cc}_global_aggregates"
  object {
    name                        = "pos_${local.cc}.prod_gold.sales_by_store_hour"
    data_object_type            = "TABLE"
    history_data_sharing_status = "DISABLED"
    comment                     = "Aggregates only; no PII columns exist in this table"
  }
  object {
    name             = "pos_${local.cc}.prod_gold.data_as_of"
    data_object_type = "TABLE"
  }
}

resource "databricks_recipient" "global_workspace" {
  provider                           = databricks.workspace
  name                               = "global-analytics-workspace"
  authentication_type                = "DATABRICKS"
  data_recipient_global_metastore_id = var.global_metastore_sharing_id
}

resource "databricks_grants" "share" {
  provider = databricks.workspace
  share    = databricks_share.global_aggregates.name
  grant {
    principal  = databricks_recipient.global_workspace.name
    privileges = ["SELECT"]
  }
}
