# FinOps controls. Budgets ALERT; policies and quotas ENFORCE.
#   - serverless budget policy: every serverless job/pipeline/notebook carries the tags below,
#     so system.billing.usage can attribute spend (custom_tags) even for users who forget tags.
#   - cluster policy: classic compute (if anyone uses it) is capped and must carry tags.
#   - account budget: emails at 80% / 100% of the monthly budget per country. It does not stop
#     anything; stopping is done by the policies, the warehouse max clusters and job concurrency.

resource "databricks_budget_policy" "serverless" {
  provider    = databricks.account
  policy_name = "pos-${local.cc}-serverless"
  custom_tags = [
    { key = "cost_center", value = "retail-data" },
    { key = "data_country", value = upper(var.country) },
  ]
}

resource "databricks_cluster_policy" "classic" {
  provider = databricks.workspace
  name     = "pos-${local.cc}-classic-capped"
  definition = jsonencode({
    "autotermination_minutes" : { "type" : "range", "maxValue" : 30, "defaultValue" : 15 },
    "autoscale.max_workers" : { "type" : "range", "maxValue" : 8 },
    "custom_tags.cost_center" : { "type" : "fixed", "value" : "retail-data" },
    "custom_tags.data_country" : { "type" : "fixed", "value" : upper(var.country) },
    "custom_tags.env" : { "type" : "allowlist", "values" : ["dev", "staging", "prod"] },
    "node_type_id" : { "type" : "allowlist", "values" : ["m7gd.xlarge", "m7gd.2xlarge"] },
  })
}

resource "databricks_budget" "monthly" {
  provider     = databricks.account
  display_name = "pos-${local.cc}-monthly"

  dynamic "alert_configurations" {
    for_each = [0.8, 1.0]
    content {
      time_period        = "MONTH"
      trigger_type       = "CUMULATIVE_SPENDING_EXCEEDED"
      quantity_type      = "LIST_PRICE_DOLLARS_USD"
      quantity_threshold = tostring(var.monthly_budget_usd * alert_configurations.value)
      action_configurations {
        action_type = "EMAIL_NOTIFICATION"
        target      = var.finops_email
      }
    }
  }

  filter {
    workspace_id {
      operator = "IN"
      values   = [var.workspace_id]
    }
    tags {
      key = "data_country"
      value {
        operator = "IN"
        values   = [upper(var.country)]
      }
    }
  }
}
