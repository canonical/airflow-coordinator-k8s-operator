resource "juju_application" "airflow_coordinator_k8s" {
  name       = var.app_name
  model_uuid = var.model_uuid

  charm {
    name     = "airflow-coordinator-k8s"
    revision = var.revision
    channel  = var.channel
  }

  constraints = var.constraints
  config      = var.config

  units = var.units
}
