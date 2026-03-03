# CC008: charm modules must output the deployed application object.
output "application" {
  value = juju_application.airflow_coordinator_k8s
}

output "provides" {
  value = {
    airflow_coordinator = "airflow-coordinator"
  }
}

output "requires" {
  value = {
    postgres           = "postgres"
    airflow-api-server = "airflow-api-server"
  }
}
