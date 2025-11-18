# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Constants to be used in the Airflow Coordiantor charm."""

AIRFLOW_COORDINATOR_RELATION_NAME = "airflow-coordinator"

AIRFLOW_COORDINATOR_RELATION_SECRET_LABEL = "airflow-coordinator-relation-secret"
REQUIRED_AIRFLOW_CORE_COMPONENTS = [
    "scheduler",
    "api-server",
    "triggerer",
    "dag-processor",
]
