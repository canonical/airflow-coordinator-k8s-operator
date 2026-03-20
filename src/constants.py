# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Constants to be used in the Airflow Coordiantor charm."""

AIRFLOW_COORDINATOR_RELATION_NAME = "airflow-coordinator"
AIRFLOW_API_SERVER_ENDPOINT_NAME = "airflow-api-server"
S3_ENDPOINT_NAME = "s3"

PEER_RELATION_NAME = "coordinator-peers"

POSTGRES_RELATION_NAME = "postgres"
AIRFLOW_DATABASE_NAME = "airflow"

WORKLOAD_CONTAINER_NAME = "airflow-coordinator"
AIRFLOW_CONFIG_PATH = "/opt/airflow/airflow.cfg"

MISSING_POSTGRES_INTEGRATION_MESSAGE = "Missing integration with postgres"
WAITING_FOR_DATABASE_TO_BE_CREATED_MESSAGE = "Waiting for airflow database to be created"
WAITING_FOR_DATABASE_CONNECTION_MESSAGE = "Waiting for database connection info from postgres"
WAITING_FOR_CONTAINER_MESSAGE = "Waiting for workload container"
WAITING_FOR_PEER_RELATION_MESSAGE = "Waiting for peer relation"
DB_MIGRATION_FAILED_MESSAGE = "Database migration failed"
WAITING_FOR_API_SERVER_RELATION_MESSAGE = "Waiting for airflow_api_server interface integration"
WAITING_FOR_API_SERVER_HOST_PORT_MESSAGE = "Waiting for host+port information from api server"
MISMATCHED_AIRFLOW_VERSIONS_MESSAGE = "Integrated apps with mismatched airflow versions"
MISMATCHED_WORKLOAD_IMAGE_HASHES_MESSAGE = "Integrated apps with mismatched workload image hashes"
MISSING_INTEGRATIONS_MESSAGE_TEMPLATE = "Missing integrations with: {missing_core_components}"
INVALID_S3_RELATIONS_MESSAGE_TEMPLATE = "Invalid S3 relations: {relation_ids}"
