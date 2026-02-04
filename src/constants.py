# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Constants to be used in the Airflow Coordiantor charm."""

AIRFLOW_COORDINATOR_RELATION_NAME = "airflow-coordinator"

PEER_RELATION_NAME = "coordinator-peers"

POSTGRES_RELATION_NAME = "postgres"
AIRFLOW_DATABASE_NAME = "airflow"

WORKLOAD_CONTAINER_NAME = "airflow-coordinator"
AIRFLOW_CONFIG_PATH = "/airflow.cfg"

UNAUTHORIZED_ACCESS_TO_SECRET_MESSAGE = (
    "Charm is unauthorized to access sensitive custom config secret"
)
CUSTOM_CONFIG_SECRET_NOT_FOUND = "Sensitive custom config secret not found"
CUSTOM_CONFIG_OVERLAP_MESSAGE = "Sensitive and non-sensitive custom configs have overlap"
CUSTOM_CONFIG_HAS_BLACKLIST_KEY = "Sensitive or non-sensitive custom configs have blacklisted keys"

MISSING_POSTGRES_INTEGRATION_MESSAGE = "Missing integration with postgres"
WAITING_FOR_DATABASE_TO_BE_CREATED_MESSAGE = "Waiting for airflow database to be created"
WAITING_FOR_DATABASE_CONNECTION_MESSAGE = "Waiting for database connection info from postgres"
WAITING_FOR_CONTAINER_MESSAGE = "Waiting for workload container"
WAITING_FOR_PEER_RELATION_MESSAGE = "Waiting for peer relation"
DB_MIGRATION_FAILED_MESSAGE = "Database migration failed"
MISMATCHED_AIRFLOW_VERSIONS_MESSAGE = "Integrated apps with mismatched airflow versions"
MISMATCHED_WORKLOAD_IMAGE_HASHES_MESSAGE = "Integrated apps with mismatched workload image hashes"
MISSING_INTEGRATIONS_MESSAGE_TEMPLATE = "Missing integrations with: {missing_core_components}"
MISMATCHED_WORKLOAD_IMAGE_HASHES_MESSAGE = "Integrated apps with mismatched workload image hashes"
MISSING_INTEGRATIONS_MESSAGE_TEMPLATE = "Missing integrations with: {missing_core_components}"

SENSITIVE_CUSTOM_CONFIG = "sensitive_airflow_configuration_secret"
SENSITIVE_CUSTOM_CONFIG_SECRET_KEY = "sensitive-custom-airflow-configuration"
CUSTOM_CONFIG = "custom_airflow_configuration"
