# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Constants to be used in the Airflow Coordinator charm."""

AIRFLOW_COORDINATOR_RELATION_NAME = "airflow-coordinator"
AIRFLOW_API_SERVER_ENDPOINT_NAME = "airflow-api-server"
S3_ENDPOINT_NAME = "s3"
GIT_ENDPOINT_NAME = "git"
OAUTH_ENDPOINT_NAME = "oauth"
AIRFLOW_KUBERNETES_EXECUTOR_CONFIG_RELATION_NAME = "airflow-kubernetes-executor-config"

PEER_RELATION_NAME = "coordinator-peers"

POSTGRES_RELATION_NAME = "postgres"

WORKLOAD_CONTAINER_NAME = "airflow-coordinator"
AIRFLOW_HOME = "/opt/airflow"
AIRFLOW_CONFIG_PATH = f"{AIRFLOW_HOME}/airflow.cfg"
TLS_CA_CHAIN_FILEPATH_TEMPLATE = AIRFLOW_HOME + "/connection_certs/{filename}.pem"

WORKLOAD_USER = "ubuntu"
WORKLOAD_GROUP = "ubuntu"

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
INVALID_GIT_RELATIONS_MESSAGE_TEMPLATE = "Invalid git relations: {relation_ids}"
AIRFLOW_KEYS_SECRET_ERROR_MESSAGE = "Issue retrieving secret hosting airflow keys"
AIRFLOW_KEYS_SECRET_ADD_ERROR_MESSAGE = "Issue adding secret for airflow keys"
WAITING_FOR_KUBERNETES_EXECUTOR_CONFIG_MESSAGE = (
    "Waiting for configuration from the kubernetes executor charm"
)
MISSING_FERNET_KEY_SECRET_CONFIG_MESSAGE = "Waiting for fernet key secret configuration"
INVALID_FERNET_KEY_SECRET_MESSAGE = "Fernet key secret not valid"
MISSING_FERNET_KEY_IN_SECRET_MESSAGE = "Missing fernet key in secret"
ISSUE_RECONCILING_AIRFLOW_CONNECTIONS_MESSAGE = (
    "Internal issue while reconciling S3/git Airflow connections"
)

CORE_DEFAULT_TIMEZONE_CONFIG = "core_default_timezone"
CORE_MAX_ACTIVE_RUNS_PER_DAG_CONFIG = "core_max_active_runs_per_dag"
CORE_MAX_ACTIVE_TASKS_PER_DAG_CONFIG = "core_max_active_tasks_per_dag"
CORE_PARALLELISM_CONFIG = "core_parallelism"
DAG_PROCESSOR_PARSING_PROCESSES_CONFIG = "dag_processor_parsing_processes"
DATABASE_SQL_ALCHEMY_POOL_SIZE_CONFIG = "database_sql_alchemy_pool_size"
TRIGGERER_CAPACITY_CONFIG = "triggerer_capacity"

INVALID_CONFIG_MESSAGE = "Invalid value for `{config_name}` config"

AIRFLOW_KEYS_SECRET = "airflow_keys_secret_id"
AIRFLOW_KEYS_SECRET_LABEL = "airflow-keys-secret"

FERNET_KEY_SECRET_CONFIG = "fernet_key_secret"
FERNET_KEY = "fernet-key"

IDP_GROUPS_FOR_ADMIN_CONFIG = "idp_groups_for_admin"
IDP_GROUPS_FOR_OP_CONFIG = "idp_groups_for_op"
IDP_GROUPS_FOR_USER_CONFIG = "idp_groups_for_user"
IDP_GROUPS_FOR_VIEWER_CONFIG = "idp_groups_for_viewer"
IDP_GROUPS_FOR_PUBLIC_CONFIG = "idp_groups_for_public"

ENABLE_USER_REGISTRATION_CONFIG = "enable_user_registration"

IDP_GROUPS_CONFIGS = [
    IDP_GROUPS_FOR_ADMIN_CONFIG,
    IDP_GROUPS_FOR_OP_CONFIG,
    IDP_GROUPS_FOR_USER_CONFIG,
    IDP_GROUPS_FOR_VIEWER_CONFIG,
    IDP_GROUPS_FOR_PUBLIC_CONFIG,
]
