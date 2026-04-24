# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

import logging
import unittest.mock

import charms.git_integrator.v0.git as git
import cryptography.fernet
import ops.testing
import pytest

import command_executor
import constants
from charm import AirflowCoordinatorK8SOperatorCharm

logger = logging.getLogger(__name__)

POSTGRES_DATA = {
    "username": "airflow_user",
    "password": "airflow_password",
    "database": "airflow",
    "endpoints": "airflow_host:airflow_port",
    "read_only_endpoints": "airflow_read_only_host:airflow_read_only_port",
}


POSTGRES_SQL_ALCHEMY_STRING = (
    "postgresql+psycopg2://airflow_user:airflow_password@airflow_host:airflow_port/airflow"
)


@pytest.fixture
def airflow_coordinator_k8s_charm():
    yield AirflowCoordinatorK8SOperatorCharm


@pytest.fixture(scope="function")
def context(airflow_coordinator_k8s_charm):
    return ops.testing.Context(charm_type=airflow_coordinator_k8s_charm)


@pytest.fixture(scope="function")
def workload_container():
    return ops.testing.Container(
        constants.WORKLOAD_CONTAINER_NAME,
        can_connect=True,
    )


def core_component_metadata(
    component: str,
    airflow_version: str = "3.1.0",
    workload_image_hash: str = "somehash",
) -> dict[str, str]:
    return {
        "airflow_version": airflow_version,
        "workload_image_hash": workload_image_hash,
        "component": component,
    }


@pytest.fixture(scope="function")
def api_server_data():
    return core_component_metadata("api-server")


@pytest.fixture(scope="function")
def scheduler_data():
    return core_component_metadata("scheduler")


@pytest.fixture(scope="function")
def triggerer_data():
    return core_component_metadata("triggerer")


@pytest.fixture(scope="function")
def dag_processor_data():
    return core_component_metadata("dag-processor")


@pytest.fixture(scope="function")
def api_server_relation(api_server_data):
    return ops.testing.Relation("airflow-coordinator", remote_app_data=api_server_data)


@pytest.fixture(scope="function")
def scheduler_relation(scheduler_data):
    return ops.testing.Relation("airflow-coordinator", remote_app_data=scheduler_data)


@pytest.fixture(scope="function")
def triggerer_relation(triggerer_data):
    return ops.testing.Relation("airflow-coordinator", remote_app_data=triggerer_data)


@pytest.fixture(scope="function")
def dag_processor_relation(dag_processor_data):
    return ops.testing.Relation("airflow-coordinator", remote_app_data=dag_processor_data)


@pytest.fixture(scope="function")
def peer_relation():
    return ops.testing.PeerRelation(constants.PEER_RELATION_NAME)


@pytest.fixture(scope="function")
def postgres_relation():
    relation = ops.testing.Relation("postgres", remote_app_data=POSTGRES_DATA)

    def fetch_relation_field_side_effect(_, field):
        return POSTGRES_DATA[field]

    with (
        unittest.mock.patch(
            "charms.data_platform_libs.v0.data_interfaces.DatabaseRequires.fetch_relation_field",
            side_effect=fetch_relation_field_side_effect,
        ),
    ):
        yield relation


@pytest.fixture(scope="function")
def airflow_api_server_requires_relation():
    return ops.testing.Relation(
        "airflow-api-server",
        remote_app_data={
            "host": "test-host",
            "port": "test-port",
        },
    )


MOCK_KUBERNETES_EXECUTOR_CONFIG = {
    "core": {
        "executor": "KubernetesExecutor",
    },
    "kubernetes_executor": {
        "namespace": "airflow-ns",
        "pod_template_file": "/opt/airflow/pod_templates/worker_pod_template.yaml",
        "base_image": "airflow:latest",
    },
}

MOCK_KUBERNETES_EXECUTOR_POD_SPEC = "apiVersion: v1\nkind: Pod\nmetadata:\n  name: worker"


@pytest.fixture(scope="function")
def kubernetes_executor_config_relation_empty():
    return ops.testing.Relation(
        constants.AIRFLOW_KUBERNETES_EXECUTOR_CONFIG_RELATION_NAME,
        remote_app_data={},
    )


@pytest.fixture(scope="function")
def s3_integrator_relation_empty():
    return ops.testing.Relation(constants.S3_ENDPOINT_NAME, remote_app_data={})


S3_INTEGRATOR_DATA = {
    "bucket": "test-bucket",
    "access-key": "test-access-key",
    "secret-key": "test-secret-key",
    "path": "test-path",
    "endpoint": "test-endpoint",
    "region": "test-region",
    "tls-ca-chain": '["test-ca-chain1","test-ca-chain2"]',
}


@pytest.fixture(scope="function")
def s3_integrator_relation():
    return ops.testing.Relation(
        constants.S3_ENDPOINT_NAME,
        remote_app_data=S3_INTEGRATOR_DATA,
    )


@pytest.fixture(scope="function")
def s3_integrator_relation2():
    return ops.testing.Relation(
        constants.S3_ENDPOINT_NAME,
        remote_app_data={
            **S3_INTEGRATOR_DATA,
            "bucket": "test-bucket2",
        },
    )


@pytest.fixture(scope="function")
def git_unauthenticated_relation():
    return ops.testing.Relation(
        constants.GIT_ENDPOINT_NAME,
        remote_app_data={
            "repository-url": "test-repo-url1",
            "path": "test/path/1/",
            "tracking-ref": "test-tracking-ref1",
        },
    )


@pytest.fixture(scope="function")
def git_credentials_secret():
    return ops.testing.Secret(
        {
            "credentials-personal-access-token": "test-token",
        },
    )


@pytest.fixture(scope="function")
def git_credentials_relation(git_credentials_secret):
    return ops.testing.Relation(
        constants.GIT_ENDPOINT_NAME,
        remote_app_data={
            "repository-url": "test-repo-url2",
            "path": "test/path/2/",
            "tracking-ref": "test-tracking-ref2",
            "authentication-method": git.AuthenticationMethodEnum.CREDENTIALS,
            "credentials-username": "user",
            "secret-credentials-personal-access-token": git_credentials_secret.id,
        },
    )


@pytest.fixture(scope="function")
def git_ssh_secret():
    return ops.testing.Secret(
        {
            "ssh-private-key": "test-key",
        },
    )


@pytest.fixture(scope="function")
def git_ssh_relation(git_ssh_secret):
    return ops.testing.Relation(
        constants.GIT_ENDPOINT_NAME,
        remote_app_data={
            "repository-url": "test-repo-url3",
            "path": "test/path/3/",
            "tracking-ref": "test-tracking-ref3",
            "authentication-method": git.AuthenticationMethodEnum.SSH,
            "secret-ssh-private-key": git_ssh_secret.id,
            "ssh-strict-host-key-checking": "true",
        },
    )


@pytest.fixture(scope="function")
def git_integrator_relation_empty():
    return ops.testing.Relation(constants.GIT_ENDPOINT_NAME, remote_app_data={})


@pytest.fixture(scope="function")
def all_required_relations(
    postgres_relation,
    api_server_relation,
    scheduler_relation,
    triggerer_relation,
    dag_processor_relation,
    peer_relation,
    airflow_api_server_requires_relation,
    s3_integrator_relation,
    s3_integrator_relation2,
    git_unauthenticated_relation,
    git_credentials_relation,
    git_ssh_relation,
):
    return [
        postgres_relation,
        api_server_relation,
        scheduler_relation,
        triggerer_relation,
        dag_processor_relation,
        peer_relation,
        airflow_api_server_requires_relation,
        s3_integrator_relation,
        s3_integrator_relation2,
        git_unauthenticated_relation,
        git_credentials_relation,
        git_ssh_relation,
    ]


@pytest.fixture(scope="function")
def state_without_git(
    all_required_relations, workload_container, mock_command_executor, fernet_key_secret
):
    relations_without_git = [
        relation
        for relation in all_required_relations
        if relation.endpoint != constants.GIT_ENDPOINT_NAME
    ]
    return ops.testing.State(
        leader=True,
        relations=relations_without_git,
        secrets=[fernet_key_secret],
        containers=[workload_container],
        config={
            constants.FERNET_KEY_SECRET_CONFIG: fernet_key_secret.id,
        },
    )


@pytest.fixture(scope="function")
def state_without_s3(
    all_required_relations,
    workload_container,
    git_credentials_secret,
    git_ssh_secret,
    mock_command_executor,
    fernet_key_secret,
):
    relations_without_s3 = [
        relation
        for relation in all_required_relations
        if relation.endpoint != constants.S3_ENDPOINT_NAME
    ]
    return ops.testing.State(
        leader=True,
        relations=relations_without_s3,
        containers=[workload_container],
        secrets=[git_credentials_secret, git_ssh_secret, fernet_key_secret],
        config={
            constants.FERNET_KEY_SECRET_CONFIG: fernet_key_secret.id,
        },
    )


@pytest.fixture
def mock_run_db_migrate():
    """Mock the charm's _run_db_migrate method."""
    with unittest.mock.patch.object(
        AirflowCoordinatorK8SOperatorCharm,
        "_run_db_migrate",
    ) as mock:
        yield mock


@pytest.fixture
def mock_container_pull():
    """Mock the pebble.Container.pull method."""
    with unittest.mock.patch(
        "ops.Container.pull",
    ) as mock_pull:
        yield mock_pull


@pytest.fixture
def mock_container_push():
    """Mock the pebble.Container.push method."""
    with unittest.mock.patch(
        "ops.Container.push",
    ) as mock_push:
        yield mock_push


@pytest.fixture(scope="function")
def mock_command_executor():
    """Mock the command executor to avoid actual container operations."""
    with (
        unittest.mock.patch.object(
            command_executor.CommandExecutor, "run_db_migrate"
        ) as mock_run_db_migrate,
        unittest.mock.patch.object(
            command_executor.CommandExecutor, "list_airflow_connections"
        ) as mock_list_airflow_connections,
        unittest.mock.patch.object(
            command_executor.CommandExecutor,
            "add_airflow_s3_connection",
        ) as mock_add_airflow_s3_connection,
        unittest.mock.patch.object(
            command_executor.CommandExecutor,
            "delete_airflow_connection",
        ) as mock_delete_airflow_connection,
        unittest.mock.patch.object(
            command_executor.CommandExecutor,
            "add_airflow_git_connection",
        ) as mock_add_airflow_git_connection,
    ):
        mock_run_db_migrate.return_value = command_executor.CommandExecutionResult(
            success=True, stdout="", parsed_stdout=None, stderr="", return_code=0
        )
        mock_list_airflow_connections.return_value = command_executor.CommandExecutionResult(
            success=True, stdout="[]", parsed_stdout=[], stderr="", return_code=0
        )
        mock_add_airflow_s3_connection.return_value = command_executor.CommandExecutionResult(
            success=True, stdout="", parsed_stdout=None, stderr="", return_code=0
        )
        mock_delete_airflow_connection.return_value = command_executor.CommandExecutionResult(
            success=True, stdout="[]", parsed_stdout=[], stderr="", return_code=0
        )
        mock_add_airflow_git_connection.return_value = command_executor.CommandExecutionResult(
            success=True, stdout="", parsed_stdout=None, stderr="", return_code=0
        )

        yield {
            "run_db_migrate": mock_run_db_migrate,
            "list_airflow_connections": mock_list_airflow_connections,
            "add_airflow_s3_connection": mock_add_airflow_s3_connection,
            "delete_airflow_connection": mock_delete_airflow_connection,
            "add_airflow_git_connection": mock_add_airflow_git_connection,
        }


@pytest.fixture(scope="function")
def fernet_key():
    return cryptography.fernet.Fernet.generate_key().decode()


@pytest.fixture(scope="function")
def fernet_key_secret(fernet_key):
    return ops.testing.Secret(
        {
            constants.FERNET_KEY: fernet_key,
        },
    )


@pytest.fixture(scope="function")
def state(
    all_required_relations,
    workload_container,
    mock_command_executor,
    fernet_key_secret,
    git_credentials_secret,
    git_ssh_secret,
):
    return ops.testing.State(
        leader=True,
        relations=all_required_relations,
        containers=[workload_container],
        secrets=[fernet_key_secret, git_credentials_secret, git_ssh_secret],
        config={
            constants.FERNET_KEY_SECRET_CONFIG: fernet_key_secret.id,
        },
    )
