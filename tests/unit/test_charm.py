# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.
#
# To learn more about testing, see https://documentation.ubuntu.com/ops/latest/explanation/testing/

import configparser
import copy
import dataclasses
import io
import json
import unittest.mock

import charms.airflow_coordinator_k8s.v0.airflow_coordinator as airflow_coordinator
import charms.git_integrator.v0.git as git
import ops
import ops.testing
import pytest
from conftest import (
    MOCK_KUBERNETES_EXECUTOR_CONFIG,
    MOCK_KUBERNETES_EXECUTOR_POD_SPEC,
    S3_INTEGRATOR_DATA,
)

import command_executor
import constants
from charm import AirflowCoordinatorK8SOperatorCharm
from connection_manager import S3ConnectionInfo

MOCK_CONFIG_TEMPLATE_WITH_RUNTIME_SECRETS = """[core]
executor = {{ executor | default('LocalExecutor') }}
fernet_key = {{ core__fernet_key }}

[api]
secret_key = {{ api__secret_key }}

[api_auth]
jwt_secret = {{ api_auth__jwt_secret }}
"""


def test_non_leader_unit(context, state, mock_command_executor):
    state = dataclasses.replace(state, leader=False)

    unit_status_before = state.unit_status

    state_out = context.run(context.on.start(), state)

    assert state_out.unit_status == unit_status_before


def test_container_not_ready(
    context, state, mock_command_executor, workload_container, all_required_relations
):
    container_not_ready = dataclasses.replace(workload_container, can_connect=False)
    state = dataclasses.replace(
        state, relations=all_required_relations, containers=[container_not_ready]
    )

    state_out = context.run(context.on.start(), state)

    assert state_out.unit_status == ops.WaitingStatus(constants.WAITING_FOR_CONTAINER_MESSAGE)


def test_missing_fernet_key_secret_config(context, state):
    """Test with missing fernet_key_secret config."""
    config = state.config.copy()
    config.pop(constants.FERNET_KEY_SECRET_CONFIG)

    state = dataclasses.replace(state, config=config)

    state_out = context.run(context.on.config_changed(), state)

    assert state_out.unit_status == ops.BlockedStatus(
        constants.MISSING_FERNET_KEY_SECRET_CONFIG_MESSAGE
    )

    for relation in state_out.get_relations(constants.AIRFLOW_COORDINATOR_RELATION_NAME):
        assert relation.local_app_data == {}


@pytest.mark.parametrize("exception", [ops.SecretNotFoundError, ops.ModelError])
def test_invalid_fernet_key_secret(context, state, exception):
    """Test error while accessing the fernet key secret."""
    with unittest.mock.patch("ops.Model.get_secret", side_effect=exception):
        state_out = context.run(context.on.config_changed(), state)

        assert state_out.unit_status == ops.BlockedStatus(
            constants.INVALID_FERNET_KEY_SECRET_MESSAGE
        )

        for relation in state_out.get_relations(constants.AIRFLOW_COORDINATOR_RELATION_NAME):
            assert relation.local_app_data == {}


def test_missing_fernet_key_in_secret(context, state):
    """Test for missing fernet key in provided secret."""
    empty_secret = ops.testing.Secret(
        {
            "key-except-fernet-key": "random-value",
        }
    )
    config = state.config.copy()
    config[constants.FERNET_KEY_SECRET_CONFIG] = empty_secret.id

    state = dataclasses.replace(state, secrets=[empty_secret], config=config)

    state_out = context.run(context.on.config_changed(), state)

    assert state_out.unit_status == ops.BlockedStatus(
        constants.MISSING_FERNET_KEY_IN_SECRET_MESSAGE
    )

    for relation in state_out.get_relations(constants.AIRFLOW_COORDINATOR_RELATION_NAME):
        assert relation.local_app_data == {}


def test_missing_postgres_relation(
    context, state, all_required_relations, postgres_relation, mock_command_executor
):
    all_required_relations.remove(postgres_relation)
    state = dataclasses.replace(state, relations=all_required_relations)

    state_out = context.run(context.on.start(), state)

    assert state_out.unit_status == ops.BlockedStatus(
        constants.MISSING_POSTGRES_INTEGRATION_MESSAGE
    )

    failures = json.dumps(
        [
            {
                "component": "coordinator",
                "code": airflow_coordinator.AirflowCoreValidationErrorEnum.WAITING_FOR_DEPENDENCIES,  # noqa: E501
            }
        ]
    )

    for relation in state_out.get_relations("airflow-coordinator"):
        assert relation.local_app_data == {
            "validation-failures": failures,
        }


def test_missing_postgres_relation_data(
    context, state, all_required_relations, postgres_relation, mock_command_executor
):
    empty_postgres_relation = dataclasses.replace(postgres_relation, remote_app_data={})

    all_required_relations.remove(postgres_relation)
    all_required_relations.append(empty_postgres_relation)
    state = dataclasses.replace(state, relations=all_required_relations)

    state_out = context.run(context.on.start(), state)

    assert state_out.unit_status == ops.WaitingStatus(
        constants.WAITING_FOR_DATABASE_TO_BE_CREATED_MESSAGE
    )

    for relation in state_out.get_relations("airflow-coordinator"):
        assert relation.local_app_data == {}


def test_missing_airflow_api_server_requires_relation(
    context,
    state,
    all_required_relations,
    airflow_api_server_requires_relation,
    mock_command_executor,
):
    all_required_relations.remove(airflow_api_server_requires_relation)
    state = dataclasses.replace(state, relations=all_required_relations)

    state_out = context.run(context.on.start(), state)

    assert state_out.unit_status == ops.BlockedStatus(
        constants.WAITING_FOR_API_SERVER_RELATION_MESSAGE
    )

    failures = json.dumps(
        [
            {
                "component": "coordinator",
                "code": airflow_coordinator.AirflowCoreValidationErrorEnum.WAITING_FOR_DEPENDENCIES,  # noqa: E501
            }
        ]
    )

    for relation in state_out.get_relations("airflow-coordinator"):
        assert relation.local_app_data == {
            "validation-failures": failures,
        }


def test_missing_airflow_api_server_requires_relation_data(
    context,
    state,
    all_required_relations,
    airflow_api_server_requires_relation,
    mock_command_executor,
):
    missing_data_relation = dataclasses.replace(
        airflow_api_server_requires_relation, remote_app_data={}
    )

    all_required_relations.remove(airflow_api_server_requires_relation)
    all_required_relations.append(missing_data_relation)

    state = dataclasses.replace(state, relations=all_required_relations)

    state_out = context.run(context.on.start(), state)

    assert state_out.unit_status == ops.WaitingStatus(
        constants.WAITING_FOR_API_SERVER_HOST_PORT_MESSAGE
    )

    for relation in state_out.get_relations("airflow-coordinator"):
        assert relation.local_app_data == {}


def test_missing_core_charm_relations(
    context,
    state,
    all_required_relations,
    scheduler_relation,
    triggerer_relation,
    mock_command_executor,
):
    all_required_relations.remove(scheduler_relation)
    all_required_relations.remove(triggerer_relation)
    state = dataclasses.replace(state, relations=all_required_relations)

    state_out = context.run(context.on.start(), state)

    assert state_out.unit_status == ops.BlockedStatus(
        constants.MISSING_INTEGRATIONS_MESSAGE_TEMPLATE.format(
            missing_core_components="scheduler, triggerer"
        )
    )

    failures = json.dumps(
        [
            {
                "component": "scheduler",
                "code": "missing_component",
            },
            {
                "component": "triggerer",
                "code": "missing_component",
            },
        ]
    )

    for relation in state_out.get_relations("airflow-coordinator"):
        assert relation.local_app_data == {
            "validation-failures": failures,
        }


def test_invalid_core_charm_airflow_version(
    context,
    state,
    all_required_relations,
    scheduler_data,
    scheduler_relation,
    mock_command_executor,
):
    scheduler_data["airflow_version"] = "0.0.0"
    modified_scheduler_relation = dataclasses.replace(
        scheduler_relation, remote_app_data=scheduler_data
    )

    scheduler_relation_index = [
        index
        for index, relation in enumerate(all_required_relations)
        if hasattr(relation, "remote_app_data")
        and relation.remote_app_data.get("component") == "scheduler"
    ][0]
    del all_required_relations[scheduler_relation_index]
    all_required_relations.append(modified_scheduler_relation)

    state = dataclasses.replace(state, relations=all_required_relations)

    state_out = context.run(context.on.start(), state)

    assert state_out.unit_status == ops.BlockedStatus(
        constants.MISMATCHED_AIRFLOW_VERSIONS_MESSAGE
    )

    failures = json.dumps(
        [
            {
                "component": "scheduler",
                "code": airflow_coordinator.AirflowCoreValidationErrorEnum.INCONSISTENT_AIRFLOW_VERSION,  # noqa: E501
            },
        ]
    )

    for relation in state_out.get_relations("airflow-coordinator"):
        assert relation.local_app_data == {
            "validation-failures": failures,
        }


def test_invalid_core_charm_workload_image_hash(
    context,
    state,
    all_required_relations,
    scheduler_data,
    scheduler_relation,
    mock_command_executor,
):
    scheduler_data["workload_image_hash"] = "invalidhash"
    modified_scheduler_relation = dataclasses.replace(
        scheduler_relation, remote_app_data=scheduler_data
    )

    scheduler_relation_index = [
        index
        for index, relation in enumerate(all_required_relations)
        if hasattr(relation, "remote_app_data")
        and relation.remote_app_data.get("component") == "scheduler"
    ][0]
    del all_required_relations[scheduler_relation_index]
    all_required_relations.append(modified_scheduler_relation)

    state = dataclasses.replace(state, relations=all_required_relations)

    state_out = context.run(context.on.start(), state)

    assert state_out.unit_status == ops.BlockedStatus(
        constants.MISMATCHED_WORKLOAD_IMAGE_HASHES_MESSAGE
    )

    failures = json.dumps(
        [
            {
                "component": "scheduler",
                "code": airflow_coordinator.AirflowCoreValidationErrorEnum.INCONSISTENT_WORKLOAD_IMAGE_HASH,  # noqa: E501
            },
        ]
    )

    for relation in state_out.get_relations("airflow-coordinator"):
        assert relation.local_app_data == {"validation-failures": failures}


def test_db_migration_does_not_run_on_state_true(
    context,
    state,
    all_required_relations,
    mock_command_executor,
    workload_container,
    peer_relation,
):
    """Verify that database migration does not happen when peer relation state is True."""
    # Update peer relation with migration already ran
    peer_relation_with_state = dataclasses.replace(
        peer_relation, local_app_data={"db_migration_ran": "true"}
    )
    relations = [r for r in all_required_relations if r.endpoint != constants.PEER_RELATION_NAME]
    relations.append(peer_relation_with_state)

    state = dataclasses.replace(state, relations=relations)

    with unittest.mock.patch(
        "config_generator.AirflowConfigGenerator.config_template",
        new_callable=unittest.mock.PropertyMock(
            return_value="[core]\nexecutor = {{ executor | default('LocalExecutor') }}\n"
        ),
    ):
        state_in = dataclasses.replace(state, relations=relations)

        context.run(
            context.on.pebble_ready(workload_container),
            state_in,
        )

    mock_command_executor["run_db_migrate"].assert_not_called()


def test_db_migration_runs_on_state_false(
    context,
    state,
    mock_command_executor,
    workload_container,
):
    """Verify that database migration happens when peer relation state is False."""
    # Peer relation with no migration state (defaults to False)
    with unittest.mock.patch(
        "config_generator.AirflowConfigGenerator.config_template",
        new_callable=unittest.mock.PropertyMock(
            return_value="[core]\nexecutor = {{ executor | default('LocalExecutor') }}\n"
        ),
    ):
        context.run(
            context.on.pebble_ready(workload_container),
            state,
        )

    mock_command_executor["run_db_migrate"].assert_called_once()


def test_db_migration_failure(context, state, mock_command_executor, workload_container):
    mock_command_executor["run_db_migrate"].return_value = command_executor.CommandExecutionResult(
        success=False, stdout="", parsed_stdout=None, stderr="Migration failed", return_code=1
    )

    with unittest.mock.patch(
        "config_generator.AirflowConfigGenerator.config_template",
        new_callable=unittest.mock.PropertyMock(
            return_value="[core]\nexecutor = {{ executor | default('LocalExecutor') }}\n"
        ),
    ):
        state_out = context.run(context.on.pebble_ready(workload_container), state)

    assert state_out.unit_status == ops.BlockedStatus(constants.DB_MIGRATION_FAILED_MESSAGE)

    # Verify that config was not distributed to core charms
    for relation in state_out.get_relations("airflow-coordinator"):
        assert "config-template" not in relation.local_app_data


class TestDagBundles:
    def test_issue_querying_airflow_connections(self, context, state, mock_command_executor):
        """Ensure proper handling if there is an issue querying Airflow connections."""
        mock_command_executor[
            "list_airflow_connections"
        ].return_value = command_executor.CommandExecutionResult(
            success=True,
            stdout="",
            parsed_stdout=None,
            stderr="Some sqlalchemy error",
            return_code=0,
        )

        state_out = context.run(context.on.start(), state)

        assert state_out.unit_status == ops.BlockedStatus(
            constants.ISSUE_RECONCILING_AIRFLOW_CONNECTIONS_MESSAGE
        )

    def test_invalid_data_from_s3_integrators(
        self,
        context,
        state_without_git,
        s3_integrator_relation,
    ):
        """Charm goes into BlockedStatus if related s3-integrator data invalid."""
        invalid_s3_relation_data = copy.deepcopy(S3_INTEGRATOR_DATA)
        invalid_s3_relation_data.pop("bucket")

        invalid_s3_relation = dataclasses.replace(
            s3_integrator_relation,
            remote_app_data=invalid_s3_relation_data,
        )

        relations_with_invalid_s3 = [
            relation
            for relation in state_without_git.relations
            if relation.endpoint != constants.S3_ENDPOINT_NAME
        ]
        relations_with_invalid_s3.append(invalid_s3_relation)

        state_without_git = dataclasses.replace(
            state_without_git, relations=relations_with_invalid_s3
        )

        state_out = context.run(context.on.start(), state_without_git)

        assert state_out.unit_status == ops.BlockedStatus(
            constants.INVALID_S3_RELATIONS_MESSAGE_TEMPLATE.format(
                relation_ids=str(s3_integrator_relation.id)
            )
        )

    def test_empty_s3_relation_succeeds(
        self, context, state_without_git, s3_integrator_relation_empty
    ):
        """An empty s3 relation is skipped from DAG bundles until data is ready."""
        relations_with_empty_s3 = [
            relation
            for relation in state_without_git.relations
            if relation.endpoint != constants.S3_ENDPOINT_NAME
        ]
        relations_with_empty_s3.append(s3_integrator_relation_empty)

        state_without_git = dataclasses.replace(
            state_without_git, relations=relations_with_empty_s3
        )

        state_out = context.run(context.on.start(), state_without_git)

        assert state_out.unit_status == ops.ActiveStatus()

        for relation in state_out.get_relations("airflow-coordinator"):
            config_template = relation.local_app_data.get("config-template", "")
            parsed = configparser.ConfigParser()
            parsed.read_string(config_template)

            try:
                parsed.get("dag_processor", "dag_bundle_config_list")

                assert False
            except (configparser.NoSectionError, configparser.NoOptionError):
                pass

    def test_valid_s3_relations_succeed(
        self,
        context,
        state_without_git,
        s3_integrator_relation,
        s3_integrator_relation2,
    ):
        """Valid DAG bundles configured when multiple related s3-integrators."""
        state_out = context.run(context.on.start(), state_without_git)

        assert state_out.unit_status == ops.ActiveStatus()

        for relation in state_out.get_relations("airflow-coordinator"):
            config_template = relation.local_app_data.get("config-template", "")
            parsed = configparser.ConfigParser()
            parsed.read_string(config_template)

            assert json.loads(parsed.get("dag_processor", "dag_bundle_config_list")) == [
                {
                    "name": f"s3_{relation_id}_dag_bundle",
                    "classpath": "airflow.providers.amazon.aws.bundles.s3.S3DagBundle",
                    "kwargs": {
                        "aws_conn_id": f"s3_relation_{relation_id}_connection",
                        "bucket_name": connection_info["bucket"],
                        "prefix": connection_info["path"],
                    },
                }
                for relation_id, connection_info in {
                    s3_integrator_relation.id: s3_integrator_relation.remote_app_data,
                    s3_integrator_relation2.id: s3_integrator_relation2.remote_app_data,
                }.items()
            ]

    def test_valid_s3_relations_non_leader(self, context, state_without_git):
        """Non-leader coordinator units no-op."""
        state_without_git = dataclasses.replace(state_without_git, leader=False)

        state_out = context.run(context.on.start(), state_without_git)

        assert state_out.unit_status == state_without_git.unit_status

        for relation in state_out.get_relations("airflow-coordinator"):
            config_template = relation.local_app_data.get("config-template", "")
            parsed = configparser.ConfigParser()
            parsed.read_string(config_template)

            try:
                parsed.get("dag_processor", "dag_bundle_config_list")

                assert False
            except (configparser.NoSectionError, configparser.NoOptionError):
                pass

    def test_no_s3_or_git_relations(self, context, state, all_required_relations):
        """Lack of S3 integrators valid (S3 integrator relations are optional)."""
        relations_without_s3_or_git = [
            relation
            for relation in all_required_relations
            if relation.endpoint != constants.S3_ENDPOINT_NAME
            and relation.endpoint != constants.GIT_ENDPOINT_NAME
        ]
        state = dataclasses.replace(state, relations=relations_without_s3_or_git)

        state_out = context.run(context.on.start(), state)

        assert state_out.unit_status == ops.ActiveStatus()

        for relation in state_out.get_relations("airflow-coordinator"):
            config_template = relation.local_app_data.get("config-template", "")
            parsed = configparser.ConfigParser()
            parsed.read_string(config_template)

            try:
                parsed.get("dag_processor", "dag_bundle_config_list")

                assert False
            except (configparser.NoSectionError, configparser.NoOptionError):
                pass

    def test_change_in_s3_connection(
        self,
        context,
        state_without_git,
        s3_integrator_relation,
        s3_integrator_relation2,
        mock_command_executor,
        mock_container_pull,
        workload_container,
    ):
        """Test change in one of the S3 relation's connection information."""
        airflow_connections = [
            {
                "conn_id": f"s3_relation_{relation_id}_connection",
                "conn_type": "aws",
                "login": "test-access-key",
                "password": "test-secret-key",
                "extra_dejson": {
                    "region_name": "test-region",
                    "endpoint_url": "test-endpoint",
                },
            }
            for relation_id in [s3_integrator_relation.id, s3_integrator_relation2.id]
        ]

        mock_container_pull.side_effect = [
            io.StringIO("\n".join(json.loads(S3_INTEGRATOR_DATA["tls-ca-chain"]))),
            io.StringIO("\n".join(json.loads(S3_INTEGRATOR_DATA["tls-ca-chain"]))),
        ]

        mock_command_executor["list_airflow_connections"].side_effect = [
            command_executor.CommandExecutionResult(
                success=True, stdout="[]", parsed_stdout=[], stderr="", return_code=0
            ),
            command_executor.CommandExecutionResult(
                success=True, stdout="[]", parsed_stdout=[], stderr="", return_code=0
            ),
            command_executor.CommandExecutionResult(
                success=True, stdout="[]", parsed_stdout=[], stderr="", return_code=0
            ),
            command_executor.CommandExecutionResult(
                success=True,
                stdout=json.dumps(airflow_connections),
                parsed_stdout=airflow_connections,
                stderr="",
                return_code=0,
            ),
            command_executor.CommandExecutionResult(
                success=True,
                stdout=json.dumps(airflow_connections),
                parsed_stdout=airflow_connections,
                stderr="",
                return_code=0,
            ),
            command_executor.CommandExecutionResult(
                success=True,
                stdout=json.dumps(airflow_connections),
                parsed_stdout=airflow_connections,
                stderr="",
                return_code=0,
            ),
        ]

        state_out = context.run(context.on.start(), state_without_git)

        assert state_out.unit_status == ops.ActiveStatus()

        for relation in state_out.get_relations("airflow-coordinator"):
            config_template = relation.local_app_data.get("config-template", "")
            parsed = configparser.ConfigParser()
            parsed.read_string(config_template)

            assert json.loads(parsed.get("dag_processor", "dag_bundle_config_list")) == [
                {
                    "name": f"s3_{relation_id}_dag_bundle",
                    "classpath": "airflow.providers.amazon.aws.bundles.s3.S3DagBundle",
                    "kwargs": {
                        "aws_conn_id": f"s3_relation_{relation_id}_connection",
                        "bucket_name": connection_info["bucket"],
                        "prefix": connection_info["path"],
                    },
                }
                for relation_id, connection_info in {
                    s3_integrator_relation.id: s3_integrator_relation.remote_app_data,
                    s3_integrator_relation2.id: s3_integrator_relation2.remote_app_data,
                }.items()
            ]

        assert mock_command_executor["add_airflow_s3_connection"].call_count == 2
        mock_command_executor["add_airflow_s3_connection"].reset_mock()

        modified_s3_relation2 = ops.testing.Relation(
            constants.S3_ENDPOINT_NAME,
            id=s3_integrator_relation2.id,
            remote_app_data={
                **S3_INTEGRATOR_DATA,
                "access-key": "modified-access-key",
            },
        )

        relations_with_modified_s3 = [
            relation
            for relation in state_without_git.relations
            if relation.endpoint != constants.S3_ENDPOINT_NAME
        ]
        relations_with_modified_s3.extend([s3_integrator_relation, modified_s3_relation2])

        state_modified = dataclasses.replace(
            state_without_git,
            relations=relations_with_modified_s3,
            containers=[workload_container],
        )

        # note: we're using update_status to trigger reconciler
        # (due to an issue with object_storage charm lib emitting storage_connection_info_changed)
        state_out = context.run(context.on.update_status(), state_modified)

        assert state_out.unit_status == ops.ActiveStatus()

        for relation in state_out.get_relations("airflow-coordinator"):
            config_template = relation.local_app_data.get("config-template", "")
            parsed = configparser.RawConfigParser()
            parsed.read_string(config_template)

            assert json.loads(parsed.get("dag_processor", "dag_bundle_config_list")) == [
                {
                    "name": f"s3_{relation_id}_dag_bundle",
                    "classpath": "airflow.providers.amazon.aws.bundles.s3.S3DagBundle",
                    "kwargs": {
                        "aws_conn_id": f"s3_relation_{relation_id}_connection",
                        "bucket_name": connection_info["bucket"],
                        "prefix": connection_info["path"],
                    },
                }
                for relation_id, connection_info in {
                    s3_integrator_relation.id: s3_integrator_relation.remote_app_data,
                    modified_s3_relation2.id: modified_s3_relation2.remote_app_data,
                }.items()
            ]

        mock_command_executor["add_airflow_s3_connection"].mock_calls == [
            unittest.mock.call(
                f"s3_relation_{modified_s3_relation2.id}_connection",
                S3ConnectionInfo.from_s3_info(modified_s3_relation2.remote_app_data),
            ),
        ]

    def test_removed_s3_relation_results_in_airflow_connection_deletion(
        self,
        context,
        state_without_git,
        s3_integrator_relation,
        s3_integrator_relation2,
        mock_command_executor,
        mock_container_pull,
        workload_container,
    ):
        """Ensure stale s3 airflow connections removed if corresponding relation removed."""
        airflow_connections = [
            {
                "conn_id": f"s3_relation_{s3_integrator_relation.id}_connection",
                "conn_type": "aws",
                "login": "test-access-key",
                "password": "test-secret-key",
                "extra_dejson": {
                    "region_name": "test-region",
                    "endpoint_url": "test-endpoint",
                },
            },
        ]

        mock_container_pull.side_effect = [
            io.StringIO("\n".join(json.loads(S3_INTEGRATOR_DATA["tls-ca-chain"]))),
            # io.StringIO("\n".join(json.loads(S3_INTEGRATOR_DATA["tls-ca-chain"]))),
        ]

        mock_command_executor["list_airflow_connections"].side_effect = [
            command_executor.CommandExecutionResult(
                success=True, stdout="[]", parsed_stdout=[], stderr="", return_code=0
            ),
            command_executor.CommandExecutionResult(
                success=True, stdout="[]", parsed_stdout=[], stderr="", return_code=0
            ),
            command_executor.CommandExecutionResult(
                success=True, stdout="[]", parsed_stdout=[], stderr="", return_code=0
            ),
            command_executor.CommandExecutionResult(
                success=True,
                stdout=json.dumps(airflow_connections),
                parsed_stdout=airflow_connections,
                stderr="",
                return_code=0,
            ),
            command_executor.CommandExecutionResult(
                success=True,
                stdout=json.dumps(airflow_connections),
                parsed_stdout=airflow_connections,
                stderr="",
                return_code=0,
            ),
            command_executor.CommandExecutionResult(
                success=True,
                stdout=json.dumps(airflow_connections),
                parsed_stdout=airflow_connections,
                stderr="",
                return_code=0,
            ),
        ]

        state_out = context.run(context.on.start(), state_without_git)

        assert state_out.unit_status == ops.ActiveStatus()

        for relation in state_out.get_relations("airflow-coordinator"):
            config_template = relation.local_app_data.get("config-template", "")
            parsed = configparser.ConfigParser()
            parsed.read_string(config_template)

            assert json.loads(parsed.get("dag_processor", "dag_bundle_config_list")) == [
                {
                    "name": f"s3_{relation_id}_dag_bundle",
                    "classpath": "airflow.providers.amazon.aws.bundles.s3.S3DagBundle",
                    "kwargs": {
                        "aws_conn_id": f"s3_relation_{relation_id}_connection",
                        "bucket_name": connection_info["bucket"],
                        "prefix": connection_info["path"],
                    },
                }
                for relation_id, connection_info in {
                    s3_integrator_relation.id: s3_integrator_relation.remote_app_data,
                    s3_integrator_relation2.id: s3_integrator_relation2.remote_app_data,
                }.items()
            ]

        assert mock_command_executor["add_airflow_s3_connection"].call_count == 2
        mock_command_executor["add_airflow_s3_connection"].reset_mock()

        relations_with_removed_s3_relation = [
            relation
            for relation in state_without_git.relations
            if relation.endpoint != constants.S3_ENDPOINT_NAME
        ]
        relations_with_removed_s3_relation.append(s3_integrator_relation)

        state_modified = dataclasses.replace(
            state_without_git,
            relations=relations_with_removed_s3_relation,
            containers=[workload_container],
        )

        # note: we're using update_status to trigger reconciler
        # (due to an issue with object_storage charm lib emitting storage_connection_info_changed)
        state_out = context.run(context.on.update_status(), state_modified)

        assert state_out.unit_status == ops.ActiveStatus()

        for relation in state_out.get_relations("airflow-coordinator"):
            config_template = relation.local_app_data.get("config-template", "")
            parsed = configparser.ConfigParser()
            parsed.read_string(config_template)

            assert json.loads(parsed.get("dag_processor", "dag_bundle_config_list")) == [
                {
                    "name": f"s3_{s3_integrator_relation.id}_dag_bundle",
                    "classpath": "airflow.providers.amazon.aws.bundles.s3.S3DagBundle",
                    "kwargs": {
                        "aws_conn_id": f"s3_relation_{s3_integrator_relation.id}_connection",
                        "bucket_name": s3_integrator_relation.remote_app_data["bucket"],
                        "prefix": s3_integrator_relation.remote_app_data["path"],
                    },
                },
            ]

        mock_command_executor["add_airflow_s3_connection"].assert_not_called()
        mock_command_executor["delete_airflow_connection"].mock_calls == [
            unittest.mock.call(f"s3_relation_{s3_integrator_relation2.id}_connection"),
        ]

    def test_invalid_data_from_git_integrators(
        self,
        context,
        state,
        all_required_relations,
        git_unauthenticated_relation,
    ):
        """Charm goes into BlockedStatus if related git-integrator data invalid."""
        invalid_git_relation_data = copy.deepcopy(git_unauthenticated_relation.remote_app_data)
        invalid_git_relation_data.pop("repository-url")

        invalid_git_relation = dataclasses.replace(
            git_unauthenticated_relation,
            remote_app_data=invalid_git_relation_data,
        )

        relations_with_invalid_git = [
            relation
            for relation in all_required_relations
            if relation.endpoint != constants.GIT_ENDPOINT_NAME
        ]
        relations_with_invalid_git.append(invalid_git_relation)

        state = dataclasses.replace(state, relations=relations_with_invalid_git)

        state_out = context.run(context.on.start(), state)

        assert state_out.unit_status == ops.BlockedStatus(
            constants.INVALID_GIT_RELATIONS_MESSAGE_TEMPLATE.format(
                relation_ids=str(invalid_git_relation.id)
            )
        )

    def test_empty_git_relation_succeeds(
        self, context, state_without_s3, git_integrator_relation_empty
    ):
        """An empty git relation is skipped from DAG bundles until data is ready."""
        relations_with_empty_git = [
            relation
            for relation in state_without_s3.relations
            if relation.endpoint != constants.GIT_ENDPOINT_NAME
        ]
        relations_with_empty_git.append(git_integrator_relation_empty)

        state_without_s3 = dataclasses.replace(
            state_without_s3, relations=relations_with_empty_git
        )

        state_out = context.run(context.on.start(), state_without_s3)

        assert state_out.unit_status == ops.ActiveStatus()

        for relation in state_out.get_relations("airflow-coordinator"):
            config_template = relation.local_app_data.get("config-template", "")
            parsed = configparser.ConfigParser()
            parsed.read_string(config_template)

            try:
                parsed.get("dag_processor", "dag_bundle_config_list")

                assert False
            except (configparser.NoSectionError, configparser.NoOptionError):
                pass

    def test_valid_git_relations_succeed(
        self,
        context,
        state_without_s3,
        git_unauthenticated_relation,
        git_credentials_relation,
        git_ssh_relation,
    ):
        """Valid DAG bundles configured when multiple related git-integrators."""
        state_out = context.run(context.on.start(), state_without_s3)

        assert state_out.unit_status == ops.ActiveStatus()

        for relation in state_out.get_relations("airflow-coordinator"):
            config_template = relation.local_app_data.get("config-template", "")
            parsed = configparser.ConfigParser()
            parsed.read_string(config_template)

            assert json.loads(parsed.get("dag_processor", "dag_bundle_config_list")) == [
                {
                    "name": f"git_{relation_id}_dag_bundle",
                    "classpath": "airflow.providers.git.bundles.git.GitDagBundle",
                    "kwargs": {
                        key: value
                        for key, value in {
                            "git_conn_id": f"git_relation_{relation_id}_connection"
                            if connection_info.get("authentication-method")
                            else None,
                            "repo_url": connection_info["repository-url"],
                            "tracking_ref": connection_info.get("tracking-ref"),
                            "subdir": connection_info.get("path"),
                            "submodules": False,
                            "prune_dotgit_folder": True,
                        }.items()
                        if value is not None
                    },
                }
                for relation_id, connection_info in {
                    git_unauthenticated_relation.id: git_unauthenticated_relation.remote_app_data,
                    git_credentials_relation.id: git_credentials_relation.remote_app_data,
                    git_ssh_relation.id: git_ssh_relation.remote_app_data,
                }.items()
            ]

    def test_change_in_git_connection(
        self,
        context,
        state_without_s3,
        git_unauthenticated_relation,
        git_credentials_relation,
        git_ssh_relation,
        mock_command_executor,
        mock_container_pull,
        workload_container,
    ):
        """Test change in one of the git relation's connection information."""
        airflow_connections = [
            {
                "conn_id": f"git_relation_{git_credentials_relation.id}_connection",
                "conn_type": "git",
                "host": "test-repo-url2",
                "login": "user",
                "password": "test-token",
                "extra_dejson": {},
            },
            {
                "conn_id": f"git_relation_{git_ssh_relation.id}_connection",
                "conn_type": "git",
                "host": "test-repo-url3",
                "extra_dejson": {
                    "private_key": "test-key",
                    "strict_host_key_checking": "true",
                },
            },
        ]

        mock_command_executor["list_airflow_connections"].side_effect = [
            command_executor.CommandExecutionResult(
                success=True, stdout="[]", parsed_stdout=[], stderr="", return_code=0
            ),
            command_executor.CommandExecutionResult(
                success=True, stdout="[]", parsed_stdout=[], stderr="", return_code=0
            ),
            command_executor.CommandExecutionResult(
                success=True, stdout="[]", parsed_stdout=[], stderr="", return_code=0
            ),
            command_executor.CommandExecutionResult(
                success=True,
                stdout=json.dumps(airflow_connections),
                parsed_stdout=airflow_connections,
                stderr="",
                return_code=0,
            ),
            command_executor.CommandExecutionResult(
                success=True,
                stdout=json.dumps(airflow_connections),
                parsed_stdout=airflow_connections,
                stderr="",
                return_code=0,
            ),
            command_executor.CommandExecutionResult(
                success=True,
                stdout=json.dumps(airflow_connections),
                parsed_stdout=airflow_connections,
                stderr="",
                return_code=0,
            ),
        ]

        state_out = context.run(context.on.start(), state_without_s3)

        assert state_out.unit_status == ops.ActiveStatus()

        for relation in state_out.get_relations("airflow-coordinator"):
            config_template = relation.local_app_data.get("config-template", "")
            parsed = configparser.ConfigParser()
            parsed.read_string(config_template)

            assert json.loads(parsed.get("dag_processor", "dag_bundle_config_list")) == [
                {
                    "name": f"git_{relation_id}_dag_bundle",
                    "classpath": "airflow.providers.git.bundles.git.GitDagBundle",
                    "kwargs": {
                        key: value
                        for key, value in {
                            "git_conn_id": f"git_relation_{relation_id}_connection"
                            if connection_info.get("authentication-method")
                            else None,
                            "repo_url": connection_info["repository-url"],
                            "tracking_ref": connection_info.get("tracking-ref")
                            if connection_info.get("tracking-ref")
                            else None,
                            "subdir": connection_info.get("path")
                            if connection_info.get("path")
                            else None,
                            "submodules": False,
                            "prune_dotgit_folder": True,
                        }.items()
                        if value is not None
                    },
                }
                for relation_id, connection_info in {
                    git_unauthenticated_relation.id: git_unauthenticated_relation.remote_app_data,
                    git_credentials_relation.id: git_credentials_relation.remote_app_data,
                    git_ssh_relation.id: git_ssh_relation.remote_app_data,
                }.items()
            ]

        expected_add_airflow_connection_calls = [
            unittest.mock.call(
                "git_default",
                git.GitProviderModel(repository_url="https://github.com"),
            ),
            unittest.mock.call(
                f"git_relation_{git_credentials_relation.id}_connection",
                git.GitProviderModel(
                    **git_credentials_relation.remote_app_data,
                    credentials_personal_access_token="test-token",
                ),
            ),
            unittest.mock.call(
                f"git_relation_{git_ssh_relation.id}_connection",
                git.GitProviderModel(
                    **git_ssh_relation.remote_app_data,
                    ssh_private_key="test-key",
                ),
            ),
        ]

        assert sorted(mock_command_executor["add_airflow_git_connection"].mock_calls) == sorted(
            expected_add_airflow_connection_calls
        )
        mock_command_executor["add_airflow_git_connection"].reset_mock()

        modified_git_unauthenticated_relation = ops.testing.Relation(
            constants.GIT_ENDPOINT_NAME,
            id=git_unauthenticated_relation.id,
            remote_app_data={
                **git_unauthenticated_relation.remote_app_data,
                "repository_url": "modified-repo_url",
            },
        )

        relations_with_modified_git = [
            relation
            for relation in state_without_s3.relations
            if relation.endpoint != constants.GIT_ENDPOINT_NAME
        ]
        relations_with_modified_git.extend(
            [modified_git_unauthenticated_relation, git_credentials_relation, git_ssh_relation]
        )

        state_modified = dataclasses.replace(
            state_without_s3,
            relations=relations_with_modified_git,
            containers=[workload_container],
        )

        # note: we're using update_status to trigger reconciler
        # (due to an issue with object_storage charm lib emitting storage_connection_info_changed)
        state_out = context.run(context.on.update_status(), state_modified)

        assert state_out.unit_status == ops.ActiveStatus()

        for relation in state_out.get_relations("airflow-coordinator"):
            config_template = relation.local_app_data.get("config-template", "")
            parsed = configparser.RawConfigParser()
            parsed.read_string(config_template)

            assert json.loads(parsed.get("dag_processor", "dag_bundle_config_list")) == [
                {
                    "name": f"git_{relation_id}_dag_bundle",
                    "classpath": "airflow.providers.git.bundles.git.GitDagBundle",
                    "kwargs": {
                        key: value
                        for key, value in {
                            "git_conn_id": f"git_relation_{relation_id}_connection"
                            if connection_info.get("authentication-method")
                            else None,
                            "repo_url": connection_info["repository-url"],
                            "tracking_ref": connection_info.get("tracking-ref")
                            if connection_info.get("tracking-ref")
                            else None,
                            "subdir": connection_info.get("path")
                            if connection_info.get("path")
                            else None,
                            "submodules": False,
                            "prune_dotgit_folder": True,
                        }.items()
                        if value is not None
                    },
                }
                for relation_id, connection_info in {
                    modified_git_unauthenticated_relation.id: modified_git_unauthenticated_relation.remote_app_data,  # noqa: E501
                    git_credentials_relation.id: git_credentials_relation.remote_app_data,
                    git_ssh_relation.id: git_ssh_relation.remote_app_data,
                }.items()
            ]

        mock_command_executor["add_airflow_git_connection"].mock_calls == [
            unittest.mock.call(
                f"git_relation_{modified_git_unauthenticated_relation.id}_connection",
                git.GitProviderModel.model_validate(
                    modified_git_unauthenticated_relation.remote_app_data
                ),
            ),
        ]

    def test_removed_git_relation_results_in_airflow_connection_deletion(
        self,
        context,
        state_without_s3,
        git_unauthenticated_relation,
        git_credentials_relation,
        git_ssh_relation,
        mock_command_executor,
        mock_container_pull,
        workload_container,
    ):
        """Ensure stale git airflow connections removed if corresponding relation removed."""
        airflow_connections = [
            {
                "conn_id": f"git_relation_{git_credentials_relation.id}_connection",
                "conn_type": "git",
                "host": "test-repo-url2",
                "login": "user",
                "password": "test-token",
                "extra_dejson": {},
            },
            {
                "conn_id": f"git_relation_{git_ssh_relation.id}_connection",
                "conn_type": "git",
                "host": "test-repo-url3",
                "extra_dejson": {
                    "private_key": "test-key",
                    "strict_host_key_checking": "true",
                },
            },
        ]

        mock_command_executor["list_airflow_connections"].side_effect = [
            command_executor.CommandExecutionResult(
                success=True, stdout="[]", parsed_stdout=[], stderr="", return_code=0
            ),
            command_executor.CommandExecutionResult(
                success=True, stdout="[]", parsed_stdout=[], stderr="", return_code=0
            ),
            command_executor.CommandExecutionResult(
                success=True,
                stdout=json.dumps(airflow_connections),
                parsed_stdout=airflow_connections,
                stderr="",
                return_code=0,
            ),
            command_executor.CommandExecutionResult(
                success=True,
                stdout=json.dumps(airflow_connections),
                parsed_stdout=airflow_connections[1:],
                stderr="",
                return_code=0,
            ),
        ]

        state_out = context.run(context.on.start(), state_without_s3)

        assert state_out.unit_status == ops.ActiveStatus()

        for relation in state_out.get_relations("airflow-coordinator"):
            config_template = relation.local_app_data.get("config-template", "")
            parsed = configparser.ConfigParser()
            parsed.read_string(config_template)

            assert json.loads(parsed.get("dag_processor", "dag_bundle_config_list")) == [
                {
                    "name": f"git_{relation_id}_dag_bundle",
                    "classpath": "airflow.providers.git.bundles.git.GitDagBundle",
                    "kwargs": {
                        key: value
                        for key, value in {
                            "git_conn_id": f"git_relation_{relation_id}_connection"
                            if connection_info.get("authentication-method")
                            else None,
                            "repo_url": connection_info["repository-url"],
                            "tracking_ref": connection_info.get("tracking-ref")
                            if connection_info.get("tracking-ref")
                            else None,
                            "subdir": connection_info.get("path")
                            if connection_info.get("path")
                            else None,
                            "submodules": False,
                            "prune_dotgit_folder": True,
                        }.items()
                        if value is not None
                    },
                }
                for relation_id, connection_info in {
                    git_unauthenticated_relation.id: git_unauthenticated_relation.remote_app_data,
                    git_credentials_relation.id: git_credentials_relation.remote_app_data,
                    git_ssh_relation.id: git_ssh_relation.remote_app_data,
                }.items()
            ]

        expected_add_airflow_connection_calls = [
            unittest.mock.call(
                "git_default",
                git.GitProviderModel(repository_url="https://github.com"),
            ),
            unittest.mock.call(
                f"git_relation_{git_credentials_relation.id}_connection",
                git.GitProviderModel(
                    **git_credentials_relation.remote_app_data,
                    credentials_personal_access_token="test-token",
                ),
            ),
            unittest.mock.call(
                f"git_relation_{git_ssh_relation.id}_connection",
                git.GitProviderModel(
                    **git_ssh_relation.remote_app_data,
                    ssh_private_key="test-key",
                ),
            ),
        ]

        assert sorted(mock_command_executor["add_airflow_git_connection"].mock_calls) == sorted(
            expected_add_airflow_connection_calls
        )
        mock_command_executor["add_airflow_git_connection"].reset_mock()

        relations_with_removed_git_relation = [
            relation
            for relation in state_without_s3.relations
            if relation.endpoint != constants.GIT_ENDPOINT_NAME
        ]
        relations_with_removed_git_relation.append(git_ssh_relation)

        state_modified = dataclasses.replace(
            state_without_s3,
            relations=relations_with_removed_git_relation,
            containers=[workload_container],
        )

        # note: we're using update_status to trigger reconciler
        # (due to an issue with object_storage charm lib emitting storage_connection_info_changed)
        state_out = context.run(context.on.update_status(), state_modified)

        assert state_out.unit_status == ops.ActiveStatus()

        for relation in state_out.get_relations("airflow-coordinator"):
            config_template = relation.local_app_data.get("config-template", "")
            parsed = configparser.ConfigParser()
            parsed.read_string(config_template)

            assert json.loads(parsed.get("dag_processor", "dag_bundle_config_list")) == [
                {
                    "name": f"git_{git_ssh_relation.id}_dag_bundle",
                    "classpath": "airflow.providers.git.bundles.git.GitDagBundle",
                    "kwargs": {
                        "git_conn_id": f"git_relation_{git_ssh_relation.id}_connection",
                        "repo_url": git_ssh_relation.remote_app_data["repository-url"],
                        "tracking_ref": git_ssh_relation.remote_app_data["tracking-ref"],
                        "subdir": git_ssh_relation.remote_app_data["path"],
                        "submodules": False,
                        "prune_dotgit_folder": True,
                    },
                },
            ]

        mock_command_executor["add_airflow_git_connection"].assert_not_called()
        mock_command_executor["delete_airflow_connection"].mock_calls == [
            unittest.mock.call(f"git_relation_{git_credentials_relation.id}_connection"),
        ]


def test_runtime_secrets_generated_and_stored_in_app_secret(
    context, state, mock_command_executor, workload_container
):
    """Verify runtime secrets are generated and stored in a Juju application secret."""
    with unittest.mock.patch(
        "config_generator.AirflowConfigGenerator.config_template",
        new_callable=unittest.mock.PropertyMock(
            return_value=MOCK_CONFIG_TEMPLATE_WITH_RUNTIME_SECRETS
        ),
    ):
        state_out = context.run(context.on.pebble_ready(workload_container), state)

    peer = state_out.get_relations(constants.PEER_RELATION_NAME)[0]

    # Secret ID stored in peer data, no plaintext fields
    assert constants.AIRFLOW_KEYS_SECRET in peer.local_app_data
    assert "secret_key" not in peer.local_app_data
    assert "jwt_secret" not in peer.local_app_data

    # Verify secret contents have expected lengths
    secret = [
        s for s in state_out.secrets if s.tracked_content and "secret-key" in s.tracked_content
    ][0]
    assert len(secret.tracked_content["secret-key"]) == 64  # token_hex(32)
    assert len(secret.tracked_content["jwt-secret"]) == 64

    # Verify distributed config includes secret fields
    for relation in state_out.get_relations("airflow-coordinator"):
        config_template = relation.local_app_data["config-template"]
        assert "secret_key =" in config_template
        assert "jwt_secret =" in config_template

        sensitive_secret_id = relation.local_app_data["secret-sensitive-data"]
        sensitive_data = json.loads(
            state_out.get_secret(id=sensitive_secret_id).latest_content["sensitive-data"]
        )
        assert sensitive_data["api__secret_key"]
        assert sensitive_data["api_auth__jwt_secret"]


def test_runtime_secrets_reused_across_events(
    context,
    state,
    all_required_relations,
    mock_command_executor,
    workload_container,
    peer_relation,
):
    """Verify the same application secret is reused on subsequent events."""
    with unittest.mock.patch(
        "config_generator.AirflowConfigGenerator.config_template",
        new_callable=unittest.mock.PropertyMock(
            return_value=MOCK_CONFIG_TEMPLATE_WITH_RUNTIME_SECRETS
        ),
    ):
        state_out = context.run(
            context.on.pebble_ready(workload_container),
            state,
        )

    peer = state_out.get_relations(constants.PEER_RELATION_NAME)[0]
    first_secret_id = peer.local_app_data[constants.AIRFLOW_KEYS_SECRET]
    first_secret = [s for s in state_out.secrets if s.id == first_secret_id][0]

    # Run again with existing secret and peer data
    peer_with_secret_id = dataclasses.replace(
        peer_relation,
        local_app_data={
            constants.AIRFLOW_KEYS_SECRET: first_secret_id,
            "db_migration_ran": "true",
        },
    )
    relations = [r for r in all_required_relations if r.endpoint != constants.PEER_RELATION_NAME]
    relations.append(peer_with_secret_id)

    state = dataclasses.replace(state, relations=relations, secrets=[*state.secrets, first_secret])

    with unittest.mock.patch(
        "config_generator.AirflowConfigGenerator.config_template",
        new_callable=unittest.mock.PropertyMock(
            return_value=MOCK_CONFIG_TEMPLATE_WITH_RUNTIME_SECRETS
        ),
    ):
        state_in = dataclasses.replace(
            state, relations=relations, secrets=[*state.secrets, first_secret]
        )
        state_out_2 = context.run(
            context.on.start(),
            state_in,
        )

    peer_2 = state_out_2.get_relations(constants.PEER_RELATION_NAME)[0]
    assert peer_2.local_app_data[constants.AIRFLOW_KEYS_SECRET] == first_secret_id

    # Verify mapped secret values are present in relation sensitive-data secret.
    for relation in state_out_2.get_relations("airflow-coordinator"):
        sensitive_secret_id = relation.local_app_data["secret-sensitive-data"]
        sensitive_data = json.loads(
            state_out_2.get_secret(id=sensitive_secret_id).latest_content["sensitive-data"]
        )
        assert sensitive_data["api__secret_key"]
        assert sensitive_data["api_auth__jwt_secret"]


def test_runtime_secret_created_when_peer_has_no_plaintext_fields(
    context,
    state,
    all_required_relations,
    mock_command_executor,
    workload_container,
    peer_relation,
):
    """Verify runtime secret is created and only secret ID is stored in peer app data."""
    clean_peer = dataclasses.replace(peer_relation, local_app_data={})
    relations = [r for r in all_required_relations if r.endpoint != constants.PEER_RELATION_NAME]
    relations.append(clean_peer)

    state_in = dataclasses.replace(state, relations=relations)

    with unittest.mock.patch(
        "config_generator.AirflowConfigGenerator.config_template",
        new_callable=unittest.mock.PropertyMock(
            return_value=MOCK_CONFIG_TEMPLATE_WITH_RUNTIME_SECRETS
        ),
    ):
        state_out = context.run(
            context.on.pebble_ready(workload_container),
            state_in,
        )

    peer = state_out.get_relations(constants.PEER_RELATION_NAME)[0]

    # No plaintext fields and secret ID present
    assert "secret_key" not in peer.local_app_data
    assert "jwt_secret" not in peer.local_app_data
    assert constants.AIRFLOW_KEYS_SECRET in peer.local_app_data

    # Secret content contains generated values
    secret = [
        s for s in state_out.secrets if s.tracked_content and "secret-key" in s.tracked_content
    ][0]
    assert len(secret.tracked_content["secret-key"]) == 64
    assert len(secret.tracked_content["jwt-secret"]) == 64

    for relation in state_out.get_relations("airflow-coordinator"):
        config_template = relation.local_app_data["config-template"]
        assert "secret_key =" in config_template
        assert "jwt_secret =" in config_template

        sensitive_secret_id = relation.local_app_data["secret-sensitive-data"]
        sensitive_data = json.loads(
            state_out.get_secret(id=sensitive_secret_id).latest_content["sensitive-data"]
        )
        assert sensitive_data["api__secret_key"]
        assert sensitive_data["api_auth__jwt_secret"]


class TestAirflowConfigurability:
    @pytest.mark.parametrize(
        "config_option_values",
        [
            (constants.CORE_MAX_ACTIVE_RUNS_PER_DAG_CONFIG, -1),
            (constants.CORE_MAX_ACTIVE_TASKS_PER_DAG_CONFIG, -1),
            (constants.CORE_PARALLELISM_CONFIG, -1),
            (constants.DAG_PROCESSOR_PARSING_PROCESSES_CONFIG, 0),
            (constants.DAG_PROCESSOR_PARSING_PROCESSES_CONFIG, -1),
            (constants.DATABASE_SQL_ALCHEMY_POOL_SIZE_CONFIG, -1),
            (constants.TRIGGERER_CAPACITY_CONFIG, -1),
            (constants.TRIGGERER_CAPACITY_CONFIG, 0),
        ],
    )
    def test_negative_value_configs(
        self, context, state, mock_command_executor, config_option_values
    ):
        """Ensure negative values integer configs are not accepted."""
        state = dataclasses.replace(
            state,
            config={
                **state.config,
                config_option_values[0]: config_option_values[1],
            },
        )

        state_out = context.run(context.on.config_changed(), state)

        assert state_out.unit_status == ops.BlockedStatus(
            constants.INVALID_CONFIG_MESSAGE.format(config_name=config_option_values[0])
        )

    def test_invalid_timezone_config(self, context, state, mock_command_executor):
        """Ensure invalid timezone configs are not accepted."""
        state = dataclasses.replace(
            state,
            config={
                **state.config,
                constants.CORE_DEFAULT_TIMEZONE_CONFIG: "invalid",
            },
        )

        state_out = context.run(context.on.config_changed(), state)

        assert state_out.unit_status == ops.BlockedStatus(
            constants.INVALID_CONFIG_MESSAGE.format(
                config_name=constants.CORE_DEFAULT_TIMEZONE_CONFIG
            )
        )

    @pytest.mark.parametrize("valid_timezone", ["utc", "system", "America/New_York"])
    def test_valid_configs_update_airflow_cfg(
        self, context, state, mock_command_executor, valid_timezone
    ):
        """Ensure properly specified configs are accepted."""
        airflow_configs = {
            constants.CORE_DEFAULT_TIMEZONE_CONFIG: valid_timezone,
            constants.CORE_MAX_ACTIVE_RUNS_PER_DAG_CONFIG: 6,
            constants.CORE_MAX_ACTIVE_TASKS_PER_DAG_CONFIG: 6,
            constants.CORE_PARALLELISM_CONFIG: 6,
            constants.DAG_PROCESSOR_PARSING_PROCESSES_CONFIG: 6,
            constants.DATABASE_SQL_ALCHEMY_POOL_SIZE_CONFIG: 6,
            constants.TRIGGERER_CAPACITY_CONFIG: 6,
        }
        state = dataclasses.replace(
            state,
            config={
                **state.config,
                **airflow_configs,
            },
        )

        state_out = context.run(context.on.config_changed(), state)

        assert state_out.unit_status == ops.ActiveStatus()

        for relation in state_out.get_relations("airflow-coordinator"):
            config_template = relation.local_app_data.get("config-template", "")
            parsed = configparser.ConfigParser()
            parsed.read_string(config_template)

            # Ensure config options propagate to airflow.cfg
            assert parsed["core"]["default_timezone"] == valid_timezone
            assert parsed["core"]["max_active_runs_per_dag"] == "6"
            assert parsed["core"]["max_active_tasks_per_dag"] == "6"
            assert parsed["core"]["parallelism"] == "6"

            assert parsed["dag_processor"]["parsing_processes"] == "6"

            assert parsed["database"]["sql_alchemy_pool_size"] == "6"

            assert parsed["triggerer"]["capacity"] == "6"

            # Ensure configs with updated default propagate to airflow.cfg
            assert parsed["api"]["enable_swagger_ui"] == "False"

            assert parsed["core"]["dagbag_import_error_tracebacks"] == "False"
            assert parsed["core"]["check_migrations"] == "False"
            assert parsed["core"]["load_examples"] == "False"
            assert parsed["core"]["default_impersonation"] == "ubuntu"

            assert parsed["scheduler"]["enable_healthcheck"] == "True"


class TestKubernetesExecutorConfig:
    def test_kubernetes_executor_config_returns_none_without_relation(
        self, context, state, mock_command_executor
    ):
        """Verify _kubernetes_executor_config returns None when relation is absent."""
        with context(context.on.start(), state) as manager:
            charm = manager.charm
            assert charm._kubernetes_executor_config is None

    def test_kubernetes_executor_config_returns_none_when_relation_empty(
        self,
        context,
        state,
        all_required_relations,
        kubernetes_executor_config_relation_empty,
        mock_command_executor,
    ):
        """Verify _kubernetes_executor_config returns None when relation has no data yet."""
        all_required_relations.append(kubernetes_executor_config_relation_empty)

        state = dataclasses.replace(state, relations=all_required_relations)

        with context(context.on.start(), state) as manager:
            charm = manager.charm
            assert charm._kubernetes_executor_config is None

    def test_kubernetes_executor_config_merges_into_distributed_template(
        self, context, state, mock_command_executor
    ):
        """Verify executor config sections are merged into the distributed config template."""
        with (
            unittest.mock.patch(
                "config_generator.AirflowConfigGenerator.config_template",
                new_callable=unittest.mock.PropertyMock(
                    return_value="[core]\nexecutor = {{ executor | default('LocalExecutor') }}\n"
                ),
            ),
            unittest.mock.patch.object(
                AirflowCoordinatorK8SOperatorCharm,
                "_kubernetes_executor_config",
                new_callable=unittest.mock.PropertyMock(
                    return_value=MOCK_KUBERNETES_EXECUTOR_CONFIG
                ),
            ),
        ):
            state_out = context.run(context.on.start(), state)

        assert state_out.unit_status == ops.ActiveStatus()

        for relation in state_out.get_relations("airflow-coordinator"):
            config_template = relation.local_app_data.get("config-template", "")
            parsed = configparser.ConfigParser()
            parsed.read_string(config_template)
            assert parsed["core"]["executor"] == "KubernetesExecutor"
            assert parsed["kubernetes_executor"]["namespace"] == "airflow-ns"
            assert parsed["kubernetes_executor"]["base_image"] == "airflow:latest"

    def test_kubernetes_executor_pod_spec_returns_none_without_relation(
        self, context, state, mock_command_executor
    ):
        """Verify _kubernetes_executor_pod_spec returns None when relation is absent."""
        with context(context.on.start(), state) as manager:
            charm = manager.charm
            assert charm._kubernetes_executor_pod_spec is None

    def test_kubernetes_executor_pod_spec_distributed_to_core_charms(
        self, context, state, mock_command_executor
    ):
        """Verify the pod spec template is distributed to core charms."""
        with (
            unittest.mock.patch(
                "config_generator.AirflowConfigGenerator.config_template",
                new_callable=unittest.mock.PropertyMock(
                    return_value="[core]\nexecutor = {{ executor | default('LocalExecutor') }}\n"
                ),
            ),
            unittest.mock.patch.object(
                AirflowCoordinatorK8SOperatorCharm,
                "_kubernetes_executor_config",
                new_callable=unittest.mock.PropertyMock(
                    return_value=MOCK_KUBERNETES_EXECUTOR_CONFIG
                ),
            ),
            unittest.mock.patch.object(
                AirflowCoordinatorK8SOperatorCharm,
                "_kubernetes_executor_pod_spec",
                new_callable=unittest.mock.PropertyMock(
                    return_value=MOCK_KUBERNETES_EXECUTOR_POD_SPEC
                ),
            ),
        ):
            state_out = context.run(context.on.start(), state)

        for relation in state_out.get_relations("airflow-coordinator"):
            assert (
                relation.local_app_data.get("kubernetes-executor-pod-spec")
                == MOCK_KUBERNETES_EXECUTOR_POD_SPEC
            )

    def test_default_executor_is_local_without_kubernetes_executor_relation(
        self, context, state, mock_command_executor
    ):
        """Verify executor defaults to LocalExecutor when no executor relation exists."""
        with unittest.mock.patch(
            "config_generator.AirflowConfigGenerator.config_template",
            new_callable=unittest.mock.PropertyMock(
                return_value="[core]\nexecutor = {{ executor | default('LocalExecutor') }}\n"
            ),
        ):
            state_out = context.run(context.on.start(), state)

        for relation in state_out.get_relations("airflow-coordinator"):
            config_template = relation.local_app_data.get("config-template", "")
            assert "executor = KubernetesExecutor" not in config_template


def test_base_url_uses_ingress_path_when_available(
    context,
    state,
    all_required_relations,
    airflow_api_server_requires_relation,
    mock_command_executor,
):
    """Verify base_url appends ingress path when api-server shares one."""
    ingress_relation = dataclasses.replace(
        airflow_api_server_requires_relation,
        remote_app_data={
            "host": "test-host",
            "port": "test-port",
            "ingress_path": "test-airflow-api-server-k8s",
        },
    )
    all_required_relations.remove(airflow_api_server_requires_relation)
    all_required_relations.append(ingress_relation)

    state = dataclasses.replace(state, relations=all_required_relations)

    state_out = context.run(context.on.start(), state)
    assert state_out.unit_status == ops.ActiveStatus()

    for relation in state_out.get_relations("airflow-coordinator"):
        config_template = relation.local_app_data.get("config-template", "")
        assert "http://test-host:test-port/test-airflow-api-server-k8s" in config_template


def test_base_url_falls_back_to_internal_url_without_ingress(
    context,
    state,
    mock_command_executor,
):
    """Verify base_url uses internal host:port when no ingress path."""
    state_out = context.run(context.on.start(), state)
    assert state_out.unit_status == ops.ActiveStatus()

    for relation in state_out.get_relations("airflow-coordinator"):
        config_template = relation.local_app_data.get("config-template", "")
        assert "http://test-host:test-port" in config_template


class TestPebbleLayer:
    def test_pebble_layer_structure(
        self, context, state, mock_command_executor, workload_container
    ):
        with context(context.on.start(), state) as manager:
            charm = manager.charm
            layer = charm._airflow_coordinator_layer

        assert "services" in layer
        assert "airflow" in layer["services"]
        assert layer["services"]["airflow"]["override"] == "merge"
        assert layer["services"]["airflow"]["startup"] == "disabled"

    def test_pebble_layer_disables_health_check(
        self, context, state, mock_command_executor, workload_container
    ):
        with context(context.on.start(), state) as manager:
            charm = manager.charm
            layer = charm._airflow_coordinator_layer

        assert "checks" in layer
        assert "airflow-running" in layer["checks"]
        assert layer["checks"]["airflow-running"]["override"] == "replace"
        assert layer["checks"]["airflow-running"]["exec"]["command"] == "/bin/true"
