# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.
#
# To learn more about testing, see https://documentation.ubuntu.com/ops/latest/explanation/testing/

import dataclasses
import json
import unittest.mock

import charms.airflow_coordinator_k8s.v0.airflow_coordinator as airflow_coordinator
import ops
import ops.testing

import command_executor
import constants


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
    all_required_relations,
    mock_run_db_migrate,
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

    with unittest.mock.patch(
        "config_generator.AirflowConfigGenerator.config_template",
        new_callable=unittest.mock.PropertyMock(
            return_value="mock_config: "
            "{{ sql_alchemy_connection_string }} "
            "{{ secret_key }} "
            "{{ jwt_secret }} "
            "{{ fernet_key }}"
        ),
    ):
        state_in = ops.testing.State(
            leader=True,
            containers=[workload_container],
            relations=relations,
        )

        context.run(
            context.on.pebble_ready(workload_container),
            state_in,
        )

    mock_run_db_migrate.assert_not_called()


def test_db_migration_runs_on_state_false(
    context,
    all_required_relations,
    mock_run_db_migrate,
    workload_container,
    peer_relation,
):
    """Verify that database migration happens when peer relation state is False."""
    # Peer relation with no migration state (defaults to False)
    with unittest.mock.patch(
        "config_generator.AirflowConfigGenerator.config_template",
        new_callable=unittest.mock.PropertyMock(
            return_value="mock_config: "
            "{{ sql_alchemy_connection_string }} "
            "{{ secret_key }} "
            "{{ jwt_secret }} "
            "{{ fernet_key }}"
        ),
    ):
        state_in = ops.testing.State(
            leader=True,
            containers=[workload_container],
            relations=all_required_relations,
        )

        context.run(
            context.on.pebble_ready(workload_container),
            state_in,
        )

    mock_run_db_migrate.assert_called_once()


def test_db_migration_failure(context, state, mock_command_executor, workload_container):
    mock_command_executor["run_db_migrate"].return_value = command_executor.CommandExecutionResult(
        success=False, stdout="", stderr="Migration failed", return_code=1
    )

    with unittest.mock.patch(
        "config_generator.AirflowConfigGenerator.config_template",
        new_callable=unittest.mock.PropertyMock(
            return_value="mock_config: "
            "{{ sql_alchemy_connection_string }} "
            "{{ secret_key }} "
            "{{ jwt_secret }} "
            "{{ fernet_key }}"
        ),
    ):
        state_out = context.run(context.on.pebble_ready(workload_container), state)

    assert state_out.unit_status == ops.BlockedStatus(constants.DB_MIGRATION_FAILED_MESSAGE)

    # Verify that config was not distributed to core charms
    for relation in state_out.get_relations("airflow-coordinator"):
        assert "config-template" not in relation.local_app_data


def test_secret_key_generated_and_persisted(
    context, state, mock_command_executor, workload_container
):
    """Verify secret_key is generated and stored in peer relation data."""
    with unittest.mock.patch(
        "config_generator.AirflowConfigGenerator.config_template",
        new_callable=unittest.mock.PropertyMock(
            return_value="mock_config: "
            "{{ sql_alchemy_connection_string }} "
            "{{ secret_key }} "
            "{{ jwt_secret }} "
            "{{ fernet_key }}"
        ),
    ):
        state_out = context.run(context.on.pebble_ready(workload_container), state)

    peer = state_out.get_relations(constants.PEER_RELATION_NAME)[0]
    secret_key = peer.local_app_data.get("secret_key")
    assert secret_key is not None
    assert len(secret_key) == 64  # token_hex(32) produces 64 hex chars


def test_jwt_secret_generated_and_persisted(
    context, state, mock_command_executor, workload_container
):
    """Verify jwt_secret is generated and stored in peer relation data."""
    with unittest.mock.patch(
        "config_generator.AirflowConfigGenerator.config_template",
        new_callable=unittest.mock.PropertyMock(
            return_value="mock_config: "
            "{{ sql_alchemy_connection_string }} "
            "{{ secret_key }} "
            "{{ jwt_secret }} "
            "{{ fernet_key }}"
        ),
    ):
        state_out = context.run(context.on.pebble_ready(workload_container), state)

    peer = state_out.get_relations(constants.PEER_RELATION_NAME)[0]
    jwt_secret = peer.local_app_data.get("jwt_secret")
    assert jwt_secret is not None
    assert len(jwt_secret) == 64  # token_hex(32) produces 64 hex chars


def test_fernet_key_generated_and_persisted(
    context, state, mock_command_executor, workload_container
):
    """Verify fernet_key is generated and stored in peer relation data.

    The fernet_key is a URL-safe base64-encoded 32-byte key used by Airflow
    to encrypt sensitive data (e.g. connection passwords) in the metadata DB.
    """
    with unittest.mock.patch(
        "config_generator.AirflowConfigGenerator.config_template",
        new_callable=unittest.mock.PropertyMock(
            return_value="mock_config: "
            "{{ sql_alchemy_connection_string }} "
            "{{ secret_key }} "
            "{{ jwt_secret }} "
            "{{ fernet_key }}"
        ),
    ):
        state_out = context.run(context.on.pebble_ready(workload_container), state)

    peer = state_out.get_relations(constants.PEER_RELATION_NAME)[0]
    fernet_key = peer.local_app_data.get("fernet_key")
    assert fernet_key is not None
    # base64.urlsafe_b64encode(os.urandom(32)) produces a 44-char base64 string
    assert len(fernet_key) == 44


def test_secrets_reused_across_events(
    context, all_required_relations, mock_command_executor, workload_container, peer_relation
):
    """Verify secret_key, jwt_secret, and fernet_key are not regenerated on subsequent events."""
    with unittest.mock.patch(
        "config_generator.AirflowConfigGenerator.config_template",
        new_callable=unittest.mock.PropertyMock(
            return_value="mock_config: "
            "{{ sql_alchemy_connection_string }} "
            "{{ secret_key }} "
            "{{ jwt_secret }} "
            "{{ fernet_key }}"
        ),
    ):
        state_out = context.run(
            context.on.pebble_ready(workload_container),
            ops.testing.State(
                leader=True,
                relations=all_required_relations,
                containers=[workload_container],
            ),
        )

    peer = state_out.get_relations(constants.PEER_RELATION_NAME)[0]
    first_secret_key = peer.local_app_data["secret_key"]
    first_jwt_secret = peer.local_app_data["jwt_secret"]
    first_fernet_key = peer.local_app_data["fernet_key"]

    # Run again with the peer relation already populated
    peer_with_secrets = dataclasses.replace(
        peer_relation,
        local_app_data={
            "secret_key": first_secret_key,
            "jwt_secret": first_jwt_secret,
            "fernet_key": first_fernet_key,
            "db_migration_ran": "true",
        },
    )
    relations = [r for r in all_required_relations if r.endpoint != constants.PEER_RELATION_NAME]
    relations.append(peer_with_secrets)

    with unittest.mock.patch(
        "config_generator.AirflowConfigGenerator.config_template",
        new_callable=unittest.mock.PropertyMock(
            return_value="mock_config: "
            "{{ sql_alchemy_connection_string }} "
            "{{ secret_key }} "
            "{{ jwt_secret }} "
            "{{ fernet_key }}"
        ),
    ):
        state_out_2 = context.run(
            context.on.start(),
            ops.testing.State(
                leader=True,
                relations=relations,
                containers=[workload_container],
            ),
        )

    peer_2 = state_out_2.get_relations(constants.PEER_RELATION_NAME)[0]
    assert peer_2.local_app_data["secret_key"] == first_secret_key
    assert peer_2.local_app_data["jwt_secret"] == first_jwt_secret
    assert peer_2.local_app_data["fernet_key"] == first_fernet_key


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
