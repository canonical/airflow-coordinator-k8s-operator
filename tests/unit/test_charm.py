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


def test_runtime_secrets_generated_and_stored_in_app_secret(
    context, state, mock_command_executor, workload_container
):
    """Verify runtime secrets are generated and stored in a Juju application secret."""
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

    # Secret ID stored in peer data, no plaintext fields
    assert constants.AIRFLOW_KEYS_SECRET in peer.local_app_data
    assert "secret_key" not in peer.local_app_data
    assert "jwt_secret" not in peer.local_app_data
    assert "fernet_key" not in peer.local_app_data

    # Verify secret contents have expected lengths
    secret = [
        s for s in state_out.secrets if s.tracked_content and "secret-key" in s.tracked_content
    ][0]
    assert len(secret.tracked_content["secret-key"]) == 64  # token_hex(32)
    assert len(secret.tracked_content["jwt-secret"]) == 64
    assert len(secret.tracked_content["fernet-key"]) == 44  # Fernet key


def test_runtime_secrets_reused_across_events(
    context, all_required_relations, mock_command_executor, workload_container, peer_relation
):
    """Verify the same application secret is reused on subsequent events."""
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
                secrets=[first_secret],
            ),
        )

    peer_2 = state_out_2.get_relations(constants.PEER_RELATION_NAME)[0]
    assert peer_2.local_app_data[constants.AIRFLOW_KEYS_SECRET] == first_secret_id


def test_runtime_secret_created_when_peer_has_no_plaintext_fields(
    context, all_required_relations, mock_command_executor, workload_container, peer_relation
):
    """Verify runtime secret is created and only secret ID is stored in peer app data."""
    clean_peer = dataclasses.replace(peer_relation, local_app_data={})
    relations = [r for r in all_required_relations if r.endpoint != constants.PEER_RELATION_NAME]
    relations.append(clean_peer)

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
                relations=relations,
                containers=[workload_container],
            ),
        )

    peer = state_out.get_relations(constants.PEER_RELATION_NAME)[0]

    # No plaintext fields and secret ID present
    assert "secret_key" not in peer.local_app_data
    assert "jwt_secret" not in peer.local_app_data
    assert "fernet_key" not in peer.local_app_data
    assert constants.AIRFLOW_KEYS_SECRET in peer.local_app_data

    # Secret content contains generated values
    secret = [
        s for s in state_out.secrets if s.tracked_content and "secret-key" in s.tracked_content
    ][0]
    assert len(secret.tracked_content["secret-key"]) == 64
    assert len(secret.tracked_content["jwt-secret"]) == 64
    assert len(secret.tracked_content["fernet-key"]) == 44


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
