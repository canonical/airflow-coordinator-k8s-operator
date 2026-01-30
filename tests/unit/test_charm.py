# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.
#
# To learn more about testing, see https://documentation.ubuntu.com/ops/latest/explanation/testing/

import builtins
import configparser
import copy
import dataclasses
import json
import logging
import pathlib
import unittest.mock

import charms.airflow_coordinator_k8s.v0.airflow_coordinator as airflow_coordinator
import ops
import ops.testing
from conftest import (
    CONFIG_TEMPLATED,
    CUSTOM_CONFIG_SENSITIVE,
    MERGED_CONFIG_TEMPLATE,
    SENSITIVE_DATA_WITH_CUSTOM_CONFIGS,
)

import command_executor
import constants

logger = logging.getLogger(__name__)


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


def test_sensitive_custom_config_secret_not_found(context, state_with_custom_config):
    config_copy = copy.deepcopy(state_with_custom_config.config)
    config_copy.update({constants.SENSITIVE_CUSTOM_CONFIG: "secret:oi6jb9z58hre99w63v0g"})

    state_with_custom_config = dataclasses.replace(state_with_custom_config, config=config_copy)

    original_open = builtins.open

    def _mock_open(*args, **kwargs):
        if isinstance(args[0], pathlib.PosixPath) and args[0].resolve().name.endswith(
            "airflow_config.j2"
        ):
            m = unittest.mock.mock_open(read_data=CONFIG_TEMPLATED)
            return m(*args, **kwargs)

        return original_open(*args, **kwargs)

    with unittest.mock.patch(
        "builtins.open",
        side_effect=_mock_open,
    ):
        state_out = context.run(context.on.config_changed(), state_with_custom_config)

    assert state_out.unit_status == ops.BlockedStatus(constants.CUSTOM_CONFIG_SECRET_NOT_FOUND)

    for relation in state_out.get_relations("airflow-coordinator"):
        assert relation.local_app_data == {}


def test_sensitive_custom_config_secret_unauthorized(context, state_with_custom_config):
    original_open = builtins.open

    def _mock_open(*args, **kwargs):
        if isinstance(args[0], pathlib.PosixPath) and args[0].resolve().name.endswith(
            "airflow_config.j2"
        ):
            m = unittest.mock.mock_open(read_data=CONFIG_TEMPLATED)
            return m(*args, **kwargs)

        return original_open(*args, **kwargs)

    with (
        unittest.mock.patch(
            "builtins.open",
            side_effect=_mock_open,
        ),
        unittest.mock.patch(
            "ops.model.Model.get_secret", side_effect=ops.model.ModelError("unauthorized")
        ),
    ):
        state_out = context.run(context.on.config_changed(), state_with_custom_config)

    assert state_out.unit_status == ops.BlockedStatus(
        constants.UNAUTHORIZED_ACCESS_TO_SECRET_MESSAGE
    )

    for relation in state_out.get_relations("airflow-coordinator"):
        assert relation.local_app_data == {}


def test_custom_airflow_config_with_overlap_keys(context, state_with_custom_config):
    config_copy = copy.deepcopy(state_with_custom_config.config)
    config_copy.update({constants.CUSTOM_CONFIG: CUSTOM_CONFIG_SENSITIVE})

    state_with_custom_config = dataclasses.replace(state_with_custom_config, config=config_copy)

    original_open = builtins.open

    def _mock_open(*args, **kwargs):
        if isinstance(args[0], pathlib.PosixPath) and args[0].resolve().name.endswith(
            "airflow_config.j2"
        ):
            m = unittest.mock.mock_open(read_data=CONFIG_TEMPLATED)
            return m(*args, **kwargs)

        return original_open(*args, **kwargs)

    with unittest.mock.patch(
        "builtins.open",
        side_effect=_mock_open,
    ):
        state_out = context.run(context.on.config_changed(), state_with_custom_config)

    assert state_out.unit_status == ops.BlockedStatus(constants.CUSTOM_CONFIG_OVERLAP_MESSAGE)

    for relation in state_out.get_relations("airflow-coordinator"):
        assert relation.local_app_data == {}


def test_custom_airflow_config(context, state_with_custom_config, mock_command_executor):
    original_open = builtins.open

    def _mock_open(*args, **kwargs):
        if isinstance(args[0], pathlib.PosixPath) and args[0].resolve().name.endswith(
            "airflow_config.j2"
        ):
            m = unittest.mock.mock_open(read_data=CONFIG_TEMPLATED)
            return m(*args, **kwargs)

        return original_open(*args, **kwargs)

    with unittest.mock.patch(
        "builtins.open",
        side_effect=_mock_open,
    ):
        state_out = context.run(context.on.config_changed(), state_with_custom_config)

    assert state_out.unit_status == ops.ActiveStatus()

    for relation in state_out.get_relations("airflow-coordinator"):
        local_app_config_parser = configparser.ConfigParser()
        expected_config_parser = configparser.ConfigParser()

        local_app_config_parser.read_string(relation.local_app_data["config-template"])

        expected_config_parser.read_string(MERGED_CONFIG_TEMPLATE)

        assert dict(local_app_config_parser) == dict(expected_config_parser)

        assert state_out.get_secret(
            id=relation.local_app_data["secret-sensitive-data"]
        ).latest_content == {"sensitive-data": json.dumps(SENSITIVE_DATA_WITH_CUSTOM_CONFIGS)}


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
            return_value="mock_config: {{ sql_alchemy_connection_string }}"
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
            return_value="mock_config: {{ sql_alchemy_connection_string }}"
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
            return_value="mock_config: {{ sql_alchemy_connection_string }}"
        ),
    ):
        state_out = context.run(context.on.pebble_ready(workload_container), state)

    assert state_out.unit_status == ops.BlockedStatus(constants.DB_MIGRATION_FAILED_MESSAGE)

    # Verify that config was not distributed to core charms
    for relation in state_out.get_relations("airflow-coordinator"):
        assert "config-template" not in relation.local_app_data


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
