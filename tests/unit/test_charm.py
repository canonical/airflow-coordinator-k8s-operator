# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.
#
# To learn more about testing, see https://documentation.ubuntu.com/ops/latest/explanation/testing/

import dataclasses
import json
import unittest.mock

import ops
import ops.testing
from conftest import POSTGRES_DATA, POSTGRES_SQL_ALCHEMY_STRING


def test_non_leader_unit(context, state):
    state = dataclasses.replace(state, leader=False)

    unit_status_before = state.unit_status

    state_out = context.run(context.on.start(), state)

    assert state_out.unit_status == unit_status_before


def test_missing_postgres_relation(context, state, all_required_relations, postgres_relation):
    all_required_relations.remove(postgres_relation)
    state = dataclasses.replace(state, relations=all_required_relations)

    state_out = context.run(context.on.start(), state)

    assert state_out.unit_status == ops.BlockedStatus("Missing relation with postgres")

    for relation in state_out.get_relations("airflow-coordinator"):
        assert relation.local_app_data == {}


def test_missing_postgres_relation_data(context, state, all_required_relations, postgres_relation):
    empty_postgres_relation = dataclasses.replace(postgres_relation, remote_app_data={})

    all_required_relations.remove(postgres_relation)
    all_required_relations.append(empty_postgres_relation)
    state = dataclasses.replace(state, relations=all_required_relations)

    state_out = context.run(context.on.start(), state)

    assert state_out.unit_status == ops.WaitingStatus("Waiting for airflow database to be created")

    for relation in state_out.get_relations("airflow-coordinator"):
        assert relation.local_app_data == {}


def test_missing_core_charm_relations(
    context, state, all_required_relations, scheduler_relation, triggerer_relation
):
    all_required_relations.remove(scheduler_relation)
    all_required_relations.remove(triggerer_relation)
    state = dataclasses.replace(state, relations=all_required_relations)

    state_out = context.run(context.on.start(), state)

    assert state_out.unit_status == ops.BlockedStatus(
        "Missing integrations with scheduler, triggerer"
    )

    failures = json.dumps(
        [
            {
                "component": "scheduler",
                "code": "missing_component",
                "message": "Required component is missing in the cluster",
            },
            {
                "component": "triggerer",
                "code": "missing_component",
                "message": "Required component is missing in the cluster",
            },
        ]
    )

    for relation in state_out.get_relations("airflow-coordinator"):
        assert relation.local_app_data == {
            "validation-failures": failures,
        }


def test_invalid_core_charm_airflow_version(
    context, state, all_required_relations, scheduler_data, scheduler_relation
):
    scheduler_data["airflow_version"] = "0.0.0"
    modified_scheduler_relation = dataclasses.replace(
        scheduler_relation, remote_app_data=scheduler_data
    )

    scheduler_relation_index = [
        index
        for index, relation in enumerate(all_required_relations)
        if relation.remote_app_data.get("component") == "scheduler"
    ][0]
    del all_required_relations[scheduler_relation_index]
    all_required_relations.append(modified_scheduler_relation)

    state = dataclasses.replace(state, relations=all_required_relations)

    state_out = context.run(context.on.start(), state)

    assert state_out.unit_status == ops.BlockedStatus("Integrated apps with mismatched versions")

    failures = json.dumps(
        [
            {
                "component": "scheduler",
                "code": "inconsistent_airflow_version",
                "message": "Component has an airflow version inconsistent with the cluster",
            },
        ]
    )

    for relation in state_out.get_relations("airflow-coordinator"):
        assert relation.local_app_data == {
            "validation-failures": failures,
        }


def test_invalid_core_charm_workload_image_hash(
    context, state, all_required_relations, scheduler_data, scheduler_relation
):
    scheduler_data["workload_image_hash"] = "invalidhash"
    modified_scheduler_relation = dataclasses.replace(
        scheduler_relation, remote_app_data=scheduler_data
    )

    scheduler_relation_index = [
        index
        for index, relation in enumerate(all_required_relations)
        if relation.remote_app_data.get("component") == "scheduler"
    ][0]
    del all_required_relations[scheduler_relation_index]
    all_required_relations.append(modified_scheduler_relation)

    state = dataclasses.replace(state, relations=all_required_relations)

    state_out = context.run(context.on.start(), state)

    assert state_out.unit_status == ops.BlockedStatus(
        "Integrated apps with inconsistent image hashes"
    )

    failures = json.dumps(
        [
            {
                "component": "scheduler",
                "code": "inconsistent_workload_image_hash",
                "message": "Component has a workload image hash that is inconsistent with the cluster",  # noqa: E501
            },
        ]
    )

    for relation in state_out.get_relations("airflow-coordinator"):
        assert relation.local_app_data == {"validation-failures": failures}


def test_happy_path(context, state, postgres_relation):
    with (
        unittest.mock.patch(
            "config_generator.AirflowConfigGenerator.config_template",
            new_callable=unittest.mock.PropertyMock(
                return_value="mock_config: {{ sql_alchemy_connection_string }}"
            ),
        ),
        unittest.mock.patch(
            "charms.data_platform_libs.v0.data_interfaces.DatabaseRequires.fetch_my_relation_data",
            return_value={postgres_relation.id: POSTGRES_DATA},
        ),
    ):
        state_out = context.run(context.on.start(), state)

    assert state_out.unit_status == ops.ActiveStatus()

    for relation in state_out.get_relations("airflow-coordinator"):
        assert (
            relation.local_app_data["config-template"]
            == "mock_config: {{ sql_alchemy_connection_string }}"
        )

        assert state_out.get_secret(
            id=relation.local_app_data["secret-sensitive-data"]
        ).latest_content == {
            "sensitive-data": json.dumps(
                {"sql_alchemy_connection_string": POSTGRES_SQL_ALCHEMY_STRING}
            )
        }
