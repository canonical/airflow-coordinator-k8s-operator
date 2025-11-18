# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.
#
# To learn more about testing, see https://documentation.ubuntu.com/ops/latest/explanation/testing/

import dataclasses

import ops
import ops.testing


def test_non_leader_unit(context, state):
    state = dataclasses.replace(state, leader=False)

    app_status_before = state.app_status

    state_out = context.run(context.on.start(), state)

    assert state_out.app_status == app_status_before


def test_missing_core_charm_relations(
    context, state, all_required_relations, scheduler_relation, triggerer_relation
):
    all_required_relations.remove(scheduler_relation)
    all_required_relations.remove(triggerer_relation)
    state = dataclasses.replace(state, relations=all_required_relations)

    state_out = context.run(context.on.start(), state)

    assert state_out.app_status == ops.BlockedStatus(
        "Missing integrations with scheduler, triggerer"
    )


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

    assert state_out.app_status == ops.BlockedStatus(
        "Integrated apps with invalid or mismatched versions"
    )


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

    assert state_out.app_status == ops.BlockedStatus(
        "Integrated apps with inconsistent image hashes"
    )


def test_happy_path(context, state):
    state_out = context.run(context.on.start(), state)

    assert state_out.app_status == ops.ActiveStatus()
