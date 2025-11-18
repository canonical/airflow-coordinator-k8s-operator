# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

import logging

import ops.testing
import pytest

from charm import AirflowCoordinatorK8SOperatorCharm

logger = logging.getLogger(__name__)


@pytest.fixture
def airflow_coordinator_k8s_charm():
    yield AirflowCoordinatorK8SOperatorCharm


@pytest.fixture(scope="function")
def context(airflow_coordinator_k8s_charm):
    return ops.testing.Context(charm_type=airflow_coordinator_k8s_charm)


def core_component_metadata(component: str):
    return {"airflow_version": "3.1.0", "workload_image_hash": "somehash", "component": component}


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
def all_required_relations(
    api_server_relation,
    scheduler_relation,
    triggerer_relation,
    dag_processor_relation,
):
    return [
        api_server_relation,
        scheduler_relation,
        triggerer_relation,
        dag_processor_relation,
    ]


@pytest.fixture(scope="function")
def state(all_required_relations):
    return ops.testing.State(
        leader=True,
        relations=all_required_relations,
    )
