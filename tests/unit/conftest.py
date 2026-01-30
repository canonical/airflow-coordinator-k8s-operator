# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

import logging
import unittest.mock

import ops.testing
import pytest

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

SENSITIVE_DATA = {
    "sql_alchemy_connection_string": POSTGRES_SQL_ALCHEMY_STRING,
}

CUSTOM_CONFIG_SENSITIVE = """[section_a]
key1 = secret1
key2 = secret2

[section_b]
key3 = secret3
"""

CUSTOM_CONFIG_NON_SENSITIVE = """[section_a]
key4 = non-secret4

[section_b]
key5 = non-secret5
"""

CONFIG_TEMPLATED = """[section_a]
key1 = template1
key6 = template6

[section_b]
key7 = template7
"""

MERGED_CONFIG_TEMPLATE = """[section_a]
key1 = {{ section_a_key1_secret_value }}
key2 = {{ section_a_key2_secret_value }}
key4 = non-secret4
key6 = template6

[section_b]
key7 = template7
key3 = {{ section_b_key3_secret_value }}
key5 = non-secret5
"""

SENSITIVE_DATA_WITH_CUSTOM_CONFIGS = {
    "sql_alchemy_connection_string": POSTGRES_SQL_ALCHEMY_STRING,
    "section_a_key1_secret_value": "secret1",
    "section_a_key2_secret_value": "secret2",
    "section_b_key3_secret_value": "secret3",
}


@pytest.fixture
def airflow_coordinator_k8s_charm():
    yield AirflowCoordinatorK8SOperatorCharm


@pytest.fixture(scope="function")
def context(airflow_coordinator_k8s_charm):
    return ops.testing.Context(
        charm_type=airflow_coordinator_k8s_charm,
    )


def core_component_metadata(
    component: str, airflow_version: str = "3.1.0", workload_image_hash: str = "somehash"
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
def all_required_relations(
    postgres_relation,
    api_server_relation,
    scheduler_relation,
    triggerer_relation,
    dag_processor_relation,
):
    return [
        postgres_relation,
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


@pytest.fixture(scope="function")
def custom_config_secret():
    return ops.testing.Secret(
        tracked_content={
            constants.SENSITIVE_CUSTOM_CONFIG_SECRET_KEY: CUSTOM_CONFIG_SENSITIVE,
        },
    )


@pytest.fixture(scope="function")
def state_with_custom_config(custom_config_secret, all_required_relations):
    return ops.testing.State(
        leader=True,
        config={
            constants.CUSTOM_CONFIG: CUSTOM_CONFIG_NON_SENSITIVE,
            constants.SENSITIVE_CUSTOM_CONFIG: custom_config_secret.id,
        },
        relations=all_required_relations,
        secrets=[custom_config_secret],
    )
