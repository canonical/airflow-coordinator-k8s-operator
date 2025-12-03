# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Tests for the Airflow Coordinator charm lib."""

import json
import logging
import pathlib

import charms.airflow_coordinator_k8s.v0.airflow_coordinator_temp as airflow_coordinator
import ops
import ops.testing
import pytest

logger = logging.getLogger(__name__)

AIRFLOW_COORDINATOR_RELATION_INTERFACE = "airflow-coordinator"


class AirflowCoreApplicationCharm(ops.CharmBase):
    """Mock application charm to enable Airflow Coordinator charm libs."""

    def __init__(self, *args):
        super().__init__(*args)

        self.requirer = airflow_coordinator.AirflowCoordinatorRequires(
            self,
            AIRFLOW_COORDINATOR_RELATION_INTERFACE,
            component="scheduler",
            workload_container_name="workload-container",
            callback=self.reconcile,
        )

    def reconcile(self, event) -> None:
        logger.info(f"§Reacting to event: {type(event)}")


@pytest.fixture(scope="function")
def application_context():
    return ops.testing.Context(
        charm_type=AirflowCoreApplicationCharm,
        meta={
            "name": "airflow-core-application",
            "containers": {
                "workload-container": {
                    "resource": "workload-container-image",
                    "channel": "latest/edge",
                    "architectures": [
                        "amd64",
                    ],
                },
            },
            "requires": {
                "airflow-coordinator": {
                    "interface": "airflow_coordinator",
                    "limit": 1,
                },
            },
        },
    )


@pytest.fixture(scope="function")
def coordinator_relation_secret():
    return ops.testing.Secret(
        {
            "sensitive-data": json.dumps(
                {
                    "secret": "s3cret",
                }
            ),
        },
    )


@pytest.fixture(scope="function")
def missing_components_relation_data():
    return {
        "validation-failures": [
            {
                "component": "scheduler",
                "code": airflow_coordinator.MISSING_COMPONENT,
                "message": airflow_coordinator.METADATA_VALIDATION_ERROR_CODE_TO_MESSAGE[
                    airflow_coordinator.MISSING_COMPONENT
                ],
            },
            {
                "component": "triggerer",
                "code": airflow_coordinator.MISSING_COMPONENT,
                "message": airflow_coordinator.METADATA_VALIDATION_ERROR_CODE_TO_MESSAGE[
                    airflow_coordinator.MISSING_COMPONENT
                ],
            },
        ],
    }


@pytest.fixture(scope="function")
def invalid_airflow_version_relation_data():
    return {
        "validation-failures": [
            {
                "component": "scheduler",
                "code": airflow_coordinator.INCONSISTENT_AIRFLOW_VERSION,
                "message": airflow_coordinator.METADATA_VALIDATION_ERROR_CODE_TO_MESSAGE[
                    airflow_coordinator.INCONSISTENT_AIRFLOW_VERSION
                ],
            },
        ],
    }


@pytest.fixture(scope="function")
def invalid_workload_image_hash_relation_data():
    return {
        "validation-failures": [
            {
                "component": "scheduler",
                "code": airflow_coordinator.INCONSISTENT_WORKLOAD_IMAGE_HASH,
                "message": airflow_coordinator.METADATA_VALIDATION_ERROR_CODE_TO_MESSAGE[
                    airflow_coordinator.INCONSISTENT_WORKLOAD_IMAGE_HASH
                ],
            },
        ],
    }


@pytest.fixture(scope="function")
def valid_relation_data(coordinator_relation_secret):
    return {
        "config-template": "test-config",
        "kubernetes-pod-executor-spec": "test-pod-spec",
        "secret-sensitive-data": coordinator_relation_secret.id,
    }


@pytest.fixture(scope="function")
def application_airflow_coordinator_relation(valid_relation_data):
    return ops.testing.Relation(
        "airflow-coordinator", interface="airflow_coordinator", remote_app_data=valid_relation_data
    )


@pytest.fixture(scope="function")
def application_workload_container():
    return ops.testing.Container(
        "workload-container",
        can_connect=True,
    )


@pytest.fixture(scope="function")
def application_state(
    application_workload_container,
    application_airflow_coordinator_relation,
    coordinator_relation_secret,
):
    return ops.testing.State(
        leader=True,
        containers=[application_workload_container],
        relations=[application_airflow_coordinator_relation],
        secrets=[coordinator_relation_secret],
    )


class TestAirflowCoordinatorRequires:
    def test_write_airflow_config(self, application_context, application_state):
        with application_context(application_context.on.start(), application_state) as manager:
            state_out = manager.run()

            assert manager.charm.requirer.write_airflow_config("/config/path")

            filesystem = state_out.get_container("workload-container").get_filesystem(
                application_context
            )

            config_file_path = pathlib.Path(f"{filesystem.absolute()}/config/path")

            assert config_file_path.exists()
            assert config_file_path.is_file()
            assert config_file_path.read_text(encoding="utf-8") == "test-config"
            assert config_file_path.stat().st_mode & 0o777 == 0o644
