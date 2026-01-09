# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Tests for the Airflow Coordinator charm lib."""

import abc
import dataclasses
import json
import logging
import pathlib
import typing

import charms.airflow_coordinator_k8s.v0.airflow_coordinator as airflow_coordinator
import ops
import ops.testing
import pytest

logger = logging.getLogger(__name__)

AIRFLOW_COORDINATOR_RELATION_INTERFACE = "airflow-coordinator"


class AirflowCoreApplicationCharm(ops.CharmBase):
    """Mock application charm to enable testing Airflow Coordinator charm libs."""

    def __init__(self, *args):
        super().__init__(*args)

        self.requirer = airflow_coordinator.AirflowCoordinatorRequires(
            self,
            AIRFLOW_COORDINATOR_RELATION_INTERFACE,
            component="scheduler",
            workload_container=self.unit.get_container("workload-container"),
            callback=self.reconcile,
        )

    def reconcile(self, event) -> None:
        logger.info(f"§Requirer reacting to event: {type(event)}")


class AirflowCoordianatorCharmBase(ops.CharmBase):
    """Mock coordinator charm base class to enable testing Airflow Coordinator charm libs."""

    def __init__(self, *args):
        super().__init__(*args)

        self.provider = airflow_coordinator.AirflowCoordinatorProvides(
            self,
            AIRFLOW_COORDINATOR_RELATION_INTERFACE,
            callback=self.reconcile,
            dependencies_check_callable=self.dependencies_check_callable,
        )

    def reconcile(self, event) -> None:
        logger.info(f"§Provider reacting to event: {type(event)}")

    @abc.abstractmethod
    def dependencies_check_callable(self) -> bool:
        pass


class AirflowCoordianatorCharmReady(AirflowCoordianatorCharmBase):
    """Mock coordinator charm that presents all dependencies available."""

    def __init__(self, *args):
        super().__init__(*args)

    def dependencies_check_callable(self) -> bool:
        return True


class AirflowCoordianatorCharmWaiting(AirflowCoordianatorCharmBase):
    """Mock coordinator charm that presents all dependencies as waiting."""

    def __init__(self, *args):
        super().__init__(*args)

    def dependencies_check_callable(self) -> bool:
        return False


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
        "validation-failures": json.dumps(
            [
                {
                    "component": "scheduler",
                    "code": airflow_coordinator.AirflowCoreValidationErrorEnum.MISSING_COMPONENT,
                },
                {
                    "component": "triggerer",
                    "code": airflow_coordinator.AirflowCoreValidationErrorEnum.MISSING_COMPONENT,
                },
            ],
        ),
    }


@pytest.fixture(scope="function")
def invalid_airflow_version_relation_data():
    return {
        "validation-failures": json.dumps(
            [
                {
                    "component": "scheduler",
                    "code": airflow_coordinator.AirflowCoreValidationErrorEnum.INCONSISTENT_AIRFLOW_VERSION,  # noqa: E501
                },
            ],
        ),
    }


@pytest.fixture(scope="function")
def invalid_workload_image_hash_relation_data():
    return {
        "validation-failures": json.dumps(
            [
                {
                    "component": "scheduler",
                    "code": airflow_coordinator.AirflowCoreValidationErrorEnum.INCONSISTENT_WORKLOAD_IMAGE_HASH,  # noqa: E501
                },
            ],
        ),
    }


@pytest.fixture(scope="function")
def coordinator_awaiting_dependencies_relation_data():
    return {
        "validation-failures": json.dumps(
            [
                {
                    "component": "coordinator",
                    "code": airflow_coordinator.AirflowCoreValidationErrorEnum.WAITING_FOR_DEPENDENCIES,  # noqa: E501
                },
            ],
        ),
    }


@pytest.fixture(scope="function")
def valid_relation_data(coordinator_relation_secret):
    return {
        "config-template": "test-config: {{ secret }}",
        "kubernetes-executor-pod-spec": "test-pod-spec: {{ secret }}",
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


@pytest.fixture(scope="function")
def coordinator_context():
    return ops.testing.Context(
        charm_type=AirflowCoordianatorCharmReady,
        meta={
            "name": "airflow-coordinator-application",
            "provides": {
                "airflow-coordinator": {
                    "interface": "airflow_coordinator",
                },
            },
        },
    )


@pytest.fixture(scope="function")
def coordinator_waiting_context():
    return ops.testing.Context(
        charm_type=AirflowCoordianatorCharmWaiting,
        meta={
            "name": "airflow-coordinator-application",
            "provides": {
                "airflow-coordinator": {
                    "interface": "airflow_coordinator",
                },
            },
        },
    )


def core_charm_relation(
    component: str, airflow_version: str = "3.1.0", workload_image_hash: str = "somehash"
) -> ops.testing.Relation:
    return ops.testing.Relation(
        "airflow-coordinator",
        interface="airflow_coordinator",
        remote_app_data={
            "component": component,
            "airflow_version": airflow_version,
            "workload_image_hash": workload_image_hash,
        },
    )


def generate_coordinator_state(
    component_permutations: dict[str, typing.Any] = {
        "scheduler": {},
        "api-server": {},
        "triggerer": {},
        "dag-processor": {},
    },
) -> ops.testing.State:
    return ops.testing.State(
        leader=True,
        relations=[
            core_charm_relation(component, **variation)
            for component, variation in component_permutations.items()
        ],
    )


class TestAirflowCoordinatorRequires:
    def get_juju_log_line(self, log_level: str, event: ops.EventBase):
        return ops.testing.JujuLogLine(
            level=log_level, message=f"§Requirer reacting to event: {event}"
        )

    def test_airflow_core_validation_failures(
        self, application_context, application_state, application_airflow_coordinator_relation
    ):
        with application_context(
            application_context.on.relation_changed(application_airflow_coordinator_relation),
            application_state,
        ) as manager:
            manager.run()
            assert (
                self.get_juju_log_line("INFO", airflow_coordinator.AirflowConfigAvailableEvent)
                in application_context.juju_log
            )

            assert len(manager.charm.requirer.airflow_core_validation_failures) == 0

    def test_validation_failure_messages(
        self, application_context, application_state, application_airflow_coordinator_relation
    ):
        with application_context(
            application_context.on.relation_changed(application_airflow_coordinator_relation),
            application_state,
        ) as manager:
            manager.run()

            assert len(manager.charm.requirer.validation_failure_messages) == 0

    def test_missing_postgres_relation(
        self,
        application_context,
        application_state,
        application_airflow_coordinator_relation,
        coordinator_awaiting_dependencies_relation_data,
    ):
        relation_awaiting_dependencies = dataclasses.replace(
            application_airflow_coordinator_relation,
            remote_app_data=coordinator_awaiting_dependencies_relation_data,
        )
        state_awaiting_dependencies = dataclasses.replace(
            application_state, relations=[relation_awaiting_dependencies]
        )

        with application_context(
            application_context.on.relation_changed(relation_awaiting_dependencies),
            state_awaiting_dependencies,
        ) as manager:
            manager.run()

            assert not manager.charm.requirer._ready

            assert len(manager.charm.requirer.airflow_core_validation_failures) == 1
            assert len(manager.charm.requirer.validation_failure_messages) == 1

            assert sorted(manager.charm.requirer.validation_failure_messages) == [
                failure["code"]
                for failure in json.loads(
                    coordinator_awaiting_dependencies_relation_data["validation-failures"]
                )
            ]

    def test_airflow_core_validation_failures_with_missing_components_failures(
        self,
        application_context,
        application_state,
        application_airflow_coordinator_relation,
        missing_components_relation_data,
    ):
        relation_missing_commponents = dataclasses.replace(
            application_airflow_coordinator_relation,
            remote_app_data=missing_components_relation_data,
        )
        state_mismatched_airflow_versions = dataclasses.replace(
            application_state, relations=[relation_missing_commponents]
        )

        with application_context(
            application_context.on.relation_changed(relation_missing_commponents),
            state_mismatched_airflow_versions,
        ) as manager:
            manager.run()

            assert len(manager.charm.requirer.airflow_core_validation_failures) == 2

            assert sorted(manager.charm.requirer.airflow_core_validation_failures) == [
                failure["code"]
                for failure in json.loads(missing_components_relation_data["validation-failures"])
            ]

    def test_validation_failure_messages_with_missing_components_failures(
        self,
        application_context,
        application_state,
        application_airflow_coordinator_relation,
        missing_components_relation_data,
    ):
        relation_missing_commponents = dataclasses.replace(
            application_airflow_coordinator_relation,
            remote_app_data=missing_components_relation_data,
        )
        state_mismatched_airflow_versions = dataclasses.replace(
            application_state, relations=[relation_missing_commponents]
        )

        with application_context(
            application_context.on.relation_changed(relation_missing_commponents),
            state_mismatched_airflow_versions,
        ) as manager:
            manager.run()

            assert len(manager.charm.requirer.validation_failure_messages) == 1

            assert sorted(manager.charm.requirer.validation_failure_messages) == [
                failure["code"]
                for failure in json.loads(missing_components_relation_data["validation-failures"])
                if failure["component"] == "scheduler"
            ]

    def test_write_airflow_config(
        self,
        application_context,
        application_state,
        application_airflow_coordinator_relation,
    ):
        with application_context(
            application_context.on.relation_changed(application_airflow_coordinator_relation),
            application_state,
        ) as manager:
            state_out = manager.run()
            assert (
                self.get_juju_log_line("INFO", airflow_coordinator.AirflowConfigAvailableEvent)
                in application_context.juju_log
            )

            assert manager.charm.requirer.can_write_airflow_config

            manager.charm.requirer.write_airflow_config("/config/path")

            filesystem = state_out.get_container("workload-container").get_filesystem(
                application_context
            )

            config_file_path = pathlib.Path(f"{filesystem.absolute()}/config/path")

            assert config_file_path.exists()
            assert config_file_path.is_file()
            assert config_file_path.read_text(encoding="utf-8") == "test-config: s3cret"
            assert config_file_path.stat().st_mode & 0o777 == 0o644

    def test_can_write_airflow_config_with_mismatched_airflow_version_failures(
        self,
        application_context,
        application_state,
        application_airflow_coordinator_relation,
        invalid_airflow_version_relation_data,
    ):
        relation_mismatched_airflow_versions = dataclasses.replace(
            application_airflow_coordinator_relation,
            remote_app_data=invalid_airflow_version_relation_data,
        )
        state_mismatched_airflow_versions = dataclasses.replace(
            application_state, relations=[relation_mismatched_airflow_versions]
        )

        with application_context(
            application_context.on.relation_changed(relation_mismatched_airflow_versions),
            state_mismatched_airflow_versions,
        ) as manager:
            manager.run()
            assert (
                self.get_juju_log_line(
                    "INFO", airflow_coordinator.AirflowCoreMetadataValidationFailed
                )
                in application_context.juju_log
            )

            assert not manager.charm.requirer.can_write_airflow_config

    def test_can_write_airflow_config_with_mismatched_workload_image_hash(
        self,
        application_context,
        application_state,
        application_airflow_coordinator_relation,
        invalid_workload_image_hash_relation_data,
    ):
        relation_mismatched_workload_image_hash = dataclasses.replace(
            application_airflow_coordinator_relation,
            remote_app_data=invalid_workload_image_hash_relation_data,
        )
        state_mismatched_airflow_versions = dataclasses.replace(
            application_state, relations=[relation_mismatched_workload_image_hash]
        )

        with application_context(
            application_context.on.relation_changed(relation_mismatched_workload_image_hash),
            state_mismatched_airflow_versions,
        ) as manager:
            manager.run()
            assert (
                self.get_juju_log_line(
                    "INFO", airflow_coordinator.AirflowCoreMetadataValidationFailed
                )
                in application_context.juju_log
            )

            assert not manager.charm.requirer.can_write_airflow_config

    def test_write_k8s_executor_pod_spec(
        self, application_context, application_state, application_airflow_coordinator_relation
    ):
        with application_context(
            application_context.on.relation_changed(application_airflow_coordinator_relation),
            application_state,
        ) as manager:
            state_out = manager.run()
            assert (
                self.get_juju_log_line("INFO", airflow_coordinator.AirflowConfigAvailableEvent)
                in application_context.juju_log
            )

            assert manager.charm.requirer.can_write_kubernetes_executor_pod_spec

            manager.charm.requirer.write_kubernetes_executor_pod_spec(
                "/k8s_executor_pod_spec/path"
            )

            filesystem = state_out.get_container("workload-container").get_filesystem(
                application_context
            )

            config_file_path = pathlib.Path(f"{filesystem.absolute()}/k8s_executor_pod_spec/path")

            assert config_file_path.exists()
            assert config_file_path.is_file()
            assert config_file_path.read_text(encoding="utf-8") == "test-pod-spec: s3cret"
            assert config_file_path.stat().st_mode & 0o777 == 0o644

    def test_can_write_k8s_executor_pod_spec_with_mismatched_airflow_version_failures(
        self,
        application_context,
        application_state,
        application_airflow_coordinator_relation,
        invalid_airflow_version_relation_data,
    ):
        relation_mismatched_airflow_versions = dataclasses.replace(
            application_airflow_coordinator_relation,
            remote_app_data=invalid_airflow_version_relation_data,
        )
        state_mismatched_airflow_versions = dataclasses.replace(
            application_state, relations=[relation_mismatched_airflow_versions]
        )

        with application_context(
            application_context.on.relation_changed(relation_mismatched_airflow_versions),
            state_mismatched_airflow_versions,
        ) as manager:
            manager.run()
            assert (
                self.get_juju_log_line(
                    "INFO", airflow_coordinator.AirflowCoreMetadataValidationFailed
                )
                in application_context.juju_log
            )

            assert not manager.charm.requirer.can_write_kubernetes_executor_pod_spec

    def test_can_write_k8s_executor_pod_spec_with_mismatched_workload_image_hash(
        self,
        application_context,
        application_state,
        application_airflow_coordinator_relation,
        invalid_workload_image_hash_relation_data,
    ):
        relation_mismatched_workload_image_hash = dataclasses.replace(
            application_airflow_coordinator_relation,
            remote_app_data=invalid_workload_image_hash_relation_data,
        )
        state_mismatched_airflow_versions = dataclasses.replace(
            application_state, relations=[relation_mismatched_workload_image_hash]
        )

        with application_context(
            application_context.on.relation_changed(relation_mismatched_workload_image_hash),
            state_mismatched_airflow_versions,
        ) as manager:
            manager.run()
            assert (
                self.get_juju_log_line(
                    "INFO", airflow_coordinator.AirflowCoreMetadataValidationFailed
                )
                in application_context.juju_log
            )

            assert not manager.charm.requirer.can_write_kubernetes_executor_pod_spec


class TestAirflowCoordinatorProvides:
    def get_juju_log_line(self, log_level: str, event: ops.EventBase):
        return ops.testing.JujuLogLine(
            level=log_level, message=f"§Provider reacting to event: {event}"
        )

    def test_set_validation_errors_with_no_issues(self, coordinator_context):
        state = generate_coordinator_state()

        with coordinator_context(
            coordinator_context.on.relation_changed(state.get_relations("airflow-coordinator")[0]),
            state,
        ) as manager:
            state_out = manager.run()
            assert (
                self.get_juju_log_line(
                    "INFO", airflow_coordinator.AirflowCoreMetadataAvailableEvent
                )
                in coordinator_context.juju_log
            )

            manager.charm.provider.set_validation_errors()

            for relation in state_out.relations:
                assert "validation-failures" not in relation.local_app_data

    def test_provider_methods_with_missing_required_dependencies(
        self, coordinator_waiting_context
    ):
        state = generate_coordinator_state()

        with coordinator_waiting_context(
            coordinator_waiting_context.on.relation_changed(
                state.get_relations("airflow_coordinator")[0]
            ),
            state,
        ) as manager:
            state_out = manager.run()
            assert (
                self.get_juju_log_line(
                    "INFO", airflow_coordinator.AirflowCoreMetadataAvailableEvent
                )
                in coordinator_waiting_context.juju_log
            )

            manager.charm.provider.set_validation_errors()

            for relation in state_out.relations:
                validation_failures = json.loads(relation.local_app_data["validation-failures"])

                assert validation_failures == [
                    {
                        "component": "coordinator",
                        "code": airflow_coordinator.AirflowCoreValidationErrorEnum.WAITING_FOR_DEPENDENCIES,  # noqa: E501
                    }
                ]

    def test_provider_methods_when_missing_components(
        self, coordinator_context, missing_components_relation_data
    ):
        missing_components = {
            failure["component"]
            for failure in json.loads(missing_components_relation_data["validation-failures"])
        }
        present_components = set(airflow_coordinator.AirflowCoreComponentEnum) - missing_components

        state = generate_coordinator_state({component: {} for component in present_components})

        with coordinator_context(
            coordinator_context.on.relation_changed(state.get_relations("airflow-coordinator")[0]),
            state,
        ) as manager:
            state_out = manager.run()
            assert (
                self.get_juju_log_line(
                    "INFO", airflow_coordinator.AirflowCoreMetadataAvailableEvent
                )
                in coordinator_context.juju_log
            )

            assert manager.charm.provider.missing_core_components

            manager.charm.provider.set_validation_errors()

            for relation in state_out.relations:
                assert (
                    relation.local_app_data["validation-failures"]
                    == missing_components_relation_data["validation-failures"]
                )

    def test_provider_methods_when_invalid_airflow_version(
        self, coordinator_context, invalid_airflow_version_relation_data
    ):
        invalid_components = {
            failure["component"]
            for failure in json.loads(invalid_airflow_version_relation_data["validation-failures"])
        }

        component_permutations = {
            component: {"airflow_version": "0.0.0"} for component in invalid_components
        }
        for component in set(airflow_coordinator.AirflowCoreComponentEnum) - invalid_components:
            component_permutations[component] = {}

        state = generate_coordinator_state(component_permutations)

        with coordinator_context(
            coordinator_context.on.relation_changed(state.get_relations("airflow-coordinator")[0]),
            state,
        ) as manager:
            state_out = manager.run()
            assert (
                self.get_juju_log_line(
                    "INFO", airflow_coordinator.AirflowCoreMetadataAvailableEvent
                )
                in coordinator_context.juju_log
            )

            assert not manager.charm.provider.missing_core_components
            assert not manager.charm.provider.are_airflow_versions_consistent
            assert manager.charm.provider.are_workload_image_hashes_consistent

            manager.charm.provider.set_validation_errors()

            for relation in state_out.relations:
                assert (
                    relation.local_app_data["validation-failures"]
                    == invalid_airflow_version_relation_data["validation-failures"]
                )

    def test_provider_methods_when_invalid_workload_image_hash(
        self, coordinator_context, invalid_workload_image_hash_relation_data
    ):
        invalid_components = {
            failure["component"]
            for failure in json.loads(
                invalid_workload_image_hash_relation_data["validation-failures"]
            )
        }

        component_permutations = {
            component: {"workload_image_hash": "0.0.0"} for component in invalid_components
        }
        for component in set(airflow_coordinator.AirflowCoreComponentEnum) - invalid_components:
            component_permutations[component] = {}

        state = generate_coordinator_state(component_permutations)

        with coordinator_context(
            coordinator_context.on.relation_changed(state.get_relations("airflow-coordinator")[0]),
            state,
        ) as manager:
            state_out = manager.run()
            assert (
                self.get_juju_log_line(
                    "INFO", airflow_coordinator.AirflowCoreMetadataAvailableEvent
                )
                in coordinator_context.juju_log
            )

            assert not manager.charm.provider.missing_core_components
            assert manager.charm.provider.are_airflow_versions_consistent
            assert not manager.charm.provider.are_workload_image_hashes_consistent

            manager.charm.provider.set_validation_errors()

            for relation in state_out.relations:
                assert (
                    relation.local_app_data["validation-failures"]
                    == invalid_workload_image_hash_relation_data["validation-failures"]
                )

    def test_set_airflow_config(self, coordinator_context):
        state = generate_coordinator_state()

        with coordinator_context(
            coordinator_context.on.relation_changed(state.get_relations("airflow-coordinator")[0]),
            state,
        ) as manager:
            airflow_config_params = {
                "config_template": "test-config-template",
                "k8s_executor_pod_spec_template": "test-k8s-executor-pod-spec",
                "sensitive_data": {
                    "secret": "s3cret",
                },
            }

            assert manager.charm.provider.set_airflow_config(**airflow_config_params) is None

            state_out = manager.run()
            assert (
                self.get_juju_log_line(
                    "INFO", airflow_coordinator.AirflowCoreMetadataAvailableEvent
                )
                in coordinator_context.juju_log
            )

            for relation in state_out.relations:
                assert "validation-failures" not in relation.local_app_data

                assert (
                    relation.local_app_data["config-template"]
                    == airflow_config_params["config_template"]
                )
                assert (
                    relation.local_app_data["kubernetes-executor-pod-spec"]
                    == airflow_config_params["k8s_executor_pod_spec_template"]
                )

                secret_id = relation.local_app_data["secret-sensitive-data"]
                assert secret_id is not None

                assert state_out.get_secret(id=secret_id).latest_content == {
                    "sensitive-data": json.dumps(airflow_config_params["sensitive_data"])
                }
