#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""A mock Airflow Core charm to be used in testing the Airflow Coordinator charm.."""

import logging

import ops
from charms.airflow_coordinator_k8s.v0 import airflow_coordinator

logger = logging.getLogger(__name__)

AIRFLOW_COORDINATOR_RELATION_NAME = "airflow-coordinator"
AIRFLOW_API_SERVER_RELATION_NAME = "airflow-api-server"
CONTAINER_NAME = "workload"
AIRFLOW_CONFIG_PATH = "/airflow.cfg"
K8S_EXECUTOR_POD_SPEC_PATH = "/k8s_executor_pod_spec"


class MockCoreCharmCharm(ops.CharmBase):
    """Charm that indicates the state of relation with Airflow Coordinator."""

    def __init__(self, framework: ops.Framework):
        super().__init__(framework)

        if self.config and all(
            config in self.config
            for config in ["component", "airflow_version", "workload_image_hash"]
        ):
            self.config_requirer = airflow_coordinator.AirflowCoordinatorCoreRequires(
                self,
                AIRFLOW_COORDINATOR_RELATION_NAME,
                self.config["component"],
                self.unit.get_container(CONTAINER_NAME),
                self.log_event_and_set_status,
            )
        else:
            self.unit.status = ops.BlockedStatus("Missing required config")
            return

        for event in [
            self.on.start,
            self.on.update_status,
            self.on[AIRFLOW_COORDINATOR_RELATION_NAME].relation_joined,
            self.on[AIRFLOW_COORDINATOR_RELATION_NAME].relation_changed,
            self.on[AIRFLOW_COORDINATOR_RELATION_NAME].relation_broken,
            self.on[AIRFLOW_API_SERVER_RELATION_NAME].relation_joined,
            self.on[AIRFLOW_API_SERVER_RELATION_NAME].relation_changed,
            self.on[AIRFLOW_API_SERVER_RELATION_NAME].relation_broken,
        ]:
            self.framework.observe(event, self.log_event_and_set_status)

        self.framework.observe(
            self.on[AIRFLOW_API_SERVER_RELATION_NAME].relation_joined,
            self._populate_api_server_relation,
        )

        self.framework.observe(self.on.check_ready_action, self._check_ready)
        self.framework.observe(self.on.get_airflow_config_action, self._get_airflow_config)

        self.framework.observe(
            self.on.check_can_write_kubernetes_executor_pod_spec_action,
            self._check_can_write_kubernetes_executor_pod_spec,
        )
        self.framework.observe(
            self.on.get_kubernetes_executor_pod_spec_action,
            self._on_get_kubernetes_executor_pod_spec,
        )

        self.framework.observe(
            self.on.get_relation_sensitive_data_action, self._get_relation_sensitive_data
        )
        self.framework.observe(
            self.on.get_component_validation_failures_action,
            self._get_component_validation_failures,
        )
        self.framework.observe(
            self.on.get_all_validation_failures_action, self._get_all_validation_failures
        )

        self.framework.observe(self.on.clean_files_action, self._clean_files)

    def log_event_and_set_status(self, event) -> None:
        """Log info about the handled event + sets unit statuses based on relation data."""
        logger.info(f"§Handled event: {event}")

        if not self.model.get_relation(AIRFLOW_COORDINATOR_RELATION_NAME):
            self.unit.status = ops.BlockedStatus("Missing relation with Airflow Coordinator")
            return

        if self.config_requirer.missing_core_components_exist:
            self.unit.status = ops.BlockedStatus("Missing core components exist")
            return

        if self.config_requirer.validation_failure_messages:
            self.unit.status = ops.BlockedStatus("Validation failures for this component exist")
            return

        if self.config_requirer.can_write_airflow_config:
            self.config_requirer.write_airflow_config(AIRFLOW_CONFIG_PATH)
        else:
            self.unit.status = ops.BlockedStatus("Waiting for config from coordinator")
            return

        self.unit.status = ops.ActiveStatus()

    def _populate_api_server_relation(self, event: ops.RelationJoinedEvent) -> None:
        """Populate api-server relation with host+port data."""
        event.relation.data[self.app]["host"] = (
            f"{self.app.name}-endpoints.{self.model.name}.svc.cluster.local"
        )
        event.relation.data[self.app]["port"] = "8080"

    def _check_ready(self, event: ops.ActionEvent) -> None:
        """Exposes whether relation indicates that the Airflow config can be written."""
        event.set_results(
            {
                "ready": self.config_requirer._ready,
            },
        )

    def _get_airflow_config(self, event: ops.ActionEvent) -> None:
        """Get the Airflow config from the workload container."""
        try:
            file = self.unit.get_container(CONTAINER_NAME).pull(
                AIRFLOW_CONFIG_PATH, encoding="utf-8"
            )

            event.set_results(
                {
                    "airflow-config": file.read(),
                },
            )
        except:  # noqa: E722
            event.fail("Unable to get the Airflow config from the workload container")

    def _check_can_write_kubernetes_executor_pod_spec(self, event: ops.ActionEvent) -> None:
        """Exposes whether relation indicates that the K8s executor pod spec can be written."""
        pod_spec = self.config_requirer.can_write_kuberenetes_executor_pod_spec

        event.set_results(
            {
                "can-write-kuberenetes-executor-pod-spec": pod_spec,
            },
        )

    def _on_get_kubernetes_executor_pod_spec(self, event: ops.ActionEvent) -> None:
        """Get the K8s executor pod spec from the workload container."""
        try:
            file = self.unit.get_container(CONTAINER_NAME).pull(
                K8S_EXECUTOR_POD_SPEC_PATH, encoding="utf-8"
            )

            event.set_results(
                {
                    "kubernetes-executor-pod-spec": file.read(),
                },
            )
        except:  # noqa: E722
            event.fail("Unable to get the K8s executor pod spec from the workload container")

    def _get_relation_sensitive_data(self, event: ops.ActionEvent) -> None:
        """Retrieve sensitive data from juju secret in the relation with coordinator."""
        provider_content = self.config_requirer._requirer_handler.provider_content

        if any(
            [
                not provider_content,
                not provider_content.secret_sensitive_data,
                not provider_content.sensitive_data,
            ]
        ):
            event.fail("Sensitive data not available in coordinator relation")
            return

        event.set_results(
            {
                "sensitive-data": provider_content.sensitive_data,
            },
        )

    def _get_component_validation_failures(self, event: ops.ActionEvent) -> None:
        """Retrieve this component's validation failures shared by the coordinator."""
        event.set_results(
            {
                "validation-failures": self.config_requirer.validation_failures,
            }
        )

    def _get_all_validation_failures(self, event: ops.ActionEvent) -> None:
        """Retrieve all components' validation failures shared by the coordinator."""
        event.set_results(
            {
                "all-validation-failures": self.config_requirer.airflow_core_validation_failures,
            },
        )

    def _clean_files(self, event: ops.ActionEvent) -> None:
        """Clean airflow config + k8s executor pod spec files."""
        container = self.unit.get_container(CONTAINER_NAME)

        # recursive=True to ensure rm -f in case file does not exist
        container.remove_path(AIRFLOW_CONFIG_PATH, recursive=True)
        container.remove_path(K8S_EXECUTOR_POD_SPEC_PATH, recursive=True)

        event.set_results({})


if __name__ == "__main__":  # pragma: nocover
    ops.main(MockCoreCharmCharm)
