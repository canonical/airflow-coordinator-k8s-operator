#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""The Airflow Coordinator charm application."""

import logging

import charms.airflow_coordinator_k8s.v0.airflow_coordinator as airflow_coordinator
import charms.data_platform_libs.v0.data_interfaces as data_interfaces_v0
import ops

import config_generator
import constants

logger = logging.getLogger(__name__)


class ExceptionWithStatusError(Exception):
    """Base class of exceptions for when a method has an opinion on the unit status."""

    def __init__(self, message: str, status_type):
        super().__init__(str(message))
        self.message = str(message)
        self.status_type = status_type

    @property
    def status(self):
        """Returns an instance of self.status_type with a message."""
        return self.status_type(self.message)


class AirflowCoordinatorK8SOperatorCharm(ops.CharmBase):
    """Charm the application."""

    def __init__(self, framework: ops.Framework):
        super().__init__(framework)

        self._config_generator = config_generator.AirflowConfigGenerator(self)

        self._database_requires = data_interfaces_v0.DatabaseRequires(
            self, constants.POSTGRES_RELATION_NAME, database_name=constants.AIRFLOW_DATABASE_NAME
        )
        self._config_provider = airflow_coordinator.AirflowCoordinatorProvides(
            self, constants.AIRFLOW_COORDINATOR_RELATION_NAME, callback=self._reconcile
        )

        for event in [
            self.on.start,
            self.on.update_status,
            self._database_requires.on.database_created,
            self._database_requires.on.endpoints_changed,
            self.on[constants.POSTGRES_RELATION_NAME].relation_broken,
        ]:
            self.framework.observe(event, self._reconcile)

    def _perform_checks(self) -> None:
        """Checks to ensure the charm is able to generate and distribute configs."""
        if not self.model.get_relation(constants.POSTGRES_RELATION_NAME):
            raise ExceptionWithStatusError("Missing integration with postgres", ops.BlockedStatus)

        if not self._database_requires.is_resource_created():
            raise ExceptionWithStatusError(
                "Waiting for airflow database to be created", ops.WaitingStatus
            )

        missing_core_components = self._config_provider.missing_core_components
        if missing_core_components:
            self._config_provider.set_validation_errors()

            raise ExceptionWithStatusError(
                f"Missing integrations with: {', '.join(missing_core_components)}",
                ops.BlockedStatus,
            )

        if not self._config_provider.are_airflow_versions_consistent:
            self._config_provider.set_validation_errors()

            raise ExceptionWithStatusError(
                "Integrated apps with mismatched airflow versions", ops.BlockedStatus
            )

        if not self._config_provider.are_workload_image_hashes_consistent:
            self._config_provider.set_validation_errors()

            raise ExceptionWithStatusError(
                "Integrated apps with mismatched workload image hashes", ops.BlockedStatus
            )

    def _reconcile(self, _) -> None:
        """Idempotent reconcile method to handle most relevant charm events."""
        if not self.unit.is_leader():
            return

        try:
            self._perform_checks()

            self._config_provider.set_airflow_config(
                self._config_generator.config_template,
                sensitive_data=self._config_generator.sensitive_config_values,
            )
        except ExceptionWithStatusError as e:
            self.unit.status = e.status
            return

        self.unit.status = ops.ActiveStatus()


if __name__ == "__main__":  # pragma: nocover
    ops.main(AirflowCoordinatorK8SOperatorCharm)
