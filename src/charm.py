#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""The Airflow Coordinator charm application."""

import logging

import charms.data_platform_libs.v0.data_interfaces as data_interfaces_v0
import ops

# TODO: change to official charm lib name after charmhub registration + lib is published
from charms.airflow_coordinator_k8s.v0.airflow_coordinator_temp import AirflowCoordinatorProvides

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
        self._config_provider = AirflowCoordinatorProvides(
            self, constants.AIRFLOW_COORDINATOR_RELATION_NAME, callback=self._reconcile
        )

        self._database_requires = data_interfaces_v0.DatabaseRequires(
            self, constants.POSTGRES_RELATION_NAME, database_name=constants.AIRFLOW_DATABASE_NAME
        )
        self.framework.observe(self._database_requires.on.database_created, self._reconcile)
        self.framework.observe(self._database_requires.on.endpoints_changed, self._reconcile)
        self.framework.observe(
            self.on[constants.POSTGRES_RELATION_NAME].relation_broken, self._reconcile
        )

        self.framework.observe(self.on.start, self._reconcile)
        self.framework.observe(self.on.update_status, self._reconcile)

    def _is_ready(self, raise_exceptions: bool = True) -> bool:
        """Check whether the charm is ready; all necessary relations active."""
        # TODO: restrict the application to 1 unit?
        if not self.unit.is_leader():
            return False

        if not self.model.get_relation(constants.POSTGRES_RELATION_NAME):
            if raise_exceptions:
                raise ExceptionWithStatusError("Missing relation with postgres", ops.BlockedStatus)

            return False

        if not self._database_requires.is_resource_created():
            if raise_exceptions:
                raise ExceptionWithStatusError(
                    "Waiting for airflow database to be created", ops.WaitingStatus
                )

            return False

        error_message = self._config_provider.validate_core_components()
        if error_message:
            if raise_exceptions:
                raise ExceptionWithStatusError(error_message, ops.BlockedStatus)

            return False

        return True

    def _reconcile(self, _) -> None:
        """Idempotent reconcile method to handle most relevant charm events."""
        try:
            if not self._is_ready():
                return
        except ExceptionWithStatusError as e:
            self.unit.status = e.status
            return

        self._config_provider.set_airflow_config(
            self._config_generator.config_template,
            sensitive_data=self._config_generator.sensitive_config_values,
        )

        self.unit.status = ops.ActiveStatus()


if __name__ == "__main__":  # pragma: nocover
    ops.main(AirflowCoordinatorK8SOperatorCharm)
