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
            self,
            constants.AIRFLOW_COORDINATOR_RELATION_NAME,
            callback=self._reconcile,
            dependencies_check_callable=self._required_dependencies_exist,
        )

        # TODO: confirm if we can observe custom config secret changed events
        for event in [
            self.on.start,
            self.on.config_changed,
            self._database_requires.on.database_created,
            self._database_requires.on.endpoints_changed,
            self.on[constants.POSTGRES_RELATION_NAME].relation_broken,
            self.on.update_status,
        ]:
            self.framework.observe(event, self._reconcile)

    @property
    def _all_database_connection_details_present(self) -> None:
        """Confirm if all database connection details present in postgres relation."""
        if not self._database_requires.relations:
            return False

        postgres_relation_id = self._database_requires.relations[0].id
        postgres_relation_data = self._database_requires.fetch_relation_data(
            [postgres_relation_id]
        )[postgres_relation_id]

        return all(
            field in postgres_relation_data
            for field in ["username", "password", "endpoints", "database"]
        )

    def _required_dependencies_exist(self) -> bool:
        """Returns whether all required dependencies for the coordinator exist."""
        # TODO: add k8s executor configurator relation here too
        return all(
            [
                self.model.get_relation(constants.POSTGRES_RELATION_NAME),
            ]
        )

    def _perform_checks(self) -> None:
        """Checks to ensure the charm is able to generate and distribute configs."""
        if not self.config:
            raise ExceptionWithStatusError(
                constants.WAITING_FOR_CHARM_SETUP_MESSAGE, ops.WaitingStatus
            )

        if self.config[constants.SENSITIVE_CUSTOM_CONFIG]:
            try:
                self.model.get_secret(id=self.config[constants.SENSITIVE_CUSTOM_CONFIG], label=constants.SENSITIVE_CUSTOM_CONFIG_LABEL)
            except ops.ModelError:
                raise ExceptionWithStatusError(
                    constants.UNAUTHORIZED_ACCESS_TO_SECRET_MESSAGE, ops.BlockedStatus
                )
            except ops.SecretNotFoundError:
                raise ExceptionWithStatusError(
                    constants.CUSTOM_CONFIG_SECRET_NOT_FOUND, ops.BlockedStatus
                )

        if not self.model.get_relation(constants.POSTGRES_RELATION_NAME):
            self._config_provider.set_validation_errors()
            raise ExceptionWithStatusError(
                constants.MISSING_POSTGRES_INTEGRATION_MESSAGE, ops.BlockedStatus
            )

        if not self._database_requires.is_resource_created():
            raise ExceptionWithStatusError(
                constants.WAITING_FOR_DATABASE_TO_BE_CREATED_MESSAGE, ops.WaitingStatus
            )

        if not self._all_database_connection_details_present:
            raise ExceptionWithStatusError(
                constants.WAITING_FOR_DATABASE_CONNECTION_MESSAGE, ops.WaitingStatus
            )

        missing_core_components = self._config_provider.missing_core_components
        if missing_core_components:
            self._config_provider.set_validation_errors()

            raise ExceptionWithStatusError(
                constants.MISSING_INTEGRATIONS_MESSAGE_TEMPLATE.format(
                    missing_core_components=", ".join(missing_core_components)
                ),
                ops.BlockedStatus,
            )

        if not self._config_provider.are_airflow_versions_consistent:
            self._config_provider.set_validation_errors()

            raise ExceptionWithStatusError(
                constants.MISMATCHED_AIRFLOW_VERSIONS_MESSAGE, ops.BlockedStatus
            )

        if not self._config_provider.are_workload_image_hashes_consistent:
            self._config_provider.set_validation_errors()

            raise ExceptionWithStatusError(
                constants.MISMATCHED_WORKLOAD_IMAGE_HASHES_MESSAGE, ops.BlockedStatus
            )

        if self._config_generator.do_custom_configs_overlap:
            raise ExceptionWithStatusError(
                constants.CUSTOM_CONFIG_OVERLAP_MESSAGE, ops.BlockedStatus
            )

        if self._config_generator.custom_configs_have_blacklisted_keys:
            raise ExceptionWithStatusError(
                constants.CUSTOM_CONFIG_HAS_BLACKLIST_KEY, ops.BlockedStatus
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
            logger.error(e)
            self.unit.status = e.status
            return

        self.unit.status = ops.ActiveStatus()


if __name__ == "__main__":  # pragma: nocover
    ops.main(AirflowCoordinatorK8SOperatorCharm)
