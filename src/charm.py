#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""The Airflow Coordinator charm application."""

import logging

import charms.airflow_coordinator_k8s.v0.airflow_coordinator as airflow_coordinator
import charms.data_platform_libs.v0.data_interfaces as data_interfaces_v0
import ops
from ops.pebble import LayerDict

import command_executor
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

        self._container = self.unit.get_container(constants.WORKLOAD_CONTAINER_NAME)
        self._config_generator = config_generator.AirflowConfigGenerator(self)
        self._command_executor = command_executor.CommandExecutor(self._container)

        self._database_requires = data_interfaces_v0.DatabaseRequires(
            self,
            constants.POSTGRES_RELATION_NAME,
            database_name=constants.AIRFLOW_DATABASE_NAME,
        )
        self._config_provider = airflow_coordinator.AirflowCoordinatorProvides(
            self,
            constants.AIRFLOW_COORDINATOR_RELATION_NAME,
            callback=self._reconcile,
            dependencies_check_callable=self._required_dependencies_exist,
        )

        for event in [
            self.on.start,
            self.on.config_changed,
            self.on.secret_changed,
            self.on[constants.WORKLOAD_CONTAINER_NAME].pebble_ready,
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

    @property
    def _airflow_coordinator_layer(self) -> LayerDict:
        """Return the service Pebble layer."""
        # Pebble layer to disable the default airflow service and health check from the rock.
        # The rock runs "airflow standalone" by default with an "airflow-running" health check,
        # but the coordinator only needs to run "airflow db migrate" and should not run
        # the service.
        layer: LayerDict = {
            "summary": "Airflow coordinator layer",
            "description": "Pebble layer for Airflow coordinator to run db migrations",
            "services": {
                "airflow": {
                    "override": "merge",
                    "startup": "disabled",
                }
            },
            "checks": {
                "airflow-running": {
                    "override": "replace",
                    "level": "alive",
                    "exec": {
                        "command": "/bin/true",
                    },
                }
            },
        }
        return layer

    @property
    def _peer_relation(self) -> ops.Relation:
        """Return the peer relation.

        Raises:
            ExceptionWithStatusError: If peer relation is not available.
        """
        peer_relation = self.model.get_relation(constants.PEER_RELATION_NAME)
        if not peer_relation:
            raise ExceptionWithStatusError(
                constants.WAITING_FOR_PEER_RELATION_MESSAGE, ops.WaitingStatus
            )
        return peer_relation

    @property
    def _db_migration_ran(self) -> bool:
        """Check if database migration has been run."""
        return self._peer_relation.data[self.app].get("db_migration_ran") == "true"

    @_db_migration_ran.setter
    def _db_migration_ran(self, value: bool) -> None:
        """Set database migration state."""
        # We need to cast the bool to str, Juju relation data bags can only store strings
        self._peer_relation.data[self.app]["db_migration_ran"] = str(value).lower()

    def _required_dependencies_exist(self) -> bool:
        """Returns whether all required dependencies for the coordinator exist."""
        # TODO: add k8s executor configurator relation here too
        return all(
            [
                self.model.get_relation(constants.POSTGRES_RELATION_NAME),
            ]
        )

    def _perform_checks(self) -> None:  # noqa: C901
        """Checks to ensure the charm is able to generate and distribute configs."""
        if self.config is None:
            raise ExceptionWithStatusError(
                constants.WAITING_FOR_CHARM_SETUP_MESSAGE, ops.WaitingStatus
            )

        if self.config.get(constants.SENSITIVE_CUSTOM_CONFIG):
            try:
                self.model.get_secret(
                    id=self.config[constants.SENSITIVE_CUSTOM_CONFIG],
                )
            except ops.SecretNotFoundError as e:  # SecretNotFoundError is a subclass of ModelError
                logger.error(e)
                raise ExceptionWithStatusError(
                    constants.CUSTOM_CONFIG_SECRET_NOT_FOUND, ops.BlockedStatus
                )
            except ops.ModelError as e:
                logger.error(e)
                raise ExceptionWithStatusError(
                    constants.UNAUTHORIZED_ACCESS_TO_SECRET_MESSAGE, ops.BlockedStatus
                )

        if not self._container.can_connect():
            raise ExceptionWithStatusError(
                constants.WAITING_FOR_CONTAINER_MESSAGE, ops.WaitingStatus
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

    def _configure_pebble_layer(self) -> None:
        """Configure the Pebble layer to disable the default airflow service.

        The rock image runs 'airflow standalone' by default, but the coordinator
        only needs to run 'airflow db migrate'. This method disables the default
        service to ensure Airflow components are not running during migration.
        """
        self._container.add_layer(
            "airflow-coordinator", self._airflow_coordinator_layer, combine=True
        )
        self._container.replan()

    def _write_airflow_config(self) -> None:
        """Write the Airflow configuration file to the container."""
        airflow_coordinator.write_airflow_config(
            self._container,
            constants.AIRFLOW_CONFIG_PATH,
            self._config_generator.config_template,
            self._config_generator.sensitive_config_values,
        )

    def _run_db_migrate(self) -> None:
        """Run database migration in the workload container."""
        result = self._command_executor.run_db_migrate()
        if not result.success:
            raise ExceptionWithStatusError(
                constants.DB_MIGRATION_FAILED_MESSAGE, ops.BlockedStatus
            )

    def _reconcile(self, event: ops.EventBase) -> None:
        """Idempotent reconcile method to handle most relevant charm events."""
        if not self.unit.is_leader():
            return

        try:
            self._perform_checks()
            self._configure_pebble_layer()
            self._write_airflow_config()

            # Use peer relation data to track if db migration has run
            # TODO: once we have upgrade logic, we'll need to change the
            # conditions under which this state will be True/False.
            if not self._db_migration_ran:
                self._run_db_migrate()
                self._db_migration_ran = True

            self._config_provider.set_airflow_config(
                self._config_generator.config_template,
                sensitive_data=self._config_generator.sensitive_config_values,
            )
        except ExceptionWithStatusError as e:
            logger.error(e)
            self.unit.status = e.status
            return
        except command_executor.CommandExecutionError as e:
            logger.error(e)
            self.unit.status = ops.BlockedStatus(e.message)
            return

        self.unit.status = ops.ActiveStatus()


if __name__ == "__main__":  # pragma: nocover
    ops.main(AirflowCoordinatorK8SOperatorCharm)
