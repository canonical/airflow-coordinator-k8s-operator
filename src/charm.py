#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""The Airflow Coordinator charm application."""

import json
import logging
import secrets
import zoneinfo

import charms.airflow_api_server_k8s.v0.airflow_api_server as airflow_api_server
import charms.airflow_coordinator_k8s.v0.airflow_coordinator as airflow_coordinator
import charms.data_platform_libs.v0.data_interfaces as data_interfaces_v0
import charms.git_integrator.v0.git as git
import charms.hydra.v0.oauth as oauth
import charms.spark_integration_hub_k8s.v0.spark_service_account as spark_service_account
import mergedeep
import object_storage
import ops
from ops.pebble import LayerDict

import command_executor
import config_generator
import connection_manager
import constants
import webserver_config_generator

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
        self._webserver_config_generator = webserver_config_generator.WebserverConfigGenerator(
            self
        )
        self._command_executor = command_executor.CommandExecutor(self._container)
        self._connection_manager = connection_manager.AirflowConnectionManager(
            self, self._container
        )

        self._database_requires = data_interfaces_v0.DatabaseRequires(
            self,
            constants.POSTGRES_RELATION_NAME,
            database_name=f"{self.app.name}_{self.model.uuid}".replace("-", "_")[:63],
        )
        self._config_provider = airflow_coordinator.AirflowCoordinatorProvides(
            self,
            constants.AIRFLOW_COORDINATOR_RELATION_NAME,
            callback=self._reconcile,
            dependencies_check_callable=self._required_dependencies_exist,
        )
        self._api_server_requires = airflow_api_server.AirflowAPIServerRequires(
            self,
            constants.AIRFLOW_API_SERVER_ENDPOINT_NAME,
            callback=self._reconcile,
        )
        self._s3_requires = object_storage.S3Requirer(self, constants.S3_ENDPOINT_NAME)

        self._git_requires = git.GitRequires(self, constants.GIT_ENDPOINT_NAME, self._reconcile)

        self._kubernetes_executor_requires = airflow_coordinator.AirflowCoordinatorRequires(
            self,
            constants.AIRFLOW_KUBERNETES_EXECUTOR_CONFIG_RELATION_NAME,
            callback=self._reconcile,
        )

        self._oauth_requirer = oauth.OAuthRequirer(
            self, relation_name=constants.OAUTH_ENDPOINT_NAME
        )
        self._spark_service_account_requirer = spark_service_account.SparkServiceAccountRequirer(
            self,
            relation_name=constants.SPARK_SERVICE_ACCOUNT_RELATION_NAME,
            service_account=f"{constants.SPARK_NAMESPACE}:{constants.SPARK_USERNAME}",
            skip_creation=False,
        )

        for event in [
            self.on.start,
            self.on.config_changed,
            self.on.update_status,
            self.on[constants.WORKLOAD_CONTAINER_NAME].pebble_ready,
            self._database_requires.on.database_created,
            self._database_requires.on.endpoints_changed,
            self.on[constants.POSTGRES_RELATION_NAME].relation_broken,
            self.on[constants.AIRFLOW_KUBERNETES_EXECUTOR_CONFIG_RELATION_NAME].relation_changed,
            self._s3_requires.on.storage_connection_info_changed,
            self._s3_requires.on.storage_connection_info_gone,
            self._oauth_requirer.on.oauth_info_changed,
            self._oauth_requirer.on.oauth_info_removed,
            self._spark_service_account_requirer.on.account_granted,
            self._spark_service_account_requirer.on.account_gone,
            self._spark_service_account_requirer.on.properties_changed,
        ]:
            self.framework.observe(event, self._reconcile)

    @property
    def _all_database_connection_details_present(self) -> bool:
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
    def _spark_service_account_data(self) -> dict | None:
        """Return Spark service account namespace and username from the Integration Hub relation."""
        relation = self.model.get_relation(constants.SPARK_SERVICE_ACCOUNT_RELATION_NAME)
        if not relation:
            return None
        try:
            service_account = self._spark_service_account_requirer.fetch_relation_field(
                relation.id, "service-account"
            )
            if not service_account:
                return None
            namespace, username = service_account.split(":", 1)
            return {"spark_namespace": namespace, "spark_username": username}
        except Exception:
            return None

    @property
    def _peer_application_data(self) -> ops.RelationDataContent:
        """Return the application databag of the peer relation."""
        return self._peer_relation.data[self.app]

    def _generate_keys_secret_content(self) -> dict[str, str]:
        """Generate the content for the airflow keys secret."""
        return {
            "secret-key": secrets.token_hex(32),
            "jwt-secret": secrets.token_hex(32),
        }

    def _ensure_airflow_keys_generated(self) -> None:
        """Ensure the Juju application secret holding airflow keys exists.

        Creates the secret on first call and stores the secret ID in the
        peer relation databag.  Subsequent calls are a no-op.
        """
        if self._peer_application_data.get(constants.AIRFLOW_KEYS_SECRET):
            return

        try:
            content = self._generate_keys_secret_content()
            keys_secret = self.app.add_secret(content, label=constants.AIRFLOW_KEYS_SECRET_LABEL)
            if not keys_secret.id:
                raise ValueError("Secret created without an ID")
            self._peer_application_data[constants.AIRFLOW_KEYS_SECRET] = keys_secret.id
        except ValueError:
            logger.error(constants.AIRFLOW_KEYS_SECRET_ADD_ERROR_MESSAGE)
            raise ExceptionWithStatusError(
                constants.AIRFLOW_KEYS_SECRET_ADD_ERROR_MESSAGE, ops.BlockedStatus
            )

    def get_keys_secret(self) -> ops.Secret:
        """Retrieve the Juju application secret containing airflow keys."""
        keys_secret_id = self._peer_application_data.get(constants.AIRFLOW_KEYS_SECRET)
        if not keys_secret_id:
            raise ExceptionWithStatusError(
                constants.AIRFLOW_KEYS_SECRET_ERROR_MESSAGE, ops.BlockedStatus
            )
        try:
            return self.model.get_secret(
                id=keys_secret_id, label=constants.AIRFLOW_KEYS_SECRET_LABEL
            )
        except (ops.SecretNotFoundError, ops.ModelError):
            logger.error(constants.AIRFLOW_KEYS_SECRET_ERROR_MESSAGE)
            raise ExceptionWithStatusError(
                constants.AIRFLOW_KEYS_SECRET_ERROR_MESSAGE, ops.BlockedStatus
            )

    @property
    def _fernet_key(self) -> str:
        """Fernet key from juju user secret in configuration."""
        try:
            secret = self.model.get_secret(
                id=self.config[constants.FERNET_KEY_SECRET_CONFIG],
                label=constants.FERNET_KEY,
            )
        except (ops.SecretNotFoundError, ops.ModelError):
            logger.exception("Issue retrieving fernet key secret")
            raise ExceptionWithStatusError(
                constants.INVALID_FERNET_KEY_SECRET_MESSAGE,
                ops.BlockedStatus,
            )

        fernet_key = secret.get_content().get(constants.FERNET_KEY)
        if not fernet_key:
            raise ExceptionWithStatusError(
                constants.MISSING_FERNET_KEY_IN_SECRET_MESSAGE,
                ops.BlockedStatus,
            )

        return fernet_key

    @property
    def _db_migration_ran(self) -> bool:
        """Check if database migration has been run."""
        return self._peer_application_data.get("db_migration_ran") == "true"

    @_db_migration_ran.setter
    def _db_migration_ran(self, value: bool) -> None:
        """Set database migration state."""
        # We need to cast the bool to str, Juju relation data bags can only store strings
        self._peer_application_data["db_migration_ran"] = str(value).lower()

    @property
    def s3_relation_connections(self) -> dict[int, connection_manager.S3ConnectionInfo]:
        """S3 connections for DAG bundles from related S3 integrator charms."""
        if not self._s3_requires.relations:
            return {}

        # valid relations include:
        # - empty databags (waiting for relation data) => S3ConnectionInfo = None
        # - relation data that contains bucket, access-key and secret-key => S3ConnectionInfo
        return {
            relation.id: connection_info
            for relation in self._s3_requires.relations
            if relation
            and (
                connection_info := connection_manager.S3ConnectionInfo.from_s3_info(
                    self._s3_requires.get_storage_connection_info(relation)
                )
            )
            is not None
        }

    @property
    def git_relation_connections(self) -> dict[int, git.GitProviderModel]:
        """Git connections for DAG bundles from relation git integrator charms."""
        if not self._git_requires.relations:
            return {}

        return self._git_requires.get_git_connection_information()

    @property
    def s3_tls_ca_chains(self) -> dict[str, str]:
        """TLS CA chain paths for S3 connections."""
        return {
            constants.TLS_CA_CHAIN_FILEPATH_TEMPLATE.format(
                filename=f"s3_relation_{relation_id}_connection"
            ): tls_ca_chain
            for relation_id, connection_info in self.s3_relation_connections.items()
            if (tls_ca_chain := "\n".join(connection_info.tls_ca_chain))
        }

    def _required_dependencies_exist(self) -> bool:
        """Returns whether all required dependencies for the coordinator exist."""
        return all(
            [
                self.model.get_relation(constants.POSTGRES_RELATION_NAME),
                self.model.get_relation(constants.AIRFLOW_API_SERVER_ENDPOINT_NAME),
            ]
        )

    @property
    def _kubernetes_executor_config(self) -> dict | None:
        """Return the parsed executor config dict from the executor relation.

        The executor stores its config as a JSON-serialised dict in the
        ``config_template`` field of the relation databag.

        Returns None when the executor relation is not established or
        the executor has not yet shared its configuration.
        """
        if not self.model.get_relation(constants.AIRFLOW_KUBERNETES_EXECUTOR_CONFIG_RELATION_NAME):
            return None
        content = self._kubernetes_executor_requires.provider_content
        if not content or not content.config_template:
            logger.warning(constants.WAITING_FOR_KUBERNETES_EXECUTOR_CONFIG_MESSAGE)
            return None
        if isinstance(content.config_template, dict):
            return content.config_template
        try:
            return json.loads(content.config_template)
        except (json.JSONDecodeError, TypeError):
            logger.warning(constants.WAITING_FOR_KUBERNETES_EXECUTOR_CONFIG_MESSAGE)
            return None

    @property
    def _kubernetes_executor_pod_spec(self) -> str | None:
        """Return the rendered pod spec template from the K8s executor relation.

        Returns None when the executor relation is not established or
        the executor has not yet shared its pod spec.
        """
        if not self.model.get_relation(constants.AIRFLOW_KUBERNETES_EXECUTOR_CONFIG_RELATION_NAME):
            return None
        content = self._kubernetes_executor_requires.provider_content
        if not content or not content.kubernetes_executor_pod_spec:
            logger.warning(constants.WAITING_FOR_KUBERNETES_EXECUTOR_CONFIG_MESSAGE)
            return None
        return content.kubernetes_executor_pod_spec

    @property
    def _oauth_active(self) -> bool:
        """Return True when the oauth relation has valid provider credentials."""
        if not self.model.get_relation(constants.OAUTH_ENDPOINT_NAME):
            return False

        provider_info = self._oauth_requirer.get_provider_info()

        return bool(provider_info and provider_info.client_id and provider_info.client_secret)

    @property
    def _airflow_config_template(self) -> str:
        """Airflow config template merged with additional runtime compiled configs."""
        return self._config_generator.config_template_with_extra_config(
            **mergedeep.merge(
                {},
                self._config_generator.api_server_uri_config,
                self._config_generator.dag_bundle_config,
                (self._kubernetes_executor_config or {}),
                self._config_generator.coordinator_charm_core_config,
                self._config_generator.auth_manager_config,
            ),
        )

    def _validate_configs(self):
        """Ensure validity of charm configurations."""
        if not self.config:
            return

        if not self.config.get(constants.FERNET_KEY_SECRET_CONFIG):
            raise ExceptionWithStatusError(
                constants.MISSING_FERNET_KEY_SECRET_CONFIG_MESSAGE,
                ops.BlockedStatus,
            )

        # Ensures validity of provided secret and proper retrieval of fernet key
        self._fernet_key

        try:
            timezone = self.config.get(constants.CORE_DEFAULT_TIMEZONE_CONFIG, "")

            if timezone.lower() not in ["utc", "system"]:
                zoneinfo.ZoneInfo(timezone)
        except zoneinfo.ZoneInfoNotFoundError:
            raise ExceptionWithStatusError(
                constants.INVALID_CONFIG_MESSAGE.format(
                    config_name=constants.CORE_DEFAULT_TIMEZONE_CONFIG
                ),
                ops.BlockedStatus,
            )

        for config, first_valid_value in [
            (constants.CORE_MAX_ACTIVE_RUNS_PER_DAG_CONFIG, 0),
            (constants.CORE_MAX_ACTIVE_TASKS_PER_DAG_CONFIG, 0),
            (constants.CORE_PARALLELISM_CONFIG, 0),
            (constants.DATABASE_SQL_ALCHEMY_POOL_SIZE_CONFIG, 0),
            (constants.DAG_PROCESSOR_PARSING_PROCESSES_CONFIG, 1),
            (constants.TRIGGERER_CAPACITY_CONFIG, 1),
        ]:
            if self.config[config] < first_valid_value:
                raise ExceptionWithStatusError(
                    constants.INVALID_CONFIG_MESSAGE.format(config_name=config),
                    ops.BlockedStatus,
                )

    def _perform_dag_bundle_connection_checks(self) -> None:
        """Check validity of all present S3/git relations."""
        if self._s3_requires.relations:
            erroneous_relation_ids = [
                str(relation.id)
                for relation in self._s3_requires.relations
                if relation.id not in self.s3_relation_connections
                and self._s3_requires.get_storage_connection_info(relation)
            ]

            if erroneous_relation_ids:
                raise ExceptionWithStatusError(
                    constants.INVALID_S3_RELATIONS_MESSAGE_TEMPLATE.format(
                        relation_ids=", ".join(erroneous_relation_ids)
                    ),
                    ops.BlockedStatus,
                )

        if self._git_requires.relations:
            erroneous_relation_ids = [
                str(relation.id)
                for relation in self._git_requires.relations
                if relation.id not in self.git_relation_connections and relation.data[relation.app]
            ]

            if erroneous_relation_ids:
                raise ExceptionWithStatusError(
                    constants.INVALID_GIT_RELATIONS_MESSAGE_TEMPLATE.format(
                        relation_ids=", ".join(erroneous_relation_ids)
                    ),
                    ops.BlockedStatus,
                )

    def _perform_database_related_checks(self) -> None:
        """Check validity of database relation."""
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

    def _perform_checks(self) -> None:
        """Checks to ensure the charm is able to generate and distribute configs."""
        self._validate_configs()

        if not self._container.can_connect():
            raise ExceptionWithStatusError(
                constants.WAITING_FOR_CONTAINER_MESSAGE, ops.WaitingStatus
            )

        self._perform_database_related_checks()

        try:
            # Validates proper oauth relation data if oauth relation exists
            self._oauth_active
        except Exception:
            raise ExceptionWithStatusError(
                constants.INVALID_OAUTH_RELATION_MESSAGE,
                ops.BlockedStatus,
            )

        if not self.model.get_relation(constants.AIRFLOW_API_SERVER_ENDPOINT_NAME):
            self._config_provider.set_validation_errors()
            raise ExceptionWithStatusError(
                constants.WAITING_FOR_API_SERVER_RELATION_MESSAGE, ops.BlockedStatus
            )

        if (
            not self._api_server_requires.api_server_host
            or not self._api_server_requires.api_server_port
        ):
            raise ExceptionWithStatusError(
                constants.WAITING_FOR_API_SERVER_HOST_PORT_MESSAGE, ops.WaitingStatus
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

        self._perform_dag_bundle_connection_checks()

    def _reconcile_dag_bundle_remote_connections(self) -> None:
        """Create/delete necessary Airflow connections for DAG bundle remotes."""
        if not self.unit.is_leader():
            return

        try:
            self._connection_manager.delete_stale_connections()
            self._connection_manager.create_or_update_s3_connections()
            self._connection_manager.create_or_update_git_connections()
        except (
            command_executor.CommandExecutionError,
            ops.pebble.PathError,
            connection_manager.InvalidAirflowConnectionsError,
        ):
            raise ExceptionWithStatusError(
                constants.ISSUE_RECONCILING_AIRFLOW_CONNECTIONS_MESSAGE,
                ops.BlockedStatus,
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
            self._airflow_config_template,
            {
                **self._config_generator.sensitive_config_values,
                "render_sensitive_data": True,
            },
            user=constants.WORKLOAD_USER,
            group=constants.WORKLOAD_GROUP,
        )

    def _run_db_migrate(self) -> None:
        """Run database migration in the workload container."""
        result = self._command_executor.run_db_migrate()
        if not result.success:
            raise ExceptionWithStatusError(
                constants.DB_MIGRATION_FAILED_MESSAGE, ops.BlockedStatus
            )

    def _update_oauth_config(self) -> None:
        """Update OAuth client config if relation exists."""
        if self.model.get_relation(constants.OAUTH_ENDPOINT_NAME):
            try:
                self._oauth_requirer.update_client_config(
                    oauth.ClientConfig(
                        redirect_uri=f"{self._config_generator._api_server_base_url}/oauth-authorized/hydra",
                        scope="openid email profile offline",
                        grant_types=["authorization_code", "refresh_token"],
                    )
                )
            except Exception:
                raise ExceptionWithStatusError(
                    constants.OAUTH_CLIENT_CONFIG_UPDATE_FAILED_MESSAGE, ops.BlockedStatus
                )

    def _reconcile(self, event: ops.EventBase) -> None:
        """Idempotent reconcile method to handle most relevant charm events."""
        if not self.unit.is_leader():
            return

        try:
            self._perform_checks()
            self._ensure_airflow_keys_generated()
            self._update_oauth_config()
            self._configure_pebble_layer()
            self._write_airflow_config()

            # Use peer relation data to track if db migration has run
            # TODO: once we have upgrade logic, we'll need to change the
            # conditions under which this state will be True/False.
            if not self._db_migration_ran:
                self._run_db_migrate()
                self._db_migration_ran = True

            self._reconcile_dag_bundle_remote_connections()

            sensitive_data = {
                **self._config_generator.sensitive_config_values,
                **self._webserver_config_generator.sensitive_values,
                "render_sensitive_data": True,
            }

            # We can decide which extra config we want to pass to the config_generator
            # Right now we only have one extra, but in the future we can make decisions
            # based on executors, providers, etc.
            self._config_provider.set_airflow_config(
                self._airflow_config_template,
                k8s_executor_pod_spec_template=self._kubernetes_executor_pod_spec,
                webserver_config_template=self._webserver_config_generator.webserver_config_template,
                sensitive_data=sensitive_data,
                tls_ca_chains=self.s3_tls_ca_chains,
                extra_data=self._spark_service_account_data,
            )

        except ExceptionWithStatusError as e:
            logger.error(e)
            self.unit.status = e.status
            return
        except webserver_config_generator.WebserverConfigError as e:
            self.unit.status = ops.BlockedStatus(str(e))
            return
        except command_executor.CommandExecutionError as e:
            logger.error(e)
            self.unit.status = ops.BlockedStatus(e.message)
            return

        self.unit.status = ops.ActiveStatus()


if __name__ == "__main__":  # pragma: nocover
    ops.main(AirflowCoordinatorK8SOperatorCharm)
