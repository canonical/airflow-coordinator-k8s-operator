# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Airflow connection management abstraction for Airflow Coordinator charm."""

import dataclasses
import functools
import json
import logging
import typing

import charms.git_integrator.v0.git as git
import object_storage
import ops

import constants

logger = logging.getLogger(__name__)


class InvalidAirflowConnectionsError(Exception):
    """Custom exception raised when `airflow connections list` output is invalid."""


@dataclasses.dataclass
class AirflowConnection:
    """Dataclass representing an Airflow connection."""

    conn_id: str
    conn_type: str

    host: str | None = None
    login: str | None = None
    password: str | None = None

    extra_dejson: dict = dataclasses.field(default_factory=dict)

    @classmethod
    def from_airflow_connections_list_output(cls, data: list[dict]):
        """Instantiate from parsed json output of `airflow connections list`."""
        if not data:
            return []

        if not all(connection.get("conn_type") for connection in data):
            raise InvalidAirflowConnectionsError()

        return [
            cls(**{key.replace("-", "_"): value for key, value in datum.items()}) for datum in data
        ]


@dataclasses.dataclass
class S3ConnectionInfo:
    """S3 Connection Info extracted from object_storage lib."""

    bucket: str
    access_key: str
    secret_key: str
    region: typing.Optional[str] = None
    storage_class: typing.Optional[str] = None
    endpoint: typing.Optional[str] = None
    path: typing.Optional[str] = None
    s3_api_version: typing.Optional[str] = None
    s3_uri_style: typing.Optional[str] = None
    tls_ca_chain: list[str] = dataclasses.field(default_factory=list)
    delete_older_than_days: typing.Optional[str] = None

    @classmethod
    def from_s3_info(cls, data: object_storage.domain.S3Info):
        """Instantiate from typed dict (S3Info)."""
        if not data:
            return None

        if not all(
            data.get(required_key) for required_key in ["bucket", "access-key", "secret-key"]
        ):
            return None

        normalized_data = {key.replace("-", "_"): value for key, value in data.items()}

        return cls(**normalized_data)


class AirflowConnectionManager:
    """Encapsulate business logic around management of Airflow connections."""

    def __init__(self, charm: ops.CharmBase, container: ops.Container):
        self._charm = charm
        self._container = container

    @functools.cached_property
    def airflow_connections(self) -> list[AirflowConnection]:
        """Airflow connections from the database."""
        raw_connections = self._charm._command_executor.list_airflow_connections().parsed_stdout

        if raw_connections is None:
            raise InvalidAirflowConnectionsError()

        return AirflowConnection.from_airflow_connections_list_output(raw_connections)

    def refresh(self):
        """Refresh cached properties of this class.

        Must be run to ensure re-fetch/re-compute of cached data.
        """
        if "airflow_connections" in self.__dict__:
            del self.__dict__["airflow_connections"]

    def has_connection_for_s3_changed(
        self, connection_id: str, connection_info: S3ConnectionInfo
    ) -> bool:
        """Return True if S3 connection relation data has changed; False otherwise."""
        filtered_airflow_connections = list(
            filter(
                lambda connection: connection.conn_id == connection_id, self.airflow_connections
            )
        )

        if not filtered_airflow_connections:
            return True

        airflow_connection = filtered_airflow_connections[0]

        try:
            tls_ca_chain = (
                self._container.pull(
                    f"/{constants.AIRFLOW_HOME}/connection_certs/{connection_id}.pem",
                    encoding="utf-8",
                ).read()
                if connection_info.tls_ca_chain
                else ""
            )
        except ops.pebble.PathError:
            tls_ca_chain = ""

        return not (
            airflow_connection.conn_type == "aws"
            and airflow_connection.login == connection_info.access_key
            and airflow_connection.password == connection_info.secret_key
            and airflow_connection.extra_dejson.get("region_name") == connection_info.region
            and airflow_connection.extra_dejson.get("endpoint_url") == connection_info.endpoint
            and tls_ca_chain == "\n".join(connection_info.tls_ca_chain)
        )

    def create_or_update_s3_connections(self) -> None:
        """Create or update s3 connections that have changed."""
        for relation_id, connection_info in self._charm.s3_relation_connections.items():
            if not connection_info:
                continue

            connection_id = f"s3_relation_{relation_id}_connection"

            if self.has_connection_for_s3_changed(connection_id, connection_info):
                logger.info(
                    f"Connection info for {connection_id} changed. Updating Airflow connection"
                )

                if connection_info.tls_ca_chain:
                    tls_ca_chain_path = constants.TLS_CA_CHAIN_FILEPATH_TEMPLATE.format(
                        filename=connection_id
                    )

                    try:
                        self._container.push(
                            path=tls_ca_chain_path,
                            source="\n".join(connection_info.tls_ca_chain),
                            make_dirs=True,
                            user=constants.WORKLOAD_USER,
                            group=constants.WORKLOAD_GROUP,
                        )
                    except ops.pebble.PathError as e:
                        logger.error(f"Unexpected error pushing TLS CA chain: {e}")
                        raise
                else:
                    tls_ca_chain_path = None

                self._charm._command_executor.add_airflow_s3_connection(
                    connection_id,
                    connection_info,
                    tls_ca_chain_path=tls_ca_chain_path,
                )

                self.refresh()

    def has_connection_for_git_changed(
        self, connection_id: str, git_provider_model: git.GitProviderModel
    ) -> bool:
        """Return True if git connection relation data has changed; False otherwise."""
        filtered_airflow_connections = list(
            filter(
                lambda connection: connection.conn_id == connection_id, self.airflow_connections
            )
        )

        if not filtered_airflow_connections:
            return True

        airflow_connection = filtered_airflow_connections[0]

        if airflow_connection.conn_type != "git":
            return False

        if git_provider_model.authentication_method == git.AuthenticationMethodEnum.CREDENTIALS:
            # Existing airflow connection has SSH parameters or
            # credentials parameters changed
            has_authentication_changed = (
                airflow_connection.extra_dejson.get("private_key")
                or airflow_connection.extra_dejson.get("strict_host_key_checking")
                or airflow_connection.extra_dejson.get("private_key_passphrase")
                or airflow_connection.extra_dejson.get("ssh_port")
                or airflow_connection.login != git_provider_model.credentials_username
                or airflow_connection.password
                != git_provider_model.credentials_personal_access_token
            )
        elif git_provider_model.authentication_method == git.AuthenticationMethodEnum.SSH:
            # Existing airflow connection has credentials or
            # SSH parameters changed
            has_authentication_changed = (
                airflow_connection.login
                or airflow_connection.password
                or airflow_connection.extra_dejson.get("private_key")
                != git_provider_model.ssh_private_key
                or airflow_connection.extra_dejson.get("private_key_passphrase")
                != git_provider_model.ssh_passphrase
                or json.loads(airflow_connection.extra_dejson.get("strict_host_key_checking"))
                != git_provider_model.ssh_strict_host_key_checking
                or airflow_connection.extra_dejson.get("ssh_port")
                != (str(git_provider_model.ssh_port) if git_provider_model.ssh_port else None)
            )
        else:
            has_authentication_changed = False

        return not (
            airflow_connection.host == git_provider_model.repository_url
            and not has_authentication_changed
        )

    def create_or_update_git_connections(self) -> None:
        """Create or update git connections that have changed."""
        airflow_connection_ids = [connection.conn_id for connection in self.airflow_connections]

        for (
            relation_id,
            git_provider_model,
        ) in self._charm._git_requires.get_git_connection_information().items():
            if git_provider_model.authentication_method is None:
                logger.debug(
                    f"Skipping Airflow connection creation for relation {relation_id}"
                    " since it has no authentication"
                )
                continue

            connection_id = f"git_relation_{relation_id}_connection"

            if self.has_connection_for_git_changed(connection_id, git_provider_model):
                logger.info(
                    f"Connection info for {connection_id} changed. Deleting old Airflow connection"
                )

                if connection_id in airflow_connection_ids:
                    self._charm._command_executor.delete_airflow_connection(connection_id)

                logger.info(f"Adding new Airflow connection for {connection_id}")

                self._charm._command_executor.add_airflow_git_connection(
                    connection_id,
                    git_provider_model,
                )

                self.refresh()

    def delete_stale_connections(self) -> None:
        """Delete stale S3/git Airflow connections (those without relations)."""
        s3_relation_connection_ids = [
            f"s3_relation_{relation_id}_connection"
            for relation_id in self._charm.s3_relation_connections
        ]

        git_relation_connection_ids = [
            f"git_relation_{relation_id}_connection"
            for relation_id in self._charm.git_relation_connections
        ]

        for airflow_connection_id in [
            connection.conn_id
            for connection in self.airflow_connections
            if connection.conn_id.startswith("s3_relation_")
            or connection.conn_id.startswith("git_relation_")
        ]:
            if (
                airflow_connection_id not in s3_relation_connection_ids
                and airflow_connection_id not in git_relation_connection_ids
            ):
                logger.info(f"Deleting Airflow connection {airflow_connection_id}")

                self._charm._command_executor.delete_airflow_connection(airflow_connection_id)

                self.refresh()
