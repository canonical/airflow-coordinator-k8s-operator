# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Command execution support for the Airflow Coordinator charm."""

import functools
import json
import logging
import typing

import charms.git_integrator.v0.git as git
import ops

import charm
import constants

logger = logging.getLogger(__name__)


class CommandExecutionError(Exception):
    """Exception raised when command execution fails."""

    def __init__(self, message: str, return_code: int | None = None, stderr: str | None = None):
        super().__init__(message)
        self.message = message
        self.return_code = return_code
        self.stderr = stderr


class CommandExecutionResult(typing.NamedTuple):
    """Result of a command execution."""

    success: bool
    stdout: str
    parsed_stdout: list | dict | None
    stderr: str
    return_code: int | None


def execute_pebble_exec_process(func: typing.Callable):
    """Decorator to standardize CommandExecutor methods that run ops.pebble.ExecProcess."""

    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        """Execute the ops.pebble.ExecProcess returned by func with sensible checks.

        Returns:
            CommandExecutionResult with execution details
        Raises:
            CommandExecutorError with details of encountered error
        """
        if not isinstance(self, CommandExecutor):
            raise TypeError(
                "Decorator 'ensure_pebble_exec' can only wrap methods of CommandExecutor"
            )

        if not self._container.can_connect():
            raise CommandExecutionError("Cannot connect to workload container")

        try:
            logger.debug(f"Starting command for {func.__name__}")

            process = func(self, *args, **kwargs)

            logger.debug(f"Executing {' '.join(process._command)} command")

            stdout, stderr = process.wait_output()

            logger.debug(f"'{' '.join(process._command)}' completed successfully")

            try:
                print(stdout)
                parsed_stdout = json.loads(stdout) if stdout else None
            except json.JSONDecodeError:
                parsed_stdout = None

            return CommandExecutionResult(
                success=True,
                stdout=stdout or "",
                parsed_stdout=parsed_stdout,
                stderr=stderr or "",
                return_code=0,
            )
        except ops.pebble.ExecError as e:
            logger.error(
                f"'{' '.join(e.command)}' failed with exit code {e.exit_code}: {e.stderr}"
            )
            return CommandExecutionResult(
                success=False,
                stdout=e.stdout or "",
                parsed_stdout=None,
                stderr=e.stderr or "",
                return_code=e.exit_code,
            )
        except Exception as e:
            logger.error(f"Unexpected error executing command: {e}")
            raise CommandExecutionError(f"Failed to execute command with error: {e}") from e

    return wrapper


class CommandExecutor:
    """Handles command execution in the workload container."""

    def __init__(self, container: ops.Container):
        """Initialize the command executor.

        Args:
            container: The Pebble container to execute commands in.
        """
        self._container = container

    @execute_pebble_exec_process
    def run_db_migrate(self) -> ops.pebble.ExecProcess:
        """Execute the 'airflow db migrate' command."""
        return self._container.exec(
            ["airflow", "db", "migrate"],
            environment={
                "AIRFLOW_HOME": constants.AIRFLOW_HOME,
            },
            user=constants.WORKLOAD_USER,
            group=constants.WORKLOAD_GROUP,
        )

    @execute_pebble_exec_process
    def add_airflow_s3_connection(
        self,
        connection_id: str,
        connection_info: charm.S3ConnectionInfo,
        tls_ca_chain_path: typing.Optional[str] = None,
    ) -> ops.pebble.ExecProcess:
        """Add/update Airflow S3 connection.

        The 'airflow connections add' creates or updates an existing connection.
        """
        extras = {}

        if connection_info.region:
            extras["region_name"] = connection_info.region

        if connection_info.endpoint:
            extras["endpoint_url"] = connection_info.endpoint

        if tls_ca_chain_path:
            extras["verify"] = tls_ca_chain_path

        extras_options = ["--conn-extra", json.dumps(extras)] if extras else []

        # airflow connections add also updates existing connections
        return self._container.exec(
            [
                "airflow",
                "connections",
                "add",
                connection_id,
                "--conn-type",
                "aws",
                "--conn-login",
                connection_info.access_key,
                "--conn-password",
                connection_info.secret_key,
                *extras_options,
            ],
            user=constants.WORKLOAD_USER,
            group=constants.WORKLOAD_GROUP,
            environment={
                "AIRFLOW_HOME": constants.AIRFLOW_HOME,
            },
        )

    @execute_pebble_exec_process
    def delete_airflow_connection(self, connection_id: str) -> ops.pebble.ExecProcess:
        """Delete Airflow S3 connection."""
        return self._container.exec(
            ["airflow", "connections", "delete", connection_id],
            user=constants.WORKLOAD_USER,
            group=constants.WORKLOAD_GROUP,
            environment={
                "AIRFLOW_HOME": constants.AIRFLOW_HOME,
            },
        )

    @execute_pebble_exec_process
    def list_airflow_connections(self) -> ops.pebble.ExecProcess:
        """List all Airflow connections."""
        return self._container.exec(
            ["airflow", "connections", "list", "--output", "json"],
            user=constants.WORKLOAD_USER,
            group=constants.WORKLOAD_GROUP,
            environment={
                "AIRFLOW_HOME": constants.AIRFLOW_HOME,
            },
        )

    @execute_pebble_exec_process
    def add_airflow_git_connection(
        self,
        connection_id: str,
        git_provider_model: git.GitProviderModel,
    ) -> ops.pebble.ExecProcess:
        """Add/update Airflow S3 connection.

        The 'airflow connections add' creates or updates an existing connection.

        """
        extras = {}
        if git_provider_model.authentication_method == git.AuthenticationMethodEnum.SSH:
            extras["private_key"] = git_provider_model.ssh_private_key

            if git_provider_model.ssh_strict_host_key_checking is not None:
                extras["strict_host_key_checking"] = (
                    git_provider_model.ssh_strict_host_key_checking
                )

        extras_options = ["--conn-extra", json.dumps(extras)] if extras else []

        credentials_options = (
            [
                "--conn-login",
                git_provider_model.credentials_username,
                "--conn-password",
                git_provider_model.credentials_personal_access_token,
            ]
            if git_provider_model.authentication_method == git.AuthenticationMethodEnum.CREDENTIALS
            else []
        )

        return self._container.exec(
            [
                "airflow",
                "connections",
                "add",
                connection_id,
                "--conn-type",
                "git",
                "--conn-host",
                git_provider_model.repository_url,
                *credentials_options,
                *extras_options,
            ],
            user=constants.WORKLOAD_USER,
            group=constants.WORKLOAD_GROUP,
            environment={
                "AIRFLOW_HOME": constants.AIRFLOW_HOME,
            },
        )
