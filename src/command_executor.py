# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Command execution support for the Airflow Coordinator charm."""

import logging
import typing

import ops

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
    stderr: str
    return_code: int | None


class CommandExecutor:
    """Handles command execution in the workload container."""

    def __init__(self, container: ops.Container):
        """Initialize the command executor.

        Args:
            container: The Pebble container to execute commands in.
        """
        self._container = container

    def run_db_migrate(self) -> CommandExecutionResult:
        """Execute the 'airflow db migrate' command.

        Returns:
            CommandExecutionResult with execution details.

        Raises:
            CommandExecutionError: If unable to connect to the container.
        """
        if not self._container.can_connect():
            raise CommandExecutionError("Cannot connect to workload container")

        logger.info("Executing 'airflow db migrate' command")

        try:
            process = self._container.exec(
                ["airflow", "db", "migrate"],
                environment={
                    "AIRFLOW_HOME": "/opt/airflow",
                },
            )
            stdout, stderr = process.wait_output()

            logger.info("'airflow db migrate' completed successfully")
            return CommandExecutionResult(
                success=True,
                stdout=stdout or "",
                stderr=stderr or "",
                return_code=0,
            )
        except ops.pebble.ExecError as e:
            logger.error(f"'airflow db migrate' failed with exit code {e.exit_code}: {e.stderr}")
            return CommandExecutionResult(
                success=False,
                stdout=e.stdout or "",
                stderr=e.stderr or "",
                return_code=e.exit_code,
            )
        except Exception as e:
            logger.error(f"Unexpected error executing 'airflow db migrate': {e}")
            raise CommandExecutionError(f"Failed to execute 'airflow db migrate': {e}") from e
