# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Tests for the command_executor module."""

import unittest.mock

import ops.pebble
import pytest

import command_executor
import constants


@pytest.fixture
def mock_container():
    """Create a mock container."""
    container = unittest.mock.MagicMock(spec=ops.Container)
    container.can_connect.return_value = True
    return container


@pytest.fixture
def executor(mock_container):
    """Create a CommandExecutor instance with a mock container."""
    return command_executor.CommandExecutor(mock_container)


class TestCommandExecutor:
    def test_run_db_migrate_success(self, executor, mock_container):
        mock_process = unittest.mock.MagicMock()
        mock_process.wait_output.return_value = ("Migration successful", "")
        mock_container.exec.return_value = mock_process

        result = executor.run_db_migrate()

        assert result.success is True
        assert result.stdout == "Migration successful"
        assert result.stderr == ""
        assert result.return_code == 0

        mock_container.exec.assert_called_once_with(
            ["airflow", "db", "migrate"],
            environment={
                "AIRFLOW_HOME": "/opt/airflow",
            },
            user=constants.WORKLOAD_USER,
            group=constants.WORKLOAD_GROUP,
        )

    def test_run_db_migrate_not_connected(self, executor, mock_container):
        mock_container.can_connect.return_value = False

        with pytest.raises(command_executor.CommandExecutionError) as exc_info:
            executor.run_db_migrate()

        assert "Cannot connect to workload container" in str(exc_info.value)

    def test_run_db_migrate_exec_error(self, executor, mock_container):
        mock_container.exec.side_effect = ops.pebble.ExecError(
            command=["airflow", "db", "migrate"],
            exit_code=1,
            stdout="",
            stderr="Migration failed",
        )

        result = executor.run_db_migrate()

        assert result.success is False
        assert result.return_code == 1
        assert result.stderr == "Migration failed"

    def test_run_db_migrate_unexpected_error(self, executor, mock_container):
        mock_container.exec.side_effect = Exception("Unexpected error")

        with pytest.raises(command_executor.CommandExecutionError) as exc_info:
            executor.run_db_migrate()

        assert "Failed to execute command with error: Unexpected error" in str(exc_info.value)
