# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Tests for the command_executor module."""

import json
import unittest.mock

import charms.git_integrator.v0.git as git
import ops.pebble
import pytest

import command_executor
import connection_manager
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


@pytest.fixture
def mock_s3_connection_info():
    """Mock S3ConnectionInfo instance."""
    return connection_manager.S3ConnectionInfo.from_s3_info(
        {
            "bucket": "test-bucket",
            "access-key": "test-access-key",
            "secret-key": "test-secret-key",
            "region": "test-region",
            "endpoint": "test-endpoint",
            "path": "test/path",
            "tls-ca-chain": ["test-chain1", "test-chain2"],
        }
    )


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

    def test_add_airflow_s3_connection_success(
        self, executor, mock_container, mock_s3_connection_info
    ):
        mock_process = unittest.mock.MagicMock()
        mock_process.wait_output.return_value = ("Airflow connection added", "")
        mock_container.exec.return_value = mock_process

        result = executor.add_airflow_s3_connection(
            "test-connection-id",
            mock_s3_connection_info,
            tls_ca_chain_path="/opt/airflow/connection_certs/test-connection-id.pem",
        )

        assert result.success is True
        assert result.stdout == "Airflow connection added"
        assert result.stderr == ""
        assert result.return_code == 0

        mock_container.exec.assert_called_once_with(
            [
                "airflow",
                "connections",
                "add",
                "test-connection-id",
                "--conn-type",
                "aws",
                "--conn-login",
                "test-access-key",
                "--conn-password",
                "test-secret-key",
                "--conn-extra",
                json.dumps(
                    {
                        "region_name": "test-region",
                        "endpoint_url": "test-endpoint",
                        "verify": "/opt/airflow/connection_certs/test-connection-id.pem",
                    },
                ),
            ],
            user=constants.WORKLOAD_USER,
            group=constants.WORKLOAD_GROUP,
            environment={
                "AIRFLOW_HOME": constants.AIRFLOW_HOME,
            },
        )

    def test_add_airflow_s3_connection_failure_pushing_tls_ca_chain(
        self, executor, mock_container, mock_s3_connection_info
    ):
        mock_container.push.side_effect = ops.pebble.PathError(
            kind="generic-file-error",
            message="Test error pushing file to mock container",
        )

        with pytest.raises(command_executor.CommandExecutionError) as exc_info:
            executor.add_airflow_s3_connection(
                "test-connection-id",
                mock_s3_connection_info,
                environment={
                    "AIRFLOW_HOME": constants.AIRFLOW_HOME,
                },
            )

            assert "Unexpected error pushing TLS CA chain: " in str(exc_info.value)

        mock_container.exec.assert_not_called()

    def test_delete_airflow_connection_success(self, executor, mock_container):
        mock_process = unittest.mock.MagicMock()
        mock_process.wait_output.return_value = ("Airflow connection deleted", "")
        mock_container.exec.return_value = mock_process

        result = executor.delete_airflow_connection("test-connection-id")

        assert result.success is True
        assert result.stdout == "Airflow connection deleted"
        assert result.stderr == ""
        assert result.return_code == 0

        mock_container.exec.assert_called_once_with(
            [
                "airflow",
                "connections",
                "delete",
                "test-connection-id",
            ],
            user=constants.WORKLOAD_USER,
            group=constants.WORKLOAD_GROUP,
            environment={
                "AIRFLOW_HOME": constants.AIRFLOW_HOME,
            },
        )

    def test_list_airflow_connection_success(self, executor, mock_container):
        mock_process = unittest.mock.MagicMock()
        mock_process.wait_output.return_value = ("Airflow connections list output", "")
        mock_container.exec.return_value = mock_process

        result = executor.list_airflow_connections()

        assert result.success is True
        assert result.stdout == "Airflow connections list output"
        assert result.stderr == ""
        assert result.return_code == 0

        mock_container.exec.assert_called_once_with(
            [
                "airflow",
                "connections",
                "list",
                "--output",
                "json",
            ],
            user=constants.WORKLOAD_USER,
            group=constants.WORKLOAD_GROUP,
            environment={
                "AIRFLOW_HOME": constants.AIRFLOW_HOME,
            },
        )

    def test_add_airflow_git_credentials_connection_success(self, executor, mock_container):
        mock_process = unittest.mock.MagicMock()
        mock_process.wait_output.return_value = ("Airflow connection added", "")
        mock_container.exec.return_value = mock_process

        git_provider_model_credentials = git.GitProviderModel(
            repository_url="test-repo-url",
            path="test/path/",
            tracking_ref="test-tracking-ref",
            authentication_method=git.AuthenticationMethodEnum.CREDENTIALS,
            credentials_username="test-user",
            credentials_personal_access_token="test-personal-access-token",
        )

        result = executor.add_airflow_git_connection(
            "test-connection-id",
            git_provider_model_credentials,
        )

        assert result.success is True
        assert result.stdout == "Airflow connection added"
        assert result.stderr == ""
        assert result.return_code == 0

        mock_container.exec.assert_called_once_with(
            [
                "airflow",
                "connections",
                "add",
                "test-connection-id",
                "--conn-type",
                "git",
                "--conn-host",
                "test-repo-url",
                "--conn-login",
                "test-user",
                "--conn-password",
                "test-personal-access-token",
            ],
            user=constants.WORKLOAD_USER,
            group=constants.WORKLOAD_GROUP,
            environment={
                "AIRFLOW_HOME": constants.AIRFLOW_HOME,
            },
        )

    def test_add_airflow_git_ssh_connection_success(self, executor, mock_container):
        mock_process = unittest.mock.MagicMock()
        mock_process.wait_output.return_value = ("Airflow connection added", "")
        mock_container.exec.return_value = mock_process

        git_provider_model_ssh = git.GitProviderModel(
            repository_url="test-repo-url",
            path="test/path/",
            tracking_ref="test-tracking-ref",
            authentication_method=git.AuthenticationMethodEnum.SSH,
            ssh_private_key="test-private-key",
            ssh_strict_host_key_checking=True,
        )

        result = executor.add_airflow_git_connection(
            "test-connection-id",
            git_provider_model_ssh,
        )

        assert result.success is True
        assert result.stdout == "Airflow connection added"
        assert result.stderr == ""
        assert result.return_code == 0

        mock_container.exec.assert_called_once_with(
            [
                "airflow",
                "connections",
                "add",
                "test-connection-id",
                "--conn-type",
                "git",
                "--conn-host",
                "test-repo-url",
                "--conn-extra",
                json.dumps(
                    {
                        "private_key": "test-private-key",
                        "strict_host_key_checking": True,
                    },
                ),
            ],
            user=constants.WORKLOAD_USER,
            group=constants.WORKLOAD_GROUP,
            environment={
                "AIRFLOW_HOME": constants.AIRFLOW_HOME,
            },
        )
