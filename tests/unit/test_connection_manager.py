# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Tests for the connection_manager module."""

import io
import json
import unittest.mock

import charms.git_integrator.v0.git as git
import pytest

import command_executor
import connection_manager
import constants


@pytest.fixture(scope="function")
def airflow_connection_manager(context, state):
    # install is an event not handled by the charm's reconciler event handler
    with context(context.on.install(), state) as manager:
        yield connection_manager.AirflowConnectionManager(
            manager.charm,
            manager.charm._container,
        )


def command_executor_with_json_result(
    return_value: dict | list | None,
) -> command_executor.CommandExecutionResult:
    """Return a CommandExecutionResult with the provided return_value as JSON output."""
    return command_executor.CommandExecutionResult(
        success=True,
        stdout=json.dumps(return_value),
        parsed_stdout=return_value,
        stderr="",
        return_code=0,
    )


@pytest.fixture(scope="function")
def invalid_list_airflow_connections_output(mock_command_executor):
    mock_command_executor[
        "list_airflow_connections"
    ].return_value = command_executor_with_json_result(None)

    yield


@pytest.fixture(scope="function")
def empty_list_airflow_connections_output(mock_command_executor):
    mock_command_executor[
        "list_airflow_connections"
    ].return_value = command_executor_with_json_result([])

    yield {
        "mock_command_executor": mock_command_executor,
    }


@pytest.fixture(scope="function")
def valid_list_airflow_connections_output(mock_command_executor):
    mock_command_executor[
        "list_airflow_connections"
    ].return_value = command_executor_with_json_result(
        [
            {
                "conn_id": "git_relation_2_connection",
                "conn_type": "git",
                "host": "test-repo-url",
                "login": "test-login",
                "password": "test-personal-access-token",
                "extra-dejson": {
                    "region_name": "test-region",
                    "endpoint_url": "test-endpoint",
                },
            },
            {
                "conn_id": "s3_relation_1_connection",
                "conn_type": "aws",
                "login": "test-access-key",
                "password": "test-secret-key",
                "extra-dejson": {
                    "region_name": "test-region",
                    "endpoint_url": "test-endpoint",
                },
            },
        ]
    )

    yield {
        "mock_command_executor": mock_command_executor,
    }


@pytest.fixture(scope="function")
def valid_git_provider_model():
    return git.GitProviderModel(
        repository_url="test-repo-url",
        authentication_method=git.AuthenticationMethodEnum.CREDENTIALS,
        credentials_username="test-login",
        credentials_personal_access_token="test-personal-access-token",
    )


@pytest.fixture(scope="function")
def valid_git_ssh_provider_model():
    return git.GitProviderModel(
        repository_url="test-repo-url",
        authentication_method=git.AuthenticationMethodEnum.SSH,
        ssh_private_key="test-private-key",
        ssh_passphrase="test-passphrase",
        ssh_strict_host_key_checking=True,
        ssh_port=2222,
    )


@pytest.fixture(scope="function")
def airflow_connection_for_valid_git_provider_model():
    return connection_manager.AirflowConnection(
        conn_id="git_relation_2_connection",
        conn_type="git",
        host="test-repo-url",
        login="test-login",
        password="test-personal-access-token",
        extra_dejson={
            "region_name": "test-region",
            "endpoint_url": "test-endpoint",
        },
    )


@pytest.fixture(scope="function")
def valid_s3_connection_info():
    return connection_manager.S3ConnectionInfo(
        bucket="test-bucket",
        access_key="test-access-key",
        secret_key="test-secret-key",
        region="test-region",
        storage_class="test-storage-class",
        endpoint="test-endpoint",
        path="test/path",
        tls_ca_chain=["test-ca-chain"],
    )


@pytest.fixture(scope="function")
def airflow_connection_for_valid_s3_connection_info():
    return connection_manager.AirflowConnection(
        conn_id="s3_relation_1_connection",
        conn_type="aws",
        login="test-access-key",
        password="test-secret-key",
        extra_dejson={
            "region_name": "test-region",
            "endpoint_url": "test-endpoint",
        },
    )


class TestConnectionManager:
    def test_airflow_connections_invalid_json_output(
        self, airflow_connection_manager, invalid_list_airflow_connections_output
    ):
        """Test airflow_connections property when issue getting JSON Airflow connections.."""
        with pytest.raises(connection_manager.InvalidAirflowConnectionsError):
            airflow_connection_manager.airflow_connections

    def test_empty_airflow_connections_parse_to_dataclass(
        self, airflow_connection_manager, empty_list_airflow_connections_output
    ):
        """Test airflow_connections properly parses empty list of connections."""
        assert airflow_connection_manager.airflow_connections == []

    def test_proper_parsing_of_airflow_connections_to_dataclass(
        self,
        airflow_connection_manager,
        valid_list_airflow_connections_output,
        airflow_connection_for_valid_s3_connection_info,
        airflow_connection_for_valid_git_provider_model,
    ):
        """Test airflow_connections properly parses connections into dataclass return."""
        assert airflow_connection_manager.airflow_connections == [
            airflow_connection_for_valid_git_provider_model,
            airflow_connection_for_valid_s3_connection_info,
        ]

    def test_has_connection_for_s3_changed_new_connection(
        self,
        airflow_connection_manager,
        empty_list_airflow_connections_output,
        valid_s3_connection_info,
    ):
        assert airflow_connection_manager.has_connection_for_s3_changed(
            "s3_relation_1_connection", valid_s3_connection_info
        )

    def test_has_connection_for_s3_changed_updated_tls_chain(
        self,
        airflow_connection_manager,
        valid_list_airflow_connections_output,
        valid_s3_connection_info,
        mock_container_pull,
    ):
        mock_container_pull.return_value = io.StringIO("random-tls-chain")

        assert airflow_connection_manager.has_connection_for_s3_changed(
            "s3_relation_1_connection", valid_s3_connection_info
        )

        mock_container_pull.return_value = io.StringIO("test-ca-chain")
        assert not airflow_connection_manager.has_connection_for_s3_changed(
            "s3_relation_1_connection", valid_s3_connection_info
        )

    @pytest.mark.parametrize(
        "list_airflow_connections_output_fixture",
        ["empty_list_airflow_connections_output", "valid_list_airflow_connections_output"],
    )
    def test_create_or_update_s3_connection(
        self,
        airflow_connection_manager,
        valid_s3_connection_info,
        airflow_connection_for_valid_s3_connection_info,
        mock_container_pull,
        mock_container_push,
        list_airflow_connections_output_fixture,
        request,
    ):
        mock_container_pull.return_value = io.StringIO("test-ca-chain")

        mock_command_executor = request.getfixturevalue(list_airflow_connections_output_fixture)[
            "mock_command_executor"
        ]

        expect_updates = (
            list_airflow_connections_output_fixture == "empty_list_airflow_connections_output"
        )

        with unittest.mock.patch(
            "charm.AirflowCoordinatorK8SOperatorCharm.s3_relation_connections",
            new_callable=unittest.mock.PropertyMock(return_value={"1": valid_s3_connection_info}),
        ):
            connection_id = "s3_relation_1_connection"

            airflow_connection_manager.create_or_update_s3_connections()

            if expect_updates:
                mock_container_push.assert_called_once_with(
                    path=constants.TLS_CA_CHAIN_FILEPATH_TEMPLATE.format(filename=connection_id),
                    source="test-ca-chain",
                    make_dirs=True,
                    user=constants.WORKLOAD_USER,
                    group=constants.WORKLOAD_GROUP,
                )

                mock_command_executor["add_airflow_s3_connection"].assert_called_once_with(
                    connection_id,
                    valid_s3_connection_info,
                    tls_ca_chain_path=constants.TLS_CA_CHAIN_FILEPATH_TEMPLATE.format(
                        filename=connection_id
                    ),
                )
            else:
                mock_container_push.assert_not_called()
                mock_command_executor["add_airflow_s3_connection"].assert_not_called()

    def test_has_connection_for_git_with_changed_new_connection(
        self,
        airflow_connection_manager,
        empty_list_airflow_connections_output,
        valid_git_provider_model,
    ):
        assert airflow_connection_manager.has_connection_for_git_changed(
            "git_relation_2_connection", valid_git_provider_model
        )

    def test_has_connection_for_git_with_changed_password(
        self,
        airflow_connection_manager,
        valid_list_airflow_connections_output,
        valid_git_provider_model,
    ):
        assert not airflow_connection_manager.has_connection_for_git_changed(
            "git_relation_2_connection", valid_git_provider_model
        )

        valid_git_provider_model.credentials_personal_access_token = "new-test-password"

        assert airflow_connection_manager.has_connection_for_git_changed(
            "git_relation_2_connection", valid_git_provider_model
        )

    def test_has_connection_for_git_ssh_with_changed_passphrase(
        self,
        airflow_connection_manager,
        valid_git_ssh_provider_model,
        mock_command_executor,
    ):
        """Test change detection when SSH passphrase changes."""
        mock_command_executor[
            "list_airflow_connections"
        ].return_value = command_executor_with_json_result(
            [
                {
                    "conn_id": "git_relation_3_connection",
                    "conn_type": "git",
                    "host": "test-repo-url",
                    "extra-dejson": {
                        "private_key": "test-private-key",
                        "private_key_passphrase": "test-passphrase",
                        "strict_host_key_checking": "true",
                        "ssh_port": "2222",
                    },
                },
            ]
        )

        assert not airflow_connection_manager.has_connection_for_git_changed(
            "git_relation_3_connection", valid_git_ssh_provider_model
        )

        valid_git_ssh_provider_model.ssh_passphrase = "new-passphrase"

        airflow_connection_manager.refresh()
        assert airflow_connection_manager.has_connection_for_git_changed(
            "git_relation_3_connection", valid_git_ssh_provider_model
        )

    def test_has_connection_for_git_ssh_with_changed_port(
        self,
        airflow_connection_manager,
        valid_git_ssh_provider_model,
        mock_command_executor,
    ):
        """Test change detection when SSH port changes."""
        mock_command_executor[
            "list_airflow_connections"
        ].return_value = command_executor_with_json_result(
            [
                {
                    "conn_id": "git_relation_3_connection",
                    "conn_type": "git",
                    "host": "test-repo-url",
                    "extra-dejson": {
                        "private_key": "test-private-key",
                        "private_key_passphrase": "test-passphrase",
                        "strict_host_key_checking": "true",
                        "ssh_port": "2222",
                    },
                },
            ]
        )

        assert not airflow_connection_manager.has_connection_for_git_changed(
            "git_relation_3_connection", valid_git_ssh_provider_model
        )

        valid_git_ssh_provider_model.ssh_port = 3333

        airflow_connection_manager.refresh()
        assert airflow_connection_manager.has_connection_for_git_changed(
            "git_relation_3_connection", valid_git_ssh_provider_model
        )

    @pytest.mark.parametrize(
        "list_airflow_connections_output_fixture",
        ["empty_list_airflow_connections_output", "valid_list_airflow_connections_output"],
    )
    def test_create_or_update_git_connection(
        self,
        airflow_connection_manager,
        valid_git_provider_model,
        airflow_connection_for_valid_git_provider_model,
        mock_container_push,
        list_airflow_connections_output_fixture,
        request,
    ):
        mock_command_executor = request.getfixturevalue(list_airflow_connections_output_fixture)[
            "mock_command_executor"
        ]

        expect_updates = (
            list_airflow_connections_output_fixture == "empty_list_airflow_connections_output"
        )

        with unittest.mock.patch(
            "charms.git_integrator.v0.git.GitRequires.get_git_connection_information",
            return_value={"2": valid_git_provider_model},
        ):
            connection_id = "git_relation_2_connection"

            airflow_connection_manager.create_or_update_git_connections()

            if expect_updates:
                mock_command_executor["delete_airflow_connection"].assert_not_called()

                mock_command_executor["add_airflow_git_connection"].assert_called_once_with(
                    connection_id,
                    valid_git_provider_model,
                )
            else:
                mock_command_executor["delete_airflow_connection"].assert_not_called()
                mock_command_executor["add_airflow_git_connection"].assert_not_called()

    def test_delete_stale_connections(
        self, airflow_connection_manager, valid_list_airflow_connections_output
    ):
        mock_command_executor = valid_list_airflow_connections_output["mock_command_executor"]

        with (
            unittest.mock.patch(
                "charms.git_integrator.v0.git.GitRequires.get_git_connection_information",
                return_value={},
            ),
            unittest.mock.patch(
                "charm.AirflowCoordinatorK8SOperatorCharm.s3_relation_connections",
                new_callable=unittest.mock.PropertyMock(return_value={}),
            ),
        ):
            airflow_connection_manager.delete_stale_connections()

            assert sorted(mock_command_executor["delete_airflow_connection"].mock_calls) == sorted(
                [
                    unittest.mock.call("s3_relation_1_connection"),
                    unittest.mock.call("git_relation_2_connection"),
                ]
            )
