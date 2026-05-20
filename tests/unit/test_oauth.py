# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Unit tests for OAuth (OIDC) integration in the Coordinator charm."""

import configparser
import dataclasses
import unittest.mock

import charms.airflow_coordinator_k8s.v0.airflow_coordinator as airflow_coordinator
import ops
import ops.testing
import pytest
from charms.hydra.v0.oauth import OauthProviderConfig, OAuthRequirer
from conftest import TEST_MODEL

import constants

# ---------------------------------------------------------------------------
# Shared test data
# ---------------------------------------------------------------------------

VALID_PROVIDER_INFO = OauthProviderConfig(
    issuer_url="https://hydra.example.com",
    authorization_endpoint="https://hydra.example.com/oauth2/auth",
    token_endpoint="https://hydra.example.com/oauth2/token",
    introspection_endpoint="https://hydra.example.com/oauth2/introspect",
    userinfo_endpoint="https://hydra.example.com/userinfo",
    jwks_endpoint="https://hydra.example.com/.well-known/jwks.json",
    scope="openid email profile offline",
    client_id="test-client-id",
    client_secret="test-client-secret",
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="function")
def oauth_relation():
    """An oauth requires relation (Hydra acts as the provider)."""
    return ops.testing.Relation(constants.OAUTH_RELATION_ENDPOINT)


@pytest.fixture(scope="function")
def state_with_oauth(
    all_required_relations,
    workload_container,
    oauth_relation,
    mock_command_executor,
    fernet_key_secret,
    git_credentials_secret,
    git_ssh_secret,
):
    """Happy-path State that includes a formed oauth relation."""
    return ops.testing.State(
        leader=True,
        model=TEST_MODEL,
        relations=[*all_required_relations, oauth_relation],
        containers=[workload_container],
        secrets=[fernet_key_secret, git_credentials_secret, git_ssh_secret],
        config={
            constants.FERNET_KEY_SECRET_CONFIG: fernet_key_secret.id,
        },
    )


@pytest.fixture(scope="function")
def mock_get_provider_info():
    """Patch OAuthRequirer.get_provider_info to return valid provider data."""
    with unittest.mock.patch.object(
        OAuthRequirer,
        "get_provider_info",
        return_value=VALID_PROVIDER_INFO,
    ) as mock:
        yield mock


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestOAuthCoordinator:
    def test_oauth_active_adds_auth_manager_to_config_template(
        self,
        context,
        state_with_oauth,
        mock_get_provider_info,
    ):
        """When OAuth is active the config template distributed to core charms
        contains [core] auth_manager pointing to the FAB auth manager class.
        """
        state_out = context.run(context.on.start(), state_with_oauth)

        assert state_out.unit_status == ops.ActiveStatus()

        for relation in state_out.get_relations(constants.AIRFLOW_COORDINATOR_RELATION_NAME):
            config_template = relation.local_app_data.get("config-template", "")
            parsed = configparser.ConfigParser()
            parsed.read_string(config_template)
            assert parsed.get("core", "auth_manager") == constants.FAB_AUTH_MANAGER_CLASS

    def test_oauth_active_passes_webserver_config_template_to_set_airflow_config(
        self,
        context,
        state_with_oauth,
        mock_get_provider_info,
    ):
        """When OAuth is active, set_airflow_config receives a non-None
        webserver_config_template that contains the Jinja2 placeholder for
        the client secret but NOT the actual secret value.
        """
        with unittest.mock.patch.object(
            airflow_coordinator.AirflowCoordinatorProvides,
            "set_airflow_config",
        ) as mock_set:
            context.run(context.on.start(), state_with_oauth)

        assert mock_set.called
        webserver_template = mock_set.call_args.kwargs.get("webserver_config_template")
        assert webserver_template is not None
        assert "{{ webserver_config__client_secret }}" in webserver_template
        assert VALID_PROVIDER_INFO.client_secret not in webserver_template

    def test_oauth_active_includes_client_secret_in_sensitive_data(
        self,
        context,
        state_with_oauth,
        mock_get_provider_info,
    ):
        """When OAuth is active, the actual client_secret is present in
        sensitive_data under the key webserver_config__client_secret.
        """
        with unittest.mock.patch.object(
            airflow_coordinator.AirflowCoordinatorProvides,
            "set_airflow_config",
        ) as mock_set:
            context.run(context.on.start(), state_with_oauth)

        assert mock_set.called
        sensitive_data = mock_set.call_args.kwargs["sensitive_data"]
        assert (
            sensitive_data.get("webserver_config__client_secret")
            == VALID_PROVIDER_INFO.client_secret
        )

    def test_oauth_absent_no_auth_manager_in_config_template(
        self,
        context,
        state,
    ):
        """Without an oauth relation auth_manager must not appear in the
        config template distributed to core charms.
        """
        state_out = context.run(context.on.start(), state)

        assert state_out.unit_status == ops.ActiveStatus()

        for relation in state_out.get_relations(constants.AIRFLOW_COORDINATOR_RELATION_NAME):
            config_template = relation.local_app_data.get("config-template", "")
            assert "auth_manager" not in config_template

    def test_oauth_absent_no_webserver_config_in_set_airflow_config(
        self,
        context,
        state,
    ):
        """Without an oauth relation webserver_config_template must be None
        and webserver_config__client_secret must not be in sensitive_data.
        """
        with unittest.mock.patch.object(
            airflow_coordinator.AirflowCoordinatorProvides,
            "set_airflow_config",
        ) as mock_set:
            context.run(context.on.start(), state)

        assert mock_set.called
        kwargs = mock_set.call_args.kwargs
        assert kwargs.get("webserver_config_template") is None
        assert "webserver_config__client_secret" not in kwargs["sensitive_data"]

    def test_oauth_group_config_forwarded_to_webserver_template(
        self,
        context,
        mock_command_executor,
        mock_get_provider_info,
        state_with_oauth,
    ):
        """IDP group config values set on the coordinator are reflected in the
        webserver_config_template passed to set_airflow_config.
        """
        state = dataclasses.replace(
            state_with_oauth,
            config={
                **state_with_oauth.config,
                constants.EXTERNAL_IDP_GROUPS_FOR_ADMIN: "admin-group,super-users",
                constants.EXTERNAL_IDP_GROUPS_FOR_USER: "staff",
            },
        )

        with unittest.mock.patch.object(
            airflow_coordinator.AirflowCoordinatorProvides,
            "set_airflow_config",
        ) as mock_set:
            context.run(context.on.config_changed(), state)

        assert mock_set.called
        webserver_template = mock_set.call_args.kwargs.get("webserver_config_template")
        assert webserver_template is not None
        assert "admin-group" in webserver_template
        assert "super-users" in webserver_template
        assert "staff" in webserver_template

    def test_oauth_empty_group_config_produces_empty_roles_mapping_in_template(
        self,
        context,
        state_with_oauth,
        mock_get_provider_info,
    ):
        """When all IDP group config strings are empty (the default) the
        webserver_config_template must contain AUTH_ROLES_MAPPING = {}.
        """
        with unittest.mock.patch.object(
            airflow_coordinator.AirflowCoordinatorProvides,
            "set_airflow_config",
        ) as mock_set:
            state_out = context.run(context.on.start(), state_with_oauth)

        assert state_out.unit_status == ops.ActiveStatus()

        webserver_template = mock_set.call_args.kwargs.get("webserver_config_template", "")
        assert "AUTH_ROLES_MAPPING = {}" in webserver_template

    def test_oauth_provider_info_unavailable_does_not_block(
        self,
        context,
        state_with_oauth,
    ):
        """If get_provider_info raises the charm must remain Active and must
        not include auth_manager or webserver config data in its output.
        """
        with unittest.mock.patch.object(
            OAuthRequirer,
            "get_provider_info",
            side_effect=Exception("provider not ready"),
        ):
            state_out = context.run(context.on.start(), state_with_oauth)

        assert state_out.unit_status == ops.ActiveStatus()

        for relation in state_out.get_relations(constants.AIRFLOW_COORDINATOR_RELATION_NAME):
            config_template = relation.local_app_data.get("config-template", "")
            assert "auth_manager" not in config_template

    def test_oauth_provider_info_missing_client_id_does_not_block(
        self,
        context,
        state_with_oauth,
    ):
        """Provider info without client_id (incomplete handshake) must not
        activate OAuth — charm stays Active without auth_manager in config.
        """
        incomplete_provider = dataclasses.replace(VALID_PROVIDER_INFO, client_id=None)
        with unittest.mock.patch.object(
            OAuthRequirer,
            "get_provider_info",
            return_value=incomplete_provider,
        ):
            state_out = context.run(context.on.start(), state_with_oauth)

        assert state_out.unit_status == ops.ActiveStatus()

        for relation in state_out.get_relations(constants.AIRFLOW_COORDINATOR_RELATION_NAME):
            config_template = relation.local_app_data.get("config-template", "")
            assert "auth_manager" not in config_template

    def test_oauth_config_changed_event_triggers_reconcile(
        self,
        context,
        state_with_oauth,
        mock_get_provider_info,
    ):
        """config_changed (also bound to _reconcile) with an active oauth
        relation leaves the charm in ActiveStatus and includes webserver
        config data in the set_airflow_config call.
        """
        with unittest.mock.patch.object(
            airflow_coordinator.AirflowCoordinatorProvides,
            "set_airflow_config",
        ) as mock_set:
            state_out = context.run(context.on.config_changed(), state_with_oauth)

        assert state_out.unit_status == ops.ActiveStatus()
        kwargs = mock_set.call_args.kwargs
        assert kwargs.get("webserver_config_template") is not None
        assert "webserver_config__client_secret" in kwargs["sensitive_data"]

    def test_oauth_relation_removed_drops_auth_manager(
        self,
        context,
        all_required_relations,
        mock_command_executor,
        state_with_oauth,
        fernet_key_secret,
    ):
        """After the oauth relation is broken/removed auth_manager must not
        appear in the config template and webserver_config_template must be
        None in the set_airflow_config call.
        """
        state_without_oauth = dataclasses.replace(
            state_with_oauth,
            relations=all_required_relations,
            config={
                constants.FERNET_KEY_SECRET_CONFIG: fernet_key_secret.id,
            },
        )

        with unittest.mock.patch.object(
            airflow_coordinator.AirflowCoordinatorProvides,
            "set_airflow_config",
        ) as mock_set:
            state_out = context.run(context.on.start(), state_without_oauth)

        assert state_out.unit_status == ops.ActiveStatus()

        for relation in state_out.get_relations(constants.AIRFLOW_COORDINATOR_RELATION_NAME):
            config_template = relation.local_app_data.get("config-template", "")
            assert "auth_manager" not in config_template

        assert mock_set.call_args.kwargs.get("webserver_config_template") is None
