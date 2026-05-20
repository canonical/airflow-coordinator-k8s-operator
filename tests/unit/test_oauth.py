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
import webserver_config_generator

# ---------------------------------------------------------------------------
# Shared test data
# ---------------------------------------------------------------------------

MOCK_WEBSERVER_CONFIG = (
    "# webserver_config.py — auto-generated\n"
    "from flask_appbuilder.security.manager import AUTH_OAUTH\n"
    "AUTH_TYPE = AUTH_OAUTH\n"
    "AUTH_ROLES_MAPPING = {}\n"
)

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


@pytest.fixture(scope="function")
def mock_render_webserver_config():
    """Patch render_webserver_config to return a predictable sentinel string."""
    with unittest.mock.patch.object(
        webserver_config_generator,
        "render_webserver_config",
        return_value=MOCK_WEBSERVER_CONFIG,
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
        mock_render_webserver_config,
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

    def test_oauth_active_includes_webserver_config_in_sensitive_data(
        self,
        context,
        state_with_oauth,
        mock_get_provider_info,
        mock_render_webserver_config,
    ):
        """When OAuth is active webserver_config_content is included in the
        sensitive_data passed to set_airflow_config.
        """
        with unittest.mock.patch.object(
            airflow_coordinator.AirflowCoordinatorProvides,
            "set_airflow_config",
        ) as mock_set:
            context.run(context.on.start(), state_with_oauth)

        assert mock_set.called
        sensitive_data = mock_set.call_args.kwargs["sensitive_data"]
        assert "webserver_config_content" in sensitive_data
        assert sensitive_data["webserver_config_content"] == MOCK_WEBSERVER_CONFIG

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

    def test_oauth_absent_no_webserver_config_in_sensitive_data(
        self,
        context,
        state,
    ):
        """Without an oauth relation webserver_config_content must not be
        present in the sensitive_data passed to set_airflow_config.
        """
        with unittest.mock.patch.object(
            airflow_coordinator.AirflowCoordinatorProvides,
            "set_airflow_config",
        ) as mock_set:
            context.run(context.on.start(), state)

        assert mock_set.called
        sensitive_data = mock_set.call_args.kwargs["sensitive_data"]
        assert "webserver_config_content" not in sensitive_data

    def test_oauth_group_config_forwarded_to_renderer(
        self,
        context,
        mock_command_executor,
        mock_get_provider_info,
        state_with_oauth,
    ):
        """IDP group config values set on the coordinator are forwarded
        verbatim to render_webserver_config as the idp_group_config dict.
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
            webserver_config_generator,
            "render_webserver_config",
            return_value=MOCK_WEBSERVER_CONFIG,
        ) as mock_render:
            context.run(context.on.config_changed(), state)

        mock_render.assert_called_once()
        idp_group_config = mock_render.call_args.kwargs["idp_group_config"]
        assert idp_group_config["admin"] == "admin-group,super-users"
        assert idp_group_config["user"] == "staff"
        # Roles not explicitly configured must default to empty string
        assert idp_group_config["op"] == ""
        assert idp_group_config["viewer"] == ""
        assert idp_group_config["public"] == ""

    def test_oauth_empty_group_config_produces_empty_roles_mapping(
        self,
        context,
        state_with_oauth,
        mock_get_provider_info,
    ):
        """When all IDP group config strings are empty (the default) the
        rendered webserver_config.py must contain AUTH_ROLES_MAPPING = {}.
        """
        with unittest.mock.patch.object(
            airflow_coordinator.AirflowCoordinatorProvides,
            "set_airflow_config",
        ) as mock_set:
            state_out = context.run(context.on.start(), state_with_oauth)

        assert state_out.unit_status == ops.ActiveStatus()

        sensitive_data = mock_set.call_args.kwargs["sensitive_data"]
        webserver_config = sensitive_data.get("webserver_config_content", "")
        assert "AUTH_ROLES_MAPPING = {}" in webserver_config

    def test_oauth_provider_info_unavailable_does_not_block(
        self,
        context,
        state_with_oauth,
    ):
        """If get_provider_info raises the charm must remain Active and must
        not include auth_manager or webserver_config_content in its output.
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
        mock_render_webserver_config,
    ):
        """config_changed (also bound to _reconcile) with an active oauth
        relation leaves the charm in ActiveStatus and propagates the
        webserver config to sensitive_data.
        """
        with unittest.mock.patch.object(
            airflow_coordinator.AirflowCoordinatorProvides,
            "set_airflow_config",
        ) as mock_set:
            state_out = context.run(context.on.config_changed(), state_with_oauth)

        assert state_out.unit_status == ops.ActiveStatus()
        sensitive_data = mock_set.call_args.kwargs["sensitive_data"]
        assert sensitive_data.get("webserver_config_content") == MOCK_WEBSERVER_CONFIG

    def test_oauth_relation_removed_drops_auth_manager(
        self,
        context,
        all_required_relations,
        mock_command_executor,
        state_with_oauth,
    ):
        """After the oauth relation is broken/removed auth_manager must not
        appear in the config template and webserver_config_content must be
        absent from sensitive_data.
        """
        state_without_oauth = dataclasses.replace(
            state_with_oauth, relations=all_required_relations
        )

        state_out = context.run(context.on.start(), state_without_oauth)

        assert state_out.unit_status == ops.ActiveStatus()

        for relation in state_out.get_relations(constants.AIRFLOW_COORDINATOR_RELATION_NAME):
            config_template = relation.local_app_data.get("config-template", "")
            assert "auth_manager" not in config_template
