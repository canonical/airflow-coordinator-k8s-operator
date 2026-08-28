# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Tests for the OAuth integration."""

import configparser
import dataclasses
import unittest.mock

import charms.airflow_coordinator_k8s.v0.airflow_coordinator as airflow_coordinator
import charms.hydra.v0.oauth as oauth
import ops
import ops.testing
import pytest

import constants

OAUTH_CLIENT_ID = "airflow_client_id"
OAUTH_CLIENT_SECRET = "s3cR#T"
OAUTH_PROVIDER_INFO = {
    "authorization_endpoint": "https://example.oidc.com/oauth2/auth",
    "introspection_endpoint": "https://example.oidc.com/admin/oauth2/introspect",
    "issuer_url": "https://example.oidc.com",
    "jwks_endpoint": "https://example.oidc.com/.well-known/jwks.json",
    "scope": "openid profile email phone",
    "token_endpoint": "https://example.oidc.com/oauth2/token",
    "userinfo_endpoint": "https://example.oidc.com/userinfo",
}


@pytest.fixture(scope="function")
def oauth_client_secret():
    return ops.testing.Secret(
        {
            "secret": OAUTH_CLIENT_SECRET,
        }
    )


@pytest.fixture(scope="function")
def oauth_relation(oauth_client_secret):
    return ops.testing.Relation(
        "oauth",
        remote_app_data={
            "client_id": OAUTH_CLIENT_ID,
            "client_secret_id": oauth_client_secret.id,
            **OAUTH_PROVIDER_INFO,
        },
    )


@pytest.fixture(scope="function")
def state_with_oauth(state, oauth_relation, oauth_client_secret):
    return dataclasses.replace(
        state,
        relations=[*state.relations, oauth_relation],
        secrets=[*state.secrets, oauth_client_secret],
        config={
            **state.config,
            constants.IDP_GROUPS_FOR_ADMIN_CONFIG: "group1,,group2",
            constants.IDP_GROUPS_FOR_USER_CONFIG: "group3",
        },
    )


@pytest.fixture(scope="function")
def mock_get_provider_info(oauth_relation):
    """Patch OAuthRequirer.get_provider_info to return valid provider data."""
    with unittest.mock.patch.object(
        oauth.OAuthRequirer,
        "get_provider_info",
        return_value=oauth.OauthProviderConfig.from_dict(
            {
                **oauth_relation.remote_app_data,
                "client_secret": OAUTH_CLIENT_SECRET,
            }
        ),
    ) as mock:
        yield mock


class TestOAuth:
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

    def test_oauth_passes_webserver_config_template_to_set_airflow_config(
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
        assert OAUTH_CLIENT_SECRET not in webserver_template

    def test_oauth_redirect_uri_matches_fab_callback_route(
        self,
        context,
        state_with_oauth,
        mock_get_provider_info,
    ):
        """The registered redirect URI points at FAB's callback under `/auth`.

        Airflow 3 mounts the FAB auth manager at `/auth`, so a redirect URI without
        that prefix is rejected by the provider as unregistered.
        """
        with unittest.mock.patch.object(
            oauth.OAuthRequirer,
            "update_client_config",
        ) as mock_update:
            context.run(context.on.start(), state_with_oauth)

        assert mock_update.called

        client_config = mock_update.call_args.args[0]
        assert client_config.redirect_uri.endswith("/auth/oauth-authorized/hydra")
        assert client_config.redirect_uri.startswith(
            "http://test-host:test-port/"
        ), client_config.redirect_uri

    def test_oauth_requested_scope_matches_registered_scope(
        self,
        context,
        state_with_oauth,
        mock_get_provider_info,
    ):
        """FAB requests exactly the scope the client was registered with.

        The provider advertises the scopes it supports, which is a superset of what
        any one client is granted. Requesting that superset makes the provider deny
        the authorization request, so the rendered webserver config must use the
        registered scope instead.
        """
        with unittest.mock.patch.object(
            oauth.OAuthRequirer,
            "update_client_config",
        ) as mock_update:
            with unittest.mock.patch.object(
                airflow_coordinator.AirflowCoordinatorProvides,
                "set_airflow_config",
            ) as mock_set:
                context.run(context.on.start(), state_with_oauth)

        registered_scope = mock_update.call_args.args[0].scope
        assert registered_scope == constants.OAUTH_SCOPE

        webserver_template = mock_set.call_args.kwargs.get("webserver_config_template")
        assert f'"scope": "{registered_scope}"' in webserver_template
        # The provider supports `phone`; this client is not registered for it.
        assert "phone" not in webserver_template

    def test_oauth_includes_client_secret_in_sensitive_data(
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
        assert sensitive_data.get("webserver_config__client_secret") == OAUTH_CLIENT_SECRET

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
                constants.IDP_GROUPS_FOR_ADMIN_CONFIG: "admin-group,super-users",
                constants.IDP_GROUPS_FOR_USER_CONFIG: "staff",
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

    def test_oauth_provider_info_unavailable_does_not_block(
        self,
        context,
        state_with_oauth,
    ):
        """If get_provider_info raises the charm must remain Active and must
        not include auth_manager or webserver config data in its output.
        """
        with unittest.mock.patch.object(
            oauth.OAuthRequirer,
            "get_provider_info",
            side_effect=Exception("Invalid oauth provider config in relation"),
        ):
            state_out = context.run(context.on.start(), state_with_oauth)

        assert state_out.unit_status == ops.BlockedStatus(constants.INVALID_OAUTH_RELATION_MESSAGE)

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
        with unittest.mock.patch.object(
            oauth.OAuthRequirer,
            "get_provider_info",
            return_value=oauth.OauthProviderConfig.from_dict(
                OAUTH_PROVIDER_INFO
            ),  # missing 'client_id'
        ):
            state_out = context.run(context.on.start(), state_with_oauth)

        assert state_out.unit_status == ops.ActiveStatus()

        for relation in state_out.get_relations(constants.AIRFLOW_COORDINATOR_RELATION_NAME):
            config_template = relation.local_app_data.get("config-template", "")
            assert "auth_manager" not in config_template

    def test_oauth_provider_info_missing_scope_does_not_block(
        self,
        context,
        oauth_relation,
        state_with_oauth,
    ):
        """Provider info without scope (required info in oauth) must not
        activate OAuth — charm stays Active without auth_manager in config.
        """
        provider_info_without_scope = oauth.OauthProviderConfig.from_dict(
            oauth_relation.remote_app_data
        )
        provider_info_without_scope.scope = None

        with unittest.mock.patch.object(
            oauth.OAuthRequirer,
            "get_provider_info",
            return_value=provider_info_without_scope,
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
