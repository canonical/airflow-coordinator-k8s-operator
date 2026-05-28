# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Tests for the OAuth integration."""

import dataclasses

import ops
import ops.testing
import pytest

import constants

OAUTH_CLIENT_ID = "grafana_client_id"
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


# TODO: further refine assertions as the feature is incrementally implemented
def test_oauth_relation(context, state_with_oauth, oauth_relation):
    """Stub to be further refined as integration with OAuth fully implemented."""
    state_out = context.run(context.on.relation_changed(oauth_relation), state_with_oauth)

    assert state_out.unit_status == ops.ActiveStatus()


# TODO: further refine assertions as the feature is incrementally implemented
def test_oauth_config_changes(context, state_with_oauth):
    """Stub to be further refined as integration with OAuth fully implemented."""
    state_with_oauth_and_configs = dataclasses.replace(
        state_with_oauth,
        config={
            **state_with_oauth.config,
            constants.IDP_GROUPS_FOR_OP_CONFIG: "group4",
        },
    )

    state_out = context.run(context.on.config_changed(), state_with_oauth_and_configs)

    assert state_out.unit_status == ops.ActiveStatus()
