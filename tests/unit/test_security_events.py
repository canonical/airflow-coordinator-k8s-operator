"""Scenario tests for coordinator security-event auditing."""

import dataclasses
import json

import ops
import ops.testing
import pytest
from test_oauth import OAUTH_CLIENT_ID, OAUTH_PROVIDER_INFO

import constants
from charm import AirflowCoordinatorK8SOperatorCharm

FERNET_EVENT = "authz_admin:unattributed,rotate_fernet_key"
CLIENT_SECRET_EVENT = "authz_admin:unattributed,update_client_secret"


def _run(event_factory, state: ops.testing.State) -> tuple[ops.testing.State, dict[str, dict]]:
    """Run one event in a fresh context and return the output state and emitted OWASP events."""
    context = ops.testing.Context(AirflowCoordinatorK8SOperatorCharm)
    state_out = context.run(event_factory(context.on), state)
    events = {}
    for line in context.juju_log:
        if '"owasp_event"' in line.message:
            event = json.loads(line.message)["owasp_event"]
            events[event["event"]] = event
    return state_out, events


def _replace_secret(state: ops.testing.State, secret: ops.testing.Secret) -> ops.testing.State:
    return dataclasses.replace(
        state, secrets=[s for s in state.secrets if s.id != secret.id] + [secret]
    )


def _new_revision(secret: ops.testing.Secret, content: dict[str, str]) -> ops.testing.Secret:
    return dataclasses.replace(
        secret, latest_content=content, _latest_revision=secret._latest_revision + 1
    )


def _audit_state(state: ops.testing.State) -> dict:
    peer = state.get_relations(constants.PEER_RELATION_NAME)[0]
    return json.loads(peer.local_app_data[constants.SECURITY_AUDIT_STATE])


@pytest.fixture
def oauth_client_secret():
    return ops.testing.Secret({"secret": "s3cR#T"})


@pytest.fixture
def oauth_relation(oauth_client_secret):
    return ops.testing.Relation(
        constants.OAUTH_ENDPOINT_NAME,
        remote_app_data={
            "client_id": OAUTH_CLIENT_ID,
            "client_secret_id": oauth_client_secret.id,
            **OAUTH_PROVIDER_INFO,
        },
    )


@pytest.fixture
def baseline(state, oauth_relation, oauth_client_secret):
    """Output state of a first successful reconcile with OAuth enabled."""
    state_in = dataclasses.replace(
        state,
        relations=[*state.relations, oauth_relation],
        secrets=[*state.secrets, oauth_client_secret],
        config={**state.config, constants.IDP_GROUPS_FOR_ADMIN_CONFIG: "admins"},
    )
    state_out, events = _run(lambda on: on.config_changed(), state_in)
    assert state_out.unit_status == ops.ActiveStatus()
    assert not events
    # The replanned Pebble check fails Scenario's consistency check when the output
    # is fed back in; it is irrelevant to auditing, so restore the input container.
    return dataclasses.replace(state_out, containers=state.containers)


def test_first_reconcile_records_baseline_without_events(
    baseline, fernet_key_secret, oauth_client_secret
):
    snapshot = _audit_state(baseline)

    assert snapshot["policy"][constants.IDP_GROUPS_FOR_ADMIN_CONFIG] == "admins"
    assert snapshot["policy"]["oauth_enabled"] is True
    assert snapshot["credentials"] == {
        "airflow_signing_keys_revision": 1,
        "fernet_key_secret_id": fernet_key_secret.id,
        "hydra_client_id": OAUTH_CLIENT_ID,
        "hydra_client_secret_id": oauth_client_secret.id,
    }


def test_policy_change_emits_redacted_config_event(baseline):
    state_in = dataclasses.replace(
        baseline,
        config={**baseline.config, constants.IDP_GROUPS_FOR_ADMIN_CONFIG: "platform-admins"},
    )

    state_out, events = _run(lambda on: on.config_changed(), state_in)

    event = events.pop("authz_admin:unattributed,update_security_config")
    assert not events
    assert event["data"] == {
        "changed_settings": [constants.IDP_GROUPS_FOR_ADMIN_CONFIG],
        "observed_via": "coordinator_config_reconcile",
        "actor_attribution": "juju_audit_log",
    }
    assert event["type"] == "security"
    assert event["level"] == "WARN"
    assert event["actor"] == "unattributed"
    assert event["target"] == "airflow-coordinator-k8s"
    assert "platform-admins" not in json.dumps(event)
    assert _audit_state(state_out)["policy"][constants.IDP_GROUPS_FOR_ADMIN_CONFIG] == (
        "platform-admins"
    )


def test_signing_key_revision_change_emits_rotate_signing_key(baseline):
    keys_secret = next(
        s for s in baseline.secrets if s.label == constants.AIRFLOW_KEYS_SECRET_LABEL
    )
    rotated = dataclasses.replace(
        _new_revision(keys_secret, {"secret-key": "new", "jwt-secret": "new"}),
        tracked_content={"secret-key": "new", "jwt-secret": "new"},
    )

    _, events = _run(lambda on: on.config_changed(), _replace_secret(baseline, rotated))

    assert events.keys() == {"authz_admin:unattributed,rotate_signing_key"}


def test_fernet_key_secret_swap_emits_rotate_fernet_key(baseline):
    new_secret = ops.testing.Secret({constants.FERNET_KEY: "new-fernet-key"})
    state_in = dataclasses.replace(
        baseline,
        secrets=[*baseline.secrets, new_secret],
        config={**baseline.config, constants.FERNET_KEY_SECRET_CONFIG: new_secret.id},
    )

    state_out, events = _run(lambda on: on.config_changed(), state_in)

    assert events.keys() == {FERNET_EVENT}
    assert events[FERNET_EVENT]["data"]["observed_via"] == "charm_config"
    assert _audit_state(state_out)["credentials"]["fernet_key_secret_id"] == new_secret.id


def test_fernet_key_new_revision_is_tracked_and_audited(baseline, fernet_key_secret):
    rotated = _new_revision(fernet_key_secret, {constants.FERNET_KEY: "new-fernet-key"})

    state_out, events = _run(
        lambda on: on.secret_changed(rotated), _replace_secret(baseline, rotated)
    )

    assert state_out.unit_status == ops.ActiveStatus()
    assert events.keys() == {FERNET_EVENT}
    assert events[FERNET_EVENT]["data"]["observed_via"] == "juju_secret_changed"
    assert "new-fernet-key" not in json.dumps(events)
    assert state_out.get_secret(id=rotated.id).tracked_content == {
        constants.FERNET_KEY: "new-fernet-key"
    }


def test_hydra_client_secret_swap_emits_update_client_secret(baseline, oauth_relation):
    new_secret = ops.testing.Secret({"secret": "n3w-s3cR#T"})
    relation = dataclasses.replace(
        baseline.get_relation(oauth_relation.id),
        remote_app_data={
            **oauth_relation.remote_app_data,
            "client_secret_id": new_secret.id,
        },
    )
    state_in = dataclasses.replace(
        baseline,
        relations=[r for r in baseline.relations if r.id != relation.id] + [relation],
        secrets=[*baseline.secrets, new_secret],
    )

    _, events = _run(lambda on: on.relation_changed(relation), state_in)

    assert events.keys() == {CLIENT_SECRET_EVENT}
    assert events[CLIENT_SECRET_EVENT]["data"] == {
        "credential_type": "hydra_client_secret",
        "client_id": OAUTH_CLIENT_ID,
        "observed_via": "oauth_relation_data",
        "actor_attribution": "juju_audit_log",
    }


def test_hydra_client_secret_new_revision_is_tracked_and_audited(baseline, oauth_client_secret):
    rotated = _new_revision(oauth_client_secret, {"secret": "n3w-s3cR#T"})

    state_out, events = _run(
        lambda on: on.secret_changed(rotated), _replace_secret(baseline, rotated)
    )

    assert state_out.unit_status == ops.ActiveStatus()
    assert events.keys() == {CLIENT_SECRET_EVENT}
    assert events[CLIENT_SECRET_EVENT]["data"]["observed_via"] == "juju_secret_changed"
    assert "n3w-s3cR#T" not in json.dumps(events)
    assert state_out.get_secret(id=rotated.id).tracked_content == {"secret": "n3w-s3cR#T"}


def test_unrelated_secret_changed_emits_nothing(baseline, git_credentials_secret):
    rotated = _new_revision(
        git_credentials_secret, {"credentials-personal-access-token": "new-token"}
    )

    state_out, events = _run(
        lambda on: on.secret_changed(rotated), _replace_secret(baseline, rotated)
    )

    assert state_out.unit_status == ops.ActiveStatus()
    assert not events


def test_non_leader_tracks_new_fernet_revision_without_auditing(baseline, fernet_key_secret):
    rotated = _new_revision(fernet_key_secret, {constants.FERNET_KEY: "new-fernet-key"})
    state_in = dataclasses.replace(_replace_secret(baseline, rotated), leader=False)

    state_out, events = _run(lambda on: on.secret_changed(rotated), state_in)

    assert not events
    assert state_out.get_secret(id=rotated.id).tracked_content == {
        constants.FERNET_KEY: "new-fernet-key"
    }
