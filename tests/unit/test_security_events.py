"""Tests for coordinator security-event auditing."""

import json
import logging
from types import SimpleNamespace

from charm import AirflowCoordinatorK8SOperatorCharm


def _audit(charm_state: dict[str, str], current: dict) -> None:
    charm = SimpleNamespace(
        _peer_application_data=charm_state,
        app=SimpleNamespace(name="airflow-coordinator"),
    )
    AirflowCoordinatorK8SOperatorCharm._audit_security_state(charm, current)


def test_first_security_snapshot_establishes_baseline_without_event(caplog):
    state = {}
    current = {
        "policy": {"idp_groups_for_admin": "admins", "oauth_enabled": True},
        "credentials": {
            "airflow_signing_keys_revision": 1,
            "hydra_client_id": "airflow-client",
            "hydra_client_secret_revision": 1,
        },
    }

    with caplog.at_level(logging.INFO, logger="security_events"):
        _audit(state, current)

    assert not caplog.records
    assert json.loads(state["security_audit_state"]) == current


def test_policy_and_credential_changes_emit_redacted_events(caplog):
    previous = {
        "policy": {"idp_groups_for_admin": "admins", "oauth_enabled": True},
        "credentials": {
            "airflow_signing_keys_revision": 1,
            "hydra_client_id": "airflow-client",
            "hydra_client_secret_revision": 2,
        },
    }
    state = {"security_audit_state": json.dumps(previous)}
    current = {
        "policy": {"idp_groups_for_admin": "platform-admins", "oauth_enabled": True},
        "credentials": {
            "airflow_signing_keys_revision": 2,
            "hydra_client_id": "airflow-client",
            "hydra_client_secret_revision": 3,
        },
    }

    with caplog.at_level(logging.INFO, logger="security_events"):
        _audit(state, current)

    events = [json.loads(record.getMessage()) for record in caplog.records]
    assert {
        event["event"]: event["data"]
        for event in events
    } == {
        "authz_admin:unattributed,update_security_config": {
            "changed_settings": ["idp_groups_for_admin"],
            "observed_via": "coordinator_config_reconcile",
            "actor_attribution": "juju_audit_log",
        },
        "authz_admin:unattributed,rotate_signing_key": {
            "credential_set": "airflow_api_signing_keys",
            "observed_via": "juju_secret_revision",
            "actor_attribution": "juju_audit_log",
        },
        "authz_admin:unattributed,update_client_secret": {
            "credential_type": "hydra_client_secret",
            "client_id": "airflow-client",
            "observed_via": "juju_secret_revision",
            "actor_attribution": "juju_audit_log",
        },
    }
    assert json.loads(state["security_audit_state"]) == current
