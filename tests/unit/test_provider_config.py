# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.
#
# Tests for the airflow-provider-configuration requirer wiring: the coordinator
# consumes a provider's non-sensitive config template (merging its sections into
# the distributed airflow.cfg template) and its sensitive values (merging them
# into the distributed sensitive-data secret).
#
# Behaviour under test also covers the hardening applied after review:
#   * provider config merged first + reserved-key denylist (cannot override
#     coordinator-owned / security-critical config);
#   * malformed provider INI -> BlockedStatus;
#   * relation_broken -> provider config removed entirely;
#   * secret-not-ready -> soft proceed (Active), mirroring the k8s-executor path.

import configparser
import dataclasses
import json

import ops
import ops.testing

import constants

# A provider template exercising both a sensitive placeholder (conn_id, resolved
# from the charm secret) and a plain non-sensitive option (project).
PROVIDER_CONFIG_TEMPLATE = "[gcs]\nconn_id = {{ provider__gcs__conn_id }}\nproject = my-project\n"

# The flat placeholder -> value map the provider stores in its charm secret,
# JSON-encoded under the interface's single Juju-valid secret key.
PROVIDER_SENSITIVE_VALUES = {"provider__gcs__conn_id": "my-secret-conn-id"}

# Interface databag / secret keys (kept in sync with airflow_provider_configurator).
DATABAG_KEY_CONFIGURATION = "provider-configuration"
DATABAG_KEY_SECRET_URI = "provider-configuration-secret-uri"
SENSITIVE_DATA_SECRET_KEY = "sensitive-data"


def _sensitive_secret(values: dict = PROVIDER_SENSITIVE_VALUES) -> ops.testing.Secret:
    """Build a provider charm secret in the interface's expected shape."""
    return ops.testing.Secret({SENSITIVE_DATA_SECRET_KEY: json.dumps(values)})


def _provider_relation(template: str, secret_uri: str) -> ops.testing.Relation:
    """Build a provider-configuration relation with the given template + secret uri."""
    return ops.testing.Relation(
        constants.AIRFLOW_PROVIDER_CONFIGURATION_RELATION_NAME,
        remote_app_data={
            DATABAG_KEY_CONFIGURATION: template,
            DATABAG_KEY_SECRET_URI: secret_uri,
        },
    )


def _distributed_config_templates(state_out):
    """Yield the config-template distributed on each airflow-coordinator relation."""
    for relation in state_out.get_relations(constants.AIRFLOW_COORDINATOR_RELATION_NAME):
        yield relation.local_app_data.get("config-template", "")


def _parse(config_template: str) -> configparser.RawConfigParser:
    """Parse a distributed config template the same way the charm does."""
    parser = configparser.RawConfigParser()
    parser.optionxform = str  # type: ignore[assignment, method-assign]
    parser.read_string(config_template)
    return parser


def _provider_section_present(state_out) -> bool:
    """Whether the provider's [gcs] section made it into any distributed template."""
    return any(
        _parse(config_template).has_section("gcs")
        for config_template in _distributed_config_templates(state_out)
    )


def _distributed_sensitive_data(state_out, relation) -> dict:
    """Decode the sensitive-data secret the coordinator distributed on a relation."""
    sensitive_secret_id = relation.local_app_data["secret-sensitive-data"]
    return json.loads(
        state_out.get_secret(id=sensitive_secret_id).latest_content["sensitive-data"]
    )


def test_provider_config_sections_merged(context, state, workload_container):
    """Provider template + readable secret: sections and sensitive values distributed."""
    secret = _sensitive_secret()
    relation = _provider_relation(PROVIDER_CONFIG_TEMPLATE, secret.id)

    state_in = dataclasses.replace(
        state,
        relations=[*state.relations, relation],
        secrets=[*state.secrets, secret],
    )

    state_out = context.run(context.on.start(), state_in)

    assert state_out.unit_status == ops.ActiveStatus()

    coordinator_relations = state_out.get_relations(constants.AIRFLOW_COORDINATOR_RELATION_NAME)
    assert coordinator_relations
    for coordinator_relation in coordinator_relations:
        parsed = _parse(coordinator_relation.local_app_data.get("config-template", ""))

        # Non-sensitive option merged verbatim; sensitive option kept as the
        # placeholder for later rendering by the consuming core charm.
        assert parsed.get("gcs", "project") == "my-project"
        assert parsed.get("gcs", "conn_id") == "{{ provider__gcs__conn_id }}"

        # Sensitive value flows into the distributed sensitive-data secret.
        sensitive_data = _distributed_sensitive_data(state_out, coordinator_relation)
        assert sensitive_data["provider__gcs__conn_id"] == "my-secret-conn-id"


def test_provider_config_removed_when_relation_gone(context, state, workload_container):
    """Removing the provider relation drops its sections on the next reconcile."""
    secret = _sensitive_secret()
    relation = _provider_relation(PROVIDER_CONFIG_TEMPLATE, secret.id)

    state_in = dataclasses.replace(
        state,
        relations=[*state.relations, relation],
        secrets=[*state.secrets, secret],
    )

    # Sanity: sections present while related.
    state_present = context.run(context.on.start(), state_in)
    assert state_present.unit_status == ops.ActiveStatus()
    assert _provider_section_present(state_present)

    # Relation removed -> reconcile (idempotent property returns {}) -> gone.
    # update_status is used to drive the reconcile, mirroring the S3/git
    # removal tests in test_charm.py.
    relations_without_provider = [
        r
        for r in state_in.relations
        if r.endpoint != constants.AIRFLOW_PROVIDER_CONFIGURATION_RELATION_NAME
    ]
    state_removed = dataclasses.replace(
        state_in, relations=relations_without_provider, containers=[workload_container]
    )

    state_out = context.run(context.on.update_status(), state_removed)

    assert state_out.unit_status == ops.ActiveStatus()
    assert not _provider_section_present(state_out)


def test_provider_config_removed_on_relation_broken(context, state, workload_container):
    """On relation_broken the provider config is removed entirely (spec requirement).

    During the broken hook ``model.get_relation`` still returns the departing
    relation, so the charm must guard on ``relation.active`` -- which is False
    here -- to drop the config.
    """
    secret = _sensitive_secret()
    relation = _provider_relation(PROVIDER_CONFIG_TEMPLATE, secret.id)

    # The relation must be present in state for the broken event to fire against it.
    state_in = dataclasses.replace(
        state,
        relations=[*state.relations, relation],
        secrets=[*state.secrets, secret],
    )

    state_out = context.run(context.on.relation_broken(relation), state_in)

    assert state_out.unit_status == ops.ActiveStatus()
    assert not _provider_section_present(state_out)


def test_provider_config_secret_not_ready_soft_proceeds(context, state, workload_container):
    """Secret shared but not granted: log + proceed (Active), per the k8s-executor approach.

    A placeholder-free template is used so the assertion isolates the soft-catch
    path (SecretNotReadyError -> {}) and does not depend on how an unrendered
    ``{{ ... }}`` placeholder is treated in the coordinator's own db-migrate cfg.
    """
    # A well-formed secret id deliberately NOT added to state, so model.get_secret
    # raises SecretNotFoundError -> the interface raises SecretNotReadyError.
    ungranted_secret = _sensitive_secret()
    relation = _provider_relation("[gcs]\nproject = my-project\n", ungranted_secret.id)

    state_in = dataclasses.replace(
        state,
        relations=[*state.relations, relation],
        # ungranted_secret intentionally omitted from secrets.
    )

    state_out = context.run(context.on.start(), state_in)

    # Soft proceed: not blocked despite the unreadable secret.
    assert state_out.unit_status == ops.ActiveStatus()

    coordinator_relations = state_out.get_relations(constants.AIRFLOW_COORDINATOR_RELATION_NAME)
    assert coordinator_relations
    for coordinator_relation in coordinator_relations:
        # Non-sensitive provider config is still merged.
        parsed = _parse(coordinator_relation.local_app_data.get("config-template", ""))
        assert parsed.get("gcs", "project") == "my-project"

        # No provider sensitive values were distributed (secret was unreadable).
        sensitive_data = _distributed_sensitive_data(state_out, coordinator_relation)
        assert "provider__gcs__conn_id" not in sensitive_data


def test_provider_config_reserved_key_is_dropped(context, state, workload_container):
    """A provider cannot override a reserved, security-critical base-template key."""
    secret = _sensitive_secret()
    # Provider tries to hijack the fernet key alongside a benign option.
    template = "[core]\nfernet_key = pwned\n[gcs]\nproject = my-project\n"
    relation = _provider_relation(template, secret.id)

    state_in = dataclasses.replace(
        state,
        relations=[*state.relations, relation],
        secrets=[*state.secrets, secret],
    )

    state_out = context.run(context.on.start(), state_in)

    assert state_out.unit_status == ops.ActiveStatus()
    for config_template in _distributed_config_templates(state_out):
        # Reserved key override dropped: the malicious value never reaches the cfg.
        assert "pwned" not in config_template
        # Benign provider option still merged.
        assert _parse(config_template).get("gcs", "project") == "my-project"


def test_provider_config_invalid_ini_blocks(context, state, workload_container):
    """Malformed provider configuration (another app's data) -> BlockedStatus, not a crash."""
    secret = _sensitive_secret()
    # No section header -> configparser.MissingSectionHeaderError.
    relation = _provider_relation("key_without_section = value\n", secret.id)

    state_in = dataclasses.replace(
        state,
        relations=[*state.relations, relation],
        secrets=[*state.secrets, secret],
    )

    state_out = context.run(context.on.start(), state_in)

    assert state_out.unit_status == ops.BlockedStatus(constants.INVALID_PROVIDER_CONFIG_MESSAGE)
