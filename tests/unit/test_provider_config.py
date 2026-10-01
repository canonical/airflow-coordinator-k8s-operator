# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.
#
# Tests for the airflow-provider-configuration requirer wiring: the coordinator
# consumes a provider's non-sensitive config template (merging its sections into
# the distributed airflow.cfg template) and its sensitive values (merging them
# into the distributed sensitive-data secret).
#
# Behaviour under test also covers the hardening applied after review:
#   * provider config merged first, so coordinator-owned config wins;
#   * malformed provider INI -> BlockedStatus;
#   * relation_broken -> provider config removed entirely;
#   * secret unreadable -> provider config omitted entirely, reported on status;
#   * sensitive values confined to the `provider__` namespace;
#   * Jinja2 syntax in provider content escaped unless it is a backed placeholder.
#
# Collision handling between provider keys and coordinator-owned keys is Layer 1
# validation and is covered by a follow-up change.

import configparser
import dataclasses
import json

import jinja2
import ops
import ops.testing
import pytest

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


def test_provider_config_secret_unreadable_omits_config(context, state, workload_container):
    """Secret shared but not granted: drop the provider config, report it, stay Active.

    The template carries a placeholder, which is the case that matters: rendering
    it without the secret would write an empty credential into every core charm's
    airflow.cfg while the unit still reported Active.
    """
    # A well-formed secret id deliberately NOT added to state, so model.get_secret
    # raises SecretNotFoundError -> the interface raises SecretNotReadyError.
    ungranted_secret = _sensitive_secret()
    relation = _provider_relation(PROVIDER_CONFIG_TEMPLATE, ungranted_secret.id)

    state_in = dataclasses.replace(
        state,
        relations=[*state.relations, relation],
        # ungranted_secret intentionally omitted from secrets.
    )

    state_out = context.run(context.on.start(), state_in)

    # Not blocked, but the dropped configuration is visible on the status.
    assert state_out.unit_status == ops.ActiveStatus(
        constants.WAITING_FOR_PROVIDER_CONFIG_SECRET_MESSAGE
    )

    # Nothing from the provider was distributed -- config and sensitive values
    # are dropped together, so no placeholder can render blank.
    assert not _provider_section_present(state_out)
    for coordinator_relation in state_out.get_relations(
        constants.AIRFLOW_COORDINATOR_RELATION_NAME
    ):
        sensitive_data = _distributed_sensitive_data(state_out, coordinator_relation)
        assert "provider__gcs__conn_id" not in sensitive_data


def test_provider_sensitive_values_cannot_override_coordinator(
    context, state, workload_container
):
    """Sensitive keys outside the `provider__` namespace are dropped.

    Without this filter a provider could ship ``core__fernet_key`` and replace the
    coordinator's real fernet key, which Layer 1 cannot catch: it validates
    configuration keys, and this travels in the separate sensitive-data map.
    """
    secret = _sensitive_secret(
        {
            "provider__gcs__conn_id": "my-secret-conn-id",
            "core__fernet_key": "EVIL",
            "database__sql_alchemy_conn": "postgresql://evil/",
        }
    )
    relation = _provider_relation(PROVIDER_CONFIG_TEMPLATE, secret.id)

    state_in = dataclasses.replace(
        state,
        relations=[*state.relations, relation],
        secrets=[*state.secrets, secret],
    )

    state_out = context.run(context.on.start(), state_in)

    assert state_out.unit_status == ops.ActiveStatus(
        constants.DROPPED_PROVIDER_SENSITIVE_KEYS_MESSAGE
    )

    coordinator_relations = state_out.get_relations(constants.AIRFLOW_COORDINATOR_RELATION_NAME)
    assert coordinator_relations
    for coordinator_relation in coordinator_relations:
        sensitive_data = _distributed_sensitive_data(state_out, coordinator_relation)

        # The namespaced value is kept.
        assert sensitive_data["provider__gcs__conn_id"] == "my-secret-conn-id"

        # The coordinator's own secrets are untouched.
        assert sensitive_data["core__fernet_key"] != "EVIL"
        assert not sensitive_data["database__sql_alchemy_conn"].startswith("postgresql://evil/")


def test_provider_config_jinja_is_escaped(context, state, workload_container):
    """Jinja2 in provider content is neutralised unless it is a backed placeholder.

    The provider configurator syncs from a git repository, so its content is
    untrusted: the coordinator renders it, and so does every core charm. Three
    cases matter here -- a template-injection payload, a legitimate Airflow
    setting that happens to contain Jinja2, and a placeholder with no backing
    value (which would otherwise render blank).
    """
    secret = _sensitive_secret()
    template = (
        "[gcs]\n"
        "conn_id = {{ provider__gcs__conn_id }}\n"
        'injected = {{ "".__class__.__mro__[1].__subclasses__()|length }}\n'
        "stolen = {{ core__fernet_key }}\n"
        "unbacked = {{ provider__gcs__missing }}\n"
        "fused = {{{ provider__gcs__conn_id }}\n"
        "[logging]\n"
        "log_filename_template = dag_id={{ ti.dag_id }}/run.log\n"
    )
    relation = _provider_relation(template, secret.id)

    state_in = dataclasses.replace(
        state,
        relations=[*state.relations, relation],
        secrets=[*state.secrets, secret],
    )

    state_out = context.run(context.on.start(), state_in)

    assert state_out.unit_status == ops.ActiveStatus(
        constants.ESCAPED_PROVIDER_TEMPLATE_SYNTAX_MESSAGE
    )

    coordinator_relations = state_out.get_relations(constants.AIRFLOW_COORDINATOR_RELATION_NAME)
    assert coordinator_relations
    for coordinator_relation in coordinator_relations:
        config_template = coordinator_relation.local_app_data.get("config-template", "")
        sensitive_data = _distributed_sensitive_data(state_out, coordinator_relation)

        # Render the way a core charm does, to assert on the final airflow.cfg.
        rendered = _parse(jinja2.Template(config_template).render(**sensitive_data))

        # The backed placeholder still resolves.
        assert rendered.get("gcs", "conn_id") == "my-secret-conn-id"

        # The injection payload was not evaluated; it survives as literal text.
        assert (
            rendered.get("gcs", "injected")
            == '{{ "".__class__.__mro__[1].__subclasses__()|length }}'
        )

        # A coordinator placeholder supplied by the provider does not resolve.
        assert rendered.get("gcs", "stolen") == "{{ core__fernet_key }}"
        assert sensitive_data["core__fernet_key"] not in config_template

        # An unbacked provider placeholder stays visible rather than rendering blank.
        assert rendered.get("gcs", "unbacked") == "{{ provider__gcs__missing }}"

        # A stray `{` immediately before a backed placeholder must not fuse with it
        # into an unparsable `{{{ ... }}` -- that would crash every core charm.
        assert rendered.get("gcs", "fused") == "{my-secret-conn-id"

        # A legitimate Airflow setting containing Jinja2 reaches airflow.cfg intact,
        # for Airflow itself to template at runtime.
        assert (
            rendered.get("logging", "log_filename_template")
            == "dag_id={{ ti.dag_id }}/run.log"
        )


def test_provider_config_rerendered_on_secret_changed(context, state, workload_container):
    """A rotated provider secret is picked up without any relation churn."""
    secret = _sensitive_secret({"provider__gcs__conn_id": "rotated-conn-id"})
    relation = _provider_relation(PROVIDER_CONFIG_TEMPLATE, secret.id)

    state_in = dataclasses.replace(
        state,
        relations=[*state.relations, relation],
        secrets=[*state.secrets, secret],
    )

    state_out = context.run(context.on.secret_changed(secret), state_in)

    assert state_out.unit_status == ops.ActiveStatus()

    coordinator_relations = state_out.get_relations(constants.AIRFLOW_COORDINATOR_RELATION_NAME)
    assert coordinator_relations
    for coordinator_relation in coordinator_relations:
        sensitive_data = _distributed_sensitive_data(state_out, coordinator_relation)
        assert sensitive_data["provider__gcs__conn_id"] == "rotated-conn-id"


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


@pytest.mark.parametrize(
    "content",
    [
        pytest.param({"wrong-key": "{}"}, id="missing-sensitive-data-key"),
        pytest.param({SENSITIVE_DATA_SECRET_KEY: "not json"}, id="invalid-json"),
        pytest.param({SENSITIVE_DATA_SECRET_KEY: "[1, 2, 3]"}, id="json-array"),
        pytest.param({SENSITIVE_DATA_SECRET_KEY: '"a string"'}, id="json-string"),
    ],
)
def test_provider_config_malformed_secret_blocks(context, state, workload_container, content):
    """A readable-but-malformed provider secret -> BlockedStatus, not a hook crash.

    The interface only promises the shape; a buggy provider can still store a
    secret with the wrong key, invalid JSON, or JSON that is not an object. Each
    of those reaches the coordinator as a bare KeyError / ValueError /
    AttributeError and would otherwise fail the hook.
    """
    secret = ops.testing.Secret(content)
    relation = _provider_relation(PROVIDER_CONFIG_TEMPLATE, secret.id)

    state_in = dataclasses.replace(
        state,
        relations=[*state.relations, relation],
        secrets=[*state.secrets, secret],
    )

    state_out = context.run(context.on.start(), state_in)

    assert state_out.unit_status == ops.BlockedStatus(
        constants.INVALID_PROVIDER_SENSITIVE_DATA_MESSAGE
    )


def test_provider_config_recovers_when_malformed_secret_is_fixed(
    context, state, workload_container
):
    """A blocked unit self-heals once the provider repairs its secret.

    Blocking on a malformed secret is only safe if the block clears on its own
    when the provider is fixed -- otherwise an operator would have to intervene
    on every coordinator unit. The repaired secret arrives as a `secret-changed`
    event with no relation churn, so that event alone must drive the unit back to
    active *and* distribute the configuration that was withheld while blocked.
    """
    secret = _sensitive_secret()
    relation = _provider_relation(PROVIDER_CONFIG_TEMPLATE, secret.id)

    state_in = dataclasses.replace(
        state,
        relations=[*state.relations, relation],
        secrets=[*state.secrets, secret],
        unit_status=ops.BlockedStatus(constants.INVALID_PROVIDER_SENSITIVE_DATA_MESSAGE),
    )

    state_out = context.run(context.on.secret_changed(secret), state_in)

    assert state_out.unit_status == ops.ActiveStatus()
    assert _provider_section_present(state_out)
