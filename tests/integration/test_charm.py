# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.
#
# The integration tests use the Jubilant library. See https://documentation.ubuntu.com/jubilant/
# To learn more about testing, see https://documentation.ubuntu.com/ops/latest/explanation/testing/

import collections.abc
import json
import logging
import pathlib
import time

import cryptography.fernet
import jubilant
import yaml

import constants

logger = logging.getLogger(__name__)

CORE_CHARM_METADATA = yaml.safe_load(
    pathlib.Path("tests/integration/mock-core-charm/charmcraft.yaml").read_text()
)
CHARMCRAFT_FILE = yaml.safe_load(pathlib.Path("./charmcraft.yaml").read_text())
WORKLOAD_IMAGE = image_path = CHARMCRAFT_FILE["resources"]["airflow-coordinator-image"][
    "upstream-source"
]
AIRFLOW_VERSION = "3.1.0"
WORKLOAD_IMAGE_HASH = "somehash"
AIRFLOW_COMPONENTS = sorted(
    [
        "scheduler",
        "api-server",
        "triggerer",
        "dag-processor",
    ]
)

# Populated during test_relate_and_config_validation so later tests can
# verify the airflow keys remain identical across relation break/recreate cycles.
_initial_airflow_keys: dict[str, str] = {}


def test_deploy(juju: jubilant.Juju, charm: pathlib.Path, mock_core_charm: pathlib.Path):
    """Deploy the charm under test."""
    logger.info("Deploying coordinator + postgresql")

    fernet_key = cryptography.fernet.Fernet.generate_key().decode()

    fernet_key_secret_uri = juju.add_secret(
        name="fernet-key-secret",
        content={
            constants.FERNET_KEY: fernet_key,
        },
    )

    juju.deploy(
        charm.resolve(),
        app="airflow-coordinator-k8s",
        resources={"airflow-coordinator-image": WORKLOAD_IMAGE},
    )

    juju.grant_secret(fernet_key_secret_uri, "airflow-coordinator-k8s")

    juju.config(
        "airflow-coordinator-k8s",
        {
            constants.FERNET_KEY_SECRET_CONFIG: fernet_key_secret_uri,
        },
    )

    # TODO: change postgres to 16/stable once released
    juju.deploy(
        "postgresql-k8s",
        channel="14/stable",
        trust=True,
    )

    juju.wait(
        lambda status: (
            jubilant.all_blocked(status, "airflow-coordinator-k8s")
            and status.apps["airflow-coordinator-k8s"].app_status.message
            == constants.MISSING_POSTGRES_INTEGRATION_MESSAGE
        )
    )

    logger.info("Integrating coordinator <-> postgres")

    juju.integrate("airflow-coordinator-k8s", "postgresql-k8s")

    juju.wait(lambda status: jubilant.all_blocked(status, "airflow-coordinator-k8s"))

    logger.info("Deploying mocked core charms")

    for component in AIRFLOW_COMPONENTS:
        juju.deploy(
            mock_core_charm.resolve(),
            app=f"airflow-{component}-mock",
            config={
                "component": component,
                "airflow_version": AIRFLOW_VERSION,
                "workload_image_hash": WORKLOAD_IMAGE_HASH,
            },
            resources={
                "workload-container": CORE_CHARM_METADATA["resources"]["workload-container"][
                    "upstream-source"
                ],
            },
        )

    for component in AIRFLOW_COMPONENTS:
        assert (
            juju.run(
                f"airflow-{component}-mock/0",
                "check-ready",
            ).results["ready"]
            == "False"
        )


def test_relate_and_config_validation(juju: jubilant.Juju):
    """Relate all the components and confirm proper transfer of config and sensitive data."""
    logger.info(
        "Integrating coordinator:airflow-api-server <-> mocked api-server:airflow-api-server"
    )

    juju.integrate(
        "airflow-coordinator-k8s:airflow-api-server", "airflow-api-server-mock:airflow-api-server"
    )

    logger.info("Integrating coordinator <-> mocked core charms")

    for component in AIRFLOW_COMPONENTS:
        juju.integrate(
            "airflow-coordinator-k8s:airflow-coordinator",
            f"airflow-{component}-mock:airflow-coordinator",
        )

    juju.wait(jubilant.all_active)

    airflow_configs, all_sensitive_data = set(), []

    for component in AIRFLOW_COMPONENTS:
        assert (
            juju.run(
                f"airflow-{component}-mock/0",
                "check-ready",
            ).results["ready"]
            == "True"
        )

        config = juju.run(f"airflow-{component}-mock/0", "get-airflow-config").results[
            "airflow-config"
        ]
        airflow_configs.add(config)

        sensitive_data = juju.run(
            f"airflow-{component}-mock/0",
            "get-relation-sensitive-data",
        ).results["sensitive-data"]

        if sensitive_data not in all_sensitive_data:
            all_sensitive_data.append(sensitive_data)

    assert len(airflow_configs) == 1
    assert len(all_sensitive_data) == 1

    assert (
        f"base_url = http://airflow-api-server-mock-endpoints.{juju.model}.svc.cluster.local:8080"
        in next(iter(airflow_configs))
    )

    sensitive = json.loads(all_sensitive_data[0])
    assert "postgresql+psycopg2://" in sensitive["database__sql_alchemy_conn"]
    assert "api__secret_key" in sensitive
    assert "api_auth__jwt_secret" in sensitive
    assert "core__fernet_key" in sensitive
    assert len(sensitive["api__secret_key"]) == 64
    assert len(sensitive["api_auth__jwt_secret"]) == 64
    # Fernet key is base64-encoded 32 bytes = 44 chars
    assert len(sensitive["core__fernet_key"]) == 44

    # Verify secret_key and jwt_secret are rendered in the config file
    config = next(iter(airflow_configs))
    assert f"secret_key = {sensitive['api__secret_key']}" in config
    assert f"jwt_secret = {sensitive['api_auth__jwt_secret']}" in config
    assert f"fernet_key = {sensitive['core__fernet_key']}" in config

    # Store initial key values for persistence checks in later tests
    _initial_airflow_keys["api__secret_key"] = sensitive["api__secret_key"]
    _initial_airflow_keys["api_auth__jwt_secret"] = sensitive["api_auth__jwt_secret"]
    _initial_airflow_keys["core__fernet_key"] = sensitive["core__fernet_key"]


def test_remove_and_recreate_integrations(juju: jubilant.Juju):
    """Remove and recreate integrations to ensure appropriate behavior."""
    logger.info("Cleaning files in mock core charms")
    for component in AIRFLOW_COMPONENTS:
        juju.run(
            f"airflow-{component}-mock/0",
            "clean-files",
        )

    logger.info("Breaking integrations between coordinator <-> mocked core charms")

    for component in AIRFLOW_COMPONENTS:
        juju.remove_relation(
            "airflow-coordinator-k8s:airflow-coordinator",
            f"airflow-{component}-mock:airflow-coordinator",
        )

    juju.wait(
        lambda status: (
            jubilant.all_blocked(status, "airflow-coordinator-k8s")
            and status.apps["airflow-coordinator-k8s"].app_status.message
            == constants.MISSING_INTEGRATIONS_MESSAGE_TEMPLATE.format(
                missing_core_components=", ".join(AIRFLOW_COMPONENTS)
            )
        )
    )

    for component in AIRFLOW_COMPONENTS:
        assert (
            juju.run(
                f"airflow-{component}-mock/0",
                "check-ready",
            ).results["ready"]
            == "False"
        )

    for component in AIRFLOW_COMPONENTS:
        juju.integrate(
            "airflow-coordinator-k8s:airflow-coordinator",
            f"airflow-{component}-mock:airflow-coordinator",
        )

    juju.wait(jubilant.all_active)

    airflow_configs, all_sensitive_data = set(), []

    for component in AIRFLOW_COMPONENTS:
        assert (
            juju.run(
                f"airflow-{component}-mock/0",
                "check-ready",
            ).results["ready"]
            == "True"
        )

        config = juju.run(f"airflow-{component}-mock/0", "get-airflow-config").results[
            "airflow-config"
        ]
        airflow_configs.add(config)

        sensitive_data = juju.run(
            f"airflow-{component}-mock/0",
            "get-relation-sensitive-data",
        ).results["sensitive-data"]

        if sensitive_data not in all_sensitive_data:
            all_sensitive_data.append(sensitive_data)

    assert len(airflow_configs) == 1
    assert len(all_sensitive_data) == 1

    sensitive = json.loads(all_sensitive_data[0])
    assert "postgresql+psycopg2://" in sensitive["database__sql_alchemy_conn"]
    assert "api__secret_key" in sensitive
    assert "api_auth__jwt_secret" in sensitive
    assert "core__fernet_key" in sensitive
    assert len(sensitive["api__secret_key"]) == 64
    assert len(sensitive["api_auth__jwt_secret"]) == 64
    assert len(sensitive["core__fernet_key"]) == 44


def test_remove_and_recreate_limited_integrations(juju: jubilant.Juju):
    """Remove and recreate limited integrations to ensure appropriate behavior."""
    logger.info("Cleaning files in mock core charms")
    for component in AIRFLOW_COMPONENTS:
        juju.run(
            f"airflow-{component}-mock/0",
            "clean-files",
        )

    logger.info("Breaking integrations between coordinator <-> some mocked core charms")

    unrelated_components = ["api-server", "scheduler"]

    for component in unrelated_components:
        juju.remove_relation(
            "airflow-coordinator-k8s:airflow-coordinator",
            f"airflow-{component}-mock:airflow-coordinator",
        )

    juju.wait(
        lambda status: (
            jubilant.all_blocked(status, "airflow-coordinator-k8s")
            and status.apps["airflow-coordinator-k8s"].app_status.message
            == constants.MISSING_INTEGRATIONS_MESSAGE_TEMPLATE.format(
                missing_core_components=", ".join(unrelated_components)
            )
        )
    )

    for component in AIRFLOW_COMPONENTS:
        assert (
            juju.run(
                f"airflow-{component}-mock/0",
                "check-ready",
            ).results["ready"]
            == "False"
        )

    for component in unrelated_components:
        juju.integrate(
            "airflow-coordinator-k8s:airflow-coordinator",
            f"airflow-{component}-mock:airflow-coordinator",
        )

    juju.wait(jubilant.all_active)

    airflow_configs, all_sensitive_data = set(), []

    for component in AIRFLOW_COMPONENTS:
        assert (
            juju.run(
                f"airflow-{component}-mock/0",
                "check-ready",
            ).results["ready"]
            == "True"
        )

        config = juju.run(f"airflow-{component}-mock/0", "get-airflow-config").results[
            "airflow-config"
        ]
        airflow_configs.add(config)

        sensitive_data = juju.run(
            f"airflow-{component}-mock/0",
            "get-relation-sensitive-data",
        ).results["sensitive-data"]

        if sensitive_data not in all_sensitive_data:
            all_sensitive_data.append(sensitive_data)

    assert len(airflow_configs) == 1
    assert len(all_sensitive_data) == 1

    sensitive = json.loads(all_sensitive_data[0])
    assert "postgresql+psycopg2://" in sensitive["database__sql_alchemy_conn"]
    assert "api__secret_key" in sensitive
    assert "api_auth__jwt_secret" in sensitive
    assert "core__fernet_key" in sensitive
    assert len(sensitive["api__secret_key"]) == 64
    assert len(sensitive["api_auth__jwt_secret"]) == 64
    assert len(sensitive["core__fernet_key"]) == 44


def test_break_and_recreate_postgres_relation(juju: jubilant.Juju):
    """Ensure breaking postgres relation halts cluster + recreating relation resumes cluster."""
    logger.info("Breaking integration between coordinator <-> postgres")

    juju.remove_relation("airflow-coordinator-k8s", "postgresql-k8s")

    juju.wait(
        lambda status: (
            jubilant.all_blocked(status, "airflow-coordinator-k8s")
            and status.apps["airflow-coordinator-k8s"].app_status.message
            == constants.MISSING_POSTGRES_INTEGRATION_MESSAGE
        )
    )

    for component in AIRFLOW_COMPONENTS:
        assert juju.run(f"airflow-{component}-mock/0", "check-ready").results["ready"] == "False"

    logger.info("Recreate integration between coordinator <-> postgres")

    juju.integrate("airflow-coordinator-k8s", "postgresql-k8s")

    juju.wait(jubilant.all_active)

    airflow_configs, all_sensitive_data = set(), []

    for component in AIRFLOW_COMPONENTS:
        assert (
            juju.run(
                f"airflow-{component}-mock/0",
                "check-ready",
            ).results["ready"]
            == "True"
        )

        config = juju.run(f"airflow-{component}-mock/0", "get-airflow-config").results[
            "airflow-config"
        ]
        airflow_configs.add(config)

        sensitive_data = juju.run(
            f"airflow-{component}-mock/0",
            "get-relation-sensitive-data",
        ).results["sensitive-data"]

        if sensitive_data not in all_sensitive_data:
            all_sensitive_data.append(sensitive_data)

    assert len(airflow_configs) == 1
    assert len(all_sensitive_data) == 1

    sensitive = json.loads(all_sensitive_data[0])
    assert "postgresql+psycopg2://" in sensitive["database__sql_alchemy_conn"]
    assert "api__secret_key" in sensitive
    assert "api_auth__jwt_secret" in sensitive
    assert "core__fernet_key" in sensitive
    assert len(sensitive["api__secret_key"]) == 64
    assert len(sensitive["api_auth__jwt_secret"]) == 64
    assert len(sensitive["core__fernet_key"]) == 44


def test_airflow_keys_persist_across_relation_cycles(juju: jubilant.Juju):
    """Verify airflow keys remain identical after all relation break/recreate cycles."""
    assert _initial_airflow_keys, (
        "Initial keys not captured from test_relate_and_config_validation"
    )

    for component in AIRFLOW_COMPONENTS:
        sensitive_data = juju.run(
            f"airflow-{component}-mock/0",
            "get-relation-sensitive-data",
        ).results["sensitive-data"]

        sensitive = json.loads(sensitive_data)
        assert sensitive["api__secret_key"] == _initial_airflow_keys["api__secret_key"], (
            f"{component}: api__secret_key changed after relation cycles"
        )
        assert (
            sensitive["api_auth__jwt_secret"] == _initial_airflow_keys["api_auth__jwt_secret"]
        ), f"{component}: api_auth__jwt_secret changed after relation cycles"
        assert sensitive["core__fernet_key"] == _initial_airflow_keys["core__fernet_key"], (
            f"{component}: core__fernet_key changed after relation cycles"
        )


# --------------------------------------------------------------------------- #
# Provider configuration (airflow_provider_configuration relation)
#
# These run against the deployment built up by the tests above, so the
# assertions can check that provider configuration reaches every core charm,
# not just the coordinator. The mock provider publishes without validating, which
# is what makes it useful: the coordinator's sanitiser is the component under
# test and it must hold even when the provider is hostile or broken.
# --------------------------------------------------------------------------- #

PROVIDER_APP = "mock-provider"

# A template exercising every Jinja2 construct the sanitiser neutralises. The
# `leak` option is the important one: `core__fernet_key` is a real key in the
# coordinator's own render context, so if escaping regressed this renders the
# live Fernet key into a provider-controlled option.
HOSTILE_TEMPLATE = """\
[provider_escaping]
ssti = {{ ''.__class__.__mro__[1].__subclasses__() }}
leak = {{ core__fernet_key }}
loop = {% for x in range(3) %}x{% endfor %}
comment = {# hidden #}
fused = {{{ provider__demo__token }}
resolved = {{ provider__demo__token }}
"""

PROVIDER_TOKEN = "s3cr3t-provider-token"


def _core_charm_config(juju: jubilant.Juju, component: str) -> str:
    """Return the airflow.cfg one mocked core charm currently holds."""
    return juju.run(f"airflow-{component}-mock/0", "get-airflow-config").results["airflow-config"]


def _core_charm_configs(juju: jubilant.Juju) -> set[str]:
    """Return the distinct airflow.cfg contents across all mocked core charms."""
    return {_core_charm_config(juju, component) for component in AIRFLOW_COMPONENTS}


def _wait_for_core_configs(
    juju: jubilant.Juju,
    predicate: collections.abc.Callable[[str], bool],
    timeout: int = 300,
) -> str:
    """Return the config once every core charm agrees on one satisfying ``predicate``.

    Publishing provider configuration never takes any unit out of ``active``, so
    waiting on ``all_active`` would return before the relation-changed hooks have
    even fired and assert against the previous config. The settled state has to be
    identified by its content instead.

    Requiring a single distinct config across all four core charms also makes this
    assert propagation rather than just the coordinator's local view. Polling all
    four every pass is wasteful though -- an action costs several times what a
    status call does -- so each pass probes a single charm and the agreement check
    only runs once that probe looks settled.

    A charm that chokes on the published configuration lands in ``error`` and stays
    there, so ``all_active`` can never come true and the wait would otherwise burn
    its full timeout and report nothing but "timed out". Tripping on ``any_error``
    instead fails in seconds and puts the offending unit's status in the message.
    """
    deadline = time.monotonic() + timeout
    configs: set[str] = set()

    while True:
        # Without an explicit timeout `juju.wait` uses its own (far longer)
        # default, so a model that never reaches `all_active` would overrun this
        # helper's advertised deadline by minutes.
        remaining = deadline - time.monotonic()
        if remaining > 0:
            juju.wait(jubilant.all_active, error=jubilant.any_error, timeout=remaining)

            # Cheap single-charm probe first: if the coordinator has not even
            # rendered the expected config yet, there is nothing for the other
            # three to have converged on.
            if predicate(_core_charm_config(juju, AIRFLOW_COMPONENTS[0])):
                configs = _core_charm_configs(juju)
                if len(configs) == 1 and predicate(next(iter(configs))):
                    return next(iter(configs))

        if time.monotonic() > deadline:
            raise AssertionError(
                f"Core charms did not converge on the expected config within {timeout}s "
                f"({len(configs)} distinct config(s) observed)"
            )
        time.sleep(5)


def test_provider_configuration_is_merged_and_distributed(
    juju: jubilant.Juju, mock_provider_charm: pathlib.Path
):
    """Provider config reaches every core charm with its sensitive values resolved."""
    logger.info("Deploying mock provider configurator")

    juju.deploy(mock_provider_charm.resolve(), app=PROVIDER_APP)
    juju.wait(lambda status: jubilant.all_active(status, PROVIDER_APP))

    juju.integrate(
        "airflow-coordinator-k8s:airflow-provider-configuration",
        f"{PROVIDER_APP}:airflow-provider-configuration",
    )

    juju.run(
        f"{PROVIDER_APP}/0",
        "set-configuration",
        {
            "configuration": "[provider_demo]\nconn_id = {{ provider__demo__token }}\n",
            "sensitive-data": json.dumps({"provider__demo__token": PROVIDER_TOKEN}),
        },
    )

    config = _wait_for_core_configs(juju, lambda c: "[provider_demo]" in c)

    assert f"conn_id = {PROVIDER_TOKEN}" in config

    # Coordinator-owned configuration must survive the merge untouched.
    assert f"fernet_key = {_initial_airflow_keys['core__fernet_key']}" in config


def test_provider_configuration_jinja_is_escaped(juju: jubilant.Juju):
    """A hostile template renders as literal text instead of being evaluated."""
    juju.run(
        f"{PROVIDER_APP}/0",
        "set-configuration",
        {
            "configuration": HOSTILE_TEMPLATE,
            "sensitive-data": json.dumps({"provider__demo__token": PROVIDER_TOKEN}),
        },
    )

    config = _wait_for_core_configs(juju, lambda c: "[provider_escaping]" in c)

    # Every construct survives as the literal characters the provider sent.
    assert "ssti = {{ ''.__class__.__mro__[1].__subclasses__() }}" in config
    assert "loop = {% for x in range(3) %}x{% endfor %}" in config
    assert "comment = {# hidden #}" in config

    # The coordinator's Fernet key is in scope while this template renders, so
    # this asserts the escaping actually prevents exfiltration rather than just
    # producing syntactically inert output.
    assert "leak = {{ core__fernet_key }}" in config
    fernet_key = _initial_airflow_keys["core__fernet_key"]
    assert f"leak = {fernet_key}" not in config

    # Namespaced placeholders are still substituted, including one deliberately
    # fused against a stray brace, which previously produced `{{{ ... }}` and
    # crashed every charm rendering the template.
    assert f"fused = {{{PROVIDER_TOKEN}" in config
    assert f"resolved = {PROVIDER_TOKEN}" in config


def test_provider_sensitive_keys_outside_namespace_are_dropped(juju: jubilant.Juju):
    """A provider cannot override coordinator-owned sensitive values."""
    juju.run(
        f"{PROVIDER_APP}/0",
        "set-configuration",
        {
            "configuration": "[provider_demo]\nconn_id = {{ provider__demo__token }}\n",
            "sensitive-data": json.dumps(
                {
                    "provider__demo__token": PROVIDER_TOKEN,
                    # Not in the `provider__` namespace: must never reach the
                    # render context, or a provider could rewrite the key that
                    # protects every stored Airflow connection.
                    "core__fernet_key": "attacker-controlled-fernet-key",
                }
            ),
        },
    )

    juju.wait(
        lambda status: (
            jubilant.all_active(status)
            and constants.DROPPED_PROVIDER_SENSITIVE_KEYS_MESSAGE
            in status.apps["airflow-coordinator-k8s"].app_status.message
        ),
        error=jubilant.any_error,
    )

    config = _wait_for_core_configs(juju, lambda c: "[provider_demo]" in c)

    assert f"fernet_key = {_initial_airflow_keys['core__fernet_key']}" in config
    assert "attacker-controlled-fernet-key" not in config


def test_provider_malformed_config_does_not_stop_distribution(juju: jubilant.Juju):
    """Malformed provider INI is dropped without wedging the coordinator.

    Blocking here would stop `set_airflow_config` running at all, so a single
    malformed character in another application's file would withhold every
    configuration update from every core charm -- a cheaper denial of service
    than any of the injection paths this suite covers. Only an integration test
    can show distribution genuinely continues, since the failure is in what the
    coordinator does *after* rendering.
    """
    juju.run(
        f"{PROVIDER_APP}/0",
        "set-configuration",
        {
            # No section header: configparser.MissingSectionHeaderError.
            "configuration": "key_without_section = value\n",
            "sensitive-data": json.dumps({"provider__demo__token": PROVIDER_TOKEN}),
        },
    )

    juju.wait(
        lambda status: (
            jubilant.all_active(status)
            and constants.INVALID_PROVIDER_CONFIG_MESSAGE
            in status.apps["airflow-coordinator-k8s"].app_status.message
        ),
        error=jubilant.any_error,
    )

    config = _wait_for_core_configs(juju, lambda c: "[provider_demo]" not in c)

    # The provider contribution is gone, but the coordinator's own configuration
    # still reached every core charm.
    assert "key_without_section" not in config
    assert f"fernet_key = {_initial_airflow_keys['core__fernet_key']}" in config

    # Restore a usable configuration so the following tests start from a state
    # where the provider contribution is actually present, and to show the
    # notice clears without operator intervention.
    juju.run(
        f"{PROVIDER_APP}/0",
        "set-configuration",
        {
            "configuration": "[provider_demo]\nconn_id = {{ provider__demo__token }}\n",
            "sensitive-data": json.dumps({"provider__demo__token": PROVIDER_TOKEN}),
        },
    )

    _wait_for_core_configs(juju, lambda c: "[provider_demo]" in c)


def test_provider_configuration_dropped_when_secret_unreadable(juju: jubilant.Juju):
    """Revoking the secret drops provider config without blocking the coordinator.

    This is the branch that only exists because a revoked secret surfaces as a
    bare ModelError. No Scenario test can reach it, since the behaviour comes
    from Juju rather than from ops.
    """
    juju.run(f"{PROVIDER_APP}/0", "revoke-secret")

    juju.wait(
        lambda status: (
            jubilant.all_active(status)
            and constants.WAITING_FOR_PROVIDER_CONFIG_SECRET_MESSAGE
            in status.apps["airflow-coordinator-k8s"].app_status.message
        ),
        error=jubilant.any_error,
    )

    config = _wait_for_core_configs(juju, lambda c: "[provider_demo]" not in c)

    # The whole provider contribution is withdrawn: partially rendered config
    # with empty placeholders would be worse than none at all.
    assert PROVIDER_TOKEN not in config
    assert f"fernet_key = {_initial_airflow_keys['core__fernet_key']}" in config


def test_provider_relation_removed_restores_baseline(juju: jubilant.Juju):
    """Removing the provider relation returns the config to its pre-provider state."""
    juju.remove_relation(
        "airflow-coordinator-k8s:airflow-provider-configuration",
        f"{PROVIDER_APP}:airflow-provider-configuration",
    )

    juju.wait(
        lambda status: (
            jubilant.all_active(status)
            and status.apps["airflow-coordinator-k8s"].app_status.message == ""
        ),
        error=jubilant.any_error,
    )

    config = _wait_for_core_configs(juju, lambda c: "provider_demo" not in c)

    assert "provider_escaping" not in config
    assert f"fernet_key = {_initial_airflow_keys['core__fernet_key']}" in config
