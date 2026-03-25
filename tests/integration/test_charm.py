# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.
#
# The integration tests use the Jubilant library. See https://documentation.ubuntu.com/jubilant/
# To learn more about testing, see https://documentation.ubuntu.com/ops/latest/explanation/testing/

import json
import logging
import pathlib

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

    juju.deploy(
        charm.resolve(),
        app="airflow-coordinator-k8s",
        resources={"airflow-coordinator-image": WORKLOAD_IMAGE},
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
