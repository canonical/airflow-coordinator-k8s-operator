# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.
#
# The integration tests use the Jubilant library. See https://documentation.ubuntu.com/jubilant/
# To learn more about testing, see https://documentation.ubuntu.com/ops/latest/explanation/testing/

import configparser
import json
import logging
import pathlib
import time

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
        lambda status: jubilant.all_blocked(status, "airflow-coordinator-k8s")
        and status.apps["airflow-coordinator-k8s"].app_status.message
        == constants.MISSING_POSTGRES_INTEGRATION_MESSAGE
    )

    logger.info("Integrating coordinator <-> postgres")

    juju.integrate("airflow-coordinator-k8s", "postgresql-k8s")

    juju.wait(
        lambda status: jubilant.all_blocked(status, "airflow-coordinator-k8s")
        and status.apps["airflow-coordinator-k8s"].app_status.message
        == constants.MISSING_INTEGRATIONS_MESSAGE_TEMPLATE.format(
            missing_core_components=", ".join(AIRFLOW_COMPONENTS)
        )
    )

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
    logger.info("Integrating coordinator <-> mocked core charms")

    for component in AIRFLOW_COMPONENTS:
        juju.integrate("airflow-coordinator-k8s", f"airflow-{component}-mock")

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
        "postgresql+psycopg2://"
        in json.loads(all_sensitive_data[0])["sql_alchemy_connection_string"]
    )


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
        juju.remove_relation("airflow-coordinator-k8s", f"airflow-{component}-mock")

    juju.wait(
        lambda status: jubilant.all_blocked(status, "airflow-coordinator-k8s")
        and status.apps["airflow-coordinator-k8s"].app_status.message
        == constants.MISSING_INTEGRATIONS_MESSAGE_TEMPLATE.format(
            missing_core_components=", ".join(AIRFLOW_COMPONENTS)
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
        juju.integrate("airflow-coordinator-k8s", f"airflow-{component}-mock")

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
        "postgresql+psycopg2://"
        in json.loads(all_sensitive_data[0])["sql_alchemy_connection_string"]
    )


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
        juju.remove_relation("airflow-coordinator-k8s", f"airflow-{component}-mock")

    juju.wait(
        lambda status: jubilant.all_blocked(status, "airflow-coordinator-k8s")
        and status.apps["airflow-coordinator-k8s"].app_status.message
        == constants.MISSING_INTEGRATIONS_MESSAGE_TEMPLATE.format(
            missing_core_components=", ".join(unrelated_components)
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
        juju.integrate("airflow-coordinator-k8s", f"airflow-{component}-mock")

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
        "postgresql+psycopg2://"
        in json.loads(all_sensitive_data[0])["sql_alchemy_connection_string"]
    )


def test_break_and_recreate_postgres_relation(juju: jubilant.Juju):
    """Ensure breaking postgres relation halts cluster + recreating relation resumes cluster."""
    logger.info("Breaking integration between coordinator <-> postgres")

    juju.remove_relation("airflow-coordinator-k8s", "postgresql-k8s")

    juju.wait(
        lambda status: jubilant.all_blocked(status, "airflow-coordinator-k8s")
        and status.apps["airflow-coordinator-k8s"].app_status.message
        == constants.MISSING_POSTGRES_INTEGRATION_MESSAGE
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

    assert (
        "postgresql+psycopg2://"
        in json.loads(all_sensitive_data[0])["sql_alchemy_connection_string"]
    )


def test_custom_nonsensitive_airflow_config(juju: jubilant.Juju):
    """Test customizing Airflow configuration with non-sensitive config file."""
    logger.info("Providing non-sensitive airflow config to coordinator")

    juju.config(
        "airflow-coordinator-k8s",
        {
            constants.CUSTOM_CONFIG: """[core]
dags_folder = custom_dags_path
non_sensitive_key = non_sensitive_value
"""
        },
    )

    logger.info("Waiting for 30s for custom config to propagate the cluster")

    time.sleep(30)

    juju.wait(jubilant.all_active)

    logger.info("Ensuring healthy state of the cluster")

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

    config_parser = configparser.ConfigParser()
    config_parser.read_string(next(iter(airflow_configs)))

    assert config_parser.get("core", "dags_folder") == "custom_dags_path"
    assert config_parser.get("core", "non_sensitive_key") == "non_sensitive_value"

    assert (
        "postgresql+psycopg2://"
        in json.loads(all_sensitive_data[0])["sql_alchemy_connection_string"]
    )


def test_custom_sensitive_airflow_config(juju: jubilant.Juju):
    """Test customizing Airflow configuration with sensitive config file."""
    logger.info("Providing sensitive Airflow config to the coordinator")

    sensitive_config_secret_uri = juju.add_secret(
        name="custom",
        content={
            constants.SENSITIVE_CUSTOM_CONFIG_SECRET_KEY: """[core]
dags_folder2 = secret_dags_folder2

[database]
secret_key2 = super_secret_value2
""",
        },
    )
    juju.grant_secret(
        sensitive_config_secret_uri,
        "airflow-coordinator-k8s",
    )

    juju.config(
        "airflow-coordinator-k8s",
        {
            constants.SENSITIVE_CUSTOM_CONFIG: sensitive_config_secret_uri,
        },
    )

    logger.info("Waiting for 30s for the sensitive custom config to propagate the cluster")

    time.sleep(30)

    juju.wait(jubilant.all_active)

    logger.info("Ensuring cluster in a healthy state")

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

    all_sensitive_data = json.loads(all_sensitive_data[0])

    config_parser = configparser.ConfigParser()
    config_parser.read_string(next(iter(airflow_configs)))

    assert config_parser.get("core", "dags_folder2") == "secret_dags_folder2"
    assert (
        config_parser.get("database", "secret_key2") == "super_secret_value2"
    )

    assert (
        "postgresql+psycopg2://"
        in all_sensitive_data["sql_alchemy_connection_string"]
    )
    assert all_sensitive_data["core_dags_folder2_secret_value"] == "secret_dags_folder2"
    assert all_sensitive_data["database_secret_key2_secret_value"] == "super_secret_value2"

    logger.info("Updating secret with sensitive custom config")

    juju.update_secret(
        identifier=sensitive_config_secret_uri,
        content={
            constants.SENSITIVE_CUSTOM_CONFIG_SECRET_KEY: """[core]
dags_folder3 = secret_dags_folder3

[database]
secret_key3 = super_secret_value3
""",
        }
    )

    logger.info("Waiting 30s for sensitive custom config change to propagate the cluster")

    time.sleep(30)

    juju.wait(jubilant.all_active)

    logger.info("Ensuring cluster healthy with new sensitive custom config in effect")

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

    all_sensitive_data = json.loads(all_sensitive_data[0])

    config_parser = configparser.ConfigParser()
    config_parser.read_string(next(iter(airflow_configs)))

    assert not config_parser.get("core", "dags_folder2", fallback=None)
    assert not config_parser.get("database", "secret_key2", fallback=None)

    assert config_parser.get("core", "dags_folder3") == "secret_dags_folder3"
    assert (
        config_parser.get("database", "secret_key3") == "super_secret_value3"
    )

    assert (
        "postgresql+psycopg2://"
        in all_sensitive_data["sql_alchemy_connection_string"]
    )
    assert all_sensitive_data["core_dags_folder3_secret_value"] == "secret_dags_folder3"
    assert all_sensitive_data["database_secret_key3_secret_value"] == "super_secret_value3"
