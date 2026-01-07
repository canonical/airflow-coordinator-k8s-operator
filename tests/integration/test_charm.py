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

logger = logging.getLogger(__name__)

CORE_CHARM_METADATA = yaml.safe_load(
    pathlib.Path("tests/integration/mock-core-charm/charmcraft.yaml").read_text()
)

AIRFLOW_VERSION = "3.1.0"
WORKLOAD_IMAGE_HASH = "somehash"
AIRFLOW_COMPONENTS = [
    "scheduler",
    "api-server",
    "triggerer",
    "dag-processor",
]


def test_deploy(juju: jubilant.Juju, charm: pathlib.Path, mock_core_charm: pathlib.Path):
    """Deploy the charm under test."""
    logger.info("Deploying coordinator + postgresql")

    juju.deploy(charm.resolve(), app="airflow-coordinator-k8s")

    # TODO: change postgres to 16/stable once released
    juju.deploy(
        "postgresql-k8s",
        channel="16/beta",
        trust=True,
    )

    juju.wait(
        lambda status: jubilant.all_blocked(status, "airflow-coordinator-k8s")
        and status.apps["airflow-coordinator-k8s"].app_status.message
        == "Missing integration with postgres"
    )

    logger.info("Integrating coordinator <-> postgres")

    juju.integrate("airflow-coordinator-k8s", "postgresql-k8s")

    juju.wait(
        lambda status: jubilant.all_blocked(status, "airflow-coordinator-k8s")
        and status.apps["airflow-coordinator-k8s"].app_status.message
        == "Missing integrations with: api-server, dag-processor, scheduler, triggerer"
    )

    logger.info("Integrating coordinator <-> mocked core charms")

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
        == "Missing integrations with: api-server, dag-processor, scheduler, triggerer"
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

    for component in ["api-server", "scheduler"]:
        juju.remove_relation("airflow-coordinator-k8s", f"airflow-{component}-mock")

    juju.wait(
        lambda status: jubilant.all_blocked(status, "airflow-coordinator-k8s")
        and status.apps["airflow-coordinator-k8s"].app_status.message
        == "Missing integrations with: api-server, scheduler"
    )

    for component in AIRFLOW_COMPONENTS:
        assert (
            juju.run(
                f"airflow-{component}-mock/0",
                "check-ready",
            ).results["ready"]
            == "False"
        )

    for component in ["api-server", "scheduler"]:
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
