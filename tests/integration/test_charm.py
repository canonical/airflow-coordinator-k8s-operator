# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.
#
# The integration tests use the Jubilant library. See https://documentation.ubuntu.com/jubilant/
# To learn more about testing, see https://documentation.ubuntu.com/ops/latest/explanation/testing/

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
    juju.deploy(charm.resolve(), app="airflow-coordinator-k8s")

    # TODO: change postgres to 16/stable once released
    juju.deploy(
        "postgresql-k8s",
        channel="16/beta",
        trust=True,
    )

    juju.integrate("airflow-coordinator-k8s", "postgresql-k8s")

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

        juju.integrate("airflow-coordinator-k8s", f"airflow-{component}-mock")

    juju.wait(jubilant.all_active)

    airflow_configs = set()

    for component in AIRFLOW_COMPONENTS:
        pass
