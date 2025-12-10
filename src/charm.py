#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""The Airflow Coordinator charm application."""

import logging

import ops

# TODO: change to official charm lib name after charmhub registration + lib is published
from charms.airflow_coordinator_k8s.v0.airflow_coordinator_temp import AirflowCoordinatorProvides

import config_generator
from constants import AIRFLOW_COORDINATOR_RELATION_NAME

logger = logging.getLogger(__name__)


class AirflowCoordinatorK8SOperatorCharm(ops.CharmBase):
    """Charm the application."""

    def __init__(self, framework: ops.Framework):
        super().__init__(framework)

        self._config_generator = config_generator.AirflowConfigGenerator(self)
        self._provider = AirflowCoordinatorProvides(
            self, AIRFLOW_COORDINATOR_RELATION_NAME, callback=self.reconcile
        )

        for event in [
            self.on.start,
            self.on.update_status,
        ]:
            self.framework.observe(event, self.reconcile)

    def reconcile(self, _) -> None:
        """Idempotent reconcile method to handle most relevant charm events."""
        # TODO: restrict the application to 1 unit?
        if not self.unit.is_leader():
            return

        if self._provider.missing_core_components:
            self._provider.set_validation_errors()

            self.unit.status = ops.BlockedStatus(
                f"Missing integrations with: {', '.join(self._provider.missing_core_components)}"
            )
            return

        if (
            not self._provider.are_airflow_versions_consistent
            or not self._provider.are_workload_image_hashes_consistent
        ):
            self._provider.set_validation_errors()

            self.unit.status = ops.BlockedStatus(
                "Integrated apps with mismatched airflow versions"
                if not self._provider.are_airflow_versions_consistent
                else "Integrated apps with mismatched workload image hashes"
            )
            return

        self._provider.set_airflow_config(
            self._config_generator.config_template,
            sensitive_data=self._config_generator.sensitive_config_values,
        )

        self.unit.status = ops.ActiveStatus()


if __name__ == "__main__":  # pragma: nocover
    ops.main(AirflowCoordinatorK8SOperatorCharm)
