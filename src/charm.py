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
        self._provider = AirflowCoordinatorProvides(self, AIRFLOW_COORDINATOR_RELATION_NAME)

        self.framework.observe(self.on.start, self.reconcile)
        self.framework.observe(self.on.update_status, self.reconcile)

    def reconcile(self, _) -> None:
        """Idempotent reconcile method to handle most relevant charm events."""
        # TODO: restrict the application to 1 unit?
        if not self.unit.is_leader():
            return

        coordinator_relations_valid, error_message = self._provider.all_required_components_valid
        if not coordinator_relations_valid:
            self.app.status = ops.BlockedStatus(error_message)
            return

        self._provider.set_config(
            self._config_generator.config_template, self._config_generator.sensitive_config_values
        )

        self.app.status = ops.ActiveStatus()


if __name__ == "__main__":  # pragma: nocover
    ops.main(AirflowCoordinatorK8SOperatorCharm)
