#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""The Airflow Coordinator charm application."""

import logging

import ops

import airflow_coordinator
import config_generator
from constants import AIRFLOW_COORDINATOR_RELATION_NAME

logger = logging.getLogger(__name__)


class AirflowCoordinatorK8SOperatorCharm(ops.CharmBase):
    """Charm the application."""

    def __init__(self, framework: ops.Framework):
        super().__init__(framework)

        self._config_generator = config_generator.AirflowConfigGenerator(self)
        self._provider = airflow_coordinator.AirflowCoordinatorProvides(
            self, AIRFLOW_COORDINATOR_RELATION_NAME
        )

        self.framework.observe(self.on.start, self.reconcile)
        self.framework.observe(self.on.update_status, self.reconcile)

    def create_or_update_secret(
        self, scope: str, secret_label: str, contents: dict[str, str]
    ) -> ops.Secret:
        """Create or update a (app or unit) secret with the provided contents."""
        if scope not in ["unit", "app"]:
            raise ValueError(f"Unknown secret scope: {scope}")

        if scope == "app" and not self.unit.is_leader():
            raise RuntimeError("Attempting to set app secret in non leader unit")

        try:
            secret = self.model.get_secret(label=secret_label)

            new_content = secret.get_content()
            new_content.update(contents)

            secret.set_content(new_content)
        except ops.SecretNotFoundError:
            if scope == "app":
                secret = self.app.add_secret(contents, label=secret_label)
            else:
                secret = self.unit.add_secret(contents, label=secret_label)

        return secret

    def reconcile(self, event) -> None:
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
