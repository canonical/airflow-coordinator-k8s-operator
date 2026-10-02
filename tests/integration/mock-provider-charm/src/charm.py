#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""A mock Airflow Provider Configurator charm for testing the Airflow Coordinator charm.

This charm drives the provider side of the ``airflow_provider_configuration``
relation. It deliberately performs no validation: every action publishes exactly
what it is given. The coordinator is the component under test and it must not
depend on the provider having sanitised anything, so these tests need a provider
capable of publishing hostile input.

Configuration is published only when an action says so; there is no reconcile
loop and nothing is re-published on relation events. Tests therefore integrate
first and then call ``set-configuration``, which keeps the publish point
explicit instead of hiding it behind stored state.
"""

import json
import logging

import ops
from airflow_provider_configurator import AirflowProviderConfiguratorProvides

# Not re-exported from the package root, but needed to look the secret up by
# label for the revoke-secret action below.
from airflow_provider_configurator.interface import CHARM_PROVIDER_CONFIG_SECRET_LABEL

logger = logging.getLogger(__name__)

RELATION_NAME = "airflow-provider-configuration"


class MockProviderCharm(ops.CharmBase):
    """Publishes arbitrary provider configuration on demand."""

    def __init__(self, framework: ops.Framework):
        super().__init__(framework)

        self.provides = AirflowProviderConfiguratorProvides(self, RELATION_NAME)

        self.framework.observe(self.on.start, self._set_active)
        self.framework.observe(self.on[RELATION_NAME].relation_joined, self._set_active)

        self.framework.observe(self.on.set_configuration_action, self._set_configuration)
        self.framework.observe(self.on.clear_configuration_action, self._clear_configuration)
        self.framework.observe(self.on.revoke_secret_action, self._revoke_secret)

    def _set_active(self, _: ops.EventBase) -> None:
        """Report active; this charm has no workload to reconcile."""
        self.unit.status = ops.ActiveStatus()

    def _set_configuration(self, event: ops.ActionEvent) -> None:
        """Publish the given template and sensitive data verbatim."""
        try:
            sensitive_data = json.loads(event.params["sensitive-data"])
        except json.JSONDecodeError as exc:
            event.fail(f"sensitive-data is not valid JSON: {exc}")
            return

        self.provides.set_configuration(
            provider_configuration=event.params["configuration"],
            provider_configuration_sensitive_data=sensitive_data,
        )
        self.unit.status = ops.ActiveStatus()
        event.set_results({"published": "True"})

    def _clear_configuration(self, event: ops.ActionEvent) -> None:
        """Withdraw the published configuration and remove the secret."""
        self.provides.clear_configuration()
        self.unit.status = ops.ActiveStatus()
        event.set_results({"cleared": "True"})

    def _revoke_secret(self, event: ops.ActionEvent) -> None:
        """Revoke the sensitive data secret but leave the databag in place.

        Leaves the coordinator holding a secret URI it can no longer read, which
        is the state the real charm reaches only transiently and which the
        coordinator reports as a non-blocking status rather than an error.
        """
        try:
            secret = self.model.get_secret(label=CHARM_PROVIDER_CONFIG_SECRET_LABEL)
        except ops.SecretNotFoundError:
            event.fail("No provider configuration secret exists to revoke.")
            return

        for relation in self.model.relations[RELATION_NAME]:
            secret.revoke(relation)

        event.set_results({"revoked": "True"})


if __name__ == "__main__":  # pragma: nocover
    ops.main(MockProviderCharm)
