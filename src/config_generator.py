# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Charm support for the Airflow config generation."""

import logging

import ops.charm

logger = logging.getLogger(__name__)


class AirflowConfigGenerator:
    """Encapsulate Airflow config generation logic."""

    def __init__(self, charm: ops.charm.CharmBase):
        self._charm = charm

    @property
    def config_template(self) -> str:
        """The Airflow config template to pass to all related components."""
        with open("templates/airflow_config.j2") as config_template_file:
            config_template = config_template_file.read()

        return config_template

    @property
    def sensitive_config_values(self) -> dict[str, str]:
        """All sensitive values that will be included in the Airflow config template."""
        return {
            "mock": "data",  # TODO: replace with actual content
        }
