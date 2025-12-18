# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Charm support for the Airflow config generation."""

import logging

import ops

logger = logging.getLogger(__name__)


class AirflowConfigGenerator:
    """Encapsulate Airflow config generation logic."""

    def __init__(self, charm: ops.CharmBase):
        self._charm = charm

    @property
    def config_template(self) -> str:
        """The Airflow config template to pass to all related components."""
        with open("src/templates/airflow_config.j2") as config_template_file:
            config_template = config_template_file.read()

        return config_template

    @property
    def _sql_alchemy_connection_string(self) -> str:
        """Create the sql alchemy connection string to the postgres database."""
        postgres_relation_id = self._charm._database_requires.relations[0].id

        username = self._charm._database_requires.fetch_relation_field(
            postgres_relation_id, "username"
        )
        password = self._charm._database_requires.fetch_relation_field(
            postgres_relation_id, "password"
        )
        database = self._charm._database_requires.fetch_relation_field(
            postgres_relation_id, "database"
        )

        endpoints = [
            endpoint
            for endpoint in self._charm._database_requires.fetch_relation_field(
                postgres_relation_id, "endpoints"
            ).split(",")
            if endpoint
        ]

        return f"postgresql+psycopg2://{username}:{password}@{endpoints[0]}/{database}"

    @property
    def sensitive_config_values(self) -> dict[str, str]:
        """All sensitive values that will be included in the Airflow config template."""
        return {
            "sql_alchemy_connection_string": self._sql_alchemy_connection_string,
        }
