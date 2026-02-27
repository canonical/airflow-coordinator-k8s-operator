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

        postgres_relation_data = self._charm._database_requires.fetch_relation_data(
            [postgres_relation_id]
        )[postgres_relation_id]

        username = postgres_relation_data["username"]
        password = postgres_relation_data["password"]
        database = postgres_relation_data["database"]

        endpoints = [
            endpoint for endpoint in postgres_relation_data["endpoints"].split(",") if endpoint
        ]

        return f"postgresql+psycopg2://{username}:{password}@{endpoints[0]}/{database}"

    def config_template_with_extra_config(self, **extra_config) -> str:
        """Return the Airflow config template merged with extra config from different integrators.

        Returns:
            Combined Jinja2 template string ready to share with core charms.
        """
        base = self.config_template
        dag_bundle_config_list = self._charm.config.get("dag_bundle_config_list", "").strip()
        if dag_bundle_config_list:
            extra_config.setdefault("dag_processor", {})
            extra_config["dag_processor"]["dag_bundle_config_list"] = dag_bundle_config_list

        if not extra_config:
            return base

        extra_lines = []
        for section, keys in extra_config.items():
            extra_lines.append(f"\n[{section}]")
            for key, value in keys.items():
                extra_lines.append(f"{key} = {value}")

        return base + "\n".join(extra_lines) + "\n"

    @property
    def sensitive_config_values(self) -> dict[str, str]:
        """All sensitive values that will be included in the Airflow config template."""
        return {
            "sql_alchemy_conn": self._sql_alchemy_connection_string,
        }
