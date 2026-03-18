# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Charm support for the Airflow config generation."""

import logging

import jinja2
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

        return (
            jinja2.Environment(loader=jinja2.BaseLoader(), undefined=jinja2.DebugUndefined)
            .from_string(config_template)
            .render(
                {
                    "api_server_base_url": f"http://{self._charm._api_server_requires.api_server_host}:{self._charm._api_server_requires.api_server_port}",
                    "api_server_port": self._charm._api_server_requires.api_server_port,
                }
            )
        )

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

    @property
    def sensitive_config_values(self) -> dict[str, str]:
        """All sensitive values that will be included in the Airflow config template."""
        keys_content = self._charm.get_keys_secret().get_content()
        return {
            "sql_alchemy_connection_string": self._sql_alchemy_connection_string,
            "api__secret_key": keys_content["secret-key"],
            "api_auth__jwt_secret": keys_content["jwt-secret"],
            "core__fernet_key": keys_content["fernet-key"],
        }
