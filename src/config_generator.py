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

        render_context = {
            "api_server_base_url": f"http://{self._charm._api_server_requires.api_server_host}:{self._charm._api_server_requires.api_server_port}",
            "api_server_port": self._charm._api_server_requires.api_server_port,
        }

        s3_dag_bundles = [
            {
                "name": f"s3_{relation_id}_dag_bundle",
                "classpath": "airflow.providers.amazon.aws.bundles.s3",
                "kwargs": {
                    "aws_conn_id": f"s3_relation_{relation_id}_connection",
                    "bucket_name": connection_info["bucket"],
                    "prefix": connection_info.get("path", ""),
                },
            }
            for relation_id, connection_info in self._charm.s3_connections.items()
        ]
        if s3_dag_bundles:
            render_context["dag_bundles"] = s3_dag_bundles

        return (
            jinja2.Environment(loader=jinja2.BaseLoader(), undefined=jinja2.DebugUndefined)
            .from_string(config_template)
            .render(render_context)
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
        return {
            "sql_alchemy_connection_string": self._sql_alchemy_connection_string,
        }
