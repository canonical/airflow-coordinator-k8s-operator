# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Charm support for the Airflow config generation."""

import logging
import typing

import ops

import constants

logger = logging.getLogger(__name__)


class AirflowConfigGenerator:
    """Encapsulate Airflow config generation logic."""

    def __init__(self, charm: ops.CharmBase):
        self._charm = charm

    @property
    def config_template(self) -> typing.Optional[str]:
        """The Airflow config template to pass to all related components."""
        if not self._charm._is_ready(raise_exceptions=False):
            return None

        with open("templates/airflow_config.j2") as config_template_file:
            config_template = config_template_file.read()

        return config_template

    @property
    def _sql_alchemy_connection_string(self) -> typing.Optional[str]:
        """Create the sql alchemy connection string to the postgres database."""
        if not self._charm._is_ready(raise_exceptions=False):
            return None

        postgres_relation_id = self._charm._database_requires.relations[0].id
        relation_data = self._charm._database_requires.fetch_my_relation_data()[
            postgres_relation_id
        ]

        endpoints = [
            endpoint for endpoint in relation_data.get("endpoints", "").split(",") if endpoint
        ]
        if not endpoints:
            return None

        return f"postgresql+psycopg2://{relation_data.get('username')}:{relation_data.get('password')}@{endpoints[0]}/{constants.AIRFLOW_DATABASE_NAME}"

    @property
    def sensitive_config_values(self) -> dict[str, str]:
        """All sensitive values that will be included in the Airflow config template."""
        if not self._charm._is_ready(raise_exceptions=False):
            return {}

        sql_alchemy_connection_string = self._sql_alchemy_connection_string
        if not sql_alchemy_connection_string:
            return {}

        return {
            "sql_alchemy_connection_string": sql_alchemy_connection_string,
        }
