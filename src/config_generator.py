# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Charm support for the Airflow config generation."""

import configparser
import io
import json
import logging
import pathlib

import ops

import constants

logger = logging.getLogger(__name__)


class AirflowConfigGenerator:
    """Encapsulate Airflow config generation logic."""

    def __init__(self, charm: ops.CharmBase):
        self._charm = charm

    @property
    def _api_server_base_url(self) -> str:
        """Return the API server base URL, appending the ingress path when available."""
        host = self._charm._api_server_requires.api_server_host
        port = self._charm._api_server_requires.api_server_port
        ingress_path = self._charm._api_server_requires.api_server_ingress_path
        base = f"http://{host}:{port}"
        if ingress_path:
            return f"{base}/{ingress_path}"
        return base

    @property
    def config_template(self) -> str:
        """The raw Airflow config Jinja2 template to pass to all related components.

        Returns the template as-is, preserving all Jinja2 placeholders and
        control-flow tags.  Downstream charms perform the final render with
        the appropriate context (sensitive_config_values + extras).
        """
        return pathlib.Path("src/templates/airflow_config.j2").read_text()

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
        """Return the Airflow config template merged with extra config from different sources.

        Uses configparser to parse and merge sections/keys, and ignores Jinja2
        placeholders (via the RawConfigParser()).
        Existing keys are overwritten, new keys are added to existing sections,
        and entirely new sections are appended.

        Returns:
            Combined Jinja2 template string ready to share with core charms.
        """
        base_template = self.config_template
        if not extra_config:
            return base_template

        # NOTE: RawConfigParser treats Jinja2 condition-wrapped options
        # (e.g. ``{% if x %}key = val{% endif %}``) as a single option whose
        # name includes the Jinja2 prefix.  Overriding such an option via
        # extra_config would add a *new* option instead of replacing it.
        # This is acceptable today because no extra_config key conflicts with
        # a condition-wrapped option in the template.
        parser = configparser.RawConfigParser()
        parser.read_string(base_template)

        for section, keys in extra_config.items():
            if not parser.has_section(section):
                parser.add_section(section)
            for k, v in keys.items():
                parser.set(section, k, v)

        output = io.StringIO()
        parser.write(output)
        return output.getvalue()

    @property
    def api_server_uri_config(self) -> dict[str, dict[str, str]]:
        """Return the API server config as extra config sections.

        Uses the same {section: {key: value}} pattern as executor config.

        FIXME: This config should be returned by the API server charm itself,
        not generated here by the coordinator.
        """
        return {
            "api": {
                "base_url": self._api_server_base_url,
                "port": str(self._charm._api_server_requires.api_server_port),
            },
        }

    @property
    def coordinator_charm_core_config(self) -> dict[str, dict[str, str | int]]:
        """Return the Airflow core config extracted from this charm's juju config.

        Uses the same {section: {key: value}} pattern as executor config.
        """
        return {
            "core": {
                "default_timezone": self._charm.config[constants.CORE_DEFAULT_TIMEZONE_CONFIG],
                "max_active_runs_per_dag": self._charm.config[
                    constants.CORE_MAX_ACTIVE_RUNS_PER_DAG_CONFIG
                ],
                "max_active_tasks_per_dag": self._charm.config[
                    constants.CORE_MAX_ACTIVE_TASKS_PER_DAG_CONFIG
                ],
                "parallelism": self._charm.config[constants.CORE_PARALLELISM_CONFIG],
                "default_impersonation": constants.WORKLOAD_USER,
            },
            "dag_processor": {
                "parsing_processes": self._charm.config[
                    constants.DAG_PROCESSOR_PARSING_PROCESSES_CONFIG
                ],
            },
            "database": {
                "sql_alchemy_pool_size": self._charm.config[
                    constants.DATABASE_SQL_ALCHEMY_POOL_SIZE_CONFIG
                ],
            },
            "triggerer": {
                "capacity": self._charm.config[constants.TRIGGERER_CAPACITY_CONFIG],
            },
        }

    @property
    def dag_bundle_config(self) -> dict[str, dict[str, str]]:
        """Return the DAG processor config as extra config sections.

        Uses the same {section: {key: value}} pattern as executor config.
        """
        s3_dag_bundles = [
            {
                "name": f"s3_{relation_id}_dag_bundle",
                "classpath": "airflow.providers.amazon.aws.bundles.s3.S3DagBundle",
                "kwargs": {
                    "aws_conn_id": f"s3_relation_{relation_id}_connection",
                    "bucket_name": connection_info.bucket,
                    "prefix": connection_info.path,
                },
            }
            for relation_id, connection_info in self._charm.s3_connections.items()
            if connection_info
        ]

        if not s3_dag_bundles:
            return {}

        return {
            "dag_processor": {
                "dag_bundle_config_list": json.dumps(s3_dag_bundles),
            },
        }

    @property
    def sensitive_config_values(self) -> dict[str, str]:
        """All sensitive values that will be included in the Airflow config template."""
        keys_content = self._charm.get_keys_secret().get_content()
        return {
            "database__sql_alchemy_conn": self._sql_alchemy_connection_string,
            "api__secret_key": keys_content["secret-key"],
            "api_auth__jwt_secret": keys_content["jwt-secret"],
            "core__fernet_key": keys_content["fernet-key"],
        }
