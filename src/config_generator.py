# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Charm support for the Airflow config generation."""

import configparser
import io
import json
import logging
import pathlib
import typing

import charms.git_integrator.v0.git as git
import ops

import connection_manager
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

    def _dag_bundle_for_s3_connection(
        self, relation_id: int, s3_connection_info: connection_manager.S3ConnectionInfo
    ) -> typing.Optional[dict]:
        """Generate DAG bundle config dict for provided S3 connection."""
        if not s3_connection_info:
            return None

        return {
            "name": f"s3_{relation_id}_dag_bundle",
            "classpath": "airflow.providers.amazon.aws.bundles.s3.S3DagBundle",
            "kwargs": {
                "aws_conn_id": f"s3_relation_{relation_id}_connection",
                "bucket_name": s3_connection_info.bucket,
                "prefix": s3_connection_info.path or "",
            },
        }

    def _dag_bundle_for_git_connection(
        self, relation_id: int, git_provider_model: git.GitProviderModel
    ) -> typing.Optional[dict]:
        git_dag_bundle_kwargs = {
            "repo_url": git_provider_model.repository_url,
            "tracking_ref": git_provider_model.tracking_ref,
            "subdir": git_provider_model.path,
            "submodules": False,
            "prune_dotgit_folder": True,
        }

        if git_provider_model.authentication_method:
            git_dag_bundle_kwargs["git_conn_id"] = f"git_relation_{relation_id}_connection"

        return {
            "name": f"git_{relation_id}_dag_bundle",
            "classpath": "airflow.providers.git.bundles.git.GitDagBundle",
            "kwargs": git_dag_bundle_kwargs,
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
    def auth_manager_config(self) -> dict[str, dict[str, str]]:
        """Return the FAB auth manager config when OAuth is active, otherwise empty dict.

        Uses the same {section: {key: value}} pattern as other extra configs.
        """
        if not self._charm._oauth_active:
            return {}

        return {"core": {"auth_manager": constants.FAB_AUTH_MANAGER_CLASS}}

    @property
    def dag_bundle_config(self) -> dict[str, dict[str, str]]:
        """Return the DAG processor config as extra config sections.

        Uses the same {section: {key: value}} pattern as executor config.
        """
        s3_dag_bundles = [
            dag_bundle_config
            for relation_id, s3_connection_info in self._charm.s3_relation_connections.items()
            if (
                dag_bundle_config := self._dag_bundle_for_s3_connection(
                    relation_id, s3_connection_info
                )
            )
        ]

        git_dag_bundles = [
            self._dag_bundle_for_git_connection(relation_id, git_provider_model)
            for relation_id, git_provider_model in self._charm.git_relation_connections.items()  # noqa: E501
        ]

        if not s3_dag_bundles and not git_dag_bundles:
            return {}

        return {
            "dag_processor": {
                "dag_bundle_config_list": json.dumps(
                    sorted(s3_dag_bundles + git_dag_bundles, key=lambda element: element["name"]),
                    indent=4,
                ),
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
            "core__fernet_key": self._charm._fernet_key,
            **self._connection_env_vars,
        }

    @property
    def _connection_env_vars(self) -> dict[str, str]:
        """S3 connection URIs for AIRFLOW_CONN_* env var injection."""
        return {
            f"connections__s3_relation_{rid}_connection": json.dumps(
                {
                    "conn_type": "aws",
                    "login": info.access_key,
                    "password": info.secret_key,
                    **({"extra": info.connection_extras()} if info.connection_extras() else {}),
                }
            )
            for rid, info in self._charm.s3_relation_connections.items()
        }
