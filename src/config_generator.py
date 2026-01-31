# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Charm support for the Airflow config generation."""

import collections
import configparser
import io
import logging
import pathlib

import ops

import constants

logger = logging.getLogger(__name__)

BlacklistConfigKey = collections.namedtuple("BlacklistConfigKey", ["section", "option"])

BLACKLIST_CUSTOM_CONFIG_KEYS = [
    BlacklistConfigKey(section="core", option="executor"),
    BlacklistConfigKey(section="database", option="sql_alchemy_conn"),
]


class AirflowConfigGenerator:
    """Encapsulate Airflow config generation logic."""

    def __init__(self, charm: ops.CharmBase):
        self._charm = charm

        self._custom_config_parser, self._sensitive_custom_config_parser = None, None

        if self._charm.config.get(constants.CUSTOM_CONFIG):
            self._custom_config_parser = configparser.ConfigParser()
            self._custom_config_parser.read_string(self._charm.config[constants.CUSTOM_CONFIG])

        if self._charm.config.get(constants.SENSITIVE_CUSTOM_CONFIG):
            try:
                custom_config_secret = self._charm.model.get_secret(
                    id=self._charm.config[constants.SENSITIVE_CUSTOM_CONFIG],
                )

                self._sensitive_custom_config_parser = configparser.ConfigParser()
                self._sensitive_custom_config_parser.read_string(
                    custom_config_secret.get_content(refresh=True)[
                        constants.SENSITIVE_CUSTOM_CONFIG_SECRET_KEY
                    ],
                )
            except (ops.model.SecretNotFoundError, ops.model.ModelError):
                pass

    @property
    def do_custom_configs_overlap(self) -> bool:
        """Return whether there are overlapping keys in custom configs."""
        if not self._custom_config_parser or not self._sensitive_custom_config_parser:
            return False

        for section in self._sensitive_custom_config_parser.sections():
            if not self._custom_config_parser.has_section(section):
                continue

            sensitive_section_options = self._sensitive_custom_config_parser.options(section)
            normal_section_options = self._custom_config_parser.options(section)

            if set(sensitive_section_options) & set(normal_section_options):
                return True

        return False

    @property
    def custom_configs_have_blacklisted_keys(self) -> bool:
        """Return whether any of the custom configs have blacklisted keys."""
        if not self._custom_config_parser and not self._sensitive_custom_config_parser:
            return False

        has_blacklist = False

        for blacklist in BLACKLIST_CUSTOM_CONFIG_KEYS:
            if self._custom_config_parser and self._custom_config_parser.has_option(
                blacklist.section, blacklist.option
            ):
                logger.error(
                    f"Custom config has blacklisted key {blacklist.section}.{blacklist.option}"
                )
                has_blacklist = True

            if (
                self._sensitive_custom_config_parser
                and self._sensitive_custom_config_parser.has_option(
                    blacklist.section, blacklist.option
                )
            ):
                logger.error(
                    "Sensitive custom config has blacklisted key "
                    f"{blacklist.section}.{blacklist.option}"
                )
                has_blacklist = True

        return has_blacklist

    @property
    def config_template(self) -> str:
        """The Airflow config template to pass to all related components."""
        final_config_parser = configparser.ConfigParser()
        final_config_parser.read(pathlib.Path("src/templates/airflow_config.j2"))

        if self._sensitive_custom_config_parser:
            for section in self._sensitive_custom_config_parser.sections():
                for option in self._sensitive_custom_config_parser.options(section):
                    if not final_config_parser.has_section(section):
                        final_config_parser.add_section(section)

                    final_config_parser.set(
                        section, option, f"{{{{ {section}_{option}_secret_value }}}}"
                    )

        if self._custom_config_parser:
            for section in self._custom_config_parser.sections():
                for option in self._custom_config_parser.options(section):
                    if not final_config_parser.has_section(section):
                        final_config_parser.add_section(section)

                    final_config_parser.set(
                        section, option, self._custom_config_parser.get(section, option)
                    )

        string_buffer = io.StringIO()
        final_config_parser.write(string_buffer)

        return string_buffer.getvalue()

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
        sensitive_data = {
            "sql_alchemy_connection_string": self._sql_alchemy_connection_string,
        }

        if self._sensitive_custom_config_parser:
            for section in self._sensitive_custom_config_parser.sections():
                for option in self._sensitive_custom_config_parser.options(section):
                    sensitive_data[f"{section}_{option}_secret_value"] = (
                        self._sensitive_custom_config_parser.get(section, option)
                    )

        return sensitive_data
