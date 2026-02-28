# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Charm support for the Airflow config generation."""

import logging
import pathlib
import re

import ops

logger = logging.getLogger(__name__)


class AirflowConfigGenerator:
    """Encapsulate Airflow config generation logic."""

    def __init__(self, charm: ops.CharmBase):
        self._charm = charm

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

    @staticmethod
    def _section_body(template: str, section: str) -> str | None:
        """Return the text between a section header and the next header (or EOF)."""
        match = re.search(
            rf"^\[{re.escape(section)}\][^\S\n]*\n(.*?)(?=^\[|\Z)",
            template,
            re.MULTILINE | re.DOTALL,
        )
        return match.group(1) if match else None

    def config_template_with_extra_config(self, **extra_config) -> str:
        """Return the Airflow config template merged with extra config from different integrators.

        Existing keys are replaced with the integrator's value, new keys are
        inserted under the existing section header, and entirely new sections
        are appended.  This avoids ``DuplicateSectionError`` from ConfigParser
        while allowing integrators to override defaults.

        Returns:
            Combined Jinja2 template string ready to share with core charms.
        """
        # FIXME: this regex-based merge is fragile. A more elegant solution
        # would be to use configparser or Jinja2 native solutions, which requires
        # changes to the library and the core charms.
        result = self.config_template
        if not extra_config:
            return result

        new_sections = []
        for section, keys in extra_config.items():
            body = self._section_body(result, section)
            if body is not None:
                existing_keys = set(re.findall(r"^(\w+)\s*=", body, re.MULTILINE))
                new_key_lines = []
                for k, v in keys.items():
                    if k in existing_keys:
                        result = re.sub(
                            rf"^({re.escape(k)}\s*=\s*).*$",
                            rf"\g<1>{v}",
                            result,
                            count=1,
                            flags=re.MULTILINE,
                        )
                    else:
                        new_key_lines.append(f"{k} = {v}")
                if new_key_lines:
                    key_lines = "\n".join(new_key_lines)
                    pattern = rf"(\[{re.escape(section)}\][^\S\n]*\n)"
                    result = re.sub(pattern, rf"\g<1>{key_lines}\n", result, count=1)
            else:
                key_lines = "\n".join(f"{k} = {v}" for k, v in keys.items())
                new_sections.append(f"\n[{section}]\n{key_lines}")

        if new_sections:
            result += "\n".join(new_sections) + "\n"

        return result

    @property
    def api_server_config(self) -> dict[str, dict[str, str]]:
        """Return the API server config as extra config sections.

        Uses the same {section: {key: value}} pattern as executor config.

        FIXME: This config should be returned by the API server charm itself,
        not generated here by the coordinator.
        """
        host = self._charm._api_server_requires.api_server_host
        port = self._charm._api_server_requires.api_server_port
        return {
            "api": {
                "base_url": f"http://{host}:{port}",
                "port": str(port),
            },
        }

    @property
    def sensitive_config_values(self) -> dict[str, str]:
        """All sensitive values that will be included in the Airflow config template."""
        return {
            "database__sql_alchemy_conn": self._sql_alchemy_connection_string,
        }
