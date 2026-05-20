# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Webserver config generation for Airflow Coordinator OAuth integration."""

import logging
import pathlib

import ops
from charms.hydra.v0.oauth import OauthProviderConfig
from jinja2 import Environment, FileSystemLoader, TemplateError

import constants

logger = logging.getLogger(__name__)

# Maps Airflow role names (as used in idp_group_config keys) to the
# display names expected by Flask-AppBuilder's AUTH_ROLES_MAPPING.
_ROLE_DISPLAY_NAMES: dict[str, str] = {
    "admin": "Admin",
    "op": "Op",
    "user": "User",
    "viewer": "Viewer",
    "public": "Public",
}


class WebserverConfigError(Exception):
    """Raised when webserver_config.py cannot be rendered."""


def _parse_groups(raw: str) -> list[str]:
    """Parse a comma-delimited string of group names, dropping blank entries."""
    return [g.strip() for g in raw.split(",") if g.strip()]


def _build_roles_mapping(idp_group_config: dict[str, str]) -> dict[str, list[str]]:
    """Build the AUTH_ROLES_MAPPING dict from the idp_group_config.

    Args:
        idp_group_config: Mapping of lowercase Airflow role name (e.g. "admin")
            to a comma-delimited string of external IdP group names.

    Returns:
        Dict mapping each IdP group name to a single-element list containing
        the corresponding Flask-AppBuilder role display name.
    """
    mapping: dict[str, list[str]] = {}
    for role_key, raw_groups in idp_group_config.items():
        display_name = _ROLE_DISPLAY_NAMES.get(role_key.lower())
        if not display_name:
            logger.warning("Unknown Airflow role key %r in idp_group_config — skipping", role_key)
            continue
        for group in _parse_groups(raw_groups):
            mapping[group] = [display_name]
    return mapping


class WebserverConfigGenerator:
    """Encapsulate webserver_config.py generation logic for OAuth integration.

    Mirrors the structure of AirflowConfigGenerator: produces a Jinja2 template
    string (non-sensitive values baked in, client_secret as a placeholder) and a
    separate dict of sensitive values for inclusion in the relation secret.
    """

    def __init__(self, charm: ops.CharmBase):
        self._charm = charm

    @property
    def _provider_info(self) -> OauthProviderConfig | None:
        """Return provider info when OAuth is active, otherwise None."""
        if not self._charm._oauth_active:
            return None
        try:
            return self._charm._oauth_requirer.get_provider_info()
        except Exception:
            logger.exception("Error retrieving OAuth provider info for webserver config")
            return None

    @property
    def _idp_group_config(self) -> dict[str, str]:
        """Return the IDP group config from charm config as a role→groups mapping."""
        return {
            "admin": str(self._charm.config.get(constants.EXTERNAL_IDP_GROUPS_FOR_ADMIN, "")),
            "op": str(self._charm.config.get(constants.EXTERNAL_IDP_GROUPS_FOR_OP, "")),
            "user": str(self._charm.config.get(constants.EXTERNAL_IDP_GROUPS_FOR_USER, "")),
            "viewer": str(self._charm.config.get(constants.EXTERNAL_IDP_GROUPS_FOR_VIEWER, "")),
            "public": str(self._charm.config.get(constants.EXTERNAL_IDP_GROUPS_FOR_PUBLIC, "")),
        }

    @property
    def webserver_config_template(self) -> str | None:
        """Render the webserver config Jinja2 template with non-sensitive values baked in.

        Returns a Python source string that still contains the Jinja2 placeholder
        ``{{ webserver_config__client_secret }}`` for the OAuth client secret.
        This template is distributed to core charms via the coordinator relation
        and rendered downstream with the actual secret value.

        Returns None when OAuth is not active or required fields are missing.
        """
        provider_info = self._provider_info
        if not provider_info:
            return None

        missing = [
            field
            for field in (
                "client_id",
                "scope",
                "token_endpoint",
                "authorization_endpoint",
                "jwks_endpoint",
            )
            if not getattr(provider_info, field, None)
        ]
        if missing:
            logger.warning(
                "OauthProviderConfig missing required fields: %s — cannot render webserver config",
                missing,
            )
            return None

        api_base_url = self._charm._config_generator._api_server_base_url
        if not api_base_url:
            logger.warning("api_base_url is empty; cannot render webserver config template")
            return None

        auth_roles_mapping = _build_roles_mapping(self._idp_group_config)

        try:
            templates_dir = pathlib.Path(__file__).parent / "templates"
            env = Environment(loader=FileSystemLoader(str(templates_dir)), autoescape=False)
            template = env.get_template("webserver_config.py.j2")
            return template.render(
                client_id=provider_info.client_id,
                # client_secret stays as a Jinja2 placeholder so that downstream
                # charms render it with the actual secret from sensitive_data.
                webserver_config__client_secret="{{ webserver_config__client_secret }}",
                api_base_url=api_base_url,
                scope=provider_info.scope,
                token_endpoint=provider_info.token_endpoint,
                authorization_endpoint=provider_info.authorization_endpoint,
                jwks_endpoint=provider_info.jwks_endpoint,
                auth_roles_mapping=auth_roles_mapping,
            )
        except TemplateError as e:
            logger.exception("Failed to render webserver_config template: %s", e)
            return None

    @property
    def sensitive_values(self) -> dict[str, str]:
        """Return the sensitive values required to render the webserver config template.

        Keys follow the ``section__key`` convention used by other sensitive config
        values.  Returns an empty dict when OAuth is not active or the provider
        has not yet shared a client_secret.
        """
        provider_info = self._provider_info
        if not provider_info or not provider_info.client_secret:
            return {}
        return {"webserver_config__client_secret": provider_info.client_secret}
