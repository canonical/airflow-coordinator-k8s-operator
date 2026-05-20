# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Webserver config generation for Airflow Coordinator OAuth integration."""

import logging
import pathlib

from charms.hydra.v0.oauth import OauthProviderConfig
from jinja2 import Environment, FileSystemLoader, TemplateError

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


def render_webserver_config(
    provider_info: OauthProviderConfig,
    api_base_url: str,
    idp_group_config: dict[str, str],
) -> str:
    """Render webserver_config.py from the Jinja2 template.

    Args:
        provider_info: OAuth provider data returned by OAuthRequirer.get_provider_info().
        api_base_url: Base URL of the Airflow API server, used as redirect URI.
        idp_group_config: Mapping of lowercase Airflow role name (e.g. "admin", "op")
            to a comma-delimited string of external IdP group names. Unknown keys are
            logged and skipped; missing keys produce no mapping entries for that role.

    Returns:
        Rendered webserver_config.py content as a string.

    Raises:
        WebserverConfigError: If required provider fields are missing or template
            rendering fails.
    """
    missing = [
        field
        for field in (
            "client_id",
            "client_secret",
            "scope",
            "token_endpoint",
            "authorization_endpoint",
            "jwks_endpoint",
        )
        if not getattr(provider_info, field, None)
    ]
    if missing:
        raise WebserverConfigError(
            f"OauthProviderConfig is missing required fields: {', '.join(missing)}"
        )

    if not api_base_url:
        raise WebserverConfigError("api_base_url must not be empty")

    auth_roles_mapping = _build_roles_mapping(idp_group_config)

    try:
        templates_dir = pathlib.Path(__file__).parent / "templates"
        env = Environment(loader=FileSystemLoader(str(templates_dir)), autoescape=False)
        template = env.get_template("webserver_config.py.j2")
        return template.render(
            client_id=provider_info.client_id,
            client_secret=provider_info.client_secret,
            api_base_url=api_base_url,
            scope=provider_info.scope,
            token_endpoint=provider_info.token_endpoint,
            authorization_endpoint=provider_info.authorization_endpoint,
            jwks_endpoint=provider_info.jwks_endpoint,
            auth_roles_mapping=auth_roles_mapping,
        )
    except TemplateError as e:
        raise WebserverConfigError(f"Failed to render webserver_config template: {e}") from e
