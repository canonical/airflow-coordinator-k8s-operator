"""Structured security-event emission for the Airflow coordinator charm."""

import json
import logging
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)

_LEVELS = {
    "INFO": logging.INFO,
    "WARN": logging.WARNING,
    "CRITICAL": logging.CRITICAL,
}
UNATTRIBUTED_ACTOR = "unattributed"


def emit_security_event(
    *,
    appid: str,
    event_type: str,
    parameters: tuple[str, ...],
    level: str,
    actor: str,
    target: str,
    description: str,
    data: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Write a security event as one JSON record and return its payload."""
    normalized_level = level.upper()
    payload = {
        "datetime": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
        "appid": appid,
        "event": event_type + (":" + ",".join(parameters) if parameters else ""),
        "level": normalized_level,
        "actor": actor,
        "target": target,
        "description": description,
        "data": data or {},
    }
    logger.log(_LEVELS[normalized_level], json.dumps(payload, separators=(",", ":")))
    return payload


def emit_admin_action(
    *,
    appid: str,
    target: str,
    action: str,
    description: str,
    data: dict[str, Any],
) -> dict[str, Any]:
    """Emit an administrative event with consistent severity and attribution."""
    return emit_security_event(
        appid=appid,
        event_type="authz_admin",
        parameters=(UNATTRIBUTED_ACTOR, action),
        level="WARN",
        actor=UNATTRIBUTED_ACTOR,
        target=target,
        description=description,
        data=data,
    )


def changed_fields(previous: dict[str, Any], current: dict[str, Any]) -> list[str]:
    """Return sorted field names whose values differ between snapshots."""
    return sorted(
        key
        for key in previous.keys() | current.keys()
        if previous.get(key) != current.get(key)
    )
