"""Structured security-event emission for the Airflow coordinator charm."""

import json
import logging
from datetime import datetime, timezone
from typing import Any

from owasp_logger import OWASPLogger

logger = logging.getLogger(__name__)
UNATTRIBUTED_ACTOR = "unattributed"
_OWASP_EVENT_KEY = "owasp_event"


class _ContextLogger(logging.LoggerAdapter):
    """Add required Canonical context to an OWASP logger event."""

    def process(self, message: str, kwargs: dict[str, Any]) -> tuple[str, dict[str, Any]]:
        extra = dict(kwargs.get("extra") or {})
        event = extra.get(_OWASP_EVENT_KEY)
        if event is not None:
            event = dict(event)
            event.update(self.extra)
            event["datetime"] = datetime.now(timezone.utc).isoformat(timespec="milliseconds")
            if event.get("level") == "WARNING":
                event["level"] = "WARN"
            extra[_OWASP_EVENT_KEY] = event
            kwargs["extra"] = extra
            message = json.dumps({_OWASP_EVENT_KEY: event}, separators=(",", ":"))
        return message, kwargs


def emit_admin_action(
    *,
    appid: str,
    target: str,
    action: str,
    description: str,
    data: dict[str, Any],
) -> None:
    """Emit an OWASP event and enrich its nested event data with product context."""
    context_logger = _ContextLogger(
        logger,
        {
            "target": target,
            "actor": UNATTRIBUTED_ACTOR,
            "data": data,
        },
    )
    owasp_logger = OWASPLogger(appid=appid, logger=context_logger)
    owasp_logger.authz_admin(UNATTRIBUTED_ACTOR, action, description=description)


def changed_fields(previous: dict[str, Any], current: dict[str, Any]) -> list[str]:
    """Return sorted field names whose values differ between snapshots."""
    return sorted(
        key
        for key in previous.keys() | current.keys()
        if previous.get(key) != current.get(key)
    )
