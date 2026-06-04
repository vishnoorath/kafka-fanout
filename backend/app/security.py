"""Secret redaction helpers.

SASL passwords are write-only: accepted in `POST` / `PUT`, never returned
in any `GET`. The UI shows a `••••••••` placeholder once a secret has
been saved; leaving the field unchanged on update means "keep existing
secret" (handled in the Pydantic schemas, not here).
"""
from __future__ import annotations

from typing import Any, Dict, List


def redact_source(source: Dict[str, Any] | None) -> Dict[str, Any] | None:
    if source is None:
        return None
    out = dict(source)
    out["sasl_password"] = None
    return out


def redact_destination(dest: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(dest)
    out["sasl_password"] = None
    return out


def redact_env(env: Dict[str, Any]) -> Dict[str, Any]:
    """Redact secrets on a serialized env dict (already in read shape)."""
    if "source" in env and env["source"] is not None:
        env["source"] = redact_source(env["source"])
    for mapping in env.get("mappings", []) or []:
        mapping["destinations"] = [
            redact_destination(d) for d in mapping.get("destinations", []) or []
        ]
    return env


def redact_env_list(envs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [redact_env(e) for e in envs]
