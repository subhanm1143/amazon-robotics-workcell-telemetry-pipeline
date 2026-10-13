"""The event contract Cosmos and this project agree on. A dict is "valid"
only if every required field is present with the right type; anything else —
a truncated payload, a field with the wrong type, JSON that does not even
parse — routes to the dead-letter stream instead of failing the job or
getting silently dropped."""
from __future__ import annotations

import json
from dataclasses import dataclass

REQUIRED_FIELDS = {
    "eventId": str,
    "workcellId": str,
    "workcellType": str,
    "eventType": str,
    "status": str,
    "occurredAtMillis": int,
}


@dataclass
class ValidationResult:
    valid: bool
    parsed: dict | None
    error: str | None


def validate(raw: str) -> ValidationResult:
    try:
        obj = json.loads(raw)
    except json.JSONDecodeError as exc:
        return ValidationResult(False, None, f"invalid JSON: {exc}")

    if not isinstance(obj, dict):
        return ValidationResult(False, None, "payload is not a JSON object")

    for field, expected_type in REQUIRED_FIELDS.items():
        if field not in obj:
            return ValidationResult(False, None, f"missing field: {field}")
        if not isinstance(obj[field], expected_type):
            return ValidationResult(False, None, f"{field} has wrong type")

    if obj["eventType"] not in ("status", "fault"):
        return ValidationResult(False, None, f"unknown eventType: {obj['eventType']}")

    return ValidationResult(True, obj, None)
