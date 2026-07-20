"""Pure, database-agnostic helpers shared by the Postgres store.

Every function here is copied verbatim from the original ``backend_store.py``
so the business rules (money rounding, canonical inventory shape, client
identity keys, status normalization) behave identically on Supabase/Postgres.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import re
from copy import deepcopy
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any, Mapping, Optional, Sequence


SCHEMA_VERSION = "icon-mobile.postgres.v2"
TRANSACTION_TYPES = {"Sale", "Issue", "Return", "B2B_Payment"}
_PARTNER_PREFIX = "Partner:"


class StoreError(Exception):
    def __init__(self, status: int, code: str, message: str, details: Any = None) -> None:
        super().__init__(message)
        self.status = status
        self.code = code
        self.message = message
        self.details = details


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def json_object(value: Any, fallback: Optional[dict] = None) -> dict:
    if isinstance(value, dict):
        return deepcopy(value)
    if isinstance(value, str) and value.strip():
        try:
            loaded = json.loads(value)
            return loaded if isinstance(loaded, dict) else (fallback or {})
        except (TypeError, ValueError, json.JSONDecodeError):
            pass
    return deepcopy(fallback or {})


def first_value(mapping: Mapping[str, Any], keys: Sequence[str], default: Any = "") -> Any:
    for key in keys:
        value = mapping.get(key)
        if value is not None and str(value).strip() != "":
            return value
    return default


def clean_text(value: Any, *, maximum: int = 500) -> str:
    text = str(value if value is not None else "").strip()
    if len(text) > maximum:
        raise StoreError(422, "field_too_long", f"Text field exceeds {maximum} characters")
    return text


def safe_int(value: Any, default: int = 0, *, minimum: Optional[int] = None, maximum: Optional[int] = None) -> int:
    try:
        result = int(Decimal(str(value).replace(",", "").strip()))
    except (InvalidOperation, ValueError, TypeError):
        result = default
    if minimum is not None and result < minimum:
        result = minimum
    if maximum is not None and result > maximum:
        raise StoreError(422, "number_too_large", f"Number exceeds {maximum}")
    return result


def money_to_cents(value: Any, default: int = 0) -> int:
    if value is None or value == "":
        return default
    raw = str(value).replace(",", "").strip()
    raw = re.sub(r"[^0-9.\-+]", "", raw)
    try:
        amount = Decimal(raw)
        if not amount.is_finite():
            raise InvalidOperation
        cents = int((amount * 100).quantize(Decimal("1"), rounding=ROUND_HALF_UP))
    except (InvalidOperation, ValueError):
        return default
    if abs(cents) > 100_000_000_000_00:
        raise StoreError(422, "amount_too_large", "Money amount is outside the supported range")
    return cents


def cents_to_legacy(cents: int) -> Any:
    if cents % 100 == 0:
        return cents // 100
    return float(Decimal(cents) / Decimal(100))


def normalized_status(value: Any, default: str = "Available") -> str:
    status = clean_text(value or default, maximum=220)
    lowered = status.lower()
    if lowered == "available":
        return "Available"
    if lowered == "sold":
        return "Sold"
    if lowered == "returned":
        return "Returned"
    if lowered == "deleted":
        return "Deleted"
    if lowered.startswith("partner:"):
        name = status.split(":", 1)[1].strip()
        if not name:
            raise StoreError(422, "invalid_status", "Partner inventory status needs a partner name")
        return f"{_PARTNER_PREFIX}{name}"
    raise StoreError(422, "invalid_status", f"Unsupported inventory status '{status}'")


def request_digest(payload: Mapping[str, Any]) -> str:
    return hashlib.sha256(canonical_json(payload).encode("utf-8")).hexdigest()
