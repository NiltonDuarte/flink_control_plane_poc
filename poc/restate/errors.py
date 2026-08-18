"""Stable transport encoding for Restate saga terminal failures."""

from __future__ import annotations

import base64
import re

from poc.common.domain import SAGA_ERROR_TYPES, SagaFailure, SagaOutcome
from restate import TerminalError

ENVELOPE_VERSION = "RESTATE_SAGA_V1"
_ENVELOPE = re.compile(
    rf"{ENVELOPE_VERSION}:"
    r"(REJECTED|COMPENSATED|COMPENSATION_INCOMPLETE):"
    r"(SagaRejectedError|SagaCompensatedError|SagaCompensationIncompleteError):"
    r"([A-Za-z0-9_-]+)"
)

SAGA_HTTP_STATUS = {
    SagaOutcome.REJECTED: 400,
    SagaOutcome.COMPENSATED: 409,
    SagaOutcome.COMPENSATION_INCOMPLETE: 500,
}


def encode_saga_failure(failure: SagaFailure) -> str:
    """Encode a searchable prefix plus an opaque, stable Pydantic payload."""
    payload = base64.urlsafe_b64encode(failure.model_dump_json().encode()).decode()
    return (
        f"{ENVELOPE_VERSION}:{failure.outcome.value}:"
        f"{SAGA_ERROR_TYPES[failure.outcome]}:{payload.rstrip('=')}"
    )


def decode_saga_failure(text: str | None) -> tuple[SagaFailure, str] | None:
    """Find an encoded verdict in a Restate exception or HTTP response body."""
    if not text:
        return None
    match = _ENVELOPE.search(text)
    if match is None:
        return None
    encoded = match.group(3)
    encoded += "=" * (-len(encoded) % 4)
    try:
        failure = SagaFailure.model_validate_json(
            base64.urlsafe_b64decode(encoded).decode()
        )
    except (ValueError, UnicodeDecodeError):
        return None
    error_type = match.group(2)
    if error_type != SAGA_ERROR_TYPES[failure.outcome]:
        return None
    return failure, error_type


def saga_terminal_error(failure: SagaFailure) -> TerminalError:
    """Build the engine terminal error and outcome-specific ingress status."""
    return TerminalError(
        encode_saga_failure(failure),
        status_code=SAGA_HTTP_STATUS[failure.outcome],
    )
