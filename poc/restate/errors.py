"""Stable transport encoding for Restate saga terminal failures."""

from __future__ import annotations

import json
import re
import traceback

from poc.common.domain import SAGA_ERROR_TYPES, SagaFailure, SagaOutcome
from restate import TerminalError

ENVELOPE_VERSION = "RESTATE_SAGA_V1"
_ENVELOPE = re.compile(
    rf"{ENVELOPE_VERSION}:"
    r"(REJECTED|COMPENSATED|COMPENSATION_INCOMPLETE):"
    r"(SagaRejectedError|SagaCompensatedError|SagaCompensationIncompleteError)"
)

SAGA_HTTP_STATUS = {
    SagaOutcome.REJECTED: 400,
    SagaOutcome.COMPENSATED: 409,
    SagaOutcome.COMPENSATION_INCOMPLETE: 500,
}


def encode_saga_failure(failure: SagaFailure) -> str:
    """Encode a searchable prefix."""
    return (
        f"{ENVELOPE_VERSION}:{failure.outcome.value}:"
        f"{SAGA_ERROR_TYPES[failure.outcome]}"
    )


def decode_saga_failure(text: str | bytes | None) -> tuple[SagaFailure, str] | None:
    """Find an encoded verdict in a Restate exception or HTTP response body."""
    if not text:
        return None

    if isinstance(text, bytes):
        text = text.decode("utf-8")

    match = _ENVELOPE.search(text)
    if match is None:
        return None

    error_type = match.group(2)

    try:
        body = json.loads(text)
        payload = body.get("metadata", {}).get("payload")
        if not payload:
            return None

        failure = SagaFailure.model_validate_json(payload)
        return failure, error_type
    except (ValueError, TypeError):
        return None


def saga_terminal_error(
    failure: SagaFailure, original_error: BaseException | None = None
) -> TerminalError:
    metadata = {}
    if original_error:
        metadata["traceback"] = "".join(traceback.format_exception(original_error))
    metadata["payload"] = failure.model_dump_json()

    return TerminalError(
        encode_saga_failure(failure),
        status_code=SAGA_HTTP_STATUS[failure.outcome],
        metadata=metadata,
    )
