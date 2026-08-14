"""The Restate adapter - reserved, not implemented. See issue #11.

Nothing here yet on purpose. The extraction of `poc/core/` (issue #10) came
first precisely so that this package can be written without touching the saga:
what it owes the core is the four things listed in `poc/adapters/__init__.py`,
and nothing else.

The shape it is expected to take, from the RFC:

* a **Virtual Object** keyed by job family replaces `FlinkJobFamilyActor` - the
  single-writer guarantee is native, so the `asyncio.Lock` has no counterpart;
* the saga calls that object's handlers **directly**, so there is no equivalent
  of `actor_proxy.py` - which is the single sharpest difference between the two
  engines and the reason the proxy is documented as carefully as it is;
* Restate's own idempotency key replaces `workflow_id:run_id:step`.

`poc/cli.py --engine restate` reports this rather than pretending.
"""

ENGINE = "restate"


def unavailable() -> RuntimeError:
    """The single place that says "not yet", so the CLI and any test agree."""
    return RuntimeError(
        "the Restate adapter is not implemented yet - see issue #11, which is "
        "blocked on the core/adapter split this package exists to make possible"
    )
