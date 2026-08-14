"""The engine-agnostic core: what a "move datatype" *means*, and nothing else.

Everything in this package is **synchronous**. Not as a style preference - it is
the enforcement mechanism. The saga and the family handlers are generators: they
`yield` the operation they want performed and receive its result back, so they
cannot await, cannot open a socket, and cannot import an engine SDK. An adapter
supplies the execution (see `poc/adapters/driver.py`).

That buys three things:

1. **One domain implementation, two engines.** The whole point of the exercise -
   comparing Temporal against Restate is only meaningful if both are running the
   same saga, not two sagas that were written to look alike.
2. **The engine boundary is visible.** What is genuinely engine-specific ends up
   in an adapter because it has nowhere else to go: retry policies, idempotency
   key derivation, the single-writer mechanism.
3. **The compensation matrix is testable with no engine at all.** Drive the
   generator with canned results and assert the exact command sequence, LIFO
   unwind included, in milliseconds - see `tests/test_core_saga.py`.

`tests/test_core_is_engine_free.py` enforces the rule mechanically.
"""
