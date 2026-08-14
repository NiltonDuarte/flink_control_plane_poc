"""Engine adapters: one package per durable-execution engine.

An adapter supplies exactly four things to `poc/core/`, and they are the four
things that differ between engines:

* **execution** - how a yielded operation actually runs (an activity, a durable
  call, a plain function);
* **idempotency-key derivation** - what makes a retried call attach to the
  original attempt rather than issuing the command twice;
* **retry configuration** - which errors are retried and how many times;
* **the single-writer mechanism** for a job family.

Everything else is shared. `driver.py` in this package is the loop that runs a
core generator against an adapter's dispatch function; it is engine-agnostic but
async, which is why it lives here rather than in `poc/core/`.
"""
