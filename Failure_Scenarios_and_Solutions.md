
# User Prompt
This has been generated from a brand new conversation using a custom generic purpose "Basic" Gem, no extra context was given.

<details>
<summary>Click to expand "Basic" Gem</summary>

Technical details:
- Only add code if requested
- Prioritize readable code
- Use the type system to add a layer of communication and readability
- Be mindful of security and performance
- Abbreviated wery well know abbreviations such API for application programming interface. Avoid abbreviations that can me misleading
- Do not add emojis to the code
- When generating python code, use syntax for version 3.13+

Social details:
- **Writes concise responses.**
- **Skip compliments**
- **Gets direct to the point**
- **Do not use emojis**
- **Have opinions.** React to facts instead of neutrally listing pros and cons.
- **Vary rhythm.** Short sentences. Then longer ones that take their time. Mix it up.
- **Acknowledge complexity.** "Impressive but also kind of unsettling" beats "impressive."
- **Use "I" when it fits.** First person isn't unprofessional.
- **Let some mess in.** Perfect structure looks machine-made.
- **Be specific.** Not "this is concerning" but "there's something unsettling about agents churning away at 3am."

Drop: filler (just/really/basically/actually/simply), pleasantries (sure/certainly/of course/happy to), hedging. Fragments OK. Short synonyms (big not extensive, fix not "implement a solution for"), Puffery ("pivotal moment", "testament to", "evolving landscape"), AI vocabulary (Additionally/crucial/delve/enduring/enhance/fostering/garner)
Technical terms exact. Code blocks unchanged. 

</details>

<details>
<summary>Click to expand prompt</summary>

I have a highly distrubuted system running flink jobs processing multiple data sources and writing to multiple iceberg tables. I am developing a control plane to manage those resources, one of the key features that I want to focuses here is the need to move data sources between jobs.
For this to be achieved safely, without data loss or messing up the system I need to guarantee the following workflow steps happen on this exact order:
- all participating jobs are suspended
- all participating jobs are updated
- all participating jobs are resumed

if any of those operations fails I need to retry and I cant left the system inconsistent and need to restore it to a previous state. Many workflows can run at the same time targeting the same jobs, which can be a problem as many steps should be executed and mixing them can cause problems

What are the problems that I need to be aware of and how do I tackle them? 

I need a comprehensive markdown file with a list of scenarios with their assumptions, example problem and solution. Use the move data source for the example
I need all the cases, starting from the very naivy happy path, a scenario that only a single process will be ever running at a time and nothing can possibly fail.
Progress to more complex scenario, adding network failures, request that get to the destination but the responses are unprocessable. finally add multiple workflows running at the same time, trying to modify the same jobs. 

each section should build another layer of failure and assume that the previous solution has already been applied
</details>

---

## Gemini Response

## Data Source Migration Workflow: Failure Scenarios and Solutions

This document outlines scenarios for moving data sources between Flink jobs writing to Iceberg tables. The workflow requires suspending, updating, and resuming jobs.

### Scenario 1: Naive Happy Path

**Assumptions:**
- Single execution thread globally.
- Network is 100% reliable.
- Control plane and Flink APIs process requests instantaneously and never fail.
- No system crashes.

**Example Problem:**
Move data source `S1` from `JobA` to `JobB`.

**Solution:**
Sequential, blocking API calls.
1. Suspend `JobA`.
2. Suspend `JobB`.
3. Update `JobA` config (remove `S1`).
4. Update `JobB` config (add `S1`).
5. Resume `JobA`.
6. Resume `JobB`.

If any step stops, the system remains in that exact state indefinitely.

---

### Scenario 2: Network Failures and Timeouts

**Assumptions:**
- Scenario 1 solution applied.
- Network packets drop.
- APIs return HTTP 5xx errors or timeout.
- Single execution thread.

**Example Problem:**
Workflow suspends `JobA`. The network request to suspend `JobB` times out. The workflow aborts. `JobA` remains suspended. Data source `S1` is unassigned or partially assigned. System state is inconsistent.

**Solution:**
Introduce idempotency, automatic retries with exponential backoff, and the Saga pattern for rollbacks.

1. **Idempotency:** All API endpoints must accept an `Idempotency-Key` header. Repeated calls with the same key yield the same outcome without duplicate side effects.
2. **Retries:** Wrap network calls in retry logic.
3. **Rollback (Saga):** Maintain a log of applied operations. If the update phase fails exhaustively, execute compensating transactions in reverse order (e.g., revert job configs, resume jobs).


---

### Scenario 3: Unprocessable Responses (State Divergence)

**Assumptions:**
- Scenario 2 solution applied.
- Network fails after the server processes the request but before the client receives the response.
- Client receives corrupted data.

**Example Problem:**
Client issues `POST /jobs/JobB/suspend`. `JobB` suspends. Response drops. Client retries. The server rejects the retry because `JobB` is already suspended, but returns a generic 400 error. The workflow interprets this as a hard failure and triggers a rollback, reverting `JobA` and leaving the system thrashing.

**Solution:**
State reconciliation via explicit querying and declarative updates.

1. **Query Before Retry:** Before applying a mutating action, poll the target resource state (`GET /jobs/JobB`).
2. **Conditional Execution:** If the desired state is already met (e.g., `JobB.status == SUSPENDED`), treat the step as successful and proceed.
3. **ETags / Versioning:** Use HTTP ETags or resource versioning for updates to ensure the client mutates the exact state it expects.


---

### Scenario 4: Concurrent Workflows (Race Conditions)

**Assumptions:**
- Scenario 3 solution applied.
- Multiple control plane instances run simultaneously.
- Concurrent workflows target overlapping resources.

**Example Problem:**
Workflow W1 moves `S1` from `JobA` to `JobB`.
Workflow W2 moves `S2` from `JobA` to `JobC`.
W1 and W2 both verify `JobA` is running, then both issue suspend commands. Both read `JobA` config. W1 removes `S1`, W2 removes `S2`. W2 writes its config. W1 writes its config, overwriting W2's change. `S2` is lost. W1 resumes `JobA` before W2 finishes updating `JobC`.

**Solution:**
Distributed locking and Optimistic Concurrency Control (OCC).

1. **Distributed Locks:** Acquire locks on all participating resources (`JobA`, `JobB`) via Redis, ZooKeeper, or etcd before initiating the workflow.
2. **Deadlock Prevention:** Sort resource IDs lexically before acquiring locks to prevent deadlocks (e.g., always lock `JobA` before `JobB`).
3. **OCC:** Use resource versions fetched during lock acquisition. If the underlying resource version increments via external intervention during the workflow, the API rejects the update (409 Conflict). The workflow releases locks and restarts.
