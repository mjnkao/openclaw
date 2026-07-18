---
title: Durable Core Residual-Gap Architecture
summary: "Owner-first architecture for durable execution, attention, delivery evidence, uncertainty, and recovery on official OpenClaw 7.1."
read_when:
  - Implementing or reviewing durable runtime behavior
  - Adding durable schema, API, CLI, worker, or recovery code
  - Deciding whether an existing owner or durable core should persist a fact
---

# Durable Core Residual-Gap Architecture

## Status

This document is the implementation architecture for the local 7.1 durable-core
refactor. It is validated against official `release/2026.7.1` at
`0790d9f593ad30c940ed93b5872a8cf6d6f3cf8c` and the companion upstream gap
analysis. The implementation has been semantically rebased onto that correction;
this does not make current code a shipped contract or claim exactly-once external
delivery or arbitrary semantic replay.

## Architecture Decision

Durable core is an **owner-first cross-owner contract layer**, not a replacement
lifecycle ledger and not a general workflow engine.

Existing owners remain authoritative for facts they already persist:

- `task_runs` and `task_delivery_state` own background-task lifecycle and task
  notification state;
- `flow_runs` owns Task Flow lifecycle, revisions, waits, and blocked state;
- `subagent_runs` owns subagent lifecycle, timeout, result capture, cleanup, and
  final-delivery retry state;
- `delivery_queue_entries` owns internal and outbound queue lifecycle;
- `state_leases` owns claim, heartbeat, expiry, and stale-owner authority;
- restart, ACP, session, conversation-binding, audit, and diagnostic owners keep
  their current authority.

Durable core adds only facts that no one owner can safely own:

- source-backed wake/attention obligations;
- delivery-attempt evidence with explicit proof boundaries;
- uncertainty after a possible side effect;
- generic execution/checkpoint evidence for work that has no existing lifecycle
  owner;
- cross-owner correlations and bounded payload refs;
- side-effect-free inspection and audited owner decisions.

## User Promise

When OpenClaw accepts long-running or delegated work, it must remain possible to
answer:

- what owner accepted the work;
- what is active, waiting, terminal, stale, lost, or uncertain;
- which owner or report route must be notified;
- what handoff or delivery was attempted and what was actually proven;
- whether replay is safe, denied, or requires an owner decision;
- what the operator can inspect without mutating work.

Durable core does not promise exactly-once external effects, automatic semantic
resume of arbitrary model/tool work, or external delivery without transport
evidence.

## Longevity Boundary

Durability is a runtime property, not a model capability. Larger context windows,
better planning, and more reliable tool use may reduce failures, but they do not
remove process crashes, gateway replacement, transport ambiguity, lease expiry,
permission changes, provider outages, or uncertain side effects.

The architecture therefore treats the model as a replaceable producer and
consumer of owner facts. No correctness invariant depends on prompt wording,
context capacity, a specific model family, a profile, or a skill. Future models
may choose better owner decisions, but they must use the same inspected facts,
authorized controls, idempotency keys, and evidence boundaries.

This keeps the core upstream-friendly:

- generic owner, source, obligation, attempt, uncertainty, lease, and decision
  vocabulary instead of provider- or project-specific names;
- additive Gateway protocol surfaces and no new permanent environment variable
  for each policy choice;
- owner adapters that preserve existing OpenClaw lifecycle tables;
- no Workboard, profile, skill, channel, or model dependency in the correctness
  path.

## Layer Model

| Layer                  | Responsibility                                                                                | Forbidden responsibility                                      |
| ---------------------- | --------------------------------------------------------------------------------------------- | ------------------------------------------------------------- |
| Canonical owner        | Own lifecycle, policy, and owner-specific reconciliation                                      | Writing another owner's lifecycle                             |
| Durable contract layer | Record source refs, evidence, correlations, obligations, uncertainty, and generic checkpoints | Becoming the canonical task/flow/subagent/delivery lifecycle  |
| Recovery worker        | Reconcile expired leases and source-backed facts; fail closed                                 | Making semantic owner decisions or claiming external delivery |
| Projection/API         | Explain bounded state and safe next actions                                                   | Claiming, replaying, acknowledging, or migrating during reads |
| Product/agent policy   | Decide retry, resume, abandon, wait, or user communication                                    | Rewriting durable evidence to fit a desired answer            |

## Authority Modes

Durable recording and durable authority are distinct modes while the feature is
unshipped:

- **disabled** preserves official behavior and performs no durable DB open,
  migration, recording, worker, or projection work;
- **observation** may record bounded best-effort evidence for rollout, but must
  not advertise a no-silence guarantee;
- **authority** requires durable intake before accepted work is reported,
  source-backed terminal handoff or a visible blocked state, and an active
  obligation reconciliation/dispatch path.

Worker-enabled durable mode is authority mode. Required writes in authority mode
must never be hidden by blanket exception handling. Before acceptance, durable
storage failure fails the request or startup visibly. After acceptance, a write
failure leaves the canonical owner pending/blocked and produces a health finding
that reconciliation can act on. Observation-only write failures are logged and
counted as degraded evidence, not silently treated as success.

No new mode table or configuration key is required. The existing recording and
worker gates define the rollout modes until a normal OpenClaw config surface
replaces the temporary environment gates.

## Canonical Owner Adapter

Durable core consumes existing lifecycle facts through narrow owner adapters.
An adapter moves real responsibility: it inspects canonical facts, derives
attention obligations, resolves authorized targets, dispatches attention through
the owner route, and applies owner-validated decisions. It is not a field-renaming
or lifecycle-mirroring layer.

```ts
type DurableOwnerAdapter = {
  sourceOwner: string;
  inspect(sourceRef: string): OwnerFact | undefined;
  listAttentionFacts(options?: { limit?: number; now?: number }): OwnerFact[];
  dispatchAttention(input: DispatchInput): Promise<DispatchEvidence>;
};
```

The implemented initial adapters cover `subagent_runs`, `task_runs`, and
`session_store`; the task adapter reads `task_delivery_state` through the
existing task owner API. The session adapter resolves the current canonical
session row and hands a bounded internal notice to the existing persisted
session-delivery queue before the system-event and heartbeat front door. The
queue entry uses a stable idempotency key and is generation-fenced by default.
Its evidence distinguishes queue acceptance, attached-session consumption, and
external delivery; none implies another. `flow_runs`, `delivery_queue_entries`,
restart/boot, and ACP owners remain explicit extension points. Until those
adapters exist, their rows remain available to bounded inspection projections
but cannot be dispatched by the wake worker. Adapter outputs must be bounded,
deterministic, source-backed, and idempotent. Generic execution records are used
only when the operation genuinely has no existing lifecycle owner.

An owner front door is the narrow API that is solely allowed to mutate or
deliver for that lifecycle. It is not a new service and it is not direct table
access from durable core. The initial concrete front doors are:

| Canonical owner                       | Front door                                                     | Durable integration                                               |
| ------------------------------------- | -------------------------------------------------------------- | ----------------------------------------------------------------- |
| `subagent_runs`                       | subagent registry progress/completion delivery APIs            | Implemented                                                       |
| `task_runs` and `task_delivery_state` | task registry and `requestTaskAttentionDelivery`               | Implemented                                                       |
| `session_store`                       | persisted session-delivery queue, system events, and heartbeat | Partial: in-memory handoff must be replaced before release        |
| `flow_runs`                           | flow continue/cancel/wait owner APIs                           | Planned adapter                                                   |
| `delivery_queue_entries`              | queue claim/reconcile/ack/fail APIs                            | Planned adapter                                                   |
| restart/boot owners                   | restart handoff, sentinel, and startup reconciliation APIs     | Generic startup classification implemented; owner adapter planned |
| ACP owners                            | ACP session/control/replay APIs with current binding revision  | Planned adapter                                                   |
| `state_leases`                        | lease claim/renew/release APIs                                 | Reused as authority; no duplicate lifecycle                       |

"All OpenClaw owners/front doors" means inventorying and integrating every
canonical lifecycle that can leave unresolved work or attention. It does not
mean replacing all OpenClaw tables or routing every subsystem through a new
durable service.

## Source Contract

Every persistent durable fact must have one of these source shapes:

1. `source_owner` plus `source_ref`, naming the canonical table/service and its
   stable key; or
2. for a true root generic execution only, a bounded `root_operation_reason`
   that explains why no owner exists yet.

Child records reached through a foreign key to a source-backed execution record
inherit that source. Standalone obligations and uncertainty facts always carry
their own source owner/ref. Missing or half-populated source pairs fail before
mutation.

Canonical examples:

| Fact                        | `source_owner`                                                    | `source_ref`                           |
| --------------------------- | ----------------------------------------------------------------- | -------------------------------------- |
| Subagent terminal attention | `subagent_runs`                                                   | subagent `run_id`                      |
| Task lost attention         | `task_runs`                                                       | `task_id`                              |
| Queue delivery evidence     | `delivery_queue_entries`                                          | `<queue_name>:<id>`                    |
| Gateway restart recovery    | `gateway_restart_handoff` or `gateway_boot_lifecycle`             | handoff/boot key                       |
| Generic interactive turn    | session owner when available, otherwise documented root operation | stable session/turn ref or root reason |

## Canonical Schema

### Reused tables

The owner tables listed above are reused directly. Durable code may read their
bounded facts and may call their owner APIs. It must not duplicate their
lifecycle state into a new canonical row.

### New residual tables

| Table                       | Role                                                      |
| --------------------------- | --------------------------------------------------------- |
| `durable_execution_records` | Generic accepted execution evidence and checkpoint anchor |
| `durable_event_evidence`    | Ordered, append-only execution/recovery evidence          |
| `durable_execution_steps`   | Generic step/checkpoint/replay boundary                   |
| `durable_payload_refs`      | Bounded input/output/error/checkpoint/artifact refs       |
| `durable_run_correlations`  | Parent/child and cross-owner correlations                 |
| `durable_timer_obligations` | Generic retry/deadline/sleep obligations only             |
| `durable_signal_evidence`   | Generic approval/input/callback evidence                  |
| `wake_obligations`          | General owner/report-route attention contract             |
| `delivery_attempt_evidence` | Per-attempt internal/external delivery proof              |
| `uncertainty_facts`         | Ambiguous outcome and decision-needed facts               |

No separate `dedupe_ledger` is created while unique keys on these records are
sufficient. Cleanup belongs to the owner of the affected timer, signal,
correlation, payload ref, or obligation and is represented by owner state plus
event/audit evidence rather than a generic cleanup table.

## Wake Obligation

`WakeObligation` is the one canonical attention contract. “Parent wake” is a
producer case, not a type or storage name. `AttentionObligation` may be used in
explanatory prose but is not a second public API.

An obligation includes:

- stable id and source owner/ref;
- generalized owner and target kind/ref;
- optional report route and execution correlation;
- reason and bounded facts ref;
- target-resolution status and reason;
- stable dedupe key;
- status, attempt summary, acknowledgement/failure fields, timestamps, and
  bounded metadata.

Legal status transitions:

- `pending -> handoff_accepted | acked | failed | suspended | superseded`
- `handoff_accepted -> acked | failed | suspended | superseded`
- `failed -> handoff_accepted | acked | suspended | superseded`
- `suspended -> pending | acked | superseded`

Only `acked` and `superseded` are terminal and immutable. `failed` remains
automatically retryable within policy. `suspended` means automatic dispatch has
stopped after a deadline, retry cap, unresolved target, or decision requirement;
only an authorized owner decision or a newly reconciled canonical fact may move
it back to `pending`. Repeated source observation returns the existing obligation
by dedupe key. A missing, ambiguous, unauthorized, or inspect-only target remains
inspectable and must not be normalized to `handoff_accepted` or `acked`.
`handoff_accepted` names only the exact internal owner/queue boundary retained in
attempt evidence. It is not attached-session consumption, end-user delivery, or
external transport success.

Required producers include:

- terminal or overdue subagent requiring requester/controller attention;
- fan-in unable to settle;
- task lost or delivery failure requiring owner attention;
- gateway restart/interruption with unresolved work;
- no registered handler;
- uncertain side effect or unknown delivery;
- explicit operator request.

## Obligation Outbox And Reconciliation

`wake_obligations` is the transactional attention outbox for cross-owner work.
For every canonical source transition that requires attention, one of these must
hold:

1. the owner mutation and deduplicated obligation are committed in the same
   shared-state transaction; or
2. a bounded owner-adapter scan can deterministically derive and create the
   missing obligation from the committed source fact.

The second rule is required for owners whose existing API cannot join the same
transaction. Reconciliation may create correlations, bounded refs, obligations,
attempt evidence, uncertainty, and diagnostics. It must not fabricate external
delivery, owner acknowledgement, or a safe retry decision.

Every producer is at-least-once and every durable write is idempotent. The
dedupe key is derived from immutable source identity plus the owner revision or
attention reason. Reconciliation findings are visible when a source fact cannot
be adapted or targeted; missing facts must never disappear as a successful
no-op.

## Obligation Dispatcher And No-Silence SLA

The Gateway recovery loop owns a general obligation dispatcher. Each pass:

1. reconciles canonical owner facts into deduplicated obligations;
2. selects pending and retryable failed obligations whose retry time is due;
3. claims `state_leases(scope = "wake_obligation", lease_key = wake_id)` with a
   unique attempt token;
4. renews the same lease and projected attempt expiry while owner dispatch is
   active;
5. resolves and dispatches through the source owner adapter;
6. records one `DeliveryAttemptEvidence` with the exact proof boundary;
7. marks the obligation handoff-accepted, failed, suspended, acknowledged, or
   superseded according to evidence and owner policy;
8. emits a diagnostic when obligation age exceeds the no-silence SLA.

Retries use bounded exponential backoff with jitter. Retry count, elapsed age,
target resolution, last evidence, and next safe action are inspectable. A stalled
obligation cannot remain silently pending forever: it becomes handoff-accepted,
acknowledged, suspended, superseded, or overdue and visible to its owner and
operator surfaces.

`state_leases` is the sole dispatch-claim authority. Any claim columns on
delivery-attempt rows are projections of the same unique lease token, never an
independent lock.

## Delivery Evidence

Delivery evidence records what one attempt proves. It does not replace
`delivery_queue_entries` and does not turn internal handoff into external
delivery.

Evidence boundaries include and must be named separately:

- obligation selected for attempt;
- persisted internal queue accepted;
- target session generation validated;
- attached session consumed the notice;
- external transport accepted when the channel provides direct proof;
- failed before dispatch;
- outcome unknown after dispatch;
- superseded before or after attempt.

Each attempt links to its obligation, source owner/ref, target/route, optional
queue ref, worker claim, timestamps, evidence, and error. Attempts are
idempotent by a stable key but remain auditable.

## Uncertainty And Replay

Crashes can occur before or after tool dispatch, provider return, child spawn,
local commit, queue handoff, or channel send. If the side effect boundary cannot
be proven, durable core records an `UncertaintyFact` and creates an owner
obligation when action is required.

Canonical uncertainty kinds are generalized:

- `unknown_after_side_effect`
- `interrupted_during_tool`
- `lost_after_dispatch`
- `delivery_unknown`
- `requires_owner_decision`

Automatic replay is allowed only when all gates pass:

- registered operation and version;
- replay authority for the worker/caller;
- retained and authorized input material;
- side-effect class permits replay;
- idempotency key or reconciliation/CAS proof;
- source owner still permits the transition;
- target authorization still resolves;
- audit evidence is written.

Any missing gate fails closed and preserves uncertainty.

## Claims And Leases

`state_leases` is the authority for durable worker claims. Step claim columns
are indexed projections of the same lease. Execution-record heartbeat is
liveness evidence, not claim authority. There is no run-level claim API because
the worker schedules steps. A claim uses a unique attempt token, not a reusable
worker name.

The worker must:

- acquire the source/step lease and projected claim atomically where possible;
- renew the lease automatically while a handler promise is active;
- allow explicit handler heartbeat to add progress evidence, not to be the only
  lease-renewal mechanism;
- reject stale-token writes after expiry or replacement;
- reconcile expired leases through the operation registry before reclaim;
- create uncertainty and owner attention when safe reclaim cannot be proven.

## Restart And Compaction

Gateway restart recovery starts from `gateway_restart_*`, boot lifecycle,
canonical owner state, and `state_leases`. It must not mark a source lifecycle
terminal solely because a new process started. It classifies each execution as
recoverable, stale, lost, unknown after side effect, or owner-decision-needed.

Context compaction owns transcript transformation only. Durable source refs,
correlations, unresolved obligations, attempt evidence, uncertainty, and lease
state remain in SQLite independently of transcript summaries. A successful
compaction does not acknowledge or supersede pending work.

Agent turns whose normalized liveness is `blocked` or `abandoned` are terminal
failures even when the Gateway RPC itself completed normally. The execution
timeline records `agent.turn.blocked` or `agent.turn.abandoned`; it must not
record `agent.turn.succeeded` merely because a user-facing diagnostic payload
was returned.

Manual session compaction is a hard checkpoint when no explicit
`keepRecentTokens` value exists. The retained-tail setting must be applied
before summarization so a small session cannot send an empty conversation to
the summarizer and persist a fabricated empty-session summary. Explicit
operator retention settings continue to win.

## Result Handoff And Fan-In

`subagent_runs` remains the child lifecycle and result owner. Durable
correlations link the child to a parent execution or report route. The frozen
result is referenced through a bounded payload ref. Parent attention is a
`WakeObligation`; delivery is `DeliveryAttemptEvidence`.

There is no independent result-mailbox lifecycle competing with the obligation.
Fan-in consumes source/correlation facts and may update its own generic step, but
cannot acknowledge the owner's obligation on the owner's behalf.

## API And Authority

Read-only Gateway and CLI surfaces expose execution records, source refs,
obligations, target resolution, delivery attempts, uncertainty, leases, and
safe next controls. Reads are side-effect free and open no database when durable
runtime is disabled.

Actor classes:

| Actor         | Default authority                                                                 |
| ------------- | --------------------------------------------------------------------------------- |
| Owner         | Acknowledge/supersede owned obligations and record retry/resume/abandon decisions |
| Requester     | Read bounded state and receive notices; no owner mutation unless delegated        |
| Controller    | Coordinate delegated child/fan-in work and request owner controls                 |
| Operator      | Read diagnosis; explicit evidence-retaining controls require `operator.write`     |
| System worker | Lease-backed reconciliation and source-backed fact production                     |
| Admin         | Explicit audited maintenance and migration, not silent owner semantics            |

All mutation methods require explicit caller identity, authorization source,
idempotency, terminal guards, and retained decision evidence. Broad
administrative controls additionally require integration with the shared audit
store before release.

### Owner Decision Protocol

Runtime observation and semantic decisions remain separate:

```text
observe source -> notify owner -> inspect facts -> authorize decision
  -> owner adapter validates current revision -> mutate owner -> audit outcome
```

The full owner-decision vocabulary may include `acknowledge`, `supersede`,
`retry`, `resume`, `continue_partial`, `abandon`, `wait`, `ask_human`, and
`mark_reconciled`. The initial public slice is inspection-only. Store-internal
decision primitives do not create a supported Gateway or CLI mutation contract.

Any later owner-control PR must carry caller identity, authorization source,
source revision, idempotency key, reason, bounded evidence, and shared audit
integration. Retry and resume must call the owning API through an adapter after
authorization and revision validation; they never rewrite a source lifecycle
row directly. A dedicated decision table is not justified until existing owner
and audit evidence paths prove insufficient.

## Atomicity, Replay, And Crash Matrix

Implementation and release tests must cover crashes after every boundary in
this sequence:

```text
owner fact -> correlation -> obligation -> lease -> attempt -> dispatch proof
  -> owner continuation/decision -> acknowledgement
```

For each boundary, restart must converge to exactly one of:

- the next safe idempotent action;
- an inspectable pending or suspended obligation;
- an open uncertainty fact requiring an owner decision;
- an immutable acknowledged or superseded terminal record.

No recovery path may infer external delivery from queue acceptance, infer owner
acknowledgement from internal handoff, or replay a possibly completed side effect
without an idempotency/reconciliation proof. This contract remains necessary
regardless of model quality because these are distributed-systems failure
boundaries outside the model context.

## Configuration

Durable recording and durable worker execution remain separately gated while
the feature is unshipped. The final upstream configuration should prefer the
normal OpenClaw config/doctor surface over permanent environment-variable
growth. The disabled path must preserve current synchronous behavior and must
not create or migrate SQLite state.

## Invariants

- Existing owners remain canonical.
- Every residual fact is source-backed.
- Terminal execution, step, obligation, attempt, and uncertainty states are
  immutable except retention metadata where explicitly allowed.
- Read paths never mutate.
- Unknown/future schema fails closed before mutation.
- Recovery appends evidence and never silently rewrites history.
- Internal handoff never implies external delivery.
- Unsafe replay becomes uncertainty plus owner attention.
- Full payload capture is opt-in; bounded refs and previews are the default.
- Duplicate producer scans are harmless through source-backed unique keys.
- Every actionable source fact converges to one obligation or a visible
  reconciliation finding.
- Every obligation converges within SLA to attempted, suspended, acknowledged,
  superseded, or an overdue diagnostic.
- Model, prompt, profile, skill, and context-window changes cannot bypass owner
  authorization or evidence boundaries.

## Related

- [Upstream 7.1 Gap Analysis](/specs/durable-core-upstream-7.1-gap-analysis)
- [Missing-Fact Owner Map](/specs/durable-core-missing-fact-owner-map)
- [Durable Core Test Plan](/specs/durable-core-test-plan)
