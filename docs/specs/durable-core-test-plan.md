---
title: Durable Core Residual-Gap Test Plan
summary: "Executable proof plan for owner-first durable contracts on official OpenClaw 7.1."
read_when:
  - Implementing durable schema, runtime, recovery, API, or CLI changes
  - Reviewing claims about restart, wake, delivery, replay, or compaction safety
  - Preparing durable-core validation evidence
---

# Durable Core Residual-Gap Test Plan

## Proof Principle

Each test must prove a residual invariant without replacing or weakening an
existing 7.1 owner. A green durable test is insufficient if the corresponding
task, flow, subagent, delivery, session, restart, or lease owner contract is not
also preserved.

The implementation baseline is official `release/2026.7.1` at
`0790d9f593ad30c940ed93b5872a8cf6d6f3cf8c` or a documented later correction
commit. Validation after a rebase must run on the exact reviewed head.

## Required Proof Matrix

| Area                   | Required proof                                                                                                                                          |
| ---------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Latest 7.1 baseline    | Ancestry includes the current correction branch, including the progress-reply continuation fix                                                          |
| Disabled no-mutation   | CLI, Gateway, prompt wiring, startup, and workers create no DB/WAL/SHM/schema state when durable runtime is disabled                                    |
| Separate worker gate   | Recording/inspection may be enabled while worker recovery remains off; seeded open rows are not mutated                                                 |
| Authority intake       | Worker-enabled mode fails visibly before acceptance/startup when durable intake is unavailable; observation mode reports degradation                    |
| Owner preservation     | Task, flow, subagent, delivery, restart, session, and lease owner tests remain green; durable code does not overwrite their lifecycle                   |
| Owner adapters         | Each supported owner derives deterministic bounded facts, target resolution, dispatch evidence, and revision-validated decisions                        |
| Canonical schema       | Only justified residual tables exist; broad old names, `source_type`, cleanup ledger, and generic dedupe ledger are absent                              |
| Source contract        | Every execution root, wake, uncertainty, and delivery attempt has a valid source pair or allowed root reason                                            |
| Terminal immutability  | Terminal execution, step, obligation, uncertainty, and attempt rows reject semantic rewrites                                                            |
| Wake producers         | Child terminal/overdue, task lost delivery, restart interruption, no-handler, fan-in, and uncertain effect create exactly one obligation per source key |
| Outbox reconciliation  | Crash between owner commit and obligation insert converges to one obligation or a visible finding without duplicate side effects                        |
| Wake dispatcher        | Due wakes are lease-claimed, attempted with evidence, retried with bounds, suspended after cap, and diagnosed when overdue                              |
| Target resolution      | Resolved, missing, ambiguous, unauthorized, and inspect-only states remain distinct and fail closed                                                     |
| Delivery boundary      | Internal queue/session handoff and external transport evidence are never conflated                                                                      |
| Uncertainty/replay     | Unsafe or unknown side effects create uncertainty and deny replay until every gate passes                                                               |
| Lease authority        | `state_leases` and unique claim tokens prevent stale writers; automatic renewals keep live handlers owned                                               |
| Restart recovery       | Recovery uses restart, owner, and lease evidence before classifying work; no blind replay                                                               |
| Compaction             | Transcript compaction preserves unresolved operational facts and does not ack/supersede work                                                            |
| CLI/Gateway inspection | Bounded obligation/attempt/uncertainty/lease reads are side-effect free and authorized                                                                  |
| Owner controls         | Ack, supersede, resume, and uncertainty resolution are explicit, authorized, revision-guarded, evidence-retaining, and idempotent                       |
| Crash matrix           | Faults after owner/correlation/wake/lease/attempt/dispatch/decision/ACK boundaries converge without silent loss or unsafe replay                        |
| Privacy/retention      | Raw input is opt-in; bounded refs and explanatory terminal evidence survive compaction/retention                                                        |
| Model independence     | Minimal deterministic fixtures prove the contracts without relying on prompt, profile, skill, model family, or context size                             |

## Schema Tests

### Existing owners remain present

Assert the shared state schema still contains:

- `task_runs`
- `task_delivery_state`
- `flow_runs`
- `subagent_runs`
- `delivery_queue_entries`
- `state_leases`
- `gateway_restart_sentinel`
- `gateway_restart_intent`
- `gateway_restart_handoff`
- `gateway_boot_lifecycle`
- `acp_sessions`
- `current_conversation_bindings`

### Residual tables are exact

Assert the durable table set is:

- `durable_execution_records`
- `durable_event_evidence`
- `durable_execution_steps`
- `durable_payload_refs`
- `durable_run_correlations`
- `durable_timer_obligations`
- `durable_signal_evidence`
- `wake_obligations`
- `delivery_attempt_evidence`
- `uncertainty_facts`

Assert `continuation_cleanup`, `dedupe_ledger`, old broad physical names, and
all parent-wake names are absent.

### Source shape

- Execution records require `source_owner` and `source_ref` together or a
  bounded root-operation reason.
- Wake and uncertainty rows require `source_owner` and `source_ref`.
- Delivery attempts require a source pair and obligation link.
- Half-pairs fail before mutation.
- Repeated source/dedupe insertion returns the existing row without changing
  terminal fields.

### Future schema and migration failure

- A future shared OpenClaw state schema version fails before DDL, ALTER,
  backfill, claim, projection, or owner-control mutation.
- Durable core does not add a competing subsystem schema-version ledger.
- Locked DB or migration failure leaves prior rows inspectable and does not
  partially create the residual table set.

### Authority and degraded mode

- Disabled mode performs no DB open or mutation.
- Observation mode may continue upstream behavior after a recording failure but
  emits a bounded degraded-health signal.
- Authority mode rejects intake before acceptance when the durable store cannot
  be opened or its schema cannot be validated.
- Required terminal/handoff failure leaves the canonical owner actionable and
  is recovered by an idempotent source scan.
- No blanket catch converts a required durable failure into success.

## Existing-Owner Compatibility Tests

### Tasks and Task Flow

- Existing task list/show/audit/maintenance/notify/cancel tests remain green.
- Task lifecycle and delivery status are written only through task owner APIs.
- Durable obligation production from a lost or failed-delivery task does not
  change task status.
- Flow revision, wait, blocked, and cancel semantics remain owned by
  `flow_runs`; durable correlation does not reopen or complete a flow.

### Subagents

- Existing spawn, timeout, orphan recovery, final delivery retry, descendant
  settle, result capture, and cleanup tests remain green.
- A terminal subagent source row creates one obligation addressed to the
  requester/controller/report route.
- Re-observation after restart returns the same obligation.
- Pending final delivery remains in `subagent_runs`; durable attempt evidence
  explains each handoff without replacing that state.
- Missing parent execution still produces an inspectable source-backed
  obligation instead of dropping the result.

### Delivery queues

- Existing queue enqueue/recovery/retry tests remain green.
- Queue acceptance finalizes only internal queue evidence.
- A crash after `platform_send_started_at` with no transport result records
  `delivery_unknown`, not delivered.
- An external transport success can finalize external evidence only when the
  channel owner supplies proof.

### Restart and sessions

- Existing restart intent/handoff/sentinel and session compaction tests remain
  green.
- Durable recovery reads those owners and records correlations; it does not
  duplicate their lifecycle.
- A session-owned interrupted run resolves the current canonical session before
  handoff, queues only a bounded uncertainty notice, and records that user
  delivery is not proven.
- Gateway timeout or connection loss under durable authority fails closed; the
  CLI does not start an embedded fallback turn that could repeat side effects.

## Wake Obligation Scenarios

Test reasons:

- `child_terminal`
- `child_overdue`
- `fan_in_incomplete`
- `restart_interrupted`
- `delivery_unknown`
- `side_effect_uncertain`
- `no_handler`
- `operator_requested`

For each reason:

- source owner/ref is present and correct;
- generalized owner/target refs are bounded;
- stable dedupe key prevents duplicate rows;
- target resolution remains inspectable;
- terminal transition rules are enforced;
- repeated worker/source scans do not increment semantic state unexpectedly.

Legal transitions:

- `pending -> delivered | acked | failed | suspended | superseded`
- `delivered -> acked | failed | suspended | superseded`
- `failed -> delivered | acked | suspended | superseded`
- `suspended -> pending | acked | superseded`

Only `acked` and `superseded` are terminal. Illegal transitions and terminal
metadata rewrites return an explicit failure or no-op according to the store
contract. Prove that a `failed` attention attempt can be retried without
creating a duplicate obligation. Prove that `suspended` cannot resume without an
authorized owner decision or a newly reconciled source revision.

## Owner Adapter And Outbox Scenarios

For each supported owner adapter:

- inspect one existing and one missing source ref;
- derive the same attention fact and dedupe key across repeated scans;
- keep lifecycle status in the owner table and never create a mirrored owner
  lifecycle;
- resolve `resolved`, `missing`, `ambiguous`, `unauthorized`, and `inspect_only`
  distinctly;
- inject a crash after owner commit and before wake insert, then prove the next
  scan creates exactly one wake;
- inject a crash after wake insert and before producer return, then prove replay
  returns the existing wake;
- reject a stale owner revision before applying a decision;
- preserve a visible reconciliation finding when adaptation or target resolution
  cannot complete.

Subagent proof must query canonical `subagent_runs` directly through its owner
adapter. Durable execution records may correlate a genuine parent continuation,
but must not decide child running/terminal/lost state.

## Obligation Dispatcher And SLA Scenarios

- A pending wake is claimed by one unique `state_leases` token.
- Two workers racing the same wake produce one active claim and one attempt.
- A long owner dispatch renews the same wake lease and projected attempt expiry.
- Lease expiry permits a new unique attempt token; stale-token finalization
  fails.
- Resolved internal handoff records attempted/delivered evidence without claiming
  owner ACK or external delivery.
- Missing/ambiguous/unauthorized/inspect-only targets suspend or remain
  decision-required without dispatch.
- Transient failure uses bounded exponential backoff and does not busy-loop.
- Retry cap or deadline moves the wake to `suspended` and emits an overdue
  diagnostic.
- Reconciliation of a changed canonical source revision may requeue a suspended
  wake exactly once.
- Restart during dispatch records unknown evidence when the proof boundary is
  ambiguous and creates/retains uncertainty.

## Uncertainty And Replay Scenarios

Inject failure before and after:

- tool dispatch;
- provider return;
- child spawn;
- local commit;
- internal queue handoff;
- external channel send.

Expected results:

- before proven dispatch, operation policy may permit a normal retry;
- after possible dispatch, record `unknown_after_side_effect`,
  `lost_after_dispatch`, or `delivery_unknown`;
- create an owner obligation when a decision or notification is required;
- deny replay when operation registration, authority, retained input,
  idempotency, reconciliation/CAS, owner state, target permission, or audit is
  missing;
- allow replay only in a test where every gate is explicitly satisfied;
- repeated uncertainty observation dedupes and resolution is immutable.

## Claim And Lease Scenarios

- Claiming a step creates/updates one `state_leases` row with a unique attempt
  token and matching projected claim fields.
- Automatic renewal occurs while a long handler promise is active even if the
  handler never calls its progress callback.
- Explicit progress heartbeat records evidence and renews the same lease.
- A replacement token causes all writes from the stale token to fail.
- An expired lease is inspectable before reclaim.
- Reclaim is allowed only when operation replay gates pass.
- Unknown side-effect reclaim creates uncertainty and an obligation instead of
  executing.
- Stopping the worker releases or expires claims without changing owner
  lifecycle state.

## Restart Scenarios

Seed combinations of:

- open execution with live/unexpired lease;
- open execution with expired lease;
- source task/subagent already terminal;
- source subagent still active in its canonical owner;
- dispatch evidence with unknown result;
- pending owner obligation already present;
- no source owner row.

On gateway startup:

- worker-off performs no recovery mutation;
- worker-on classifies from restart, owner, and lease evidence;
- live canonical owner state is not blindly marked lost;
- ambiguous dispatch becomes uncertainty;
- terminal source requiring attention creates one wake obligation;
- repeated startup is idempotent;
- no automatic external send or unsafe replay is claimed.
- a live gateway restart during a long tool call closes the caller visibly,
  classifies the accepted run as lost/uncertain, hands an attention notice to
  the session owner, and never runs embedded fallback.

## Context Overflow And Compaction

- Existing overflow compaction/truncation/continuation retry tests stay green.
- A single input larger than the effective model context returns a visible
  `blocked` result, records the durable turn as `failed/terminal` with
  `agent.turn.blocked`, leaves no open run, and permits a later normal turn.
- Seed an open child correlation, pending wake, open uncertainty, and delivery
  attempt before compaction.
- Run compaction and retention.
- Manual compaction without an explicit retained-tail setting must summarize
  real conversation content; an empty `<conversation>` summary is a failure,
  not a successful checkpoint.
- Assert source refs, statuses, dedupe keys, target resolution, attempt outcome,
  and explanatory terminal events remain inspectable.
- Assert compaction does not acknowledge, supersede, deliver, or replay.
- Assert sensitive previews can be removed while hashes/refs and recovery
  explanation remain.
- Continue the same session after compaction and prove that exact bounded marker
  facts from the summary and retained tail remain available.

## CLI Contract

Required read commands:

- `openclaw durable obligations list`
- `openclaw durable wakes list`
- `openclaw durable wakes inspect <wake-id>`
- `openclaw durable uncertainty list`
- `openclaw durable delivery-attempts list <wake-id>`
- execution-record/timeline/source-ref inspection

Tests assert:

- text and JSON vocabulary matches the architecture;
- `sourceOwner` and `sourceRef` are visible in JSON;
- disabled mode exits before DB creation;
- invalid arguments fail before state access;
- read commands do not claim, enqueue, ack, replay, migrate, or mutate;
- output provides safe next inspection commands without presenting unsafe
  controls as automatic actions.

Owner-control CLI tests must prove explicit caller/authority input, audit write,
idempotency, and terminal guards.

## Gateway Contract

Required operator-read methods:

- `durable.obligations.list`
- `durable.wakes.list`
- `durable.wakes.inspect`
- `durable.uncertainty.list`
- `durable.delivery-attempts.list`
- existing coordination/execution inspection as retained

Required owner/controller mutation methods may be added only with explicit auth
proof. Tests cover method scope, protocol validation, disabled no-mutation,
invalid-param no-mutation, bounded projections, and no worker side effects.

Required additive mutations are:

- `durable.wakes.acknowledge`
- `durable.wakes.supersede`
- `durable.wakes.resume`
- `durable.uncertainty.resolve`

Mutation tests prove caller identity, authorization source, source revision,
idempotency key, owner validation, audit reference, and stale/terminal guards.

Protocol schema/type/validator exports and method descriptors must be complete;
dead schemas without handlers do not satisfy the contract.

## Naming Guard

Static checks reject:

- `ParentWake`
- `DurableRuntimeParentWake`
- `parent_wake`
- `durable_runtime_parent_wakes`
- `WakeDeliveryAttempt`
- `SideEffectUncertaintyFact`
- `requires_parent_decision`
- durable storage/API use of `source_type` or `sourceType`
- old broad physical table names after the unshipped rename
- `continuation_cleanup` and `dedupe_ledger`

Explicit historical analysis may mention rejected terms only in a clearly
non-contract section and must not be included in generated/runtime artifacts.

## Validation Commands

At minimum run on the exact head:

```bash
node scripts/check-durable-canonical-naming.mjs
pnpm db:kysely:gen
pnpm test src/durable/sqlite-store.test.ts
pnpm test src/durable/executor.test.ts
pnpm test src/durable/recovery.test.ts
pnpm test src/durable/subagent.test.ts
pnpm test src/durable/worker.test.ts
pnpm test src/commands/durable.test.ts
pnpm test src/gateway/server-methods/durable.test.ts
pnpm docs:map:check
git diff --check
```

Also run focused existing-owner suites for every touched owner. A runtime or
delivery claim requires direct live proof only when deterministic local tests
cannot prove the claimed boundary.

Run the crash matrix at owner fact, correlation, wake, lease, attempt, dispatch
proof, owner decision, and acknowledgement boundaries. At least one scenario
must use a deliberately tiny model-independent fixture so passing proof cannot
depend on model reasoning or context capacity.

## Completion Gate

Durable core is complete only when:

- the schema matches the owner map exactly;
- all persistent residual facts are source-backed;
- existing owners remain canonical and their suites pass;
- wake, evidence, uncertainty, lease, restart, and compaction scenarios pass;
- CLI/Gateway read surfaces are complete and side-effect free;
- owner controls are authorized, revision-guarded, and retain decision evidence;
- shared audit-store integration is complete before broad administrative
  controls are claimed;
- worker-enabled authority cannot accept untracked work;
- every actionable owner fact produces one wake or visible reconciliation
  finding;
- every wake is attempted, suspended, acknowledged, superseded, or overdue
  within the configured SLA;
- no test or document overstates external delivery or arbitrary replay.

## Related

- [Upstream 7.1 Gap Analysis](/specs/durable-core-upstream-7.1-gap-analysis)
- [Durable Core Architecture](/specs/durable-core-architecture)
- [Missing-Fact Owner Map](/specs/durable-core-missing-fact-owner-map)
