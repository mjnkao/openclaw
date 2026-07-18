---
title: Durable Core Upstream 7.1 Gap Analysis
summary: "Evidence-backed gap analysis between official OpenClaw 7.1 and the residual durable-core behavior required for long-running and multi-agent work."
read_when:
  - Reviewing why OpenClaw needs a residual durable core
  - Deciding whether a durable fact belongs in an existing owner or a new table
  - Auditing durable schema, API, CLI, restart, delivery, or compaction work
---

# Durable Core Upstream 7.1 Gap Analysis

## Decision Summary

OpenClaw 7.1 already has substantial durable behavior. It persists background
tasks, Task Flow state, subagent runs and final-delivery retry state, delivery
queues, session and ACP bindings, process leases, gateway restart handoff, audit
events, and compaction/retry diagnostics. These owners remain authoritative.

The missing capability is not another master run ledger. The residual gap is a
small cross-owner contract layer that can answer four questions after process
loss or an ambiguous handoff:

1. Which existing owner fact requires attention, and from whom?
2. What delivery or handoff was attempted, and what was actually proven?
3. Is replay safe, or is the outcome uncertain after a possible side effect?
4. How are related owner records correlated and inspected without changing
   their lifecycle authority?

The architecture decision is therefore **owner-first durable contracts**:

- extend the existing owner when the fact belongs to one lifecycle;
- add a new record only for source-backed cross-owner evidence, correlation,
  obligation, or uncertainty;
- keep generic durable execution records limited to interactive agent/turn/tool
  work that has no existing lifecycle owner;
- never infer external delivery, safe replay, or owner acknowledgement from an
  internal queue handoff alone.

## Baseline And Evidence

This analysis uses the official `release/2026.7.1` branch at
`0790d9f593ad30c940ed93b5872a8cf6d6f3cf8c` on 2026-07-18. The original
`v2026.7.1` tag peels to `2d2ddc43d0dcf71f31283d780f9fe9ff4cc04fe4`.
The correction branch contains nine later commits, including
`ad807549c61eb2b4d559bfa2482b102142cafc1a`, which fixes Codex turns stopping
after a progress reply. Durable-core work must be based on the correction
branch and must not recreate that provider-specific fix.

The architecture discussion in `openclaw/openclaw#107375` is also part of the
evidence. The linked comment `issuecomment-4993844512` records an interrupted
automated re-review, not architecture approval. The completed review requires a
concrete residual-gap inventory and keeps Task Flow and the other current owners
canonical until maintainers sponsor a new boundary. This document supplies that
missing inventory.

Local source documents reviewed:

- `durable-core-architecture.md`
- `durable-core-test-plan.md`
- `durable-core-missing-fact-owner-map.md`
- generated `docs_map.md`

The generated docs map is an index, not a source of truth.

## Why A Residual Durable Core Is Still Required

OpenClaw 7.1 can keep individual owner records durable while still losing the
meaning of a transition between owners. A subagent may be terminal while its
requester has not consumed the result. A delivery queue may prove enqueueing but
not external transport acceptance. A gateway restart may prove that the old
process died but not whether a dispatched tool or channel side effect completed.
A compacted session may preserve conversation text but lose the operational
reason a parent was waiting for a child.

Prompt instructions, progress messages, and in-memory callbacks cannot close
these gaps because they disappear with the process and cannot be inspected or
deduplicated later. Existing owner tables also cannot safely write each other's
lifecycle state. The residual durable core is needed to persist the contract
between owners while leaving each owner's lifecycle authoritative.

This distinction separates two kinds of silence:

- **Provider/runtime silence already handled locally:** idle watches, progress
  replies, turn completion, bounded compaction retries, task reconciliation,
  subagent orphan recovery, and queue recovery belong to their current owners.
- **Cross-owner silence still unhandled:** no durable attention obligation,
  ambiguous result handoff, uncertain side effect, missing owner target, or no
  inspectable proof that a parent/report route was notified.

## Failure-Mode Gap Matrix

| Failure mode                                            | Official 7.1 behavior                                                                                            | Residual gap                                                                                                                       | Required action                                                                                                                |
| ------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------ |
| Long turn emits progress                                | Codex and embedded runtimes have progress watches; the 7.1 correction continues after confirmed progress replies | Other owners still cannot prove a deferred completion obligation                                                                   | Reuse runtime progress logic; create an obligation only when work is intentionally deferred across owner/process boundaries    |
| Gateway restarts during a turn                          | Restart intent, handoff, boot lifecycle, task reconciliation, and session state survive                          | No generic classification of a possibly dispatched side effect; no guaranteed owner attention                                      | Correlate the restart with source work, record uncertainty, and create an owner obligation; never blindly replay               |
| Background task owner disappears                        | `task_runs` can become `lost`; audit and maintenance reconcile by runtime                                        | `lost` does not itself prove the requester was informed or chose a next action                                                     | Keep `task_runs` authoritative; add a deduped wake obligation sourced from the task row when owner attention is required       |
| Subagent hangs or becomes orphaned                      | `subagent_runs` stores timeouts, liveness, orphan recovery, outcome, and pending final delivery                  | Parent/report-route attention is not a general inspectable contract, and announce state can compete with a separate result mailbox | Keep `subagent_runs` authoritative; project terminal/overdue/pending-delivery facts into one wake obligation                   |
| Child finishes repeatedly observed                      | Subagent announce has idempotency and retry state                                                                | Cross-owner parent attention and delivery evidence are not one canonical contract                                                  | Unique source-backed wake key plus append-only delivery-attempt evidence                                                       |
| Internal delivery queue accepts item                    | `delivery_queue_entries` stores retry count, last attempt, error, recovery state, and platform-send start        | Queue acceptance does not prove session consumption or external delivery                                                           | Reuse queue row; record evidence with explicit boundary: queued, handed off, externally accepted, failed, or unknown           |
| Tool/channel side effect may have occurred before crash | Runtime-specific retry logic exists and some operations are idempotent                                           | No shared uncertainty fact and no owner decision gate across operation types                                                       | Add `UncertaintyFact`; require operation registry, idempotency, reconciliation, payload retention, and authority before replay |
| Context overflow or auto compaction                     | Embedded runner retries compaction/truncation and records diagnostics                                            | Pending operational obligations are not a first-class compaction invariant                                                         | Preserve source refs, unresolved obligations, uncertainty, and correlation independently of transcript compaction              |
| Worker handler is missing                               | Current durable executor can mark `unknown_after_side_effect`                                                    | It does not create an uncertainty fact or notify an owner                                                                          | Record uncertainty plus a `no_handler` wake obligation; fail closed                                                            |
| Long handler exceeds claim TTL                          | Durable executor has heartbeat callback and claim columns                                                        | Handler heartbeats are voluntary and do not reliably renew a shared lease; stale owners can race                                   | Use `state_leases` as claim authority, issue unique claim tokens, and renew automatically while the handler is active          |
| Operator needs to diagnose silence                      | Tasks and sessions have separate CLI/API surfaces                                                                | No unified read-only view of obligations, attempts, uncertainty, leases, and source refs                                           | Add bounded durable CLI/Gateway inspection; keep reads side-effect free                                                        |

## Existing Schema Inventory

### Reuse As Canonical Owners

| Existing table/owner                                     | Facts already present                                                                                                               | Decision                                                           |
| -------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------ |
| `task_runs`                                              | Task identity, runtime, owner/requester, parent flow, child session, status, delivery status, progress, terminal outcome, retention | Reuse and extend only for task-owned fields                        |
| `task_delivery_state`                                    | Requester origin and last notified event                                                                                            | Reuse for task notification projection                             |
| `flow_runs`                                              | Flow identity, owner, revision, lifecycle, current/blocked step, wait state, cancellation                                           | Reuse; do not mirror flow lifecycle into durable execution records |
| `subagent_runs`                                          | Requester/controller/child refs, timeout, lifecycle, frozen result, pending final delivery, attempts, errors, acknowledgement       | Reuse as the subagent source of truth                              |
| `delivery_queue_entries`                                 | Queue identity, target/session/channel, retries, errors, recovery and platform-send state                                           | Reuse as transport queue authority                                 |
| `state_leases`                                           | Scope/key owner, expiry, heartbeat, payload                                                                                         | Reuse as claim and stale-owner authority                           |
| `gateway_restart_*` and `gateway_boot_lifecycle`         | Restart intent, handoff, old/new process evidence, boot outcome                                                                     | Reuse as restart source evidence                                   |
| `acp_sessions`, replay tables, and conversation bindings | Session/runtime identity, activity, replay stream, routing provenance                                                               | Reuse for target resolution and ACP state                          |
| audit and diagnostic tables                              | Mutation and operator diagnostics                                                                                                   | Reuse for authorization and maintenance audit                      |

### New Tables That Are Justified

| New table                   | Why no existing owner can safely own it                                                                                               | Required constraints                                                                                       |
| --------------------------- | ------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------- |
| `wake_obligations`          | Attention crosses task/subagent/flow/session/operator/report-route ownership                                                          | Required `source_owner` + `source_ref`, generalized target/owner, stable dedupe key, terminal immutability |
| `delivery_attempt_evidence` | A single attempt can correlate an obligation, internal queue, and external transport without becoming any transport's lifecycle owner | Link to obligation, explicit evidence boundary/status, source refs, append/auditable finalization          |
| `uncertainty_facts`         | Ambiguous effects and replay decisions span operation and owner boundaries                                                            | Required source refs, dedupe, open/resolved/superseded lifecycle, owner decision evidence                  |
| `durable_execution_records` | Interactive agent turns and generic durable operations do not always have a task/flow/subagent owner                                  | Must be source-backed or document a true root operation; evidence/checkpoint role only                     |
| `durable_event_evidence`    | Ordered recovery evidence for a generic execution record                                                                              | Append-only per execution record                                                                           |
| `durable_execution_steps`   | Checkpoint/replay and claim boundaries for generic operations                                                                         | Operation registry and lease-backed claims; not a task/flow lifecycle mirror                               |
| `durable_payload_refs`      | Privacy-bounded inputs, outputs, errors, checkpoints, and artifacts                                                                   | Ref/hash/retention metadata by default; raw capture opt-in                                                 |
| `durable_run_correlations`  | Generic parent/child and source-owner correlation not owned by one lifecycle                                                          | Correlation only; source owner remains canonical                                                           |
| `durable_timer_obligations` | Generic durable sleep/retry/deadline not represented by a Task Flow wait                                                              | Use only for generic execution; Task Flow keeps `wait_json` authority                                      |
| `durable_signal_evidence`   | Generic approval/input/callback evidence for a durable execution                                                                      | Evidence only; owner policy decides meaning                                                                |

### Proposed Tables That Are Not Justified

| Current proposal                                     | Decision                                            | Reason                                                                                                            |
| ---------------------------------------------------- | --------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------- |
| `continuation_cleanup`                               | Remove                                              | Cleanup belongs to the timer/signal/mailbox/obligation owner; an event or owner update is sufficient              |
| `dedupe_ledger`                                      | Remove until a proven cross-owner query requires it | Unique source/dedupe keys on obligation, uncertainty, correlation, and attempt tables already enforce idempotency |
| Separate result-mailbox lifecycle in `durable_steps` | Collapse into payload ref plus wake obligation      | It currently competes with parent attention and delivery-attempt state                                            |
| Durable claim fields as independent authority        | Keep only as indexed projection if needed           | `state_leases` already owns lease expiry and stale-owner protection                                               |
| `durable_schema_migrations`                          | Remove                                              | Shared state schema and `PRAGMA user_version` already own schema compatibility; a second version ledger is unsafe |

## Pre-Refactor Durable Branch Assessment

At the start of this analysis, the durable branch had useful foundations:
feature and worker gates were
separate, disabled reads avoid database creation, terminal store guards exist,
input capture is bounded, fan-in/result correlation is modeled, and canonical
`WakeObligation`, `DeliveryAttemptEvidence`, and `UncertaintyFact` types exist.

It was not yet the selected architecture:

- physical tables are still broad (`durable_runs`, `durable_steps`, and peers)
  instead of evidence/record names;
- `source_type` remains in storage and public APIs still carry a deprecated
  `sourceType` alias even though this is unshipped code with no compatibility
  requirement;
- wake and uncertainty records do not require `source_owner` and `source_ref`;
- the result mailbox is a second attention/delivery lifecycle;
- startup recovery can mark records lost without first using the canonical
  owner and lease evidence;
- no-handler and unsafe retry paths set a status but do not create uncertainty
  and owner attention facts;
- the worker uses local claim columns rather than `state_leases`, and heartbeat
  renewal is not automatic;
- unresolved-obligation inspection reads only the new durable tables and misses
  existing owner facts such as pending subagent final delivery and expired
  `state_leases`;
- protocol schemas define obligation/attempt inspection shapes, but Gateway
  exposes only `durable.coordination.get`;
- CLI exposes run-centric commands but no obligations, uncertainty, attempts,
  or owner-control audit surface;
- `requires_parent_decision` and actor kinds `external|parent|operator` are too
  parent-specific for the generalized owner model;
- `continuation_cleanup` and `dedupe_ledger` add schema without a demonstrated
  residual fact.

## Refactor Outcome

The owner-first refactor now addresses those findings:

- generalized evidence/record/table names replace the broad or parent-specific
  vocabulary;
- wake, uncertainty, and delivery evidence require a source owner and ref;
- the result-mailbox lifecycle, cleanup table, and generic dedupe table are
  removed;
- `subagent_runs`, `task_runs`, `delivery_queue_entries`, and expired step
  `state_leases` are projected into unified unresolved-obligation inspection;
- canonical owner adapters dispatch subagent and task attention without
  mirroring either lifecycle;
- the step worker uses one lease authority with automatic renewal and rejects
  stale-token writes;
- wake dispatch also uses `state_leases`, renews the current token while owner
  dispatch is active, and removes public evidence mutation APIs that could
  bypass its fencing token;
- no-handler and uncertain side-effect paths create source-backed uncertainty
  and owner attention instead of blindly retrying or failing;
- CLI and Gateway read surfaces plus explicit control mutations are implemented
  with bounded operator scopes, source-revision guards, retained decision
  evidence, and server-derived actor identity; shared audit-store integration
  remains a release hardening item;
- public CLI/RPC names follow resource/action vocabulary, including
  `durable.wakes.inspect` rather than treating every unresolved item as a wake.

Execution records keep heartbeat as liveness evidence only. Run-level claim
APIs and claim projection columns were removed because the worker schedules
steps, and a second claim boundary had no demonstrated owner or consumer.

## Existing API And CLI Surface

### Official 7.1

| Surface                                                    | Existing behavior                                                           | Gap                                                                          |
| ---------------------------------------------------------- | --------------------------------------------------------------------------- | ---------------------------------------------------------------------------- |
| `openclaw tasks list/show/audit/maintenance/notify/cancel` | Strong task lifecycle, reconciliation, delivery, and maintenance inspection | No cross-owner obligation, attempt, uncertainty, or lease view               |
| `openclaw tasks flow list/show/cancel`                     | Flow lifecycle and wait/blocked inspection                                  | No generic child/result handoff evidence                                     |
| `openclaw sessions list/tail/compact/cleanup`              | Session inspection and compaction controls                                  | No durable pending-work contract preserved independently of transcript state |
| `tasks.list/get/cancel` Gateway RPC                        | Operator task reads and cancellation                                        | No read-only durable contract projection or owner-decision verbs             |
| session Gateway RPCs                                       | Session lifecycle, history, compact/reset/delete                            | No source-backed obligation inspection across sessions/owners                |
| subagent tools and registry                                | Parent-controlled list/steer/kill plus persistent lifecycle/retry           | No operator-safe generalized obligation/attempt API                          |

### Required Additive Durable Surface

The durable CLI and Gateway should be additive and source-oriented:

- `durable obligations list`
- `durable wakes list`
- `durable wakes inspect <wake-id>`
- `durable uncertainty list`
- `durable delivery-attempts list <wake-id>`
- `durable leases list` or inclusion in obligation inspection
- no initial public owner mutations; add acknowledge, supersede, resume,
  uncertainty resolution, retry, or abandon only with a canonical owner adapter
  and shared audit proof

Read methods require operator-read authority and must never claim, migrate,
enqueue, acknowledge, or replay. A later mutation PR requires `operator.write`,
server-derived actor identity, revision guards, retained decision evidence, and
delegation through the canonical owner API. Broader owner/controller/admin
actions require canonical owner authorization and shared audit integration.

## Required Runtime Flow

1. A canonical owner accepts or changes work.
2. The owner writes its own lifecycle state.
3. A producer observes a source fact that crosses an owner boundary.
4. The producer creates one source-backed `WakeObligation` using a stable
   dedupe key.
5. Target resolution records resolved, missing, ambiguous, unauthorized, or
   inspect-only evidence.
6. Each internal handoff or transport attempt creates/finalizes
   `DeliveryAttemptEvidence` without overstating the proof boundary.
7. Crashes or ambiguous effects create an `UncertaintyFact`; replay remains
   blocked until operation and authority gates pass.
8. The target owner acknowledges or supersedes the obligation, or records an
   explicit decision. The worker cannot make that semantic decision.

## Implementation Order

1. Freeze canonical physical and public names; remove unshipped compatibility
   aliases.
2. Remove unjustified cleanup/dedupe tables and make every residual record
   source-backed.
3. Convert subagent terminal, pending final delivery, no-handler, unsafe retry,
   and restart recovery into obligation/uncertainty producers.
4. Use `state_leases` with unique claim tokens and automatic renewal.
5. Collapse result-mailbox attention/delivery state into payload refs,
   obligations, and delivery-attempt evidence.
6. Add side-effect-free CLI/Gateway inspection, then audited owner controls.
7. Prove restart, lease expiry, duplicate observation, compaction, delivery
   boundaries, and disabled paths before claiming runtime completion.

## Acceptance Decision

The owner-first architecture is the selected merge direction. The refactored
slice satisfies its storage, source-ref, naming, lease, uncertainty, inspection,
authority-intake, owner-adapter, and explicit-control contracts in the companion
test plan. It is semantically rebased onto the official 7.1 correction and both
core production and core test typechecks pass. Before claiming production-level
durability, upstream submission still requires live restart, overflow,
compaction, and transport fault injection, plus owner adapters for flow, queue,
restart/boot, session, and ACP sources that are currently inspection-only or
planned.
