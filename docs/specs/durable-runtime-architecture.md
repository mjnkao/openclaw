---
title: Durable Runtime Residual-Gap Architecture Proposal
summary: "Proposal anchor for residual durable runtime invariants and boundaries."
read_when:
  - Reviewing the durable runtime residual-gap proposal
  - Checking durable runtime owner boundaries before implementation
  - Auditing recovery, wake, and delivery invariants
---

# Durable Runtime Residual-Gap Architecture Proposal

This page is an unapproved durable-runtime RFC candidate for official OpenClaw
7.1. It describes the residual gap between existing lifecycle owners and
possible future runtime recovery work. It is not an accepted architecture, not
the durable runtime source of truth, and not a maintainer decision about
ownership or stack order. It does not claim that runtime behavior, external
delivery, replay, worker recovery, schema migration, or CLI/Gateway behavior is
implemented by this document.

For implementation and conformance work, this document is the canonical
candidate architecture until maintainers accept, narrow, or reject the
boundary in [the upstream feature request](https://github.com/openclaw/openclaw/issues/117052).
That issue is the single governance decision point; it does not grant runtime
authority, change an owner boundary, or authorize public mutation by itself.

## Candidate General Durable Runtime RFC

Durable runtime would be an owner-first, opt-in cross-owner contract layer beneath
OpenClaw agent, session, subagent, task, channel, and operator surfaces. Its job
would be to define invariants for runtime facts that must survive process
restarts: accepted-work identity, ordered steps and events, parent/child links,
bounded refs, leases, recovery states, wake/attention obligations, delivery
evidence, and read-only inspection state. It would not be a replacement
lifecycle ledger or a general workflow engine.

If maintainers adopt this RFC, the durable runtime layer would build on the
existing local state owners in `openclaw.sqlite`. The target base already
persists audit events, state leases, task and subagent runs, durable delivery
queues, task delivery state, and flow runs. Session, restart, ACP, conversation
binding, audit, and diagnostics also retain their current authority. This RFC
candidate must not become a competing ledger for those owners. It names only
residual contracts needed when facts cross boundaries: stable refs, append-only
event evidence, generic execution checkpoints, wake/attention obligations,
delivery-attempt evidence, uncertainty, inspection, and fail-closed recovery.

The RFC boundary is intentionally conservative:

- record accepted runtime work with stable identities and append-only or
  otherwise auditable lifecycle events;
- keep recovery explicit by separating complete, failed, cancelled, stale,
  interrupted, and owner-decision-needed states;
- expose operator and Gateway read paths that explain state without mutating
  work;
- require opt-in durable runtime and separately gated worker behavior until
  storage, recovery, and live delivery claims have direct proof;
- preserve current synchronous behavior when durable runtime is disabled.

## What Problem This Solves

OpenClaw is no longer only a synchronous chat loop. It now spans long-running
agent turns, subagents, tool calls, channel delivery, local operator inspection,
and process restarts. Existing durable owners already record important parts of
that work in `openclaw.sqlite`, including audit events, leases, task/subagent
state, delivery queue entries, task delivery state, and flow runs.

The remaining architecture question is cross-owner consistency. A task row,
subagent run, delivery queue entry, flow run, and audit event can each be valid
on its own while still leaving unclear whether a parent is waiting on fan-in,
whether a side effect already happened, which handoff was accepted or
acknowledged, or what a restart may safely reclaim. If adopted, the durable runtime
would give those owners shared invariants so OpenClaw can answer what it
accepted, what ran, what is waiting, what failed, what became stale after
restart, and which bounded recovery action is safe to present to an owner or
operator.

## Why The Residual Gap Matters

This residual-gap proposal exists because the suspected remaining failure modes
are cross-cutting rather than isolated to one channel, one prompt, or one UI.
The repeated pattern is not merely "a message did not arrive"; it is that
OpenClaw can accept work, delegate it, defer it, or route it through a channel
without a shared durable obligation that later code can inspect, recover,
acknowledge, or fail closed.

The remaining root causes implementation work must address are:

- **Coordinator silence:** a coordinator can promise later progress or
  completion, then depend on transcript intent and voluntary follow-up instead
  of a durable wake/report-route obligation.
- **Restart and interruption loss:** process exit, gateway restart, tool
  interruption, or provider return can erase process-local knowledge of accepted
  work and leave no safe recovery classification.
- **Stale running work:** rows, sessions, or in-memory markers can appear
  running after the owner is gone unless durable leases, expiry, and stale-state
  diagnostics exist.
- **Parent/child handoff gaps:** subagent completion, fan-in, and result
  delivery can be visible to a child but not durably addressed to the parent or
  human who needs the result.
- **Delivery and attention uncertainty:** a channel send, internal handoff, or
  operator notification may be accepted, attempted, failed, or unknown without
  durable attempt evidence and acknowledgement state.
- **Side-effect uncertainty:** crashes before or after tool dispatch, provider
  return, child spawn, local commit, or channel send can make automatic replay
  unsafe unless idempotency and authority gates prove it.

If accepted and implemented, this foundation would let durable-runtime work
treat those cases as inspectable runtime states instead of as unrelated prompt,
channel, or UI bugs. This page documents the proposed boundary and proof model;
implementation and runtime claims belong to the changes that add and validate
code.

## Ownership And Source-Of-Truth Position

Until maintainers explicitly adopt a durable runtime boundary, existing TaskFlow,
background-task, session, delivery, Gateway, and SQLite-backed owners remain the
source of truth for their own behavior. This RFC candidate is only a review
artifact for the residual cross-owner questions those owners may not answer
alone.

If adopted, durable runtime implementation should extend the existing state
owners where they already own the fact, then add shared contracts only for
cross-owner questions those tables cannot answer alone. New records should be
source-backed by a concrete invariant, migration, recovery path, or inspection
need.

Architecture review should check whether each proposed durable fact has a clear
owner, retention posture, privacy posture, disabled-runtime behavior, stale-owner
handling, and read path. If an existing owner can answer the question without a
new shared record, prefer the existing owner.

### Candidate Residual Schema

The candidate schema is deliberately limited to eleven residual tables.
Existing owner tables remain canonical and are not mirrored.

| Table                         | Residual fact                                                        | Existing authority it does not replace           |
| ----------------------------- | -------------------------------------------------------------------- | ------------------------------------------------ |
| `durable_execution_records`   | Reservation and accepted execution evidence when no owner row exists | Task, flow, session, cron, or subagent lifecycle |
| `durable_event_evidence`      | Ordered execution and recovery evidence                              | Security audit or canonical transcript history   |
| `durable_execution_steps`     | Registered step, checkpoint, claim, and replay boundaries            | Workflow, task, or tool-execution policy         |
| `durable_payload_refs`        | Bounded input, output, error, checkpoint, and artifact references    | Payload, transcript, media, or artifact storage  |
| `durable_run_correlations`    | Parent/child and cross-owner correlations                            | Either endpoint's lifecycle                      |
| `durable_timer_obligations`   | Registered retry, deadline, and sleep obligations                    | Cron, scheduler, or domain timer policy          |
| `durable_signal_evidence`     | Approval, input, and callback evidence                               | Live approval, credential, or callback authority |
| `wake_obligations`            | Source-backed owner or report-route attention obligations            | Source, target, session, or channel lifecycle    |
| `wake_obligation_occurrences` | Immutable occurrence identity and projection-hash evidence for wakes | Mutable wake lifecycle or generic dedupe policy  |
| `delivery_attempt_evidence`   | Per-attempt handoff and delivery proof with explicit proof boundary  | Queue, channel retry, or transport policy        |
| `uncertainty_facts`           | Ambiguous outcomes and facts requiring reconciliation or a decision  | Canonical owner's retry or resolution decision   |

No generic dedupe ledger, result mailbox, mode table, decision table, cleanup
table, capability registry, or migration ledger is justified while unique
keys, existing owner state, `state_leases`, audit evidence, and canonical
schema metadata can provide those contracts. `wake_obligation_occurrences` is
narrower than a generic dedupe ledger: one coalesced wake can represent multiple
immutable occurrence identities while its current projection and lifecycle
advance. Storing only the latest occurrence key on the mutable wake loses
exact-retry evidence and can allow a delayed retry to restore an older
projection. The occurrence table retains only canonical source scope, a bounded
opaque key, canonical wake reference, projection hash, and observation time. It
owns no lifecycle and stores no raw payload.

The eleven tables live in the existing shared SQLite file and are installed
only when durable runtime is explicitly enabled. Disabled startup and read
paths must neither open or migrate durable storage nor eagerly load the recovery
and owner-adapter module graph. Before any writable connection pragma, journal
mode change, WAL/SHM sidecar creation, DDL, metadata update, or backfill, a
read-only preflight must reject a future shared-state version, partial durable
schema, unknown Durable-owned namespace, or incompatible table, constraint,
foreign-key, or required-index shape. Writable initialization may install the
complete absent schema and repair an explicitly compatible missing canonical
index transactionally; it must not guess a table migration or create a second
schema authority. Every supported durable schema evolution advances the
canonical shared-state schema version.

Exactly eleven tables is the current residual boundary, not a promise that the
schema can never gain a typed column or index. A fact used in a mutation
predicate, compare-and-set, uniqueness rule, claim fence, retry eligibility,
authoritative absence decision, or bounded query must have typed storage and a
validated schema shape. It must not live only in generic JSON to avoid a schema
change. In particular, a wake owns a monotonic delivery revision, and a delivery
attempt captures that revision in typed evidence. A suspension class used to
authorize automatic resume is typed as well. Bounded descriptive diagnostics,
owner-specific display metadata, and unknown optional evidence may remain in a
versioned metadata envelope.

The owner and recovery contracts learned from later runtime work still map to
this schema:

| Contract                                                  | Existing storage or owner boundary                                              |
| --------------------------------------------------------- | ------------------------------------------------------------------------------- |
| Exact occurrence and owner generation                     | Execution records, correlations, and event evidence                             |
| Captured-generation cleanup                               | Process-local owner capability plus bounded recovery evidence; no stored handle |
| Parent, child, and portable progress                      | Correlations, event evidence, and wake obligations                              |
| Terminal presentation disposition                         | Execution/event evidence and visible-closeout wake projection                   |
| Multipart delivery receipts                               | Bounded refs in `delivery_attempt_evidence`                                     |
| Session retirement or route generation                    | Canonical session owner plus correlations and event evidence                    |
| Probe timeout or missing capability                       | `uncertainty_facts` and owner-decision wake evidence                            |
| Reconciliation deadline                                   | Runtime policy and timer/event evidence; no scheduler mirror                    |
| Whole-state rollback quiescence                           | Base update-owner composition hook; no durable transaction table                |
| Approval, process, cancellation, or credential capability | Process-local owner only; never durable storage                                 |

A new column or table is justified only if a proven residual fact cannot be
represented by these bounded records without overloading another owner's
lifecycle. The presence of a newer runtime API is not itself a schema reason.

## Durable Runtime Boundary

The durable runtime is proposed as a local-first runtime substrate, not a product UI
and not a general workflow engine. If maintainers adopt this boundary, it should
record enough state to inspect, explain, and recover agent/session/task work
without requiring external orchestration.

| Area             | Durable runtime owns                                                                                               | Existing owner retains                                                                |
| ---------------- | ------------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------- |
| Channel delivery | Intake correlation, delivery refs, attempt evidence, and diagnostics                                               | Queue lifecycle, formatting, transport policy, notification UX, and channel retries   |
| Agent execution  | Generic evidence only when no lifecycle owner exists; stable cross-owner refs, links, checkpoints, and uncertainty | Session/task/flow/cron/subagent lifecycle, prompt policy, model choice, and tool work |
| Subagents        | Source-backed parent/child correlations, bounded result refs, attention obligations, and attempt evidence          | Spawn, progress, timeout, cleanup, result capture, and final-delivery retry lifecycle |
| Recovery         | Cross-owner stale/lost diagnostics, append-only recovery evidence, and owner attention                             | Lease authority and decisions to retry, resume, abandon, replace, or replay           |
| Inspection       | Bounded, allowlisted, redacted projections and enforced result limits over durable facts                           | Gateway authentication, method scopes, trust policy, and canonical visibility policy  |
| UX/product       | Read models and explanations over durable facts                                                                    | Workboard grouping, Task Flow authoring, channel UX, and product-specific task states |

Layer separation is mandatory. Substrate facts are persisted in durable tables;
runtime interpretation derives safe state from those facts; projection policy
maps facts into Workboard, Task Flow, or channel views; agent policy decides what
the model or owner should do next.

## Canonical Owner Compatibility Contract

The durable runtime must remain valid whether accepted turns are owned by the official
7.1 front-door and runner paths or by a later process-local runtime host. It does
this through owner adapters, not by preserving two lifecycle authorities.

Exactly one canonical volatile execution owner is selected by the composition
root for an accepted occurrence. That owner retains active handles, duplicate
run rejection, cancellation, progress ordering, approval capabilities,
presentation settlement, and process-local cleanup. A durable execution record
is evidence that work was accepted; it is not an active-run registry and does
not authorize cancellation, resume, retry, or replay. This exclusivity is
scoped to an authoritative role and occurrence: task, flow, subagent, session,
channel, and execution owners may coexist because they own different facts, but
two adapters cannot claim the same authoritative role for the same occurrence.

Two integration profiles may implement the same contract:

- the official 7.1 profile wraps the existing session, runner, subagent, task,
  flow, channel, and Gateway front doors;
- a canonical-owner profile wraps the runtime/session owner selected by a later
  OpenClaw base.

Profile selection is explicit at startup or build composition. The durable runtime
must not register both profiles as authorities for the same occurrence, probe a
second owner after the selected owner rejects work, or fall back to an ambient
Gateway authority when a process-local host is missing. A missing or replaced
owner is a recovery fact, not permission to choose another owner.

Owner adapters expose only stable facts needed by the durable layer: accepted
occurrence identity, canonical owner and source refs, owner revision, bounded
lifecycle state, terminal outcome, terminal presentation disposition, attention
requirements, and proof-boundary evidence. Live capabilities such as host
objects, callbacks, approval leases, process handles, cancellation controllers,
credentials, and listener closures remain process-local and are never persisted
or reconstructed from durable metadata. After restart, capability-bound work
without fresh owner authority fails closed as uncertainty or
`requires_owner_decision`.

Destructive recovery captures the exact owner occurrence and live handle before
it starts. Abort, drain, expiry, force-clear, teardown, and terminal decisions
remain fenced to that captured generation. Recovery must not resolve a mutable
session key again and then operate on a replacement that appeared during
cleanup. Session identity is a routing ref, not a cleanup fence.

Execution terminal settlement includes any bounded process or resource teardown
that the execution-owner contract defines as part of completion. The durable runtime
neither performs that teardown nor persists its handles, and it cannot report
execution terminal while that owner still reports execution-resource settlement
in progress. Presentation is a separate closeout proof axis: execution may be
terminal while visible closeout remains only result-available, explicitly
uncertain, or suspended, but execution success cannot ACK user presentation.

Session resolution follows the same rule. The durable runtime consumes the one
canonical session-owner facade selected by the base and stores bounded
correlation refs; it does not mirror session entries, history, selector
precedence, key migration, visibility, or branch policy. Portable subagent
progress may be consumed from a canonical host projection, but provider-specific
progress streams and reusable tool-call identifiers cannot become Durable
ownership or logical child identity.

Committed session retirement, replacement, branch, route, or ownership changes
advance the canonical owner generation before Durable reconciliation. A
pre-commit, failed, or no-op mutation does not invalidate correlations. Once a
committed boundary is visible, stale wake targets and closeout evidence cannot
act on the retired generation; they are superseded or become explicit owner
uncertainty. The durable runtime does not infer lifecycle boundaries that an external
system never exposes.

This compatibility contract requires no additional durable table. Owner kind,
owner revision, occurrence, generation, capability requirement or outcome, and
proof evidence fit the existing records, bounded refs, events, correlations,
delivery attempts, and uncertainty facts. A value used to authorize mutation is
typed and schema-validated; optional descriptive evidence may use versioned
metadata. A declaration that a live capability currently exists is never
persisted as authority. Existing supported readers preserve unknown fields.

## Upstream-Independent Capability Contract

The durable runtime depends on a neutral owner-binding contract, not on the module,
class, method, or file names used by one OpenClaw release. A base-specific
composition root supplies one immutable binding set for execution, session,
attention, delivery evidence, and optional state-transaction coordination. The
set may contain several non-overlapping domain adapters; binding validation
rejects duplicate authoritative role-and-scope claims before intake.
Conditional imports and version-specific translation remain inside that adapter;
core storage, reconciliation, public projection, and wake logic do not branch on
upstream PR or transport identity.

The binding declares process-local capabilities at startup. Every declared
capability identifies a stable operation and is valid only when the same frozen
binding supplies that operation. Binding construction rejects duplicate
authoritative claims, duplicate adapter identities, a declared capability with
no implementation, and authoritative absence reconciliation without bounded
inspection and enumeration. Capability declarations are build/runtime
composition facts, not persisted authority, user configuration, or a public
negotiation protocol. They may describe support for
exact occurrence fencing, committed session generations, portable progress,
presentation settlement, owned-resource settlement, multipart receipts,
bounded owner enumeration, or whole-state quiescence.

Capabilities are checked per operation:

- disabled mode does not construct bindings or open durable storage;
- observe mode may record bounded facts and unsupported-capability uncertainty
  without mutating another owner;
- authority mode performs a recovery or settlement action only when the selected
  binding proves every capability required by that action;
- a missing capability produces an explicit unsupported or
  `requires_owner_decision` result and never triggers reflection, heuristic
  reconstruction, a second owner, or an ambient fallback;
- optional capability loss must not disable unrelated inspection, recording, or
  sibling reconciliation.

Profile selection remains one-time and deterministic. A capability check is
made against the already selected binding; symbol presence, a persisted
capability name, or a successful operation from another adapter is not proof.
Core code must not probe
several candidate owner implementations per run, combine half of one lifecycle
owner with half of another, or infer support from symbol presence. If upstream
has competing unmerged owner designs, only the official base adapter is active
until maintainers select a canonical direction.

| Upstream state                            | Proposed runtime behavior                                                                                                          |
| ----------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------- |
| Official 7.1 only                         | Use the 7.1 adapter; preserve current behavior and fail closed for actions whose exact authority cannot be proven                  |
| Some compatible owner work merged         | Enable only capabilities exposed by the one selected canonical adapter; unsupported operations remain inspectable and conservative |
| Canonical runtime/session owner merged    | Replace the 7.1 adapter at composition; keep the eleven-table boundary, Durable API/CLI, proof levels, and wake semantics stable   |
| Competing owner candidates remain         | Do not register both, choose per run, or copy candidate-specific state into durable runtime state                                  |
| Owner API is renamed or reorganized       | Change the adapter and its contract tests only; no schema migration or public durable rename                                       |
| Owner capability disappears after restart | Record uncertainty and require fresh authority; never reconstruct the live capability from persisted refs                          |

This anti-corruption boundary is the main future-compatibility mechanism. It
allows later owner implementations to remove duplicate 7.1 wiring without
making the durable runtime depend on their merge, naming, or internal topology.

## Bounded Reconciliation And State Transactions

Every reconciliation pass, owner probe, and paginated owner projection is a
bounded operation. The caller sets one absolute pass deadline and derives
remaining per-item budgets from it. Implementations cap concurrency, items,
serialized evidence, and cursor traversal; preserve opaque cursor values;
detect repeated or cyclic cursors; and recheck cancellation after synchronous
processing. A pass deadline prevents starting more work but cannot interrupt an
already-blocked synchronous owner API, so every adapter operation must also
enforce its own finite item, page, byte, and time contract. An adapter that
cannot provide that contract must not advertise bounded enumeration or be used
for authoritative absence reconciliation. One stalled or malformed owner must
not prevent verified siblings from converging where the base exposes an
interruptible or asynchronous probe boundary.

When a pass cannot safely finish, completed siblings retain their monotonic
facts and unresolved items remain inspectable as uncertainty or
`requires_owner_decision`. The worker does not start an unbounded retry loop,
erase the unresolved source, or convert a probe timeout into owner absence.
These bounds are runtime policy and bounded metadata, not a new durable table.

The durable runtime also composes with a base-owned whole-state update or rollback
transaction. After that owner snapshots mutable state, it must quiesce or reject
new durable intake and recovery mutation until commit, or provide an equivalent
monotonic journal/merge contract that preserves every post-snapshot accepted
fact. Restoring an old state image must never silently erase accepted work. The
update owner retains package, service, snapshot, and compensation authority;
The durable runtime exposes only its readiness and quiescence boundary.

## Store Paging, Claims, And Safe Resume

Every storage query used to prove absence, select work, reconcile identities,
or enumerate operator-visible facts has an explicit completeness contract. A
bounded page returns items, a `complete` flag, and an opaque continuation cursor
when more eligible rows exist. Cursors bind their schema version, filters,
cutoff, and stable keyset; callers cannot reuse them across a different owner or
query. Limits above the documented maximum are rejected rather than silently
clamped. Equal timestamps are ordered by a stable identity so one page neither
skips nor repeats rows.

An incomplete, timed-out, cyclic, malformed, or truncated scan is never proof
of absence and cannot authorize mutation, duplicate creation, retirement, or
owner takeover. The same rule applies to unresolved-wake enumeration and open
run recovery. Query plans for claim and reconciliation paths must use a matching
index and bounded keyset; a result `LIMIT` alone is insufficient when SQLite
must scan or sort an unbounded population while holding a writer transaction.

Wake projection updates advance a store-owned delivery revision. A delivery
claim captures that revision and all completion, failure, expiry, suspension,
or uncertainty writes compare against it. Coalescing may advance the canonical
projection, but an older claim can record evidence only for the projection it
actually attempted and cannot settle the newer one. Exact occurrence retries
remain a fast no-op; changing occurrence keys for one logical obligation are
reconciled atomically with occurrence insertion, projection update, duplicate
repair, and supersession.

Generic storage exposes only a compare-and-set resume primitive. Automatic
resume is limited to explicitly safe, pre-attempt suspension classes such as a
temporarily unavailable required capability or target. The caller supplies the
expected suspension class and delivery revision. Any attempted, unknown, or
handoff-accepted side-effect evidence, revision mismatch, unknown class, or
owner-policy decision fails closed and remains inspectable. PR-specific owner
policy decides whether to call this primitive; the store never infers resume
safety from a reason string.

## Schema, Metadata, And Protocol Evolution

One canonical shared-state migration authority owns Durable schema versions.
Each binary declares the schema versions it can read and write. Startup may use
read-only inspection for a supported additive shape, but it rejects writes when
the installed version is outside its writable range. Evolution follows an
expand, migrate or backfill, verify, then contract sequence. Rollback is allowed
only while the predecessor remains a valid writer or after the update owner has
performed a proven reverse migration or quiesced the newer feature. An older
binary must never write through a newer incompatible schema merely because the
tables it knows still exist.

String kinds and metadata are extension points, not implicit authority. A reader
may preserve and project a bounded, redacted unknown value, but a worker cannot
claim, resume, replay, compact, terminalize, or repair a record whose operation
kind, lifecycle state, proof boundary, recurrence policy, suspension class, or
internal metadata version it does not understand. Internal metadata envelopes
are namespaced and versioned. Supported read-modify-write paths preserve unknown
fields; public projections deny unknown fields by default.

Public payloads evolve additively. Unknown optional fields are safe to ignore,
unknown enum values map to an explicit unsupported or unknown projection, and
stable error categories do not reveal whether a hidden record exists. Opaque
cursors carry their own version and query binding and may be rejected after a
schema or projection change. No public client may construct or edit a cursor or
persisted Durable metadata as mutation authority.

## Admission Reservation And Owner Acceptance

Durable admission and canonical owner acceptance are separate commit points.
Every user-visible front door follows this protocol:

```text
authorize and resolve canonical route
  -> reserve one durable occurrence
  -> canonical owner accepts or rejects that exact occurrence
  -> persist owner-acceptance or rejection evidence
  -> emit external accepted or success framing only after acceptance
```

The reservation gives a crash a stable identity without falsely claiming that
the volatile owner started work. If the owner rejects, the reservation settles
as non-accepted evidence and no fallback owner runs. A crash after reservation
but before an owner receipt becomes an admission-uncertain or abandoned
reservation. Recovery may query the selected owner by exact occurrence, but it
must not redispatch automatically unless that owner proves idempotent acceptance
for the same occurrence and current generation.

The protocol requires no additional table. The execution record carries the
reservation and owner receipt state; event evidence records the boundary. An
existing domain owner may supply both facts through one atomic owner front door,
but the durable runtime still projects reservation and acceptance as distinct proof.

## Evidence Retention And Source Deletion

Retention follows unresolved responsibility rather than age alone. An unresolved
wake, claim, attempt, timer, signal, uncertainty, checkpoint, or owner decision
pins the minimum execution, occurrence, correlation, event, and bounded-ref
evidence required to explain and settle it. Compaction cannot delete a fact
referenced by unresolved work or break a causation, occurrence, claim, or proof
edge. A compacted event range leaves a bounded summary containing the removed
range, count, strongest retained proof, and an integrity digest when the source
contract requires one.

A canonical owner that retires or deletes source state first settles,
supersedes, tombstones, or explicitly transfers every residual obligation
through its owner front door. A missing source discovered after uncoordinated
deletion becomes uncertainty; the durable runtime does not infer successful completion
or silently drop the obligation.

Occurrence identities remain available for at least the source owner's declared
idempotency and retry horizon. After that horizon, a stale occurrence retry
fails as expired or requires an owner decision; it cannot silently create new
work because its dedupe evidence was compacted. Privacy deletion removes raw or
preview material through its canonical owner and retains only the minimum
non-sensitive tombstone required to prevent unsafe replay when policy permits.
Legal hold or independently managed retention policy remains a separate owner
feature and does not justify a generic Durable retention table.

## Clocks, Storage Pressure, And State Authority

Lease expiry makes a record eligible for reconciliation; it is not fencing
authority. Every claim carries a unique token or monotonic revision, and every
renewal, completion, failure, suspension, and uncertainty write compares the
exact claim plus the relevant owner occurrence and delivery revision. In-process
elapsed deadlines use a monotonic clock. Persisted wall-clock timestamps support
restart inspection and eligibility, but a clock rollback or implausible forward
jump fails closed for side-effectful replay and records component health or
uncertainty.

Durable write transactions contain only validation, bounded indexed queries,
and state mutation. They never call a model, network, channel, process, or owner
callback. Dispatch happens after the claim or intent transaction releases its
lock. Busy retry, WAL growth, checkpointing, and transaction duration are
bounded. Disk-full, I/O, corruption, read-only, or incompatible-schema failures
prevent new durable acceptance before external success is reported. If evidence
for already accepted work cannot be appended, authority mutation stops and the
affected component remains visibly degraded; retention cannot delete unresolved
evidence merely to recover space.

The local-first contract assumes one canonical shared-state authority. Multiple
connections may coordinate through the same transactional store, but independent
Gateways with asynchronously replicated local databases are not one authority.
A future backend may replace SQLite only if it preserves the same atomicity,
uniqueness, fencing, snapshot, bounded-page, and rollback semantics. Durable
Core never heuristically merges divergent local histories; distributed
scheduling or replicated consensus requires a separate architecture decision.

## Candidate Integration Boundary

An owner front door is the narrow existing API allowed to mutate or deliver for
that lifecycle. Durable code may inspect bounded facts and call that API; it
must not update another owner's lifecycle table directly. Initial adapters
should cover task, subagent, managed flow, and persisted session attention while
reusing delivery queue, restart, local runtime, ACP, and state-lease front
doors. Direct imports from a transport namespace are a base-specific adapter,
not a permanent source-of-truth contract.

The generic agent-turn record is an intake/evidence envelope, not a replacement
for session, task, cron, flow, subagent, ACP, or delivery ownership. Every
user-visible front door reserves the same bounded durable occurrence before
owner dispatch and reports acceptance only after the canonical owner receipt:

| Front door                   | Durable reservation and acceptance boundary                                   |
| ---------------------------- | ----------------------------------------------------------------------------- |
| Gateway `agent` RPC          | Reserve before owner dispatch; owner receipt before `status: accepted`        |
| OpenAI-compatible HTTP       | Reserve before owner dispatch; owner receipt before success or stream framing |
| OpenResponses HTTP           | Reserve before owner dispatch; owner receipt before response lifecycle events |
| Local `agent` command        | Reserve before internal runner dispatch; persist the runner receipt           |
| Process-local runtime host   | Reserve after authorization; persist the exact owner `start` result           |
| ACP or TUI adapter           | Reserve after canonical session resolution; persist owner acceptance          |
| Channel auto-reply           | Reserve before typing, compaction, or model work; persist owner acceptance    |
| Queued follow-up / heartbeat | Reserve each invocation and persist the selected owner receipt                |
| Isolated cron execution      | Reserve before the isolated prompt and persist owner acceptance               |

Authorization and canonical routing happen before durable mutation. Durable
reservation commits before volatile owner dispatch. The owner receipt commits
before success-shaped external acceptance. Each accepted turn then settles
through one fenced owner lifecycle path, after owner result, presentation
settlement, and owner-required resource teardown, so success, failure, and
cancellation cannot race into contradictory terminal facts. The owner also
declares whether terminal presentation is visible, silent by policy,
paused/resumable, awaiting client tool continuation, or already delivered
through an owner-controlled path. The durable runtime records that bounded disposition
and must not infer it from empty text, typing, tool activity, or a missing
optional capability.

The first public operational surface should be additive and read-only:

- Gateway methods under `durable.*`, including health, coordination,
  obligations, wakes, uncertainty, and delivery-attempt inspection;
- CLI commands under `openclaw durable`, using the same bounded projections;
- no public acknowledge, retry, resume, abandon, replay, or direct owner
  mutation in the initial stack.

Initial `durable.*` Gateway inspection uses the existing operator role and
`operator.read` scope. Gateway authorization rejects unauthenticated or
insufficient-scope callers before durable handlers open state. Within the
existing trusted-operator model, authorized inspection is gateway-wide; Agent
and session identifiers are routing and correlation facts, not authorization
principals. The durable runtime does not introduce an Agent-scoped authorization
boundary. If Gateway visibility becomes narrower in the future, durable
inspection must consume that canonical policy rather than create parallel
permissions. Local CLI inspection is trusted local-operator access and uses the
same projection policy.

Trusted local inspection and Doctor composition should load only the minimum
canonical state-path and security context they require. An unrelated invalid or
unavailable optional provider, channel, tool, or model configuration may be
reported as a diagnostic, but must not erase durable facts or unnecessarily
block side-effect-free local inspection when the state location is still
unambiguous. Gateway inspection continues to require a running authenticated
Gateway and never bypasses its startup or authorization policy.

Any later owner-control surface needs a separate authority review with caller
identity, authorization source, owner revision, idempotency key, reason, audit
evidence, and an owner-adapter mutation path.

## Intended User Value

The proposal prioritizes trustworthy inspection and diagnostics first. The core
recovery goal is to persist committed facts and surface attention to the owner
of the work. If a runtime, worker, Gateway request, child run, channel delivery,
or process dies halfway, an accepted durable runtime design should record the
facts, expose diagnostics, and surface pending owner/main-agent work. Until that
maintainer decision exists, this page is not a promise that OpenClaw will add a
new durable runtime subsystem.

The proposal does not promise:

- automatic arbitrary replay;
- exactly-once external effects;
- full Task Flow barriers;
- Workboard-native recovery UX;
- distributed scheduling;
- semantic resume of model/tool work without explicit contracts;
- external channel delivery unless the implementation PR proves it directly.

## Core Invariants

- Terminal run and step states are immutable except retention or compaction
  metadata.
- Run and step status, recovery state, and completion time advance as one
  coherent lifecycle tuple. Incoherent rows are neither open nor claimable.
- Durable reservation, canonical owner acceptance, and external accepted framing
  are distinct boundaries. A reservation without a current owner receipt is not
  accepted work and cannot be blindly dispatched.
- Claimed records include an owner and expiry; stale owner writes are rejected.
- Recovery mutations append events and do not silently rewrite history.
- Disabled durable paths do not create databases, tables, or migrations and do
  not eagerly load the recovery or owner-adapter graph.
- Future or incompatible schema versions fail closed before writable pragmas,
  journal-mode changes, sidecar creation, or mutation.
- Mutation-authority facts use typed, schema-validated fields. Unknown kinds,
  states, proof levels, policies, suspension classes, or metadata versions remain
  inspectable but unclaimable and immutable to that binary.
- Every standalone obligation or uncertainty fact is source-backed by an
  existing owner/ref; only a true root generic execution may carry a documented
  root-operation reason instead.
- Exactly one canonical volatile execution owner and one canonical session
  owner apply to an accepted occurrence. Other domain owners may coexist only
  for non-overlapping facts; Durable records never become a second active-run or
  session authority.
- One immutable base-specific binding set is selected at composition. Core
  storage, recovery, and projection logic do not branch on upstream PR, module,
  class, transport, or release names.
- Authority actions require every declared owner capability for that operation;
  missing optional capabilities degrade to bounded uncertainty without disabling
  unrelated recording, inspection, or sibling reconciliation.
- Authorization and canonical owner/session resolution complete before durable
  admission; durable admission completes before owner dispatch or an accepted
  response.
- Live capabilities, approval leases, process handles, callbacks, credentials,
  and cancellation controllers are not durable data. Missing fresh authority
  after restart fails closed without ambient-owner fallback.
- Occurrence identity, logical child/attention identity, and owner/requester
  identity remain separate even when a source reuses a tool-call or run label.
- Route evidence, queue acceptance, owner consumption, canonical result
  persistence, transport acknowledgement, and user presentation remain distinct
  proof boundaries.
- Durable execution terminal settlement follows the canonical execution result
  and owner-required execution-resource settlement. Visible closeout advances
  independently through result availability, transport, sync, and presentation
  proof; execution success alone cannot ACK presentation. Stale owner events
  cannot settle a newer occurrence.
- Destructive recovery captures one exact owner occurrence and cannot abort,
  drain, expire, clear, or settle a replacement found through the same session
  key.
- Reconciliation uses absolute deadlines, bounded concurrency and evidence,
  sibling isolation, and inspectable unresolved outcomes; timeout is not proof
  of owner absence.
- Storage pages expose explicit completeness and filter-bound opaque cursors;
  silent limit clamps, fixed prefixes, and incomplete absence decisions are
  prohibited.
- A whole-state rollback owner gates durable intake and recovery mutation after
  its snapshot unless it proves an equivalent monotonic post-snapshot journal.
- Committed session lifecycle changes advance owner generation; failed or no-op
  mutations do not invalidate durable correlations.
- Visible, intentionally silent, paused, client-tool continuation, and
  owner-delivered terminal dispositions remain owner facts. Typing, empty text,
  no tool activity, or unavailable optional capability cannot decide closeout.
- One logical delivery attempt may contain bounded ordered physical-part
  receipts. A primary message id or aggregate transport success cannot hide a
  missing required part or raise the proof level.
- Delivery claims are fenced to the projection revision they attempted. A
  coalesced projection change cannot be settled by an older claim.
- Automatic wake resume requires an expected revision, an explicitly safe
  pre-attempt suspension class, and no attempted, unknown, or handoff evidence.
- Attempt-level `delivery_unknown` is terminal for that attempt but leaves the
  closeout obligation suspended and unresolved until authorized resolution or
  supersession.
- Unresolved responsibilities pin their minimum evidence and occurrence dedupe
  horizon; source deletion and compaction cannot silently erase replay safety.
- Lease expiry is eligibility only. Exact claim tokens, owner occurrences, and
  delivery revisions fence every authority write.
- Durable writer transactions perform no external calls. Storage pressure,
  corruption, lock exhaustion, or clock anomalies fail closed and remain
  component-visible.
- One canonical transactional state authority owns a Durable history; divergent
  asynchronously replicated local histories are never merged heuristically.
- Public Gateway inspection is authorized before durable state access; Gateway
  and trusted local CLI inspection are side-effect-free, read-only, and
  projected through explicit field and result bounds.
- Trusted local inspection remains minimally coupled to unrelated optional
  runtime configuration while preserving unambiguous state resolution and all
  canonical authorization boundaries.
- Unknown metadata is preserved across supported read/modify/write paths.
- Bounded previews are the default. Full input capture is opt-in, hashes are not
  anonymization, and metadata can be sensitive.

## Durable Wake And Attention Obligations

A durable wake is a substrate record that committed runtime facts require
attention from an owner, supervisor, report route, operator, or inspection
surface. Parent wake-up is one subagent/fan-in case; it is not the whole model.

The durable runtime may record wake-needed events, owner and target refs, bounded
payload refs, delivery-attempt evidence, no-handler diagnostics, and
acknowledgement state. It must not decide whether the owner should retry,
resume, abandon, wait, ask the user, or create new work.

The candidate wake lifecycle uses `pending`, `handoff_accepted`, `acked`,
`failed`, `suspended`, and `superseded`. The term `handoff_accepted` names only
the exact internal owner or queue boundary retained in attempt evidence. It
must not be shortened to `delivered`, because queue acceptance, attached-session
consumption, end-user delivery, and external transport success are different
proof boundaries. Only `acked` and `superseded` are terminal.

Wake reconciliation distinguishes the canonical source revision, an immutable
source-scoped occurrence key, its normalized projection hash, a store-owned
delivery revision, and the logical attention identity. Exact occurrence lookup
precedes logical-identity enumeration. A retry whose key and projection hash
match is a no-op, including after later coalescing or terminal transition, and
never rolls a newer projection back. Reusing a key with different normalized
content or recurrence policy is an idempotency conflict and persists no
candidate state.

An owner attention fact declares whether occurrences are independent or
coalesced while unresolved, and whether a new occurrence is allowed after a
terminal wake. Generic storage and dispatch code must not infer this policy from
reason names. For coalesced attention, different occurrence keys for the same
normalized source, reason, parent, target, owner, and report route update at
most one unresolved logical obligation. Parent run, parent session, target,
owner, report route, reason, and recurrence policy are identity or immutable
lifecycle fields; a later occurrence cannot silently retarget or change them.

`pending`, `handoff_accepted`, `failed`, and `suspended` all remain unresolved
for this cardinality rule. Queue acceptance cannot authorize another wake.
After the wake is `acked` or `superseded`, a later occurrence may create a new
wake only when the declared recurrence policy permits it and the canonical
owner still requires attention. Reconciliation is atomic at the store boundary
and preserves append-only delivery-attempt evidence. A bounded logical scan
that cannot prove completeness returns before mutation. Safe pending duplicates
with no claim, attempt, or non-pending evidence may be repaired atomically by
retaining one canonical wake, reparenting occurrence mappings, and preserving
superseded history; ambiguous duplicates require owner inspection.

Projection changes advance the delivery revision. Existing claims retain the
revision and bounded projection evidence they attempted; they cannot ACK, fail,
or suspend a newer projection. Queued attention payloads must remain truthful
when the source projection advances, for example by using stable generic text
plus a wake reference whose latest bounded projection is resolved at inspection
time.

Retry eligibility is explicit and indexed. Dispatch selection excludes backoff
and active leases before taking a bounded candidate window, so an older delayed
prefix cannot hide later runnable attention. An expired in-flight dispatch
becomes suspended uncertainty before a new attempt. Safe resume uses the
revision- and class-fenced primitive described above; all other suspensions
remain unresolved owner work.

`delivery_unknown` terminates only the attempt whose outcome cannot be proven.
It does not terminalize or ACK the wake. The wake becomes `suspended` with a
linked unresolved uncertainty fact until an authorized owner supplies stronger
evidence, chooses a safe action, or supersedes the obligation.

Inspection returns bounded occurrence metadata with explicit completeness or
truncation and supports exact source-scoped occurrence lookup. Occurrence keys,
projection hashes, correlations, and wake targets are sensitive evidence, not
authorization principals or anonymized identifiers.

Internal session delivery-queue acceptance may prove only the
`handoff_accepted` boundary for a resolved session target. It does not prove
that an attached session consumed the notice or that a user received it.
External channel transport remains a separate delivery claim and must not be
implied by this RFC or by the internal handoff unless implementation work proves
it directly.

## Non Goals

- No runtime code, schema, worker, CLI, Gateway, or transport changes from this
  docs-only RFC.
- No accepted ownership, source-of-truth, or stack-order decision from this
  docs-only RFC.
- No default-on durable runtime behavior.
- No public mutating durable CLI or Gateway controls in the initial stack.
- No Agent-scoped authorization boundary inside an existing trusted Gateway.
- No product-specific task-card or Workboard policy in the durable runtime.
- No raw prompt, task, or tool-payload persistence by default.
- No replay of side effects without idempotency, retention, and operation
  authority gates.
- No persistence or reconstruction of process-local host, approval, cancellation,
  listener, credential, or execution capabilities.
- No support for two concurrent canonical run or session owners merely to follow
  competing unmerged implementations.
- No persisted capability manifest, user-selectable owner implementation,
  reflection-based feature detection, or per-run version fallback.
- No generic retention ledger, asynchronously replicated local-history merge,
  distributed scheduler, or consensus protocol.
- No external delivery claim without direct implementation and live or
  maintainer-grade proof.

These contracts remain useful with stronger models and larger context windows:
model capability cannot eliminate process replacement, transport ambiguity,
lease expiry, permission changes, provider outages, or uncertain side effects.

## Related

- [Durable Runtime Residual-Gap Compatibility Check Plan](/specs/durable-runtime-compatibility)
- [Security and trust model](/gateway/security)
- [Operator scopes](/gateway/operator-scopes)
