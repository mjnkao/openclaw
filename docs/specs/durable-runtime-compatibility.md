---
title: Durable Runtime Residual-Gap Compatibility Check Plan
summary: "Candidate compatibility checks for proposed residual durable runtime invariants."
read_when:
  - Planning durable runtime compatibility checks for the residual-gap proposal
  - Auditing durable runtime merge readiness
  - Converting reviewer concerns into candidate checks
---

# Durable Runtime Residual-Gap Compatibility Check Plan

This plan turns the durable runtime residual-gap RFC candidate into possible
compatibility checks for future durable runtime work. It is a review anchor only,
not an accepted proof contract, source of truth, or owner decision. It does not
require runtime proof from docs-only changes that do not alter runtime behavior.

## Scope

The plan covers the opt-in, local-first durable runtime substrate: storage,
runtime facts, recovery diagnostics, wake/attention obligations, internal
handoff, and read-only inspection. It does not require product UI, Workboard or
Task Flow authoring, distributed scheduling, automatic semantic replay, public
owner controls, or external channel delivery.

## Root-Cause Coverage

If maintainers adopt a durable runtime boundary, future implementation work should
show how general runtime root causes would be handled as durable facts, not as
product-specific conventions. The architecture proposal defines candidate
coverage for owner review; implementation changes own executable proof.

| Root cause                      | Candidate durable runtime response                                                                                                                                  | Primary proof area              |
| ------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------- |
| Coordinator silence             | Accepted deferred work creates an inspectable wake, report-route, progress, timeout, or owner-decision obligation instead of relying only on transcript intent      | Wake and owner attention        |
| Restart and interruption loss   | Runtime facts distinguish complete, failed, cancelled, interrupted, stale, and decision-needed work after process exit, gateway restart, tool failure, or handoff   | Storage and recovery            |
| Stale running work              | Claims and leases have owners, expiry, stale diagnostics, and fail-closed mutation rules so lost owners cannot keep rewriting or hiding abandoned work              | Leases and worker recovery      |
| Parent/child handoff gaps       | Child spawn, terminal outcome, fan-in, bounded result refs, and parent wake facts are durably linked and deduped before any parent/human completion claim           | Subagent and result handoff     |
| Delivery and attention unknowns | Internal handoff and any claimed external delivery record target, attempt, acknowledgement, failure, no-handler, and unresolved states with bounded inspection      | Delivery and inspection         |
| Side-effect uncertainty         | Automatic replay is denied unless operation authority, input material, side-effect class, idempotency, dedupe/CAS or reconciliation, and retention gates all pass   | Replay authority and safeguards |
| Volatile owner replacement      | Durable admission and recovery bind to one canonical run/session owner through a base-specific adapter without creating a second active lifecycle authority         | Owner compatibility             |
| Live capability loss            | Host, approval, process, cancellation, and credential capabilities are never reconstructed after restart; missing fresh authority becomes explicit uncertainty      | Capability and security         |
| Replacement cleanup race        | Destructive recovery captures one owner occurrence and cannot abort, drain, clear, or settle a newer replacement that reuses its session or run reference           | Recovery generation fencing     |
| Unbounded reconciliation        | One absolute pass budget, bounded concurrency/evidence, sibling isolation, and inspectable unresolved outcomes prevent one stalled owner from freezing recovery     | Worker resource bounds          |
| State rollback data loss        | Whole-state snapshots quiesce durable intake/mutation or preserve all post-snapshot facts through an equivalent monotonic journal before rollback is allowed        | Update/restart composition      |
| Partial upstream adoption       | One immutable base adapter exposes available capabilities; missing or renamed owner APIs cannot create dual authority, unsafe emulation, or unavailable inspection  | Adapter compatibility           |
| Phantom accepted work           | Durable reservation, canonical owner receipt, and external accepted framing remain separate so a crash before owner acceptance cannot look started or replay itself | Admission protocol              |
| Retention erases replay safety  | Unresolved obligations pin minimum evidence and occurrence identity through the declared retry horizon; source deletion and compaction fail closed                  | Retention and compaction        |
| Clock or storage failure        | Exact claim fencing, monotonic elapsed time, bounded transactions, and degraded admission prevent clock jumps, lock pressure, disk-full, or corruption from lying   | Store failure behavior          |

Reviewers can use these as candidate durable-runtime compatibility checks only
after confirming the relevant owner boundary. A future implementation change may
cover a narrow slice, but it should still name which root-cause rows it covers
and which rows remain deferred.

## Compatibility Check Hygiene

- Run compatibility checks on the change being reviewed, with the same
  configuration that enables the claimed durable behavior.
- Record the command, relevant configuration, data directory setup, and output or
  log path.
- Re-run affected checks after rebases or changes to touched runtime surfaces.
- Docs map and generated artifacts must be regenerated with repo scripts, not
  copied from another worktree.

## Docs-Only Validation Gate

For this PR, the only applicable validation is documentation hygiene and scope
hygiene. The pages do not land runtime behavior, and they do not supersede
TaskFlow, background-task, session, delivery, Gateway, or SQLite ownership.

| Check         | Candidate validation                                                        |
| ------------- | --------------------------------------------------------------------------- |
| Scope hygiene | `git diff --name-status <base>..<head>` shows docs/spec/test-plan only      |
| Docs map      | `node scripts/generate-docs-map.mjs --check` or equivalent docs-map check   |
| Docs syntax   | docs MDX/lint/format checks available in the target branch for touched docs |
| Diff hygiene  | `git diff --check <base>..<head>`                                           |
| Ancestry      | `git merge-base --is-ancestor <base> <head>`                                |

Docs-only changes should state when live proof is not applicable because they
claim no runtime delivery behavior.

## Candidate Compatibility Matrix

| Area                          | Candidate compatibility check                                                                                                                                                 | Proof surface                     |
| ----------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------- |
| Disabled-path no mutation     | CLI and Gateway inspection reject before SQLite, WAL, SHM, migration, durable tables, worker startup, or eager recovery/owner-graph loading                                   | Durable config and inspection     |
| Runtime opt-in                | Durable recording and inspection are inert by default and enabled only by explicit durable runtime config                                                                     | Runtime config                    |
| Worker separate gate          | Runtime-enabled startup records only allowed facts; worker recovery requires separate worker opt-in                                                                           | Worker recovery                   |
| Future schema fail-closed     | Newer or incompatible durable schema fails before writable pragmas, journal-mode changes, WAL/SHM creation, DDL, backfill, worker mutation, or projection mutation            | Schema migration                  |
| Schema compatibility envelope | Each binary declares readable/writable ranges; expand/migrate/verify/contract and rollback rules prevent an older binary from writing a newer incompatible shape              | Schema and update owners          |
| Terminal immutability         | Terminal runs and steps reject lifecycle rewrites; `acked` and `superseded` wakes reject further transitions except explicit retention metadata                               | Storage mutation guards           |
| Identity propagation          | Chat, `agent.run`, user turns, embedded-agent yield, task completion, and status notices carry stable durable refs                                                            | Runtime identity                  |
| Canonical owner selection     | Exactly one volatile execution owner and one session owner bind per role/scope; non-overlapping domain owners may coexist, while duplicate authority or fallback fails closed | Owner adapter composition         |
| Official 7.1 profile          | Existing front-door, runner, session, task, flow, subagent, and delivery behavior remains unchanged behind the owner-adapter contract                                         | 7.1 compatibility                 |
| Later owner profile           | A runtime/session host facade replaces base-specific adapters without changing the eleven-table boundary, public durable API, wake lifecycle, or proof semantics              | Forward-port compatibility        |
| Immutable capability binding  | One startup-selected binding advertises process-local capabilities; no persisted manifest, reflection, per-run owner probing, or mixed lifecycle authority                    | Adapter composition               |
| Per-operation capability gate | Authority actions run only when the selected binding proves every required capability; missing optional support records uncertainty without disabling siblings                | Recovery authorization            |
| Partial-merge compatibility   | Minimal 7.1, partial, and complete future adapters preserve the residual boundary and public projections while enabling only proven operations                                | Cross-version contract            |
| API rename containment        | Renaming or reorganizing owner APIs changes only the base adapter and its tests, not durable storage, public methods, evidence values, or wake semantics                      | Anti-corruption boundary          |
| Authorization ordering        | Caller authorization and canonical session/route resolution complete before any durable row; denied or ambiguous intake creates no durable execution                          | Intake and security               |
| Reservation/acceptance split  | Durable occurrence reservation precedes owner dispatch; exact owner receipt precedes external accepted/success framing; an uncertain reservation cannot blind-dispatch        | Intake and owner acceptance       |
| Live capability isolation     | Host objects, approval leases, callbacks, process handles, credentials, and cancellation controllers never enter durable metadata or inspection                               | Capability and privacy            |
| Captured-generation cleanup   | Abort, drain, expiry, force-clear, and teardown remain bound to the owner occurrence captured when recovery starts, even if the session now resolves differently              | Recovery fencing                  |
| Owner settlement ordering     | Execution terminal follows canonical result and execution-resource teardown; visible closeout advances separately and cannot be ACKed by execution success                    | Lifecycle settlement              |
| Terminal presentation policy  | Visible, intentionally silent, paused, client-tool continuation, and owner-delivered dispositions come from the owner rather than empty text or typing activity               | Closeout policy                   |
| Bounded reconciliation        | Owner scans and paginated projections use one absolute deadline, remaining per-item budgets, caps, cycle detection, cancellation, and sibling isolation                       | Worker and adapter bounds         |
| Explicit page completeness    | Storage pages return items, completeness, and filter-bound opaque continuation; silent clamps, fixed-prefix starvation, and incomplete absence decisions fail closed          | Store pagination                  |
| Query-plan boundedness        | Claim and reconciliation queries use matching indexes and stable keysets without unbounded scan/sort work inside writer transactions                                          | SQLite query plans                |
| State rollback composition    | Post-snapshot intake and recovery mutation are gated until commit unless a monotonic journal proves every accepted fact survives compensation                                 | Update and restart transaction    |
| Session lifecycle fencing     | Only committed canonical retirement/replacement advances generation; failed/no-op mutations do not, and stale correlations cannot target the new lifecycle                    | Session owner adapter             |
| Progress identity isolation   | Occurrence, logical child/attention, and requester/owner identities remain distinct when tool-call or provider identifiers are reused                                         | Progress and wake reconciliation  |
| Proof-boundary separation     | Route evidence, queue acceptance, owner consumption, canonical result persistence, transport acknowledgement, and user presentation cannot ACK one another                    | Delivery and closeout             |
| Multipart delivery evidence   | Ordered physical-part ids and kinds remain bounded and associated; aggregate or primary-id success cannot conceal a missing required part or strengthen proof                 | Delivery attempts                 |
| Optional capability isolation | An unavailable optional tool/provider and a deliberate no-tools run do not become failed admission or stale-work evidence unless the selected owner requires it               | Admission and liveness            |
| ACP/manual-spawn preservation | ACP manual-spawn child turn task suppression remains intact; plugin-subagent precedence and CLI fallback still work                                                           | Agent and subagent compatibility  |
| Pairing QR regression         | Webchat pairing QR display remains visible without persisting sensitive QR content                                                                                            | Adjacent channel compatibility    |
| Wake target resolution        | Owner, parent, peer, scheduled, Task Flow, external route, missing, unauthorized, and ambiguous targets resolve or fail closed with inspectable evidence                      | Wake routing                      |
| Wake queue contract           | Wake state, immutable occurrence mapping, logical identity, delivery revision, recurrence policy, attempts, acknowledgement, and lifecycle remain distinct                    | Wake storage                      |
| Projection claim fencing      | Coalescing advances delivery revision; an older claim records evidence for its attempted projection but cannot settle the newer projection                                    | Delivery attempts                 |
| Typed authority facts         | Delivery revision, claimed revision, suspension class, and any fact used by CAS, uniqueness, claim, retry, or authoritative query are typed and schema-validated              | Schema and mutation guards        |
| Safe resume fencing           | Automatic resume requires expected revision, an explicitly safe pre-attempt suspension class, and no attempted, unknown, or handoff evidence                                  | Wake recovery                     |
| Replay authority              | Automatic replay is denied unless operation registry, input material, idempotency, side-effect class, dedupe/CAS or reconciliation, and retention gates pass                  | Replay safeguards                 |
| CLI/Gateway inspection        | `operator.read` gates `durable.*` before state opens; gateway-wide reads use bounded/redacted allowlists. Agent/session refs do not grant access                              | Gateway auth and projections      |
| Degraded-config inspection    | Trusted local read-only inspection still works when unrelated optional runtime config is invalid, provided canonical state resolution remains unambiguous                     | CLI and Doctor composition        |
| Owner decision boundary       | Initial public API remains read-only; any later decision validates caller authority and source revision, calls the owner API, and records audit evidence                      | Owner adapters and audit          |
| Worker no-handler behavior    | Empty registry and no-handler rows fail closed and do not mark unknown side effects as handled                                                                                | Worker recovery                   |
| Claim/lease recovery          | Expired claimed run/step rows are inspectable and reclaim only when eligible, with SQLite row evidence                                                                        | Lease recovery                    |
| Clock and claim fencing       | Expiry is eligibility only; exact tokens/revisions fence writes, monotonic elapsed clocks drive in-process deadlines, and clock anomalies fail closed                         | Worker and store time semantics   |
| Retention and source deletion | Unresolved work pins minimum evidence; compaction preserves referenced proof; stale retries after the idempotency horizon cannot create new work                              | Retention and owner front doors   |
| Storage-pressure behavior     | Busy, WAL, disk-full, I/O, corruption, and read-only failures are bounded, stop new durable acceptance before success, and leave affected components degraded                 | SQLite and health                 |
| Single state authority        | Multi-connection access converges through one transactional store; divergent asynchronously replicated local histories are never merged                                       | Store composition                 |
| Internal session handoff      | Resolved session targets move through internal delivery handoff with durable evidence and no external transport claim                                                         | Internal delivery                 |
| External delivery             | Only claimed if the implementation includes external transport delivery and direct proof                                                                                      | External transport implementation |

### Exact Schema Gate

When implementation introduces the optional schema, enabled initialization
should create exactly these residual tables in addition to existing owner
tables:

`durable_execution_records`, `durable_event_evidence`,
`durable_execution_steps`, `durable_payload_refs`,
`durable_run_correlations`, `durable_timer_obligations`,
`durable_signal_evidence`, `wake_obligations`,
`wake_obligation_occurrences`, `delivery_attempt_evidence`, and
`uncertainty_facts`.

Tests should reject accidental lifecycle mirrors and extra ledgers, preserve
pre-existing official 7.1 owner rows, prove rollback across the shared-state
transaction boundary, and fail closed on future or incompatible durable schema
before any writable pragma, journal change, sidecar creation, or mutation. The
asserted table set is an explicit eleven-name contract rather than a set derived
from generated SQL. Unknown Durable-owned table namespaces, including plural
wake namespaces, are incompatible unless the canonical schema version advances.

Schema proof includes:

- fresh install and reopen with `integrity_check` and `foreign_key_check` clean;
- exact official 7.1 state upgrade without changing existing owner rows;
- injected DDL failure leaving no partial durable objects or version advance;
- future-version rejection preserving journal mode, schema, rows, user version,
  and the absence of new WAL/SHM sidecars;
- rollback to the accepted predecessor with durable tables dormant, followed by
  candidate reopen without data loss; and
- generated SQL, Kysely types, explicit table names, required indexes, and
  canonical schema metadata agreeing on one shape.

Schema-shape proof also asserts that mutation-authority facts are typed rather
than hidden only in generic JSON. The wake delivery revision, attempt-captured
revision, and auto-resume suspension class must participate in exact schema,
generated-type, migration, and compare-and-set tests. Descriptive metadata
remains bounded, namespaced, versioned, and unknown-field preserving.

### Acceptance Front-Door Gate

Runtime integration proof should cover Gateway `agent` RPC,
OpenAI-compatible HTTP, OpenResponses HTTP, local `agent` commands,
process-local runtime hosts including ACP and TUI adapters, channel auto-reply,
queued follow-up/heartbeat turns, and isolated cron execution. Each front door
should prove authorization and canonical routing before durable mutation,
durable reservation before owner dispatch, exact owner receipt before external
accepted or success framing, and one fenced terminal settlement after canonical
owner result, presentation settlement, and owner-required resource teardown.

Inject a crash after reservation and before owner dispatch, during owner start,
after owner acceptance and before receipt persistence, and after receipt
persistence but before the external response. Assert one stable occurrence, no
fallback owner, no false accepted response, and no automatic redispatch without
an exact idempotent owner-acceptance contract.

## Candidate Scenarios

### Canonical Owner Compatibility

- Bind the official 7.1 owner profile and prove existing Gateway, local command,
  channel, cron, session, task, flow, and subagent paths preserve current
  behavior with Durable disabled, observation-enabled, and authority-enabled.
- Bind a test runtime/session owner profile through the same contract; prove one
  durable reservation, one accepted durable identity, one volatile owner start,
  one terminal settlement, and unchanged public durable projections without an
  owner-specific schema migration.
- Register several non-overlapping task, flow, subagent, session, and execution
  adapters and assert composition succeeds. Then attempt two authoritative
  execution or session claims for the same scope and assert startup fails before
  intake.
- Make the selected owner reject start or disappear after reservation; assert no
  fallback owner is invoked, no external accepted response is emitted, and the
  reservation becomes denied, abandoned, admission-uncertain, or
  owner-decision-needed according to the proven boundary.
- Restart after persisting an approval ID, process ref, or capability-required
  marker but before owner settlement; assert no callback, lease, process handle,
  credential, or approval authority is reconstructed and no side effect replays.
- Reuse a run label or tool-call identifier across two owner occurrences; assert
  stale progress and settlement from the first occurrence cannot mutate the
  second, while logical child and owner/requester identities remain distinct.
- Feed canonical host progress, route-matched delivery evidence, queue
  acceptance, owner consumption, and transport acknowledgement independently;
  assert each advances only its declared proof boundary and cannot falsely ACK
  another obligation.
- Block owner-required execution-resource settlement behind a promise gate and
  assert the execution cannot become terminal. Separately block presentation
  settlement and assert execution may terminate while visible closeout remains
  at result availability and cannot claim user presentation.
- Capture a stale owner occurrence, replace it under the same session and run
  refs during abort, then prove drain, expiry, force-clear, and teardown affect
  only the captured occurrence and leave the replacement active.
- Hold owner-required process-tree or stream teardown behind a promise gate and
  prove Durable terminal settlement waits for the owner, while persisting no
  live handle.
- Return visible-timeout, intentionally silent, paused/resumable, client-tool
  continuation, and already-owner-delivered terminal dispositions with empty
  assistant text; assert only the owner-declared visible case creates closeout
  content and none is inferred from typing or tool activity.
- Start a deliberate tool-less run and a normal run with one unavailable
  optional capability; assert neither is classified as stale or failed unless
  the selected owner declares that capability required.
- Run the same contract suite against the official 7.1 base, a reproducible
  later tagged base, and current development head. Treat the latter two as
  forward-port evidence, not a reason to change the public PR base.

### Upstream Independence Matrix

- Bind a minimal official 7.1 adapter and assert recording, inspection, wake
  projection, and conservative restart classification work without importing or
  naming any later owner implementation.
- Bind a partially capable adapter that lacks destructive recovery,
  presentation-settlement, multipart-receipt, and quiescence capabilities;
  assert each unsupported authority action fails closed while unrelated facts,
  inspection, and healthy sibling reconciliation continue.
- Bind a complete future-owner adapter and run the identical storage, wake,
  proof-boundary, privacy, and public-projection suite without a schema or API
  migration.
- Register two candidate execution owners for the same scope, two session owners
  for the same scope, or a binding that mixes lifecycle phases across owners;
  assert composition fails before intake.
- Rename and reorganize every fake future-owner method behind its adapter;
  assert no durable runtime state, schema, CLI, Gateway method, or evidence-value
  change is required.
- Remove an optional capability after simulated restart; assert persisted refs
  do not reconstruct support, affected work becomes explicit uncertainty, and a
  fresh capable owner revision is required before authority resumes.
- Assert capability declarations are process-local, immutable after composition,
  absent from SQLite and public inspection, and not selectable per request.

### Bounded Recovery And State Transactions

- Stall one interruptible owner probe while healthy siblings reconcile; assert a
  fixed per-probe budget, one absolute pass deadline, bounded concurrency,
  healthy sibling convergence, and an inspectable unresolved outcome for the
  stalled item. Supply a synchronous unbounded adapter and assert binding
  validation denies authoritative absence reconciliation rather than pretending
  the outer deadline can interrupt it.
- Feed repeated, cyclic, endless-unique, and empty opaque cursors through an
  owner projection; assert cycle, item, byte, page, and deadline bounds without
  dropping a valid empty cursor or extending the absolute deadline per page.
- Cross the deadline during synchronous final-item processing and trigger caller
  cancellation after a page resolves; assert both are rechecked before mutation.
- Retry a previously unresolved item after its owner recovers; assert monotonic
  convergence without erasing sibling evidence or starting an unbounded loop.
- Snapshot whole mutable state, then attempt accepted intake and worker mutation
  during rollback-capable probation; assert they are rejected or quiesced until
  commit. In an alternate journal-capable harness, assert every post-snapshot
  fact is restored monotonically after compensation.
- Restore a pre-snapshot image and assert no accepted execution, wake,
  uncertainty, or delivery attempt can disappear silently.

### Session Lifecycle And Multipart Delivery

- Emit failed, no-op, and committed session retirement events; assert only the
  committed event advances generation and supersedes or suspends stale wake and
  closeout targets.
- Replace a session route while a stale callback is in flight; assert the old
  generation cannot mutate or settle the replacement, and no unobservable
  external lifecycle boundary is invented.
- Record one logical delivery with multiple ordered physical parts and distinct
  ids/kinds; assert part association survives dedupe and bounded inspection.
- Drop or ambiguously acknowledge one required part; assert the aggregate
  attempt cannot claim complete transport or user presentation merely because
  a primary id or another part succeeded.

### Disabled Paths Never Mutate

- Run CLI durable stats/list/why commands with durable runtime disabled and a
  fresh state directory; assert a non-success guidance result and no state files.
- Call Gateway durable inspection handlers with disabled runtime; assert
  `INVALID_REQUEST`, no result payload, and no SQLite files.
- Repeat with an existing non-durable state DB; disabled inspection must not add
  durable migration or runtime tables.
- Assert disabled Gateway startup does not load the durable startup, recovery,
  worker, or owner-adapter module graph.

### Runtime Opt-In Without Worker Mutation

- With durable runtime enabled and no worker flag, verify inspection can open the
  shared SQLite store and project existing records.
- Seed running/open records, start Gateway/runtime, and assert records remain
  running/open when worker recovery is disabled.
- Assert default prompts do not gain durable orchestration guidance unless
  durable runtime or explicit policy is enabled.

### Store Pagination And Query Plans

- Seed more than 500 unresolved wakes and open runs with tied timestamps. Page
  through every row exactly once using stable keysets and explicit `complete`
  and continuation fields.
- Reject an over-maximum limit instead of silently clamping it. Reject a cursor
  whose version, owner, cutoff, or filter does not match the request.
- Stop an owner or logical-identity scan before completion and assert it cannot
  authorize duplicate creation, supersession, retirement, absence, or recovery.
- Use multiple SQLite connections to page while eligible rows are inserted or
  updated; assert the fixed cutoff and keyset preserve deterministic convergence.
- Verify query plans for unresolved owner/global scans, logical identity lookup,
  exact occurrence lookup, claimable wakes, expired delivery claims, open runs,
  runnable steps, and uncertainty dedupe. Matching indexes must avoid temporary
  order-by trees and unbounded writer-transaction sorts.

### Schema Evolution, Retention, And Storage Faults

- Open the schema with binaries whose read/write ranges are older, current,
  supported-additive, and future-incompatible. Assert read-only behavior only
  where declared and no older writer mutates a newer incompatible shape.
- Exercise expand, backfill, verify, and contract phases with injected failure at
  each boundary. Assert version metadata advances only with the complete phase
  and predecessor rollback is denied when it is no longer a valid writer.
- Seed unknown operation kinds, lifecycle states, proof boundaries, recurrence
  policies, suspension classes, cursor versions, and internal metadata versions.
  Assert bounded redacted inspection and unknown-field preservation, but no
  claim, resume, replay, compaction, terminalization, or repair.
- Assert every correctness-critical field used by CAS, uniqueness, claim,
  retry/resume eligibility, or authoritative query is present as a typed column
  or index predicate and not sourced only from generic metadata.
- Keep a wake, attempt, uncertainty, correlation, checkpoint, and causation edge
  unresolved while compacting the root. Assert all minimum referenced evidence
  remains and each removed event range receives one bounded compaction summary.
- Delete or retire a canonical source through its owner front door and assert its
  residual obligations settle, supersede, tombstone, or transfer first. Simulate
  uncoordinated source loss and assert explicit uncertainty rather than silent
  completion or deletion.
- Retry an exact occurrence before and after its declared idempotency horizon.
  Assert the first is a no-op and the latter is expired or owner-decision-needed,
  never newly created because dedupe evidence was pruned.
- Jump wall time backward and forward while a claim is active. Assert monotonic
  in-process deadlines, exact token/revision fencing, no stale completion, and no
  side-effectful redispatch based only on wall-clock expiry.
- Inject SQLite busy/locked, WAL pressure, disk-full, read-only, I/O, corruption,
  and evidence-append failures. Assert bounded retry, no external calls in a
  write transaction, no new accepted response after failed reservation/receipt,
  no deletion of unresolved evidence, and component-scoped degraded health.
- Run concurrent connections against one shared transactional store and assert
  convergence. Present two divergent database histories and assert the system
  refuses heuristic merge or dual authority.

### Wake And Owner Attention

- Seed wake reasons including `child_terminal`, `fan_in_incomplete`,
  `restart_interrupted`, `delivery_unknown`, `side_effect_uncertain`,
  `no_handler`, and `operator_requested`.
- Assert legal transitions from `pending` to `handoff_accepted`, `acked`,
  `failed`, `suspended`, or `superseded`; from `handoff_accepted` to `acked`,
  `failed`, `suspended`, or `superseded`; and from `failed` or `suspended` only
  through their documented retry or decision paths.
- Assert only `acked` and `superseded` are terminal, and queue acceptance is not
  presented as attached-session consumption or external delivery.
- Assert duplicate wake creation from repeated child completion, worker scans,
  Gateway reports, and owner reconciliation dedupes by source revision and key.
- Assert exact retries of one occurrence remain idempotent while different
  occurrence keys for the same coalesced logical identity produce at most one
  non-terminal wake.
- Advance checkpoints and source revisions while the active wake is `pending`,
  `handoff_accepted`, `failed`, and `suspended`; assert the active projection
  advances without rewriting delivery-attempt evidence or adding queue entries.
- Race two store connections across an occurrence boundary and assert one
  unresolved wake, one stable target identity, and an explicit reconciliation
  disposition.
- Advance a wake projection while an older delivery claim is in flight. Assert
  the old attempt records evidence against its captured delivery revision but
  cannot ACK, fail, suspend, or otherwise settle the newer projection.
- End one delivery attempt as `delivery_unknown`; assert the attempt is terminal,
  the wake is `suspended`, a linked uncertainty remains unresolved, and no ACK or
  successful closeout projection is emitted.
- Resume only a suspension with the exact expected delivery revision and an
  explicitly safe pre-attempt capability/target class. Assert attempted,
  unknown, handoff-accepted, stale-revision, unknown-class, and owner-decision
  suspensions remain unchanged and inspectable.
- Inject failure after canonical projection update but before occurrence insert,
  and between occurrence remap and duplicate supersession. Assert the complete
  wake, occurrence, attempt, uncertainty, and lease transaction rolls back.
- Assert a later occurrence may be created after `acked` or `superseded` only
  when recurrence policy permits it and the canonical owner still requires
  attention.
- Assert different source owners, reasons, target identities, owner identities,
  and report routes remain distinct, and obsolete routes are superseded through
  owner reconciliation without a bounded global-scan correctness limit.
- Assert restart and repeated full reconciliation converge, queued payloads
  remain truthful after projection advancement, and historical terminal wakes
  remain inspectable without appearing as current unresolved work.

### Restart And Side-Effect Uncertainty

- Inject failures before and after tool dispatch, provider return, channel send,
  child spawn, and local commit boundaries.
- Assert recovery appends uncertainty facts such as `unknown_after_side_effect`,
  `interrupted_during_tool`, `lost_after_dispatch`, `delivery_unknown`, or
  `requires_owner_decision`.
- Assert automatic replay is denied while uncertainty is unresolved unless replay
  gates prove authority and idempotency.
- Assert a persisted owner, approval, process, or transport reference never
  satisfies a live-capability or fresh-authority gate after restart.

### Privacy, Retention, And Compaction

- Default persistence stores refs, hashes, bounded previews, and structured
  metadata, not raw prompts/tasks/tool payloads.
- Full input capture requires explicit opt-in and inspection authorization.
- Break unrelated optional provider, channel, tool, and model configuration;
  assert trusted local read-only inspection still projects existing durable
  facts when the state path is unambiguous, performs no repair, and reports the
  unrelated config error separately. Gateway inspection still requires normal
  authenticated startup.
- Call the real Gateway dispatcher without `operator.read`; assert denial before
  handler entry or durable store access and no disclosure of store or ref
  existence.
- Seed records for two Agents; assert an authorized operator can inspect both
  through the same bounded projection.
- Seed raw prompt, task, tool input/result, secret-marker, private path/endpoint,
  and unknown future metadata fields; assert the default inspection projection
  omits them.
- Enforce request, result, nested-collection, and serialization limits, plus any
  declared pagination bounds, before returning inspection data.
- Compaction preserves enough run, step, event, ref, claim, and recovery data to
  explain state while removing or truncating sensitive previews.

## Live Proof Policy

Runtime changes should include local or remote tests for their touched surface.
Live proof on an isolated test Gateway is relevant only when the change claims
runtime, session, wake, worker, or delivery behavior that local tests cannot
prove with maintainer-grade confidence. Docs-only changes can state that live
proof is not applicable when they claim no runtime behavior. This plan remains
subordinate to maintainer decisions about whether a shared durable runtime
belongs in core at all.

## Related

- [Durable Runtime Residual-Gap Architecture Proposal](/specs/durable-runtime-architecture)
- [Security and trust model](/gateway/security)
- [Operator scopes](/gateway/operator-scopes)
