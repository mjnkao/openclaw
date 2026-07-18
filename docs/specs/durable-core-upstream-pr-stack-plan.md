---
summary: "Upstream submission plan for the official 7.1 durable-core work, including the disposition of PR1 and PR2 and the replacement implementation sequence."
read_when:
  - Preparing or reviewing durable-core pull requests
  - Deciding whether to update or supersede PR 107375 or PR 110143
  - Splitting the durable-core integration branch into upstream-reviewable changes
title: "Durable Core Upstream PR Stack Plan"
---

# Durable Core Upstream PR Stack Plan

## Decision

Keep PR [#107375](https://github.com/openclaw/openclaw/pull/107375) as the
docs-only architecture decision gate. Update only its current-head/proof text
and ask the current Task Flow/durable-state owner to accept, narrow, or reject
the residual owner-first boundary.

Close PR [#110143](https://github.com/openclaw/openclaw/pull/110143) as
superseded. Do not force-push the current integration branch over it. The PR is
too stale and its persisted/runtime boundary is no longer the intended design.

Submit the replacement durable-core implementation as five serial,
independently reviewable PRs after PR1. This produces a six-PR durable-core
series including the architecture gate. Submit the context compaction and hard
overflow correction as one independent PR because it fixes existing agent-turn
correctness without requiring durable-core schema or runtime enablement.

The five high-rated beta 3 PRs remain proof archives, not merge candidates. See
[Durable Core Beta 3 PR Stack Audit](/specs/durable-core-beta3-pr-stack-audit)
for the reviewed invariants that the replacement series must preserve.

## Audited Revisions

| Item               | Revision or state                    | Finding                                                                        |
| ------------------ | ------------------------------------ | ------------------------------------------------------------------------------ |
| Official 7.1 base  | `release/2026.7.1` at `0790d9f593ad` | Canonical target for this series                                               |
| Integration branch | `e33f7fb7f22`                        | Nine commits ahead of the exact official 7.1 base; pushed to `mjnkao/openclaw` |
| PR1                | `d0294fa6c3f7`                       | Merge-clean, docs-only, and ready for a maintainer ownership decision          |
| PR2                | `cb0a5b5a9b0b`                       | Merge-conflicted and 2,266 base commits behind the official 7.1 release head   |

The integration branch is a live-tested reference and extraction source. Its
roughly 20,000 added lines across 100 files make it unsuitable as one upstream
PR.

## Why PR1 Stays

PR1 is already limited to four documentation files. Its latest automated review
found no contributor-facing code or documentation defect and correctly left one
maintainer decision open: whether the proposed residual cross-owner contract
complements or duplicates Task Flow and other existing owners.

That is the correct gate. Runtime tests cannot decide a product ownership
boundary. PR1 should remain provisional and unlisted until an owner sponsors it.
The next update should be intentionally small:

- correct the stale exact-head reference in the PR body;
- state that existing `flow_runs`, `task_runs`, `subagent_runs`, session,
  delivery, restart, ACP, audit, and lease owners remain canonical;
- state that the old PR2 will be superseded rather than used as proof of the
  proposed architecture;
- request an explicit accept/narrow/reject decision from the Task Flow or
  durable-state owner.

Do not replace PR1 with the current implementation documentation. Doing so would
erase a useful review trail and turn a small ownership question into another
large documentation review.

## Why PR2 Must Be Superseded

PR2 is not repairable by adding live logs or rebasing alone.

### Stale and oversized

- 60 files, 12,260 additions, and only 17 deletions;
- combines schema, storage, worker, recovery, CLI, Gateway, protocol, generated
  types, tests, and proof documents;
- branch merge base is `133ce01e4ae`, while official 7.1 is now
  `0790d9f593ad`;
- GitHub reports the PR as merge-conflicted.

### Wrong compatibility boundary

PR2 declares durable tables in the unconditional shared-state bootstrap while
describing durable runtime as opt-in. Its review correctly identifies that a
disabled installation would still acquire the new persistent model whenever
the normal shared database is created or upgraded.

The current integration branch still needs this boundary corrected before any
replacement foundation PR: durable DDL remains in
`src/state/openclaw-state-schema.sql`. Passing live restart, overflow, and
compaction tests does not prove disabled-install compatibility.

The replacement must move durable schema creation behind an explicit enabled
migration owner, while continuing to use the shared OpenClaw SQLite database
when enabled. This preserves the possibility of same-database owner/outbox
transactions without changing disabled installations. A disabled fresh install
must create no durable tables, and read-only disabled commands must create no
database.

### Wrong ownership shape

PR2 treats the new runtime tables as a broad control plane before the existing
Task Flow, task, subagent, delivery, session, restart, ACP, audit, and lease
owners are formally integrated. The current owner-first design is narrower:

- existing owners retain lifecycle authority;
- generic execution records are only for agent turns or operations with no
  existing lifecycle owner;
- durable core stores source-backed correlation, obligation, attempt, and
  uncertainty facts that no one owner can safely represent;
- owner adapters validate current source revisions and call owner front doors;
- recovery never infers external delivery or blindly replays an uncertain side
  effect.

Because these are architectural changes, reusing PR2's review thread as though
it were the same implementation would make review provenance misleading.

## Replacement Series

### PR1/6: Residual owner-first architecture decision

Use existing PR #107375. Docs only. No schema, runtime, API, CLI, or delivery
claims. Exit gate: an identified owner accepts, narrows, or rejects the
residual boundary and optional migration ownership.

### PR2/6: Opt-in storage and execution evidence foundation

Scope:

- explicit enabled-only durable schema lifecycle in shared SQLite;
- exact residual and generic-execution table set approved by PR1;
- storage guards, terminal immutability, idempotency, bounded payload refs, and
  future-schema fail-closed behavior;
- generic execution, event, step, ref, correlation, timer, and signal storage;
- no Gateway, CLI, startup worker, owner adapter, or agent integration.

Required proof:

- disabled fresh install: no database from disabled durable reads and no durable
  tables from normal shared-state bootstrap;
- enabled fresh install: exact tables and indexes;
- enabled existing-state upgrade and repeated idempotent open;
- unknown future schema: no DDL, ALTER, backfill, or mutation before failure;
- generated schema/type guardrails and private-file permissions.

### PR3/6: Execution authority, leases, and recovery classification

Scope:

- intake envelopes and operation registry;
- executor, checkpoint, heartbeat, and `state_leases` authority;
- startup/recovery classification and health projection;
- fail-closed uncertain-side-effect and missing-handler behavior;
- no cross-owner dispatch, public Gateway mutation, or CLI control surface.

Required proof: crash boundaries, stale-token rejection, automatic lease
renewal, terminal immutability, no-handler uncertainty, and restart convergence
without semantic replay.

### PR4/6: Attention outbox and canonical owner adapters

Scope:

- `wake_obligations`, `uncertainty_facts`, and
  `delivery_attempt_evidence` contracts;
- source-owner/ref and source-revision requirements;
- task and subagent source adapters plus the session dispatch contract;
- lease-backed dispatcher, bounded retries, suspension, and owner validation;
- reconciliation fallback for owner APIs that cannot share one transaction;
- no broad public control API.

Required proof: exactly one source-backed obligation, crash-after-owner-commit
reconciliation, target-resolution states, persist-before-side-effect, stale
claim fencing, slow-hook claim renewal, atomic attempt/wake finalization,
close/reopen split-state repair, retry caps, and no false external-delivery or
acknowledgement claim.

### PR5/6: Agent, session, task, subagent, and restart front doors

Scope:

- durable agent-turn intake and terminal classification;
- fail-closed Gateway CLI behavior while durable authority owns a turn;
- session attention handoff through the existing persisted session-delivery
  queue, followed by the canonical system-event/heartbeat path;
- generation-fenced delivery by default, with any cross-generation policy made
  explicit and owner-controlled;
- distinct evidence for queue acceptance, attached-session consumption, and
  external user delivery;
- task and subagent source-fact production without mirrored lifecycles;
- restart handoff correlation and visible parent/report-route attention;
- no prompt-only correctness dependency.

Required proof: live process restart during a tool call, parent/subagent timeout
and orphan cases, gateway restart during deferred work, duplicate observation,
crash after queue acceptance, restart recovery, session reset/generation change,
attached-session consumption, and a normal post-fault turn. Each case must end
in safe progress, a visible obligation, or explicit uncertainty rather than
silence. Queue acceptance must not be labeled as external or user delivery.

### PR6/6: Read-only operations surface

Scope:

- protocol schemas and authorized Gateway reads;
- `openclaw durable` health, timeline, obligation, attempt, uncertainty, and
  explanation commands;
- no public wake or uncertainty mutation in the initial upstream surface;
- normal OpenClaw configuration/doctor integration instead of permanent growth
  of environment-only product controls;
- redacted current-head live proof and operator documentation.

Required proof: disabled reads are side-effect free, authorization is enforced,
invalid protocol input fails before opening state, output is bounded, generated
clients and packaged SDK consumers remain compatible, and no write method is
advertised. Owner mutations require a later, separately approved PR with caller
identity, source revision, idempotency, reason, audit evidence, and owner-adapter
delegation.

## Independent Correctness PR

Submit manual-compaction checkpointing and hard-overflow terminal
classification separately from the durable series. These changes protect
ordinary official 7.1 agent turns even when durable runtime is disabled and can
be reviewed against existing agent/session semantics without accepting a new
schema or control plane.

Required proof:

- manual compaction checkpoints before summarization and preserves markers in
  the next turn;
- hard context overflow terminates visibly instead of leaving the caller
  waiting;
- successful compaction and ordinary turns retain existing behavior.

## Submission Rules

- Every implementation PR must be rebased on the current official 7.1 release
  head at submission time.
- Do not submit the integration branch as one PR.
- Do not submit PR2/6 until PR1 has an explicit owner decision.
- Keep each PR compiling and testable on its own; use serial submission after
  the previous dependency merges instead of cumulative cross-fork diffs.
- No new table or public mutation method without an owner, stable source ref,
  source revision, dedupe/idempotency rule, retention policy, authorization
  rule, recovery consumer, and executable proof.
- Preserve closed PR2 as historical review evidence; close it with a superseded
  explanation rather than deleting its branch or rewriting its history.

## Current Release Blockers

The integration branch is suitable for OpenClaw E evaluation but is not yet an
upstream-ready series. Before extracting PR2/6:

1. remove unconditional durable DDL from normal shared-state bootstrap;
2. define and test the enabled-only migration owner;
3. reduce environment-only product controls into the normal config/doctor
   surface, retaining environment variables only where project convention
   requires them;
4. remove the single-backend `OPENCLAW_DURABLE_RUNTIME_STORE` selector;
5. replace the current in-memory system-event handoff with persisted,
   generation-fenced session delivery and attached-consumption proof;
6. remove initial public wake/uncertainty writes and rename the hyphenated
   `durable.delivery-attempts.list` method to an approved camelCase RPC shape;
7. port the beta 3 close/reopen split-state, atomic-abort, slow-hook renewal, and
   session restart/reset fixtures;
8. complete planned adapters for flow, delivery queue, restart/boot, and ACP or
   explicitly move them to later scope without claiming full owner coverage;
9. convert the live fault run into redacted, reproducible current-head proof.

Until those gates pass, the correct statement is: the OpenClaw E integration is
a strong live-tested reference, not a complete upstream-ready durable-core
submission.
