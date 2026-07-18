---
summary: "Audit of the five high-rated durable-core PRs built from the 2026.7.1 beta 3 anchor, including the reviewed invariants that must survive the official 7.1 rewrite."
read_when:
  - Comparing the beta 3 durable stack with the official 7.1 owner-first implementation
  - Reusing proof or design decisions from PRs 104246, 104327, 105660, 105844, and 106443
  - Deciding which old durable PRs to close or supersede
title: "Durable Core Beta 3 PR Stack Audit"
---

# Durable Core Beta 3 PR Stack Audit

## Executive Decision

The five beta 3 PRs are valuable design and proof archives, but they are not a
mergeable upstream stack for the final official 7.1 design.

Their ratings mean that ClawSweeper found strong implementation and proof signal
at each reviewed head. The ratings do not mean that maintainers accepted the
product owner, shared-schema commitment, cross-owner source-of-truth model, or
release sequence. Every final review still required a maintainer architecture
decision.

The correct reuse policy is selective:

- preserve the crash, claim, atomicity, inspection, and internal-delivery
  invariants that earned the ratings;
- preserve exact-head live proof scenarios as regression tests;
- replace mirrored lifecycle ownership with official 7.1 owner adapters;
- fix the global-schema opt-in boundary that the later PR2 review exposed;
- restore persisted session handoff, which the current official 7.1 rewrite
  accidentally regressed to an in-memory system-event queue;
- keep the first public surface read-only;
- close the old cumulative PRs as superseded archives once their evidence is
  linked from the replacement plan.

## Base and Topology

All five PRs target `release/2026.7.1` on GitHub, but their implementation was
built from the tagged beta 3 anchor `619a8728905`. The current official 7.1 head
used by the rewrite is `0790d9f593ad`, 184 release commits after that anchor.
The beta-to-official interval changes 819 files with about 36,800 additions and
6,300 deletions.

The public PRs are cumulative snapshots rather than review-isolated GitHub
diffs:

| PR                                                          |            Public total |                                  Focused slice | Final rating            | Main unresolved decision                                           |
| ----------------------------------------------------------- | ----------------------: | ---------------------------------------------: | ----------------------- | ------------------------------------------------------------------ |
| [#104246](https://github.com/openclaw/openclaw/pull/104246) |           5 files, +381 |                                      Docs only | Platinum                | Canonical RFC, owner, and release target                           |
| [#104327](https://github.com/openclaw/openclaw/pull/104327) | 115 files, +19,042/-244 |         106 files, about +18,536/-223 over PR1 | Platinum; proof diamond | Seven-table projection, support surface, migration/retention owner |
| [#105660](https://github.com/openclaw/openclaw/pull/105660) | 121 files, +24,462/-244 |                  25 files, +5,444/-24 over PR2 | Diamond                 | Core sponsorship and predecessor schema order                      |
| [#105844](https://github.com/openclaw/openclaw/pull/105844) | 129 files, +27,288/-264 |                 25 files, +2,924/-118 over PR3 | Platinum; proof diamond | Public protocol commitment and predecessor upgrade                 |
| [#106443](https://github.com/openclaw/openclaw/pull/106443) | 129 files, +27,605/-261 | 6 files, +372/-50 over PR4 implementation head | Platinum; proof diamond | Session generation/reset behavior and attached consumption         |

The cumulative topology makes every later PR appear to add the complete stack.
It also allowed unrelated release and CI merge history into implementation
branches. The replacement series must submit serial, compiling slices against
the current official release head.

## PR1: Beta 3 RFC

PR #104246 successfully established the root-cause vocabulary:

- coordinator silence;
- restart/interruption loss;
- stale running work;
- parent/child handoff gaps;
- delivery and attention uncertainty;
- side-effect uncertainty.

Its review required two contributor fixes: acknowledge the existing
SQLite-backed owners and remove PR-stack workflow from public documentation. The
final review found no mechanical defect but still required maintainers to choose
between a shared durable core and owner-specific evolution.

The newer PR #107375 is the better architecture gate because it is unlisted,
removes beta-specific publication claims, and explicitly keeps Task Flow and
other official 7.1 owners authoritative. PR #104246 should therefore be
preserved as review history and closed in favor of #107375.

## PR2: Runtime Foundation

PR #104327 proved substantial behavior:

- fresh install and restart/reopen;
- upgrade from v2026.6.11;
- SQLite integrity;
- Gateway and CLI inspection;
- live parent-visible completion;
- terminal row immutability;
- disabled runtime and separately gated worker behavior.

The review also recorded the permanent cost: seven shared tables, five runtime
environment controls, one RPC, and two request context fields. Its final verdict
explicitly described the durable rows as a cross-owner projection whose
divergence, repair, retention, upgrade, and rollback contract required owner
approval.

Two old review lessons must be retained:

- do not expose a backend-selector setting when only SQLite exists;
- keep PR proof snapshots out of permanent public documentation.

The current official 7.1 integration regressed the first point by reintroducing
`OPENCLAW_DURABLE_RUNTIME_STORE`. It should be removed before upstream
submission.

The old review did not flag that durable DDL was part of unconditional shared
state bootstrap. The later review of PR #110143 correctly caught this: opt-in
runtime cannot mutate every disabled installation's schema. A high rating on the
older branch is not proof that this compatibility boundary is correct.

## PR3: Wake Replay

PR #105660 contains the strongest reusable distributed-systems work in the old
stack. Its focused slice established:

- stable target resolution and deduplicated wake producers;
- a persisted attempt before invoking a side effect;
- one lease claim per attempt;
- stale claimant rejection;
- automatic renewal while a slow delivery hook runs;
- atomic terminalization of attempt and parent wake;
- close/reopen repair without replaying an already terminal side effect;
- no duplicate attempt rows or callback invocation.

ClawSweeper found and drove fixes for each critical boundary in sequence:

1. claim before delivery;
2. persist the attempted boundary before the side effect;
3. retain ownership through a slow hook;
4. finalize attempt and wake in one transaction;
5. prove close/reopen repair at the exact head.

The official 7.1 rewrite preserves most of this through `state_leases`,
`delivery_attempt_evidence`, claim renewal, unknown-outcome quarantine, and the
transactional `completeWakeObligationClaim`. The old close/reopen split-state
repair and atomic-abort fixtures should still be ported explicitly; current
tests cover fencing and expired uncertainty but are thinner than the reviewed
beta 3 crash matrix.

The rewrite improves ownership by replacing generic producer modules with
source-backed task/subagent/session adapters and by removing the independent
cleanup and dedupe tables.

## PR4: Inspection-First Surface

PR #105844 intentionally removed public wake mutations after review. Its final
public contract had five read-only methods and zero write methods:

- `durable.coordination.get`;
- `durable.obligations.list`;
- `durable.wake.list`;
- `durable.wake.inspect`;
- `durable.wake.deliveryAttempts.list`.

This was not merely an implementation detail. It avoided premature commitments
for authorization, caller identity, terminal-state mutation, audit semantics,
idempotency, and owner lifecycle changes. The packaged plugin SDK and generated
protocol compatibility were also proven.

The current official 7.1 integration regressed this boundary by exposing wake
acknowledge, resume, supersede, and uncertainty resolution through both Gateway
and CLI. Those public writes must be removed from the initial upstream series.
Store-internal primitives may remain for tests and owner implementations, but a
public mutation PR must wait for accepted owner/audit policy.

The current method `durable.delivery-attempts.list` is also inconsistent with
the repository's camelCase RPC naming. The reviewed beta 3 form
`durable.wake.deliveryAttempts.list`, or a separately approved camelCase form,
is more upstream-friendly than a new hyphenated namespace.

## PR5: Persisted Session Handoff

PR #106443 added only six focused files over the PR4 implementation head. It
reused the existing persisted session delivery queue, wrote an idempotency key,
and recorded evidence that explicitly said:

- `internalDelivery: session_delivery_queue`;
- `noExternalSend: true`.

Its live proof started a real Gateway twice against the same state directory and
showed exactly one wake attempt and one persisted queue entry. Queue recovery
then consumed that one entry. This is the correct crash-safe handoff direction.

The review correctly stopped short of claiming end-user or external delivery.
It also left two design gaps:

- a queued `systemEvent` followed the canonical session key across `/new` or
  reset instead of being fenced to an expected session generation;
- the proof showed queue recovery, not attached-session prompt consumption.

The official 7.1 rewrite currently calls `enqueueSystemEventEntry`, whose module
is explicitly an in-memory, non-persistent queue. It then marks the wake
`delivered`. A Gateway crash after that mark and before heartbeat consumption
can permanently lose the notice. This is a release-blocking regression relative
to PR #106443.

The replacement session owner front door must:

1. enqueue through `session-delivery-queue-storage.ts` with a stable
   idempotency key;
2. persist the expected session id/generation or an explicit cross-generation
   delivery policy;
3. distinguish queue acceptance from attached-session consumption and external
   delivery;
4. avoid terminally acknowledging the wake before the accepted proof boundary;
5. prove crash/restart at enqueue, queue recovery, system-event admission, and
   consumption boundaries;
6. suspend or retarget safely when the session generation changed.

Default behavior should be generation-fenced. Cross-generation attention is a
separate owner policy, not an accidental consequence of canonical-key routing.

## Official 7.1 Preservation Matrix

| Reviewed beta 3 contract                           | Official 7.1 state                  | Decision                                        |
| -------------------------------------------------- | ----------------------------------- | ----------------------------------------------- |
| Existing owner stores acknowledged                 | Improved                            | Keep owner-first architecture                   |
| Terminal runtime/store immutability                | Preserved                           | Keep and expand tests                           |
| Claim before side effect                           | Preserved                           | Keep                                            |
| Persist attempted boundary                         | Preserved                           | Keep                                            |
| Slow-hook lease renewal                            | Preserved in dispatcher             | Add explicit slow-hook fixture                  |
| Atomic attempt/wake finalization                   | Preserved                           | Add atomic-abort and reopen fixtures            |
| Split-state repair without replay                  | Partial                             | Port old exact-head fixture                     |
| Internal versus external delivery evidence         | Preserved in evidence wording       | Keep explicit proof boundary                    |
| Persisted session-delivery handoff                 | Regressed to in-memory system event | Restore before release                          |
| Session generation/reset decision                  | Still unresolved                    | Fence by default and prove                      |
| Inspection-only public API                         | Regressed by public writes          | Restore read-only initial surface               |
| No single-backend selector                         | Regressed                           | Remove `OPENCLAW_DURABLE_RUNTIME_STORE`         |
| Disabled install has no durable schema side effect | Not satisfied                       | Move DDL behind enabled migration owner         |
| Minimal schema                                     | Improved from 12 to 10 tables       | Re-audit exact necessity and optional lifecycle |
| Owner source refs and revisions                    | Improved                            | Keep required                                   |
| No duplicate cleanup/dedupe lifecycle              | Improved                            | Keep removed tables removed                     |

## PR Disposition

Recommended GitHub state:

- keep #107375 open as the sole architecture decision gate;
- close #104246 as superseded by #107375;
- close #104327, #105660, #105844, and #106443 as beta 3 cumulative archives,
  with comments linking the replacement plan and naming the proof being retained;
- close #110143 as superseded because it is stale, conflicted, and still uses the
  rejected global-schema/parallel-owner shape;
- do not create a new implementation PR until the enabled-only schema boundary,
  persisted session handoff, and read-only public surface are present on a clean
  official 7.1 slice.

Closing must not delete the branches or rewrite their history. Their review
threads are durable evidence for the replacement tests.

## Effect on Stack Size

The optimal replacement remains six durable-core PRs including the RFC:

1. owner-first architecture decision;
2. enabled-only storage and generic execution evidence;
3. execution authority, leases, and recovery classification;
4. attention outbox, atomic dispatcher, and canonical owner adapters;
5. persisted generation-fenced session/restart handoff and official front-door
   integration;
6. read-only CLI/Gateway/protocol inspection plus current-head proof.

Manual compaction checkpointing and hard-overflow terminal classification remain
one independent correctness PR because they benefit official 7.1 without
durable-core enablement.
