# Durable Core Safety Slice

## Architecture judgment

Durable Core should make recovery conservative before it becomes clever. The minimum safe architecture is a durable control-plane record of runtime runs, recovery state, parent/child links, and diagnostics that lets coordinators reconcile work after restarts without fabricating hidden child progress.

The key safety boundary is side effects. If a process disappears after a child/agent/tool may have crossed a side-effect boundary, the runtime must not assume the operation is safely replayable. It should mark the state as `lost` or `unknown_after_side_effect`, expose why, and require a parent/coordinator/operator decision unless durable evidence explicitly proves retry safety.

## Must-have minimal slice

- `OPENCLAW_DURABLE_RUNTIME=1` disables synthetic subagent/child auto-resume by default.
- Deprecated synthetic subagent resume remains available only behind `OPENCLAW_LEGACY_SUBAGENT_AUTO_RESUME=1` for compatibility.
- `lost` is not automatically retryable.
- Retry controls are enabled only when recovery diagnostics say both:
  - `retryable === true`
  - `retrySafety === "safe_to_retry"`
- Post-side-effect restart ambiguity remains `unknown_after_side_effect` and is surfaced to parents rather than silently retried.
- Recovery diagnostics carry the fields consumers need for safe decisions: `recoveryReason`, `retrySafety`, `requiredAction`, `nextAction`, and safe recovery actions/evidence when available.
- Parent-led durable reconciliation is the default path for interrupted subagents and children.

## Defer / avoid

- Do not build a full coordinator platform in this slice.
- Do not introduce broad policy engines, schedulers, or new persistent background workers unless required by existing durable recovery tests.
- Do not auto-requeue child work from `lost` or `unknown_after_side_effect` states.
- Do not infer retry safety from terminal status alone.
- Do not replace existing stores or public shapes beyond the minimal diagnostic/control fields already needed by tests and projections.

## Upstream-friendly principles

- Prefer additive diagnostics and narrow guards over invasive rewrites.
- Keep compatibility behavior explicit, named, and gated.
- Preserve current public APIs where possible; tighten semantics at projection/control boundaries.
- Make unsafe states visible and actionable rather than hiding them behind automatic recovery.
- Keep code reviewable: small condition changes, comments that describe compatibility mode honestly, and tests that encode safety invariants.

## Phased plan

1. **Capture safety contract**: document the conservative Durable Core behavior and compatibility boundary.
2. **Verify gates**: inspect existing recovery, restart interruption, projection, and orphan subagent code for the minimal invariants.
3. **Patch only gaps**: update comments or code only where behavior/documentation is missing or misleading.
4. **Run targeted tests**: durable recovery, restart interruption, coordination projection, and subagent orphan recovery.
5. **Run adjacent durable tests**: durable subagent and startup coverage.
6. **Run core type gate**: `npm run tsgo:core` even if long.
7. **Stop at minimal safety**: defer broader orchestration/coordinator work unless a failing test proves it is required.

## Validation gates

- `node scripts/run-vitest.mjs src/agents/subagent-orphan-recovery.test.ts src/durable/recovery.test.ts src/durable/restart-interruption.test.ts src/durable/coordination-projection.test.ts`
- `node scripts/run-vitest.mjs src/durable/subagent.test.ts src/durable/startup.test.ts`
- `npm run tsgo:core`

Passing these gates indicates the minimal Durable Core safety slice is enforced without expanding scope into a full coordinator platform.
