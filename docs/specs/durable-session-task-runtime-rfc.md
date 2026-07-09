---
title: Durable Session and Task Runtime RFC
summary: "Proposal for an opt-in durable runtime layer for OpenClaw agent sessions, tasks, steps, subagent fan-in, and restart recovery."
read_when:
  - Evaluating durable coordination for long-running agent work
  - Designing restart-safe agent sessions, task runs, or subagent fan-in
  - Reviewing the boundary between core runtime state, sessions, tasks, Task Flow, and plugins
---

# Durable Session and Task Runtime RFC

Status: proposed

This RFC is PR1/5 in the durable runtime stack. It is intentionally docs-only:
it records the root cause, proposed core boundary, validation plan, and review
sequence before any runtime implementation lands.

OpenClaw already has sessions, background tasks, Task Flow, Workboard, plugin
state, channel delivery queues, and subagents. Those surfaces are useful, but
none of them is the shared low-level ledger that answers: which durable runtime
run owns this turn, child, tool step, signal, or timer; what state is it in; and
what should happen after a process restart?

The proposed durable core is a small, opt-in, local-first substrate for that
runtime ownership. It is not a general-purpose workflow engine and it does not
replace Task Flow, background tasks, sessions, Workboard, plugins, Temporal,
Restate, Hatchet, or LangGraph.

## Root Cause And Observed Issues

The recurring failure is not one missing UI panel or one product feature. The
root cause is that OpenClaw has several durable-ish records, but no shared
runtime identity/lifecycle ledger for agent coordination across process,
session, channel, and subagent boundaries.

Observed local and upstream symptoms include:

- Parent/child coordination can become silent: a parent session yields to
  subagent work but the durable parent/child relationship is not the canonical
  recovery boundary, so fan-in can be missed after children finish.
- Parent turns can be treated as terminal too early even though they are really
  waiting for a child, human signal, timer, retry, or outbound delivery effect.
- A gateway restart can lose in-memory run/child relationships and leave
  operators without a concise answer for whether a run is runnable, waiting,
  failed, lost, or complete.
- Parallel branches can interfere with each other: one failed or overflowing
  child can obscure sibling progress when there is no durable child-link and
  fan-in state to inspect.
- Channel/plugin-specific delivery paths have repeatedly needed bespoke
  durability repairs, which is a signal that OpenClaw lacks a common low-level
  runtime boundary for recoverable work.
- Related public work shows the same class of issue: #96853 reconciles stale
  `running` session rows after live work has disappeared; #97715 caps native
  subagent completion retries when a parent handoff is permanently non-durable;
  #90300 routes recovered agent-command replies through the durable outbound
  hook path; #97507/#97508 and the superseded #98717/#98718 durable stack
  explored overlapping RFC/implementation boundaries. PR #101828 is the
  canonical replacement RFC for the narrower durable-core boundary.

These issues justify a core runtime layer because individual fixes can repair a
single stale row, retry loop, or delivery hook, but they do not establish the
shared identity, event ordering, child links, recovery markers, and inspection
model needed by future session/task/subagent work.

## Durable Core Boundary

The durable core owns runtime semantics, not product semantics:

- durable runtime runs and steps, not product-specific cards or dashboards;
- bounded metadata, refs, hashes, ids, labels, timestamps, and event ordering,
  not raw prompts, private tool payloads, or large outputs;
- parent/child links and fan-in state, not business-process policy;
- restart-safe lifecycle states, not default-on automatic retries;
- read-only inspection first, with write controls added only when routed through
  the real session/task runtime contracts.

Task Flow, Workboard, CLI, Control UI, plugins, and channel integrations may
project from this core, but they should not define the core persistence model.

## Proposed Core Components

The core model should include:

- `runtime_run_id`: one durable unit of execution, such as an agent turn, task
  run, or subagent child run.
- `operation_kind`: stable logical operation name, for example `agent.turn`,
  `task.run`, `subagent.child`, `tool.step`, or `channel.delivery`.
- `parent_runtime_run_id`: optional parent used for subagent and branch fan-in.
- `step_id`: a step inside a runtime run, such as `agent_invocation`,
  `tool_call`, `fan_in`, `timer`, `human_signal`, or `delivery`.
- `message_id`, `turn_id`, and `agent_invocation_id`: correlation across
  inbound messages, chat turns, and model/agent invocations.
- `event_seq`: append-only event ordering inside a runtime run.
- `idempotency_key`: duplicate intake and safe retry protection.
- `checkpoint_ref`: pointer to external state/artifacts without copying large
  payloads into runtime metadata.
- `signal_id`: human approval, cancellation, resume, or other signal identity.
- `recovery_state`: restart-safe markers such as `runnable`, `claimed`,
  `running`, `waiting_child`, `waiting_signal`, `waiting_timer`,
  `retry_scheduled`, `unknown_after_side_effect`, `lost`, and `terminal`.

Durable tables should be named around runtime semantics, for example
`durable_runtime_runs`, `durable_runtime_steps`, `durable_runtime_events`,
`durable_runtime_refs`, `durable_runtime_links`, `durable_runtime_timers`, and
`durable_runtime_signals` in the shared OpenClaw state database.

## Full 5-PR Stack

This RFC intentionally splits the work so maintainers can review architecture,
core persistence, recovery behavior, schema lifecycle, and worker policy in
sequence:

1. **PR1 — Durable core RFC and root-cause plan (#101828).** Docs-only. Defines
   the canonical durable-core boundary, observed failure modes, non-goals, and
   validation plan. Current ClawSweeper gate is 🦐 gold shrimp; remaining
   decision is maintainer product direction/canonical RFC choice.
2. **PR2 — Durable runtime core (#102495).** Implements the opt-in shared
   runtime state, SQLite store, gateway/operator read surfaces, lifecycle
   primitives, typed session effects, QR-regression repair, and core tests on
   top of PR1.
3. **PR3 — Durable agent/session/subagent recovery.** Wires agent and subagent
   restart/upgrade-safety behavior on top of the core and proves parent/child
   recovery semantics.
4. **PR4 — Schema lifecycle and recovery hardening.** Focuses on durable SQLite
   schema lifecycle, upgrade safety, future-version guards, and recovery-only
   proof before worker automation depends on the persisted model.
5. **PR5 — Opt-in workflow worker loop.** Adds the worker loop last, keeping it
   opt-in/default-safe so runtime automation is reviewed separately from the
   persistence and recovery boundary.

Promotion is sequential: PR3 waits for PR1 and PR2 to meet the gold-shrimp
threshold; PR4 waits for PR3; PR5 waits for PR4.

## First Review Slice

The first implementation after this RFC should be conservative:

1. Add durable runtime schema to shared `state/openclaw.sqlite` with
   upgrade-safe tests.
2. Keep the feature disabled by default behind an explicit flag.
3. Record bounded metadata only.
4. Add read-only CLI/Gateway timeline and projection APIs before write controls.
5. Keep yielded parent runs inspectable as `waiting_child` or `waiting_signal`.
6. Ensure direct subagent continuation/fan-in can close the parent durable run
   only when the parent continuation actually succeeds.
7. Do not advertise cancel/retry/resume/signal controls until matching write
   contracts exist in the real session/task runtime.

## Non-Goals

- Do not implement a general-purpose workflow engine.
- Do not make durable runtime mandatory for simple chat.
- Do not couple core runtime state to Workboard, Task Flow, a dashboard, or a
  channel plugin.
- Do not persist raw prompts, private tool payloads, or large outputs in runtime
  metadata.
- Do not add automatic retry/resume policy as default behavior before operators
  can inspect and configure it.
- Do not expose write controls that cannot route through the real runtime.

## Maintainer Decision Requested

Please decide whether #101828 should be the canonical durable-core RFC for the
follow-up implementation stack. The recommended answer is yes, with these
constraints: keep the first slice opt-in/local-first, generic/runtime-oriented,
shared-state backed, read-only before write controls, and separate from Task Flow
or Workboard product ownership.
