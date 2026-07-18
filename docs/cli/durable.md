---
summary: "CLI reference for `openclaw durable` (opt-in durable runtime inspection)"
read_when:
  - Inspecting durable runtime runs, steps, events, refs, signals, timers, or coordination projections
  - Verifying whether the native durable runtime is enabled without mutating state
  - Reviewing durable runtime environment variables, storage, and input retention
title: "`openclaw durable`"
---

Inspect native Durable Runtime state for agent sessions, task runs, steps,
subagent links, signals, timers, refs, and coordination projections.

The durable runtime is opt-in. Unless `OPENCLAW_DURABLE_RUNTIME=1` is set,
`openclaw durable` commands report that the runtime is disabled and do not open,
create, or migrate the shared state database.

## Usage

```bash
OPENCLAW_DURABLE_RUNTIME=1 openclaw durable stats
OPENCLAW_DURABLE_RUNTIME=1 openclaw durable health
OPENCLAW_DURABLE_RUNTIME=1 openclaw durable runs
OPENCLAW_DURABLE_RUNTIME=1 openclaw durable runs --limit 25 --json
OPENCLAW_DURABLE_RUNTIME=1 openclaw durable show <runtimeRunId>
OPENCLAW_DURABLE_RUNTIME=1 openclaw durable timeline <runtimeRunId>
OPENCLAW_DURABLE_RUNTIME=1 openclaw durable steps <runtimeRunId>
OPENCLAW_DURABLE_RUNTIME=1 openclaw durable children <runtimeRunId>
OPENCLAW_DURABLE_RUNTIME=1 openclaw durable parents <runtimeRunId>
OPENCLAW_DURABLE_RUNTIME=1 openclaw durable why <runtimeRunId>
OPENCLAW_DURABLE_RUNTIME=1 openclaw durable signals <runtimeRunId>
OPENCLAW_DURABLE_RUNTIME=1 openclaw durable refs <runtimeRunId>
OPENCLAW_DURABLE_RUNTIME=1 openclaw durable timers <runtimeRunId>
OPENCLAW_DURABLE_RUNTIME=1 openclaw durable coordination <runtimeRunId>
OPENCLAW_DURABLE_RUNTIME=1 openclaw durable obligations list --limit 50
OPENCLAW_DURABLE_RUNTIME=1 openclaw durable wakes list --limit 50
OPENCLAW_DURABLE_RUNTIME=1 openclaw durable wakes inspect <wakeId>
OPENCLAW_DURABLE_RUNTIME=1 openclaw durable wakes acknowledge <wakeId> --reason <text>
OPENCLAW_DURABLE_RUNTIME=1 openclaw durable wakes resume <wakeId> --reason <text>
OPENCLAW_DURABLE_RUNTIME=1 openclaw durable wakes supersede <wakeId> --reason <text>
OPENCLAW_DURABLE_RUNTIME=1 openclaw durable uncertainty list --limit 50
OPENCLAW_DURABLE_RUNTIME=1 openclaw durable uncertainty resolve <factId> --kind <kind>
OPENCLAW_DURABLE_RUNTIME=1 openclaw durable delivery-attempts list <wakeId> --limit 50
```

## Commands

- `stats`: show the durable runtime store path and row counts.
- `health`: show enablement, authority mode, process health, and bounded store
  health counters.
- `runs`: list recent runtime runs.
- `show`: show one run with steps, links, signals, and timeline.
- `timeline`: show ordered durable runtime events for one run.
- `steps`: show durable runtime steps for one run.
- `children`: show child runtime links for one run.
- `parents`: show parent runtime links for one run.
- `why`: explain a run's current durable state, waiting reason, child counts,
  recovery diagnostic, safe next inspection commands, and available controls.
- `signals`: show pending and consumed signals for one run.
- `refs`: show state refs recorded for one run.
- `timers`: show timers for one run.
- `coordination`: show a bounded coordination projection for task or session
  runtime consumers.
- `obligations list`: list unresolved work projected from canonical source owners,
  wake obligations, uncertainty facts, child correlations, and expired leases.
- `wakes list`: list wake obligations with source owner and source reference.
- `wakes inspect`: inspect one wake, including target resolution, delivery
  attempts, and unresolved uncertainty.
- `wakes acknowledge`: record that the target consumed a wake.
- `wakes resume`: return an explicitly suspended wake to pending dispatch.
- `wakes supersede`: close a wake with an explicit operator decision.
- `uncertainty list`: list unresolved uncertainty facts.
- `uncertainty resolve`: resolve or supersede one uncertainty fact with a
  required resolution kind and optional evidence reference.
- `delivery-attempts list`: list delivery evidence for one wake obligation.

All commands support `--json`. `runs` and the new `list` commands also support
`--limit <count>`.

Wake controls accept `--expected-source-revision <revision>`, and uncertainty
resolution accepts `--expected-updated-at <timestamp>`, to reject stale
operator decisions after the inspected source changes.

## Enablement And Storage

Set `OPENCLAW_DURABLE_RUNTIME=1` to enable durable runtime recording and
inspection. Durable runtime state lives in the shared OpenClaw state SQLite
database at `state/openclaw.sqlite`.

When disabled:

- CLI inspection commands return a disabled status.
- Gateway durable coordination RPCs reject requests.
- Agent turn helpers use no-op lifecycle objects.
- The CLI does not create or migrate durable runtime tables.

When enabled, OpenClaw may create or migrate the durable runtime schema in the
shared state database before recording runs or answering durable inspection
queries.

## Environment Variables

| Variable                                   | Default  | Purpose                                                                             |
| ------------------------------------------ | -------- | ----------------------------------------------------------------------------------- |
| `OPENCLAW_DURABLE_RUNTIME`                 | disabled | Enables the durable runtime when set to `1`, `true`, `yes`, or `on`.                |
| `OPENCLAW_DURABLE_RUNTIME_STORE`           | `sqlite` | Selects the durable store backend. Only `sqlite` is supported in this slice.        |
| `OPENCLAW_DURABLE_WORKER`                  | disabled | Starts the recovery worker only when the durable runtime is also enabled.           |
| `OPENCLAW_DURABLE_WORKER_POLL_INTERVAL_MS` | `1000`   | Worker poll interval.                                                               |
| `OPENCLAW_DURABLE_WORKER_CLAIM_TTL_MS`     | `300000` | Claim lease time for worker-owned runs or steps.                                    |
| `OPENCLAW_DURABLE_WORKER_MAX_CONCURRENCY`  | `1`      | Maximum worker concurrency for this local-first slice.                              |
| `OPENCLAW_DURABLE_INPUT_PREVIEW_CHARS`     | `600`    | Maximum input preview characters stored by default. Set `0` to store metadata only. |
| `OPENCLAW_DURABLE_INPUT_TEXT`              | disabled | Store full input text inline only when set to `full` or `inline`.                   |
| `OPENCLAW_DURABLE_INPUT_FULL_MAX_CHARS`    | `16384`  | Maximum full input text characters stored when full input retention is enabled.     |

## Retention And Privacy

Durable intake records store metadata and a bounded message preview by default.
They do not store full user input unless `OPENCLAW_DURABLE_INPUT_TEXT=full` or
`OPENCLAW_DURABLE_INPUT_TEXT=inline` is set. Operators that need stricter
privacy can set `OPENCLAW_DURABLE_INPUT_PREVIEW_CHARS=0` to store metadata only.

The durable runtime records enough identity, status, recovery state, and state
refs to inspect where work stopped. With `OPENCLAW_DURABLE_WORKER=1`, it also
acts as the fail-closed authority for accepted agent/chat intake and dispatches
source-backed attention through canonical subagent and task owner APIs. It does
not replay an external side effect whose outcome is unknown.

## Related

- [Gateway protocol](/gateway/protocol#durable-coordination-rpcs)
- [Durable core architecture](/specs/durable-core-architecture)
- [Upstream 7.1 gap analysis](/specs/durable-core-upstream-7.1-gap-analysis)
- [CLI reference](/cli)
