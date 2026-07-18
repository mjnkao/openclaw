---
title: Durable Core Missing-Fact Owner Map
summary: "Source-owner map used to decide which official 7.1 tables are reused, extended, projected, or supplemented by residual durable-core records."
read_when:
  - Adding or changing a durable fact or table
  - Reviewing source ownership and schema necessity
  - Wiring task, flow, subagent, delivery, lease, restart, or session recovery
---

# Durable Core Missing-Fact Owner Map

## Rule

Extend the existing owner when it already owns the lifecycle. Add a durable-core
record only for a source-backed correlation, evidence item, obligation,
uncertainty fact, or generic execution that has no current owner.

Every added row must answer:

- What source fact caused this row?
- Which table/service owns that source fact?
- Why can the existing owner not answer the cross-owner question itself?
- What read or recovery path consumes the row?
- What dedupe, retention, privacy, authority, and terminal-state rules apply?

Every owner integration must also answer:

- Can the owner mutation and obligation share one transaction?
- If not, which bounded idempotent scan derives the missing obligation?
- Which owner adapter resolves the target, dispatches attention, and validates
  semantic decisions?
- What source revision prevents a stale decision after the owner changes?

## Owner Inventory

| Owner                           | Stable source ref       | Facts already owned                                                                                                | Missing residual fact                                                                    | Decision                                                                                  |
| ------------------------------- | ----------------------- | ------------------------------------------------------------------------------------------------------------------ | ---------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------- |
| `task_runs`                     | `task_id`               | Task lifecycle, runtime, requester/owner, parent flow, child session, progress, terminal outcome, delivery summary | Owner attention after lost/failed delivery; correlation to generic execution/uncertainty | Keep lifecycle in owner; produce source-backed obligation/uncertainty only when required  |
| `task_delivery_state`           | `task_id`               | Requester origin, last notified event                                                                              | Attempt-level proof and owner acknowledgement across delivery routes                     | Reuse state; correlate attempts/obligation, do not create another task delivery lifecycle |
| `flow_runs`                     | `flow_id`               | Flow lifecycle, revision, owner, current/blocked step, wait, cancellation                                          | Cross-owner fan-in/result attention and uncertainty                                      | Keep flow lifecycle/wait canonical; add correlation/obligation only                       |
| `subagent_runs`                 | `run_id`                | Child/requester/controller refs, timeout, lifecycle, result capture, cleanup, pending final delivery and attempts  | Generalized owner/report-route obligation and explicit attempt evidence                  | Produce exactly one obligation per terminal/overdue/pending-delivery source fact          |
| `delivery_queue_entries`        | `<queue_name>:<id>`     | Queue status, target, retry count, last attempt/error, recovery, platform send start                               | Proof boundary linking queue attempt to obligation and external result                   | Keep queue lifecycle; append/correlate delivery evidence                                  |
| `state_leases`                  | `<scope>:<lease_key>`   | Claim owner, expiry, heartbeat, payload                                                                            | Source-linked recovery classification and operator explanation                           | Use as authority; project expired lease into inspection/uncertainty                       |
| `gateway_restart_intent`        | `intent_key`            | Requested restart intent and reason                                                                                | Correlation to affected work                                                             | Add correlation/evidence only                                                             |
| `gateway_restart_handoff`       | `handoff_key`           | Old-process handoff, expiry, process instance, supervisor mode                                                     | Per-work uncertainty and owner attention                                                 | Source restart recovery facts from this row                                               |
| `gateway_boot_lifecycle`        | `boot_id`               | New-process startup outcome                                                                                        | Classification of work inherited from prior process                                      | Correlate; do not create a second boot lifecycle                                          |
| `gateway_restart_sentinel`      | `sentinel_key`          | Restart status, session/route/message/continuation diagnostics                                                     | Generalized durable obligation for unresolved continuation                               | Source obligation/uncertainty when sentinel remains actionable                            |
| `acp_sessions`                  | `session_key`           | ACP runtime identity, state, activity, errors                                                                      | Cross-owner target resolution and stale-owner evidence                                   | Reuse for resolution; obligation stores bounded resolution result                         |
| `acp_replay_sessions/events`    | session id and sequence | ACP replay stream                                                                                                  | Generic durable replay authority and owner attention                                     | Reuse stream; do not duplicate events unless cross-owner correlation is needed            |
| `current_conversation_bindings` | `binding_key`           | Current routing provenance                                                                                         | Durable target authorization result                                                      | Read for resolution; store only bounded evidence on obligation/attempt                    |
| session/agent SQLite owners     | agent/session identity  | Transcript, session lifecycle, compaction state                                                                    | Pending operational work independent of transcript compaction                            | Keep content/session state canonical; durable obligations remain separate                 |
| audit/diagnostic owners         | event/bundle id         | Operator history and diagnostics                                                                                   | Structured source links for recovery decisions                                           | Reuse and reference; do not duplicate generic audit logs                                  |

## Owner Adapter Inventory

| Status      | Adapter source owner     | Attention facts                                                        | Dispatch boundary                                           | Decision boundary                                            |
| ----------- | ------------------------ | ---------------------------------------------------------------------- | ----------------------------------------------------------- | ------------------------------------------------------------ |
| Implemented | `subagent_runs`          | Terminal, overdue/interrupted, pending/failed/suspended final delivery | Requester/controller session or stored report route         | Owner-validated retry/resume/abandon/ack; never mirror child |
| Implemented | `task_runs`              | Overdue active work, terminal/lost work, actionable delivery failure   | Task requester/owner route through task notification owner  | Task lifecycle remains in task APIs                          |
| Implemented | `session_store`          | Interrupted session-owned execution requiring owner attention          | Current canonical session system-event/heartbeat front door | Handoff only; no semantic replay or user-delivery claim      |
| Reused      | `task_delivery_state`    | Requester origin and last notification revision                        | Read and mutated only through the task owner API            | No standalone durable adapter or lifecycle                   |
| Planned     | `flow_runs`              | Blocked/overdue fan-in, unresolved wait, or owner-required decision    | Flow owner/controller route                                 | Flow continue/cancel/wait through flow APIs                  |
| Planned     | `delivery_queue_entries` | Retry exhausted or outcome unknown after dispatch                      | Existing queue/channel transport owner                      | Queue reconcile/supersede; never infer external success      |
| Planned     | restart and boot owners  | Interrupted work requiring classification or owner attention           | Affected work owner/report route                            | Reconcile/mark uncertainty; never semantic replay            |
| Planned     | ACP owners               | Missing/stale ACP target binding or interrupted ACP-owned operation    | Authorized current ACP route                                | ACP owner control with current binding revision              |

Adapters return generic bounded facts and evidence; model, provider, channel,
profile, skill, and project names do not enter the core adapter contract.

## Generic Execution Facts

Generic execution tables are allowed only for work that is not already a
`task_runs`, `flow_runs`, or `subagent_runs` lifecycle.

| Fact                            | Canonical record            | Source requirement                                                       | Notes                                              |
| ------------------------------- | --------------------------- | ------------------------------------------------------------------------ | -------------------------------------------------- |
| Accepted generic turn/operation | `durable_execution_records` | Existing owner/ref or documented root operation reason                   | Evidence/checkpoint anchor, not a task replacement |
| Ordered runtime/recovery event  | `durable_event_evidence`    | Inherited through execution-record FK                                    | Append-only sequence                               |
| Generic execution step          | `durable_execution_steps`   | Inherited through execution-record FK                                    | Operation-registry and lease governed              |
| Input/output/error/artifact     | `durable_payload_refs`      | Inherited through execution-record FK                                    | Bounded/hash/ref by default                        |
| Parent/child relation           | `durable_run_correlations`  | Both execution refs plus source metadata where owner records participate | Correlation, not lifecycle authority               |
| Generic retry/deadline/sleep    | `durable_timer_obligations` | Execution/step FK                                                        | Do not duplicate Task Flow wait state              |
| Approval/input/callback         | `durable_signal_evidence`   | Execution/step FK                                                        | Evidence; owner policy consumes it                 |

## Residual Contract Facts

| Producer/source fact                                                    | New record                                           | Required source owner/ref                                                                         | Dedupe key                                     | Consumer                                                  |
| ----------------------------------------------------------------------- | ---------------------------------------------------- | ------------------------------------------------------------------------------------------------- | ---------------------------------------------- | --------------------------------------------------------- |
| Subagent terminal requiring requester/controller/report-route attention | `wake_obligations`                                   | `subagent_runs` / `run_id`                                                                        | `subagent-terminal:<run_id>:<owner-or-route>`  | Parent/controller/operator inspection and delivery worker |
| Subagent overdue or orphaned                                            | `wake_obligations` plus optional `uncertainty_facts` | `subagent_runs` / `run_id`                                                                        | `subagent-overdue:<run_id>:<deadline>`         | Owner/controller decision                                 |
| Pending final delivery                                                  | `wake_obligations`                                   | `subagent_runs` / `run_id`                                                                        | `subagent-final-delivery:<run_id>:<target>`    | Delivery reconciliation                                   |
| Task lost or failed delivery requiring action                           | `wake_obligations`                                   | `task_runs` / `task_id`                                                                           | `task-attention:<task_id>:<terminal-revision>` | Requester/owner                                           |
| Fan-in cannot settle                                                    | `wake_obligations`                                   | `flow_runs`, `task_runs`, or source execution record                                              | `fan-in:<source>:<group>:<revision>`           | Flow/controller/owner                                     |
| No registered operation handler                                         | `uncertainty_facts` and `wake_obligations`           | Source execution owner/ref                                                                        | `no-handler:<execution>:<step>`                | Owner/operator                                            |
| Crash after possible dispatch                                           | `uncertainty_facts` and usually `wake_obligations`   | Dispatching source owner/ref                                                                      | `uncertain:<source>:<operation>:<attempt>`     | Replay gate and owner decision                            |
| Internal queue attempt                                                  | `delivery_attempt_evidence`                          | `delivery_queue_entries` / queue key, or `wake_obligations` / obligation id before queue creation | `delivery:<obligation>:<route>:<attempt>`      | Delivery inspection/reconciliation                        |
| External transport result                                               | finalize same `delivery_attempt_evidence`            | Transport/queue source ref                                                                        | Existing attempt id/dedupe                     | Owner acknowledgement logic                               |
| Restart interrupted work                                                | `uncertainty_facts` plus obligation when actionable  | restart handoff/boot source and affected work correlation                                         | `restart:<boot>:<source-work>`                 | Recovery worker/owner                                     |
| Context compaction completed                                            | event evidence only                                  | execution/session source                                                                          | execution event id                             | Inspection; does not alter obligations                    |

## Transaction And Reconciliation Map

| Source owner               | Same shared DB transaction preferred   | Required fallback scan                                                                                        |
| -------------------------- | -------------------------------------- | ------------------------------------------------------------------------------------------------------------- |
| `subagent_runs`            | Yes when owner write API exposes tx    | Scan terminal/interrupted and pending/failed/suspended delivery rows by stable run id and owner revision      |
| `task_runs`/delivery state | Yes when notification owner exposes tx | Scan actionable task/delivery revisions and dedupe by task id plus revision                                   |
| `flow_runs`                | Yes when flow mutation exposes tx      | Scan blocked/overdue/fan-in revisions                                                                         |
| `delivery_queue_entries`   | Yes for queue-attempt evidence         | Scan retry-exhausted and platform-send-started rows with no terminal transport result                         |
| restart/boot owners        | Usually reconciliation boundary        | Scan handoff/sentinel/boot facts against affected owner correlations                                          |
| session/ACP owners         | No cross-database assumption           | Resolve current binding at dispatch/decision time and suspend when missing, stale, ambiguous, or unauthorized |

Every fallback scan is at-least-once and bounded. Unique source/revision dedupe
keys provide convergence; scans never repeat an external side effect merely
because the obligation insert outcome was unknown.

## Target Resolution Sources

Target resolution is a read of canonical routing facts followed by bounded
evidence on the obligation. Resolution sources may include:

- requester/controller session refs from `subagent_runs`;
- task owner/requester and `task_delivery_state` origin;
- `flow_runs.owner_key` and requester origin;
- `current_conversation_bindings`;
- `acp_sessions` state and identity;
- stored session route, channel, target, and account facts;
- explicit report-route refs created at intake.

Resolution outcomes are `resolved`, `missing`, `ambiguous`, `unauthorized`, or
`inspect_only`. No fallback may convert the latter four into success.

## Schema Decisions

### Keep and rename before upstreaming

| Current unshipped name | Canonical name              |
| ---------------------- | --------------------------- |
| `durable_runs`         | `durable_execution_records` |
| `durable_events`       | `durable_event_evidence`    |
| `durable_steps`        | `durable_execution_steps`   |
| `durable_refs`         | `durable_payload_refs`      |
| `durable_links`        | `durable_run_correlations`  |
| `durable_timers`       | `durable_timer_obligations` |
| `durable_signals`      | `durable_signal_evidence`   |
| `source_type`          | `source_owner`              |

`wake_obligations`, `delivery_attempt_evidence`, and `uncertainty_facts` are
already canonical names but must gain/enforce `source_owner` and `source_ref`.

### Remove

| Current unshipped table/concept                 | Owner or replacement                                                                                      |
| ----------------------------------------------- | --------------------------------------------------------------------------------------------------------- |
| `continuation_cleanup`                          | Update the owning timer/signal/obligation/correlation and append event/audit evidence                     |
| `dedupe_ledger`                                 | Unique source/dedupe keys on the target records                                                           |
| Result mailbox as an independent step lifecycle | `subagent_runs` result source + `durable_payload_refs` + `wake_obligations` + `delivery_attempt_evidence` |

## Public Naming Gate

Storage, TypeScript, Gateway, CLI, tests, and docs must use generalized owner
vocabulary. Do not expose `ParentWake`, parent-only decision names,
`source_type`, or compatibility aliases for unshipped names.

Canonical terms:

- `WakeObligation`
- `DeliveryAttemptEvidence`
- `UncertaintyFact`
- `sourceOwner` / `sourceRef`
- `requires_owner_decision`
- owner, requester, controller, operator, system worker, and admin actor classes

## Review Checklist

Reject a new fact/table if any answer is missing:

- canonical source owner and stable ref;
- proof the current owner cannot safely represent the fact;
- concrete producer and consumer;
- disabled-path behavior;
- dedupe and terminal-transition rules;
- retention/privacy posture;
- actor/authorization source;
- owner adapter and source revision;
- transaction boundary or deterministic reconciliation scan;
- no-silence SLA, retry cap, and suspension behavior;
- recovery and inspection behavior;
- executable test proving the source-backed shape.

## Related

- [Upstream 7.1 Gap Analysis](/specs/durable-core-upstream-7.1-gap-analysis)
- [Durable Core Architecture](/specs/durable-core-architecture)
- [Durable Core Test Plan](/specs/durable-core-test-plan)
