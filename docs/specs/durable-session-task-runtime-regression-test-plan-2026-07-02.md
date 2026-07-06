# Durable Session Task Runtime Regression Test Plan

Date: 2026-07-02
Local instance: OpenClaw A (`local-a`, gateway `127.0.0.1:37101`)
Suite id: `durable-session-runtime-20260702`
Primary agent: `bo`

## Goal

Validate that native Durable Core can keep ordinary agent turns, session work,
delegation, child fan-in, and approved gateway restarts observable and
recoverable even when Work Module and Workboard are not used.

This suite intentionally starts with Workboard disabled and `work`/`taskflow`
config unset. Workboard and richer work-unit surfaces are tested later as
projections, not as prerequisites for durable correctness.

## Current Local Layer State

- Durable Core: enabled on LaunchAgent with `OPENCLAW_DURABLE_RUNTIME=1`.
- Work Module config: `null` in `openclaw.json`.
- TaskFlow config: `null` in `openclaw.json`.
- Workboard plugin: disabled with `openclaw plugins disable workboard`.
- MCP/AICOS: no MCP servers configured for OpenClaw A.
- Gateway entrypoint:
  `/Users/avo/aicos/providers/openclaw/worktrees/openclaw-x-durable-core/openclaw.mjs`.

Important CLI note: local `openclaw durable ...` inspection commands must pass
`OPENCLAW_DURABLE_RUNTIME=1`; otherwise the CLI reports the durable runtime as
disabled even though the LaunchAgent has it enabled.

## Past Failures Being Replayed

| Failure id | Past observed symptom                                                                          | Root behavior to verify                                                                                                                    |
| ---------- | ---------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------ |
| F1         | Bơ received Telegram/Discord work, delegated to a subagent, then appeared silent.              | Parent must not treat `sessions_spawn` as done; it must harvest/fan-in child terminal or diagnostic state.                                 |
| F2         | A subagent finished, but parent Bơ did not fan in and did not send a final/progress reply.     | Child completion/loss/overflow must become parent-visible state.                                                                           |
| F3         | Gateway restarted after user-approved restart, but the original work did not resume or report. | Approved restart must be persisted as planned interruption and enqueue or return a continuation/reconciliation path.                       |
| F4         | Active child/subagent was killed around gateway restart; user saw no useful status.            | Active runs/steps interrupted by planned restart must become `unknown_after_side_effect` and parent fan-in must continue or block visibly. |
| F5         | Workboard card existed but status stayed ready/running/stuck while real child work had ended.  | Durable Core should still provide session/task refs without Workboard; Workboard is only a projection.                                     |
| F6         | `/pair` and bot/channel work failed across Discord/Telegram and then fell silent.              | Runtime/profile should route ops to the right lane and report blocker/result; durable state should show active/lost/terminal turn.         |
| F7         | Subagent overflow/auto-compact happened normally, but coordinator did not notice.              | Overflow/unknown/failed child state must be handled as fan-in input, not silent terminal success.                                          |

## Static Proof Before Live Suite

Commands already run on branch `codex/openclaw-durable-batch-f-workboard-chat`
after adding durable restart interruption support:

```sh
npm test -- --run src/durable/restart-interruption.test.ts src/agents/tools/gateway-tool.test.ts src/durable/coordination-projection.test.ts
npm run tsgo:core
npm test -- --run src/durable src/gateway/server-methods/durable.test.ts src/agents/tools/gateway-tool.test.ts
npm test -- --run extensions/discord/src/monitor/native-command-reply.test.ts extensions/device-pair/index.test.ts extensions/device-pair/pair-command-auth.test.ts
```

Results:

- Durable restart targeted tests: passed.
- `tsgo:core`: passed.
- Durable/gateway/agent shards: 3 shards passed, 75 tests total across the
  selected files.
- Device-pair/Discord extension shards: 2 shards passed, 58 tests total.
- `npm run check:changed`: not accepted as code proof because the delegated
  Crabbox binary failed its own `--version/--help` sanity check before running
  project checks.

## Common Environment

Use this prefix for local OpenClaw A CLI tests:

```sh
OPENCLAW_DURABLE_RUNTIME=1 \
OPENCLAW_CONFIG_PATH=/Users/avo/aicos/.runtime-home/openclaw/instances/local-a/state/openclaw.json \
OPENCLAW_STATE_DIR=/Users/avo/aicos/.runtime-home/openclaw/instances/local-a/state \
/opt/homebrew/bin/node /Users/avo/aicos/providers/openclaw/worktrees/openclaw-x-durable-core/openclaw.mjs
```

Useful inspection commands:

```sh
# Recent durable runs.
OPENCLAW_DURABLE_RUNTIME=1 OPENCLAW_CONFIG_PATH=/Users/avo/aicos/.runtime-home/openclaw/instances/local-a/state/openclaw.json OPENCLAW_STATE_DIR=/Users/avo/aicos/.runtime-home/openclaw/instances/local-a/state /opt/homebrew/bin/node /Users/avo/aicos/providers/openclaw/worktrees/openclaw-x-durable-core/openclaw.mjs durable runs --limit 20 --json

# Durable store stats.
OPENCLAW_DURABLE_RUNTIME=1 OPENCLAW_CONFIG_PATH=/Users/avo/aicos/.runtime-home/openclaw/instances/local-a/state/openclaw.json OPENCLAW_STATE_DIR=/Users/avo/aicos/.runtime-home/openclaw/instances/local-a/state /opt/homebrew/bin/node /Users/avo/aicos/providers/openclaw/worktrees/openclaw-x-durable-core/openclaw.mjs durable stats --json

# Recent Bơ sessions.
OPENCLAW_CONFIG_PATH=/Users/avo/aicos/.runtime-home/openclaw/instances/local-a/state/openclaw.json OPENCLAW_STATE_DIR=/Users/avo/aicos/.runtime-home/openclaw/instances/local-a/state /opt/homebrew/bin/node /Users/avo/aicos/providers/openclaw/worktrees/openclaw-x-durable-core/openclaw.mjs sessions --agent bo --active 60 --json --limit 20
```

## Test Matrix

| Test id | Surface state                                        | Human-style prompt                                                                                          | Simulates                                                                                    | Expected pass criteria                                                                                                                                    | Status                        |
| ------- | ---------------------------------------------------- | ----------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------- |
| T1      | Durable Core only; Workboard disabled                | Short health check asking Bơ to reply in one sentence and create no Workboard card.                         | Baseline direct chat should not need Workboard or child workflow.                            | CLI returns reply; session is `done`; durable run is `succeeded/terminal`; no `workboard_*` tools in active tool surface.                                 | Passed 2026-07-02             |
| T2      | Durable Core only; Workboard disabled                | Ask Bơ a multi-step but bounded diagnostic/status request that can finish in one turn.                      | Longer work must still create observable durable run state and avoid forced Workboard usage. | Bơ answers or reports bounded blocker; durable run terminal or explicit waiting/blocker; no silence.                                                      | Passed 2026-07-02             |
| T3      | Durable Core only; Workboard disabled                | Ask Bơ to coordinate with one specialist lane for a tiny independent review and summarize.                  | Prior `sessions_spawn`/fan-in silence after subagent completion.                             | Parent sends visible transition/progress; child session/run is visible; parent final/fan-in is visible; no empty unexplained closeout.                    | Passed 2026-07-02             |
| T4      | Durable Core only; Workboard disabled                | Ask Bơ to run a multi-branch tiny task with two lanes and one intentionally risky/blocked lane.             | One child failure/blocked state must not freeze parent or sibling branches.                  | Parent reports child statuses by id/session; sibling result is not lost; parent blocks/fails/continues by policy with next owner.                         | Passed after fixes 2026-07-02 |
| T5      | Durable Core only; Workboard disabled                | Start long/active work, then perform approved safe gateway restart through the gateway tool path.           | Approved restart previously caused work to stop without resume/report.                       | Restart writes `openclaw.gateway.restart`; active runs are marked or reconciled; continuation inspects durable state and reports resumed/blocked/unknown. | Passed after fixes 2026-07-02 |
| T6      | Durable Core only; Workboard disabled                | Recreate `/pair` style ops request to Bơ, but via CLI and without requiring real external channel mutation. | Telegram/Discord `/pair` fix path fell silent after ops delegation/restart.                  | Bơ routes to operator or answers blocker; operator result is fan-in visible; no raw lifecycle command without safe continuation.                          | Passed 2026-07-02             |
| T7      | Durable Core only; Workboard disabled                | Ask for status/continue on an existing active session with old child/restart refs.                          | Short "ok/continue/status" messages were misclassified as new roots or ignored.              | Bơ inspects existing refs/history before creating new work; reports current state or blocker.                                                             | Passed 2026-07-02             |
| T8      | Work Module enabled or available; Workboard disabled | Repeat T3/T4 with generic Work Unit refs if surfaced.                                                       | Work Module should be generic lifecycle, not Workboard UI dependency.                        | Work Unit refs are generic; no board-specific status required; durable fan-in remains source of truth.                                                    | Deferred: surface disabled    |
| T9      | Workboard enabled                                    | Repeat T3/T4/T5 with Workboard as projection only.                                                          | Board status drift/stuck cards and parent fan-in visibility.                                 | Workboard reflects durable/session state; disabling Workboard later does not lose core durable truth.                                                     | Deferred: surface disabled    |

## Execution Log

### 2026-07-02 T0 - Environment Prep

Actions:

- Disabled Workboard with `openclaw plugins disable workboard`.
- Synced all Team Bơ profiles with `./scripts/sync-profiles.sh openclaw local-a`.
- Verified profile drift with `./scripts/verify-profile-drift.sh openclaw local-a`.
- Restarted OpenClaw A via `./runtime/supervisor/launchd/manage-openclaw-instance.sh local-a restart`.
- Verified enabled plugin list excludes `workboard`.
- Verified no MCP servers are configured.

Result:

- OpenClaw A running from `openclaw-x-durable-core`.
- Durable runtime env present on LaunchAgent.
- Workboard disabled for the first live suite phase.

### 2026-07-02 T1 - Durable-Core-Only Direct Chat Smoke

Command:

```sh
OPENCLAW_CONFIG_PATH=/Users/avo/aicos/.runtime-home/openclaw/instances/local-a/state/openclaw.json \
OPENCLAW_STATE_DIR=/Users/avo/aicos/.runtime-home/openclaw/instances/local-a/state \
/opt/homebrew/bin/node /Users/avo/aicos/providers/openclaw/worktrees/openclaw-x-durable-core/openclaw.mjs \
  agent --agent bo \
  --session-key agent:bo:durable-core-cli-smoke-20260702 \
  --message "Health check durable-core-only: hãy trả lời đúng một câu ngắn bằng tiếng Việt rằng Bơ đang online và không tạo Workboard card." \
  --json --timeout 300
```

Observed result:

- CLI status: `ok`.
- Reply: `Bơ đang online và không tạo Workboard card.`
- Duration: about 6.5s agent runtime.
- Active tool surface excluded `workboard_*`.
- Session status: `done`.
- Durable run:
  - `runtimeRunId`: `run_6711b481-222f-4f65-a494-ec09aed6fd1a`
  - `operationKind`: `openclaw.agent.turn`
  - `status`: `succeeded`
  - `recoveryState`: `terminal`

Conclusion:

- Direct chat works durable-core-only.
- Workboard is not required for a short agent turn.

### 2026-07-02 T2 - Durable-Core-Only Bounded Multi-Point Reply

Command:

```sh
OPENCLAW_CONFIG_PATH=/Users/avo/aicos/.runtime-home/openclaw/instances/local-a/state/openclaw.json \
OPENCLAW_STATE_DIR=/Users/avo/aicos/.runtime-home/openclaw/instances/local-a/state \
/opt/homebrew/bin/node /Users/avo/aicos/providers/openclaw/worktrees/openclaw-x-durable-core/openclaw.mjs \
  agent --agent bo \
  --session-key agent:bo:durable-core-cli-bounded-20260702 \
  --message "Durable-core-only bounded check: không tạo Workboard card và không spawn agent. Hãy trả lời ngắn 3 ý: (1) Workboard không phải điều kiện để chat ngắn chạy durable, (2) khi việc dài hơn thì cần refs/session/durable state, (3) nếu restart hoặc child lỗi thì phải báo trạng thái thay vì im lặng." \
  --json --timeout 300
```

Observed result:

- CLI status: `ok`.
- Reply contained the requested three durable-core-only points.
- Bơ did not spawn a child and did not use Workboard.
- Session status: `done`.
- Durable run:
  - `runtimeRunId`: `run_f8c57fba-5628-4b09-8b05-94596edf7320`
  - `operationKind`: `openclaw.agent.turn`
  - `status`: `succeeded`
  - `recoveryState`: `terminal`

Conclusion:

- A bounded multi-point response still works without Workboard/Work Module.
- Bơ followed the direct/no-spawn instruction and did not over-promote the work.

### 2026-07-02 Live Background Observation - Existing Telegram/Pair Recovery Work

During post-T2 durable inspection, an existing `agent:bo:main` parent from the
earlier Telegram pairing recovery thread was still open:

- Parent:
  - `runtimeRunId`: `run_20f6d421-9496-47fe-884b-015bd34bba5e`
  - `operationKind`: `openclaw.agent.turn`
  - `sourceRef`: `agent:bo:main`
  - `status`: `waiting_child`
  - `recoveryState`: `waiting_child`
- Child:
  - `runtimeRunId`: `run_2f69ffe7-9730-4029-93a3-493bc0632d25`
  - `operationKind`: `openclaw.subagent.run`
  - `sourceRef`: `agent:bo-operator:subagent:10f65407-5b24-4953-b29e-0271275a09d1`
  - `status`: `running`
  - `taskName`: `verify_pair_fix_recovery`
- Child agent turn:
  - `runtimeRunId`: `run_3c8a9c59-5d9d-4555-8032-d26aa915d602`
  - `status`: `running`

This is not a synthetic T3 run; it appears to be the real outstanding
Telegram/Discord pairing recovery work. It should be monitored before adding
more delegation load, because it directly represents failures F1/F2/F6:
parent waiting on a child, child still running, and user-facing silence risk.

Follow-up evidence:

- The child `verify_pair_fix_recovery` hit a tool-loop context overflow after
  running a broad recursive `rg` across both `.runtime-home` and the OpenClaw
  source tree. The subagent then auto-compacted, which is acceptable; the bug
  risk is that the parent/coordinator must still see a child diagnostic instead
  of going silent.
- The durable parent remained `waiting_child` while the normal session list
  showed `agent:bo:main` as `done`. That mismatch is itself a durable runtime
  issue: human/operator surfaces must not present a parent session as completed
  when durable coordination is still waiting on child fan-in.
- The child also found
  `/Users/avo/aicos/.runtime-home/openclaw/instances/local-a/state/gateway.pid`
  pointing at an old PID while LaunchAgent reported a different active gateway
  PID. This can mislead agents during recovery diagnostics; it should be
  reported as stale diagnostic state, not treated as proof that the gateway is
  down.

This observation upgrades T3/T4/T6/T7 from purely planned tests into a live
regression target: subagent overflow/compaction and restart recovery are normal
events, but coordinator-visible fan-in, bounded diagnostics, and truthful
session status are required for the durable runtime to avoid silence.

### 2026-07-02 T3 - One-Child Delegation And Fan-In

Purpose:

- Replay F1/F2 without Workboard or Work Module.
- Verify that `sessions_spawn` is treated as "child work started", not
  "parent work is done".

Command pattern:

```sh
OPENCLAW_DURABLE_RUNTIME=1 \
OPENCLAW_CONFIG_PATH=/Users/avo/aicos/.runtime-home/openclaw/instances/local-a/state/openclaw.json \
OPENCLAW_STATE_DIR=/Users/avo/aicos/.runtime-home/openclaw/instances/local-a/state \
/opt/homebrew/bin/node /Users/avo/aicos/providers/openclaw/worktrees/openclaw-x-durable-core/openclaw.mjs \
  agent --agent bo \
  --session-key agent:bo:durable-core-cli-delegation-20260702 \
  --message "<human-style one-lane delegation prompt>" \
  --json --timeout 900
```

Observed result:

- Initial CLI yielded child wait state instead of pretending the work was
  complete.
- Child session key:
  `agent:bo-operator:subagent:fbfd999c-ecff-44c0-9734-2c02f53c452f`.
- Parent runtime run:
  `run_31f68226-b9fb-4608-acb5-b06b4b996653`.
- Direct child announce runtime run:
  `run_a4f1c4cf-7847-413d-89a3-5767e96a147a`.
- Final durable state:
  - parent `openclaw.agent.turn`: `succeeded/terminal`;
  - child `openclaw.subagent.run`: `succeeded/terminal`;
  - child announce `openclaw.agent.turn`: `succeeded/terminal`.

Conclusion:

- Parent fan-in succeeds for one delegated child with Workboard disabled.
- This proves Durable Core can represent delegated work without relying on a
  Workboard card.

### 2026-07-02 T4a - Two-Branch Fan-In Exposed A Stale Continuation

Purpose:

- Replay F1/F2/F7 with two branches.
- Verify that completion of one child does not make the whole parent look
  healthy if another child continuation is still open.

Observed failing/ambiguous behavior before the fix:

- Session key:
  `agent:bo:durable-core-cli-branching-20260702`.
- Parent and child subagent runs eventually reached `succeeded/terminal`.
- One direct child announce run remained in `waiting_signal` after parent
  completion, which made durable status surfaces able to report a stale
  unfinished turn for the parent session.
- Stale run later reconciled:
  `run_7c19bc64-98ae-4029-a17f-b194fa477e25`
  became `succeeded/terminal` with event
  `agent.turn.continuation_superseded`.

Root cause:

- Child announce continuations are internal delivery attempts. Once the parent
  has terminal fan-in, older unresolved announce turns for the same parent
  session should not remain active forever.

Fix added:

- `src/durable/agent-turn-continuations.ts` closes superseded child announce
  continuations.
- Gateway startup and recovery worker both call the reconciliation path.
- Parent terminalization now also closes older announce continuations for the
  same parent session.

Proof query:

```sql
SELECT event_type, correlation_id
FROM durable_runtime_events
WHERE runtime_run_id='run_7c19bc64-98ae-4029-a17f-b194fa477e25'
ORDER BY event_seq ASC
LIMIT 20;
```

Observed proof:

```text
agent.turn.received|agent:bo:durable-core-cli-branching-20260702
agent.turn.running|agent:bo:durable-core-cli-branching-20260702
agent.turn.yielded|agent:bo:durable-core-cli-branching-20260702
agent.turn.continuation_superseded|agent:bo:durable-core-cli-branching-20260702
```

Conclusion:

- This test found a real stale-continuation bug.
- The fix is core-level and upstream-friendly because it reconciles generic
  durable agent-turn delivery state, not a Bơ- or Workboard-specific behavior.

### 2026-07-02 T4b - Two-Branch Fan-In Exposed Premature Parent Terminal

Purpose:

- Replay the exact "parent goes quiet while branches are still in flight"
  failure mode.
- Verify that a successful direct announce for child A cannot mark the parent
  terminal while child B is still running.

Observed failing/ambiguous behavior before the second fix:

- Session key:
  `agent:bo:durable-core-cli-branching-postfix-20260702`.
- Parent run:
  `run_e5c1898c-88ce-44bc-9348-0808e83eee39`.
- Child A direct announce succeeded while child B was still running.
- Parent terminalization could happen too early because direct announce success
  was treated as enough to close the parent, instead of checking all child
  links.

Root cause:

- Parent terminalization used a successful direct child continuation as the
  trigger, but it did not require every child link to be terminal.

Fix added:

- `recordDurableSubagentAnnounceDelivery` now checks
  `isParentFanInComplete()` before marking the parent
  `succeeded/terminal`.
- If any child link is still `running`, the parent stays
  `waiting_child/waiting_child`.

Unit proof added:

- `src/durable/subagent.test.ts`
  - `keeps parent waiting when a child announce succeeds before sibling fan-in is complete`
  - `closes earlier child announce continuations after parent fan-in reaches terminal`

Conclusion:

- This is the core bug behind several "Bơ delegated then went silent" cases.
- The fix is future-proof even with larger context windows: parent/child
  lifecycle must be machine-state based, not inferred from transcript length or
  model memory.

### 2026-07-02 T4c - Two-Branch Fan-In After Fix

Purpose:

- Verify the T4 fixes in live OpenClaw A after rebuilding the gateway bundle
  and restarting the LaunchAgent.

Command:

```sh
OPENCLAW_DURABLE_RUNTIME=1 \
OPENCLAW_CONFIG_PATH=/Users/avo/aicos/.runtime-home/openclaw/instances/local-a/state/openclaw.json \
OPENCLAW_STATE_DIR=/Users/avo/aicos/.runtime-home/openclaw/instances/local-a/state \
/opt/homebrew/bin/node /Users/avo/aicos/providers/openclaw/worktrees/openclaw-x-durable-core/openclaw.mjs \
  agent --agent bo \
  --session-key agent:bo:durable-core-cli-branching-postfix2-20260702 \
  --message "Bơ, kiểm tra nhanh hai nhánh nhỏ giống human giao việc: nhánh A xác nhận durable core hoạt động khi Workboard tắt; nhánh B xác nhận parent không im lặng khi child hoàn tất. Không sửa file. Hãy tự điều phối nếu cần, trả lời ngắn kết quả/blocker." \
  --json --timeout 900
```

Immediate observed state while children were still running:

- Parent run:
  `run_6d0344a8-5d74-421d-99f9-e909c643391b`.
- Parent status during active child work:
  `waiting_child/waiting_child`.
- Child sessions:
  - `agent:bo-worker:subagent:43f9e797-3c89-45a5-a09d-2e30c466e6af`;
  - `agent:bo-worker:subagent:be138c3c-661a-4462-b998-dda8ce22a4f3`.
- `sessions --agent bo --active 15 --json --limit 5` showed the transcript
  session as `done`, but also attached:

```json
{
  "durableRuntime": {
    "runtimeRunId": "run_6d0344a8-5d74-421d-99f9-e909c643391b",
    "operationKind": "openclaw.agent.turn",
    "status": "waiting_child",
    "recoveryState": "waiting_child"
  }
}
```

This is desired: the transcript can be idle/done, while the durable parent is
still waiting on child work.

Final durable result:

```text
run_6d0344a8-5d74-421d-99f9-e909c643391b|openclaw.agent.turn|succeeded|terminal|agent:bo:durable-core-cli-branching-postfix2-20260702
run_e11f364c-dac0-4c01-b522-ff233ccac440|openclaw.subagent.run|succeeded|terminal|agent:bo-worker:subagent:43f9e797-3c89-45a5-a09d-2e30c466e6af
run_e1c958b8-ffdd-428a-89ef-538a41b29166|openclaw.subagent.run|succeeded|terminal|agent:bo-worker:subagent:be138c3c-661a-4462-b998-dda8ce22a4f3
```

Final event proof:

```text
agent.turn.received|1
agent.turn.running|3
agent.turn.yielded|1
fan_in.partial|3
fan_in.ready|1
subagent.child.announce_delivered|2
subagent.child.linked|2
subagent.child.terminal|2
subagent.run.started|2
subagent.run.terminal|2
agent.turn.continuation_succeeded|1
```

Open run proof:

```sql
SELECT count(*)
FROM durable_runtime_runs
WHERE status NOT IN ('succeeded','failed','cancelled','lost')
  AND operation_kind='openclaw.agent.turn'
  AND source_ref='agent:bo:durable-core-cli-branching-postfix2-20260702';
```

Observed result: `0`.

Conclusion:

- Live two-branch delegation no longer leaves the parent falsely terminal
  during active child work.
- Once all child links are terminal, the parent and announce continuations close
  cleanly.
- This is the strongest proof so far that Durable Core can act as the
  coordination kernel without Workboard.

### 2026-07-02 T4d - Bounded Tool Output Guardrail

Purpose:

- Replay the real `verify_pair_fix_recovery` overflow behavior without tying
  the fix to one profile or one agent.
- Prevent foreground tool results from dumping massive diagnostic output into
  the agent loop and forcing avoidable overflow/auto-compact.

Root cause:

- A subagent ran a broad recursive diagnostic (`rg`) across runtime state and
  source paths. The command output was valid, but too large for a foreground
  tool result.

Fix added:

- `renderExecToolResultText()` bounds foreground exec tool output.
- Default max text body:
  `OPENCLAW_BASH_TOOL_RESULT_MAX_CHARS`, default `30000`, min `4000`, max
  `200000`.
- Long output keeps the tail and adds a truncation notice telling the agent to
  use a narrower command or background polling.

Unit proof added:

- `src/agents/bash-tools.exec-output.test.ts`
  - `bounds long foreground output and keeps the tail`

Conclusion:

- This does not hide process output from durable state; it only prevents a
  single tool result from drowning the agent turn.
- The profile rule still tells agents to use bounded diagnostics, but the core
  guardrail protects every agent.

### 2026-07-02 T7a - Sessions Must Surface Open Durable Parent State

Purpose:

- Replay the status/debug confusion where `sessions` showed a transcript as
  `done` while durable coordination was still waiting on child fan-in.

Root cause:

- Session history status and durable runtime status are different layers. The
  normal transcript can have no active foreground model call, while a durable
  parent run is still `waiting_child`.

Fix added:

- `sessions` overlays open `openclaw.agent.turn` durable runs by session key.
- JSON output includes `durableRuntime` with run id, operation kind, status,
  recovery state, and timestamps.
- Terminal table flags include `durable:<status>`.
- The overlay is best-effort and disabled when Durable Runtime is disabled.

Unit proof added:

- `src/commands/sessions.test.ts`
  - session row with transcript `status: done` and durable run
    `waiting_child` emits JSON `durableRuntime`;
  - terminal table includes `durable:waiting_child`.

Live proof:

- During T4c, `sessions --agent bo --active 15 --json --limit 5` showed
  `status: done` plus `durableRuntime.status: waiting_child`.
- After T4c completed, the same session no longer had `durableRuntime`, because
  open durable run count was `0`.

Conclusion:

- This directly addresses "agent looks done or silent while durable work is
  still alive".
- It is upstream-friendly because it exposes generic durable runtime state,
  without requiring Workboard or AICOS-specific UI.

### 2026-07-02 T6 - `/pair` Dry-Run Delegation Without Workboard

Purpose:

- Replay the `/pair`/ops-style path where Bơ previously delegated work and the
  parent appeared to finish or go silent while a child was still running.

Surface state:

- Durable Runtime: enabled.
- Work Module: disabled (`work: null`, `taskflow: null`).
- Workboard plugin: disabled (`plugins.entries.workboard.enabled: false`).

Live command shape:

```sh
OPENCLAW_DURABLE_RUNTIME=1 \
OPENCLAW_CONFIG_PATH=/Users/avo/aicos/.runtime-home/openclaw/instances/local-a/state/openclaw.json \
OPENCLAW_STATE_DIR=/Users/avo/aicos/.runtime-home/openclaw/instances/local-a/state \
/opt/homebrew/bin/node /Users/avo/aicos/providers/openclaw/worktrees/openclaw-x-durable-core/openclaw.mjs \
  agent \
  --agent bo \
  --session-key agent:bo:durable-core-cli-pair-dryrun-20260702 \
  --timeout 1200 \
  --json \
  --message "<human-style /pair dry-run request>"
```

Observed proof:

- Parent initial run:
  `run_4ecca61b-88aa-40f1-846f-06ac5f58a12f`
  (`openclaw.agent.turn`) moved through `waiting_child`.
- Bơ spawned operator child session:
  `agent:bo-operator:subagent:3c6a2de0-a80d-4a2a-85c3-3b8e47d972c4`.
- Child durable records:
  - `run_54101fbe-f5ba-476f-af97-cb69af208cc0`
    (`openclaw.subagent.run`) reached `succeeded/terminal`.
  - `run_c9c8c654-6790-42d7-8fc8-dcb74d495fdd`
    (`openclaw.agent.turn`) reached `succeeded/terminal`.
- Parent fan-in/announce run:
  `run_b48b1b87-f21a-4aff-ad41-3aab3235875c`
  reached `succeeded/terminal`.

Event proof:

```text
agent.turn.continuation_succeeded | 1
agent.turn.received               | 3
agent.turn.running                | 3
agent.turn.succeeded              | 2
agent.turn.yielded                | 1
fan_in.partial                    | 1
fan_in.ready                      | 1
subagent.child.announce_delivered | 1
subagent.child.linked             | 1
subagent.child.terminal           | 1
subagent.run.started              | 1
subagent.run.terminal             | 1
```

Conclusion:

- Bơ can delegate a `/pair`-like ops request without Workboard.
- Parent no longer treats child spawn as completion; fan-in closes only after
  child terminal state and announce delivery.

### 2026-07-02 T7 - Continue/Status On Existing Durable Session

Purpose:

- Replay the short "ok/status/continue" follow-up pattern where the coordinator
  previously treated a continuation as unrelated work or had no visible state to
  inspect.

Live session:

- `agent:bo:durable-core-cli-pair-dryrun-20260702`

Observed proof:

- Bơ reported the current state as `waiting_children` instead of creating a new
  root task.
- Bơ named the existing child session
  `agent:bo-operator:subagent:3c6a2de0-a80d-4a2a-85c3-3b8e47d972c4`
  and child run `859bda1c-4247-484d-b57d-ea66dc29846c`.
- No extra child session was spawned for the status question.

Conclusion:

- Existing durable/session state is discoverable enough for a short follow-up
  to continue the right context.
- The `sessions` durable overlay is necessary because transcript status alone
  can be `done` while the durable parent is still waiting on child fan-in.

### 2026-07-02 T5a - Safe Gateway Restart Route-Less Continuation Bug

Purpose:

- Replay approved safe gateway restart from a CLI/local-first session where no
  external Discord/Telegram route exists.

Initial live failure before fix:

- Session:
  `agent:bo:durable-core-cli-safe-restart-20260702`
- The gateway tool wrote an `openclaw.gateway.restart` run and restarted the
  gateway.
- After restart, transcript showed a heartbeat poll:

```text
{"role":"user","content":"[OpenClaw heartbeat poll]"}
{"role":"assistant","content":"HEARTBEAT_OK"}
```

Root cause:

- `server-restart-sentinel.ts` delivered queued `agentTurn` continuations only
  when a routable external channel was available.
- Route-less continuations fell back to `enqueueRestartSentinelWake`, which
  enqueued a heartbeat/system wake instead of dispatching the actual agent turn.

Fix added:

- Route-less `agentTurn` continuations now dispatch through an internal
  `webchat` session route using the same session key.
- Startup no longer enqueues a heartbeat wake for `agentTurn` continuations.
- Restart continuation dispatch is wrapped in the same durable agent-turn
  lifecycle helper so the continuation itself gets `openclaw.agent.turn`
  records and events.

Unit proof:

```sh
npm test -- --run src/gateway/server-restart-sentinel.test.ts
```

Result:

- 2 Vitest project shards passed.
- 58 tests passed.
- New regression: route-less `agentTurn` continuations run on the internal
  session route and do not call `requestHeartbeat`.

### 2026-07-02 T5b - Safe Gateway Restart Post-Fix Live Proof

Purpose:

- Verify that safe restart continuation works end-to-end with Durable Runtime
  only, Work Module disabled, and Workboard disabled.

Surface state:

```json
{
  "work": null,
  "taskflow": null,
  "workboardPlugin": { "enabled": false },
  "workboardEnabled": false
}
```

Live command shape:

```sh
OPENCLAW_DURABLE_RUNTIME=1 \
OPENCLAW_CONFIG_PATH=/Users/avo/aicos/.runtime-home/openclaw/instances/local-a/state/openclaw.json \
OPENCLAW_STATE_DIR=/Users/avo/aicos/.runtime-home/openclaw/instances/local-a/state \
/opt/homebrew/bin/node /Users/avo/aicos/providers/openclaw/worktrees/openclaw-x-durable-core/openclaw.mjs \
  agent \
  --agent bo \
  --session-key agent:bo:durable-core-cli-safe-restart-postfix2-20260702 \
  --timeout 1200 \
  --json \
  --message "<human-style safe gateway restart request>"
```

Observed output:

```text
restart_done yes
continuation_session agent:bo:durable-core-cli-safe-restart-postfix2-20260702
heartbeat_only no
```

Transcript proof:

- Initial gateway tool result reported `continuationQueued: true`.
- After restart, the transcript contains an internal-system user message from
  `restart-sentinel`, not a heartbeat poll:

```json
{
  "role": "user",
  "provenance": {
    "kind": "internal_system",
    "sourceChannel": "webchat",
    "sourceTool": "restart-sentinel"
  }
}
```

Log proof:

```text
[restart-sentinel] restart continuation dispatching through internal session route
[diagnostic] session turn created: runId=98aa60a4-1143-43e2-9dfb-fbd82b135bfa
[diagnostic] message processed ... outcome=completed
```

Durable DB proof:

```text
run_467c7f00-b3b6-4497-b023-745409b4a90e | openclaw.agent.turn     | succeeded | terminal | 08e1a50b-eec2-4509-b165-39608e90432a
run_2f2960af-c052-48a1-996e-dbed6e98b5bd | openclaw.gateway.restart | succeeded | terminal | gateway-restart:1783007052185:agent:bo:durable-core-cli-safe-restart-postfix2-20260702
run_e31a9d5b-76f0-4370-b79b-c472ef1b5159 | openclaw.agent.turn     | succeeded | terminal | restart-sentinel:agent:bo:durable-core-cli-safe-restart-postfix2-20260702:agentTurn:1783007048631
```

Durable event proof:

```text
agent.turn.received  | 2
agent.turn.running   | 2
agent.turn.succeeded | 2
gateway.restart.approved | 1
```

Conclusion:

- Durable Core now survives a safe gateway restart for CLI/local-first sessions
  without Work Module or Workboard.
- The continuation is not just visible in the transcript; it is recorded as a
  durable `openclaw.agent.turn` with received/running/succeeded lifecycle
  events.
- This keeps the feature upstream-friendly because the fix is generic to
  restart/session delivery, not tied to a board, project, or profile-specific
  convention.

### Deferred Work-Surface Tests

T8 and T9 remain intentionally deferred in this run:

- T8 requires enabling the generic Work Module/Work Unit surface.
- T9 requires enabling Workboard as a projection UI.
- The current request explicitly asked to keep Work Module and Workboard off
  while validating Durable Core. These tests should be run in the next phase
  after Durable Core-only behavior is stable.

## Build And Static Verification After T3/T4 Fixes

Commands:

```sh
npm test -- --run \
  src/durable/subagent.test.ts \
  src/durable/recovery.test.ts \
  src/commands/sessions.test.ts \
  src/agents/bash-tools.exec-output.test.ts \
  src/durable/restart-interruption.test.ts \
  src/durable/coordination-projection.test.ts

npm run build
npm run tsgo:core
```

Observed result:

- Targeted Vitest shards passed. Latest rerun at 2026-07-02 20:31:
  3 Vitest shards passed, 55 tests total across the selected regression files.
- `npm run build` passed and rebuilt `dist/entry.js`, which is required because
  `openclaw.mjs` imports the compiled bundle.
- OpenClaw A was restarted after build and `/health` returned:

```json
{ "ok": true, "status": "live" }
```

- `npm run tsgo:core` passed after the final fan-in fix.

Conclusion:

- The local instance is running the rebuilt durable-core code, not only the
  TypeScript source tree.

## Fixes Added Before This Suite

- Gateway safe restart now records a durable `openclaw.gateway.restart`
  operation before emitting the restart sentinel.
- Active `received`/`queued`/`running` runs interrupted by an approved restart
  are moved to `unknown_after_side_effect` with recovery diagnostics instead of
  disappearing silently.
- Running child/subagent links interrupted by planned restart are marked `lost`
  with `terminalOutcome: unknown_after_side_effect`, then parent fan-in is
  reconciled so one child cannot freeze the parent.
- Gateway restart gets a default durable continuation message when durable
  runtime is enabled, a session key is known, and the agent did not provide an
  explicit `continuationMessage`.
- Coordination projection now treats `unknown_after_side_effect` as resumable
  reconciliation work.
- Team Bơ profiles/skills now state that `sessions_spawn` starts supervised
  child work, not completion, and that approved restart requires post-start
  continuation/reconciliation.

## Open Questions To Resolve During The Suite

- Resolved for T3/T4: CLI yielded child wait state for delegated work, and the
  durable/session inspection surfaces now show the parent `waiting_child` state
  while the transcript session is idle/done.
- Does the default gateway restart continuation actually fire through the same
  session in live OpenClaw A, or only in unit-level sentinel tests?
- Partially resolved for stale announce continuations: old internal child
  announce continuations are now closed by parent-terminal reconciliation.
  Older queued runnable test runs still need a retention/lost-policy decision.
- Is Work Module currently a visible tool/API surface in OpenClaw A, or only a
  code-level helper/projection layer in this branch?

## 2026-07-06 Parent-Led Durable Recovery Safety Update

Implementation status: P0/P0.5 safety slice completed in `openclaw-durable-runtime-recovery-pr4-v2`.

Completed:

- Durable Core now blocks legacy synthetic child/subagent auto-resume by default in `src/agents/subagent-orphan-recovery.ts` when durable runtime recovery is enabled.
- The legacy compatibility path remains available only with explicit opt-in: `OPENCLAW_LEGACY_SUBAGENT_AUTO_RESUME=1`.
- Recovery state, runtime types, coordination projection, result mailbox, and restart interruption handling now carry typed recovery reasons plus retry safety metadata, `requiredAction`, `sideEffectBoundary`, and evidence refs.
- Parent/coordinator-facing recovery states are conservative: the runtime records observations and wakes/blocks for inspection instead of deciding an unsafe child retry/resume by itself.
- Retry affordance invariant: UI/projection surfaces may expose `controls.canRetry=true` only when `recovery.retryable === true` and `recovery.retrySafety === "safe_to_retry"`. Legacy `lost`, unknown, `inspect_first`, and unsafe states must require inspection or parent/coordinator decision before retry.

Verification run for this slice:

```sh
node scripts/run-vitest.mjs src/agents/subagent-orphan-recovery.test.ts src/durable/recovery.test.ts src/durable/restart-interruption.test.ts src/durable/coordination-projection.test.ts src/durable/subagent.test.ts src/durable/startup.test.ts
node scripts/run-vitest.mjs src/durable/coordination-projection.test.ts src/durable/recovery.test.ts src/durable/restart-interruption.test.ts src/agents/subagent-orphan-recovery.test.ts
npm run tsgo:core
```

Observed results:

- Full targeted recovery shard: 51 tests passed.
- Follow-up retry-affordance shard: 38 tests passed.
- `npm run tsgo:core` passed.

Remaining next phases/gaps:

- Coordinator decision API/event such as `coordinator.decision.recorded` before any retry/resume/cancel/reconcile action.
- Inspect/reconcile tools for parent agents and operators, including clear evidence and side-effect summaries.
- Broader side-effect boundary coverage across gateway message delivery, tool wrappers, external APIs, config mutation, and restart handoff flows.
- End-to-end restart tests that cover live parent/child interruption, parent wake, inspection, and explicit decision before retry/resume.

## 2026-07-06 Target Sync Correction: `openclaw-x-durable-core`

Implementation status: the minimal Durable Core safety slice has now been synced into this target worktree. The earlier note in this file referred to completion in `openclaw-durable-runtime-recovery-pr4-v2`; that was not sufficient by itself for `openclaw-x-durable-core` runtime behavior.

Synced into this worktree:

- `src/agents/subagent-orphan-recovery.ts` and tests: legacy synthetic subagent auto-resume is skipped by default when `OPENCLAW_DURABLE_RUNTIME=1`; the compatibility path is available only with `OPENCLAW_LEGACY_SUBAGENT_AUTO_RESUME=1` and is logged as deprecated.
- `src/durable/recovery.ts`, `src/durable/restart-interruption.ts`, `src/durable/coordination-projection.ts`, `src/durable/types.ts`, `src/durable/result-mailbox.ts`, and tests: recovery diagnostics include structured `recoveryReason`, `retrySafety`, and `requiredAction`; generic `lost` is no longer treated as safely retryable; projection retry controls require `recovery.retryable === true` and `recovery.retrySafety === "safe_to_retry"`; restart interruption keeps ambiguous post-side-effect state as `unknown_after_side_effect`.
- `docs/specs/durable-core-safety-slice.md`: added as the local contract for this minimal slice.

Validation in `openclaw-x-durable-core`:

```sh
node scripts/run-vitest.mjs src/agents/subagent-orphan-recovery.test.ts src/durable/recovery.test.ts src/durable/restart-interruption.test.ts src/durable/coordination-projection.test.ts
# passed: 38 tests across 4 files / 2 Vitest shards

node scripts/run-vitest.mjs src/durable/subagent.test.ts src/durable/startup.test.ts
# passed: 11 tests across 2 files / 1 Vitest shard

npm run tsgo:core
# passed
```

Still deferred: full coordinator decision API/events, agent/human inspect-reconcile tools, broad side-effect boundary coverage for all tool wrappers/frontdoors, and live E2E restart/fan-in recovery matrix.

## 2026-07-06 A/E Implementation Loop

Scope:

- Apply the current Durable Core branch to local OpenClaw A and E.
- Rebuild runtime bundles and restart both instances with the safe restart
  script.
- Run machine tests, then live CLI agent tests that simulate human requests.
- Classify any failures against the durable-session/task root causes before
  changing code.

Code fixes made during this loop:

- `src/status/status-text.ts`: `os.uptime()` is now guarded. If the platform
  denies uptime access, status output reports system uptime as unavailable
  instead of throwing. This is generic platform hardening, not a durable-runtime
  semantic change.
- `src/durable/coordination-projection.ts`: legacy `lost` recovery diagnostics
  without explicit `retrySafety: "safe_to_retry"` no longer expose
  `controls.canRetry=true`. The projection now defaults ambiguous lost runs to
  `retrySafety: "inspect_first"` and
  `requiredAction: "inspect_timeline_before_retry"`.
- `scripts/aClaw` and `scripts/eClaw`: wrappers now opt into
  `OPENCLAW_DURABLE_RUNTIME=1` and `OPENCLAW_DURABLE_WORKER=1`.
- `scripts/eClaw`: wrapper now points at the active durable-runtime worktree and
  local E port.

Machine verification:

```sh
node scripts/run-vitest.mjs src/status/status-text.test.ts src/durable/coordination-projection.test.ts
# passed: 11 tests

node scripts/run-vitest.mjs \
  src/durable/intake.test.ts \
  src/durable/agent-turn.test.ts \
  src/durable/subagent.test.ts \
  src/durable/fan-in.test.ts \
  src/durable/fan-in-snapshot.test.ts \
  src/durable/recovery.test.ts \
  src/durable/restart-interruption.test.ts \
  src/durable/coordination-projection.test.ts \
  src/durable/startup.test.ts \
  src/agents/subagent-orphan-recovery.test.ts
# passed: 43 durable tests + 21 agent tests

npm run tsgo:core
# passed

npm run build
# passed; required so openclaw.mjs uses the rebuilt dist bundle
```

Runtime verification:

- OpenClaw A restarted safely and reported app `2026.6.11`, gateway reachable,
  pid `96140`.
- OpenClaw E restarted safely and reported app `2026.6.11`, gateway reachable,
  pid `97900`.
- Durable projection on the historical A lost subagent run
  `run_fe190e68-787d-4eb4-9cb5-2dedd0505663` now reports:
  `canRetry=false`, `retryable=false`, `retrySafety=inspect_first`,
  `requiredAction=inspect_timeline_before_retry`.

Live CLI tests:

| Case                     | Instance | Prompt shape                                                         | Expected durable behavior                                                                                                                                      | Result                                                                                                                                                                                                                                       |
| ------------------------ | -------- | -------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Smoke direct turn        | A / Bơ   | Short health check, no task creation                                 | `openclaw.agent.turn` succeeds terminal, no open durable debt added                                                                                            | Passed. Bơ replied in one sentence; run `run_de76aa48-d823-4f10-930a-e842fe011de9` succeeded.                                                                                                                                                |
| Smoke direct turn        | E / Xu   | Short health check, no task creation                                 | `openclaw.agent.turn` succeeds terminal, no open durable debt added                                                                                            | Passed. Xu replied in one sentence; run `run_ab7f8622-ba25-4933-b2cc-0ad135d9ec0a` succeeded.                                                                                                                                                |
| Two-child fan-out/fan-in | E / Xu   | Human-style request to ask two team members and synthesize 3 bullets | Parent yields `waiting_children`, child links are durable, partial progress is surfaced, both child result mailboxes are acknowledged, parent reaches terminal | Passed. Parent `run_41d23f40-730b-47dd-ba73-5fd8bc9ab947` reached `succeeded/terminal`; 2 children succeeded; fan-in step and both result mailboxes succeeded/acknowledged. Transcript included waiting, partial, and final `done` messages. |
| Two-child fan-out/fan-in | A / Bơ   | Same shape against the historically noisy local A instance           | Same as E, plus no new historical-open debt                                                                                                                    | Passed. Parent `run_20ace731-67ab-424a-9baa-b7040e487df4` reached `succeeded/terminal`; both child subagent runs succeeded; transcript included waiting, partial, and final `done` messages.                                                 |

Observed issues and classification:

- A still has 33 open durable runs after the test. The new tests did not add new
  open debt; this is historical state from earlier restart/orphan experiments
  and needs a separate reconciliation/retention policy, not a new live fan-in
  failure.
- A and E repeatedly emit legacy state migration warnings for
  `logs/config-health.json` conflicts with shared SQLite state. This is a
  migration-cleanup/operator-noise issue. It does not block durable run
  correctness but should be reduced before upstream proof.
- `openclaw sessions --help` can display a stale commit hash while
  `openclaw --version`, `status`, and `dist/build-info.json` show the rebuilt
  branch commit. This looks like command help metadata drift, not an entrypoint
  mismatch, but should be tracked as production polish.

Architecture conclusion for this loop:

- The safe direction still holds: Durable Core should not auto-retry or
  synthesize child work after a generic lost state. It should preserve enough
  facts for parent/coordinator inspection and make unsafe retry affordances
  unavailable by default.
- The fan-in behavior now matches the desired product outcome for the tested
  happy path and staggered-child path: the parent does not treat spawn as done,
  emits visible waiting/progress state, consumes child results through durable
  mailboxes, and finalizes only when the fan-in snapshot is complete.
- The next gap is not another prompt/profile rule. The core still needs a
  first-class inspect/reconcile decision surface and historical-open-run
  retention/reconciliation policy so operators and parent agents can resolve old
  debt without unsafe hidden replay.
