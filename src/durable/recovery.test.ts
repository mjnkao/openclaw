import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { resetConfigRuntimeState, setRuntimeConfigSnapshot } from "../config/runtime-snapshot.js";
import { resolveDurableRuntimeSqlitePath } from "./config.js";
import {
  reconcileDueDurableTimers,
  reconcileDurableAgentTurnsOnGatewayStartup,
  reconcileDurableChatSendsOnGatewayStartup,
  reconcilePendingDurableSignals,
  reconcileStaleDurableAgentTurns,
  reconcileStaleDurableChatSends,
  resolveDurableStaleRuntimeRunAfterMs,
  startDurableRecoveryWorker,
} from "./recovery.js";
import {
  DURABLE_AGENT_TURN_OPERATION_KIND,
  DURABLE_CHAT_SEND_OPERATION_KIND,
} from "./runtime-ids.js";
import { openDurableRuntimeSqliteStore } from "./sqlite-store.js";

describe("durable runtime recovery", () => {
  afterEach(() => {
    resetConfigRuntimeState();
  });

  it("derives lost-heartbeat detection from the worker lease with a safe floor", () => {
    setRuntimeConfigSnapshot({
      durable: { mode: "authority", worker: { claimTtlMs: 300_000 } },
    });
    expect(resolveDurableStaleRuntimeRunAfterMs()).toBe(600_000);
    setRuntimeConfigSnapshot({
      durable: { mode: "authority", worker: { claimTtlMs: 1000 } },
    });
    expect(resolveDurableStaleRuntimeRunAfterMs()).toBe(120_000);
  });

  it("does not start the recovery worker unless authority is explicit", async () => {
    const stateDir = fs.mkdtempSync(path.join(os.tmpdir(), "openclaw-durable-recovery-worker-"));
    const env = { OPENCLAW_STATE_DIR: stateDir };
    setRuntimeConfigSnapshot({ durable: { mode: "observe" } });
    vi.useFakeTimers();
    try {
      const stop = startDurableRecoveryWorker({ processInstanceId: "process-observation", env });
      vi.advanceTimersByTime(120_000);
      await stop();
      expect(fs.existsSync(resolveDurableRuntimeSqlitePath(env))).toBe(false);
    } finally {
      vi.useRealTimers();
      fs.rmSync(stateDir, { recursive: true, force: true });
    }
  });

  it("uses the configured production poll interval for due recovery work", async () => {
    const stateDir = fs.mkdtempSync(path.join(os.tmpdir(), "openclaw-durable-recovery-worker-"));
    const env = { OPENCLAW_STATE_DIR: stateDir };
    setRuntimeConfigSnapshot({
      durable: {
        mode: "authority",
        worker: { pollIntervalMs: 25, claimTtlMs: 120 },
      },
    });
    vi.useFakeTimers();
    vi.setSystemTime(1_000);
    const store = openDurableRuntimeSqliteStore({ path: resolveDurableRuntimeSqlitePath(env) });
    const run = store.createRun({
      operationKind: "test.recovery-poll",
      rootOperationReason: "recovery_poll_test",
      status: "waiting_timer",
      recoveryState: "waiting_timer",
      now: 1_000,
    });
    store.createStep({
      runtimeRunId: run.runtimeRunId,
      stepType: "timer",
      status: "waiting",
      recoveryState: "waiting_timer",
      now: 1_000,
    });
    store.createTimer({
      runtimeRunId: run.runtimeRunId,
      timerType: "sleep",
      dueAt: 1_020,
      now: 1_000,
    });
    const stop = startDurableRecoveryWorker({ processInstanceId: "process-authority", env });
    try {
      await vi.advanceTimersByTimeAsync(24);
      expect(store.getRun(run.runtimeRunId)).toMatchObject({ status: "waiting_timer" });

      await vi.advanceTimersByTimeAsync(1);
      expect(store.getRun(run.runtimeRunId)).toMatchObject({
        status: "queued",
        recoveryState: "runnable",
      });
    } finally {
      await stop();
      store.close();
      vi.useRealTimers();
      fs.rmSync(stateDir, { recursive: true, force: true });
    }
  });

  it("marks only running agent turns lost at gateway startup", () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "openclaw-durable-recovery-"));
    const store = openDurableRuntimeSqliteStore({ path: path.join(dir, "openclaw.sqlite") });
    try {
      const running = store.createRun({
        operationKind: DURABLE_AGENT_TURN_OPERATION_KIND,
        sourceOwner: "session_store",
        sourceRef: "agent:test:running",
        idempotencyKey: "run-running",
        status: "running",
        recoveryState: "running",
        metadata: { sessionKey: "agent:test:running" },
        now: 100,
      });
      store.createStep({
        runtimeRunId: running.runtimeRunId,
        stepId: "agent_invocation",
        stepType: "agent",
        status: "running",
        recoveryState: "running",
        now: 100,
      });
      const waiting = store.createRun({
        operationKind: DURABLE_AGENT_TURN_OPERATION_KIND,
        rootOperationReason: "recovery_test_waiting_turn",
        status: "waiting_signal",
        recoveryState: "waiting_signal",
        now: 100,
      });

      expect(
        reconcileDurableAgentTurnsOnGatewayStartup({
          store,
          processInstanceId: "process-1",
          now: 200,
        }),
      ).toEqual({ scanned: 2, markedLost: 1 });
      expect(store.getRun(running.runtimeRunId)).toMatchObject({
        status: "lost",
        recoveryState: "terminal",
        completedAt: 200,
      });
      expect(store.listSteps(running.runtimeRunId)).toEqual([
        expect.objectContaining({ status: "lost", recoveryState: "terminal" }),
      ]);
      expect(store.getRun(waiting.runtimeRunId)).toMatchObject({ status: "waiting_signal" });
      expect(store.listWakeObligations()).toEqual([
        expect.objectContaining({ reason: "restart_interrupted", status: "pending" }),
      ]);
    } finally {
      store.close();
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it("refuses to mark runs lost when their snapshot or eligibility changes after scan", () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "openclaw-durable-recovery-race-"));
    const store = openDurableRuntimeSqliteStore({ path: path.join(dir, "openclaw.sqlite") });
    try {
      const running = store.createRun({
        operationKind: DURABLE_AGENT_TURN_OPERATION_KIND,
        rootOperationReason: "recovery_test_scan_race",
        status: "running",
        recoveryState: "running",
        metadata: { ownerState: "scanned" },
        now: 100,
      });
      const noLongerEligible = store.createRun({
        operationKind: DURABLE_AGENT_TURN_OPERATION_KIND,
        rootOperationReason: "recovery_test_eligibility_race",
        status: "running",
        recoveryState: "running",
        now: 100,
      });
      const listOpenRuns = store.listOpenRuns.bind(store);
      const scan = vi.spyOn(store, "listOpenRuns").mockImplementationOnce((options) => {
        const snapshots = listOpenRuns(options);
        store.updateRun({
          runtimeRunId: running.runtimeRunId,
          heartbeatAt: 150,
          metadata: { ownerState: "advanced" },
          now: 150,
        });
        store.updateRun({
          runtimeRunId: noLongerEligible.runtimeRunId,
          status: "waiting_signal",
          recoveryState: "waiting_signal",
          now: 150,
        });
        return snapshots;
      });
      try {
        expect(
          reconcileDurableAgentTurnsOnGatewayStartup({
            store,
            processInstanceId: "process-race",
            now: 200,
          }),
        ).toEqual({ scanned: 2, markedLost: 0 });
      } finally {
        scan.mockRestore();
      }

      expect(store.getRun(running.runtimeRunId)).toMatchObject({
        status: "running",
        recoveryState: "running",
        heartbeatAt: 150,
        metadata: { ownerState: "advanced" },
      });
      expect(store.getTimeline(running.runtimeRunId)).toEqual([]);
      expect(store.getRun(noLongerEligible.runtimeRunId)).toMatchObject({
        status: "waiting_signal",
        recoveryState: "waiting_signal",
      });
      expect(store.getTimeline(noLongerEligible.runtimeRunId)).toEqual([]);
      expect(store.listWakeObligations()).toEqual([]);
    } finally {
      store.close();
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it("does not mark a run lost while a step claim is active", () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "openclaw-durable-recovery-claim-"));
    const store = openDurableRuntimeSqliteStore({ path: path.join(dir, "openclaw.sqlite") });
    try {
      const running = store.createRun({
        operationKind: DURABLE_AGENT_TURN_OPERATION_KIND,
        rootOperationReason: "recovery_test_active_claim",
        status: "running",
        recoveryState: "running",
        now: 100,
      });
      store.createStep({
        runtimeRunId: running.runtimeRunId,
        stepId: "agent_invocation",
        stepType: "agent",
        status: "pending",
        recoveryState: "runnable",
        now: 100,
      });
      const claim = store.claimNextRunnableStep({
        operationKind: DURABLE_AGENT_TURN_OPERATION_KIND,
        workerId: "active-worker",
        claimTtlMs: 200,
        now: 100,
      });
      expect(claim?.step.stepId).toBe("agent_invocation");

      expect(
        reconcileDurableAgentTurnsOnGatewayStartup({
          store,
          processInstanceId: "process-active-claim",
          now: 200,
        }),
      ).toEqual({ scanned: 1, markedLost: 0 });
      expect(store.getRun(running.runtimeRunId)?.status).toBe("running");
      expect(store.listSteps(running.runtimeRunId)).toEqual([
        expect.objectContaining({
          recoveryState: "claimed",
          claimedBy: claim?.claimToken,
          claimExpiresAt: 300,
        }),
      ]);

      expect(
        reconcileDurableAgentTurnsOnGatewayStartup({
          store,
          processInstanceId: "process-expired-claim",
          now: 301,
        }),
      ).toEqual({ scanned: 1, markedLost: 1 });
      expect(store.getRun(running.runtimeRunId)?.status).toBe("lost");
      expect(store.listSteps(running.runtimeRunId)).toEqual([
        expect.objectContaining({ status: "lost", recoveryState: "terminal" }),
      ]);
    } finally {
      store.close();
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it("rolls back the lost transition when its recovery wake cannot be recorded", () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "openclaw-durable-recovery-atomic-"));
    const store = openDurableRuntimeSqliteStore({ path: path.join(dir, "openclaw.sqlite") });
    try {
      const running = store.createRun({
        operationKind: DURABLE_AGENT_TURN_OPERATION_KIND,
        sourceOwner: "session_store",
        sourceRef: "agent:test:atomic-recovery",
        status: "running",
        recoveryState: "running",
        now: 100,
      });
      store.createStep({
        runtimeRunId: running.runtimeRunId,
        stepId: "agent_invocation",
        stepType: "agent",
        status: "pending",
        recoveryState: "runnable",
        now: 100,
      });
      const claim = store.claimNextRunnableStep({
        operationKind: DURABLE_AGENT_TURN_OPERATION_KIND,
        workerId: "expired-worker",
        claimTtlMs: 50,
        now: 100,
      });
      expect(claim?.step.stepId).toBe("agent_invocation");
      const createWakeObligation = store.createWakeObligation.bind(store);
      store.createWakeObligation = () => {
        throw new Error("injected recovery wake failure");
      };
      try {
        expect(() =>
          reconcileDurableAgentTurnsOnGatewayStartup({
            store,
            processInstanceId: "process-atomic",
            now: 200,
          }),
        ).toThrow("injected recovery wake failure");
      } finally {
        store.createWakeObligation = createWakeObligation;
      }

      expect(store.getRun(running.runtimeRunId)).toMatchObject({
        status: "running",
        recoveryState: "running",
      });
      expect(store.getRun(running.runtimeRunId)?.completedAt).toBeUndefined();
      expect(store.listSteps(running.runtimeRunId)).toEqual([
        expect.objectContaining({
          status: "queued",
          recoveryState: "claimed",
          claimedBy: claim?.claimToken,
          claimExpiresAt: 150,
        }),
      ]);
      expect(store.getTimeline(running.runtimeRunId)).toEqual([]);
      expect(store.listUncertaintyFacts()).toEqual([]);
      expect(store.listWakeObligations()).toEqual([]);
      expect(
        store.releaseStepClaim({
          runtimeRunId: running.runtimeRunId,
          stepId: "agent_invocation",
          claimToken: claim!.claimToken,
          now: 201,
        }),
      ).toMatchObject({ status: "queued", recoveryState: "runnable" });
    } finally {
      store.close();
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it("marks accepted chat intake lost after restart without replaying it", () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "openclaw-durable-recovery-"));
    const store = openDurableRuntimeSqliteStore({ path: path.join(dir, "openclaw.sqlite") });
    try {
      const chat = store.createRun({
        operationKind: DURABLE_CHAT_SEND_OPERATION_KIND,
        sourceOwner: "session_store",
        sourceRef: "agent:test:main",
        status: "received",
        recoveryState: "runnable",
        now: 100,
      });
      expect(
        reconcileDurableChatSendsOnGatewayStartup({
          store,
          processInstanceId: "process-2",
          now: 200,
        }),
      ).toEqual({ scanned: 1, markedLost: 1 });
      expect(store.getRun(chat.runtimeRunId)).toMatchObject({ status: "lost" });
      expect(store.getTimeline(chat.runtimeRunId).at(-1)).toMatchObject({
        eventType: "chat.send.lost",
      });
    } finally {
      store.close();
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it("marks only stale active front-door runs lost", () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "openclaw-durable-recovery-"));
    const store = openDurableRuntimeSqliteStore({ path: path.join(dir, "openclaw.sqlite") });
    try {
      const staleAgent = store.createRun({
        operationKind: DURABLE_AGENT_TURN_OPERATION_KIND,
        rootOperationReason: "recovery_test_stale_agent",
        status: "running",
        recoveryState: "running",
        now: 100,
      });
      const freshAgent = store.createRun({
        operationKind: DURABLE_AGENT_TURN_OPERATION_KIND,
        rootOperationReason: "recovery_test_fresh_agent",
        status: "running",
        recoveryState: "running",
        now: 1_900,
      });
      const staleChat = store.createRun({
        operationKind: DURABLE_CHAT_SEND_OPERATION_KIND,
        rootOperationReason: "recovery_test_stale_chat",
        status: "received",
        recoveryState: "runnable",
        now: 100,
      });
      expect(
        reconcileStaleDurableAgentTurns({
          store,
          processInstanceId: "process-3",
          now: 2_000,
          staleAfterMs: 1_000,
        }),
      ).toMatchObject({ scanned: 1, markedLost: 1 });
      expect(
        reconcileStaleDurableChatSends({
          store,
          processInstanceId: "process-3",
          now: 2_000,
          staleAfterMs: 1_000,
        }),
      ).toMatchObject({ scanned: 1, markedLost: 1 });
      expect(store.getRun(staleAgent.runtimeRunId)?.status).toBe("lost");
      expect(store.getRun(freshAgent.runtimeRunId)?.status).toBe("running");
      expect(store.getRun(staleChat.runtimeRunId)?.status).toBe("lost");
    } finally {
      store.close();
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it("advances stale-run scans past an active-claim prefix", () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "openclaw-durable-stale-cursor-"));
    const store = openDurableRuntimeSqliteStore({ path: path.join(dir, "openclaw.sqlite") });
    try {
      for (const [index, runtimeRunId] of ["run_active_1", "run_active_2"].entries()) {
        store.createRun({
          runtimeRunId,
          operationKind: DURABLE_AGENT_TURN_OPERATION_KIND,
          rootOperationReason: `recovery_test_active_prefix_${index}`,
          status: "running",
          recoveryState: "running",
          now: 100 + index,
        });
        store.createStep({
          runtimeRunId,
          stepId: "agent_invocation",
          stepType: "agent",
          status: "pending",
          recoveryState: "runnable",
          now: 100 + index,
        });
        expect(
          store.claimNextRunnableStep({
            operationKind: DURABLE_AGENT_TURN_OPERATION_KIND,
            workerId: `active-worker-${index}`,
            claimTtlMs: 10_000,
            now: 200 + index,
          })?.step.runtimeRunId,
        ).toBe(runtimeRunId);
      }
      const lostCandidate = store.createRun({
        runtimeRunId: "run_lost_after_active_prefix",
        operationKind: DURABLE_AGENT_TURN_OPERATION_KIND,
        rootOperationReason: "recovery_test_after_active_prefix",
        status: "running",
        recoveryState: "running",
        now: 102,
      });

      const first = reconcileStaleDurableAgentTurns({
        store,
        processInstanceId: "process-cursor-1",
        now: 2_000,
        staleAfterMs: 1_000,
        limit: 1,
      });
      expect(first).toMatchObject({ scanned: 1, markedLost: 0, complete: false });
      if (!first.nextCursor) {
        throw new Error("expected a continuation after the first stale-run page");
      }
      const second = reconcileStaleDurableAgentTurns({
        store,
        processInstanceId: "process-cursor-2",
        now: 2_100,
        staleAfterMs: 1_000,
        cursor: first.nextCursor,
        limit: 1,
      });
      expect(second).toMatchObject({ scanned: 1, markedLost: 0, complete: false });
      if (!second.nextCursor) {
        throw new Error("expected a continuation after the second stale-run page");
      }
      const third = reconcileStaleDurableAgentTurns({
        store,
        processInstanceId: "process-cursor-3",
        now: 2_200,
        staleAfterMs: 1_000,
        cursor: second.nextCursor,
        limit: 1,
      });
      expect(third).toMatchObject({ scanned: 1, markedLost: 1, complete: true });
      expect(store.getRun(lostCandidate.runtimeRunId)?.status).toBe("lost");
    } finally {
      store.close();
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it("queues retry work only when its durable timer is due", () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "openclaw-durable-recovery-"));
    const store = openDurableRuntimeSqliteStore({ path: path.join(dir, "openclaw.sqlite") });
    try {
      const run = store.createRun({
        operationKind: "test.retry",
        rootOperationReason: "recovery_test_retry",
        status: "retry_scheduled",
        recoveryState: "retry_scheduled",
        now: 100,
      });
      store.createStep({
        runtimeRunId: run.runtimeRunId,
        stepId: "retry",
        stepType: "tool",
        status: "retry_scheduled",
        recoveryState: "retry_scheduled",
        now: 100,
      });
      const timer = store.createTimer({
        runtimeRunId: run.runtimeRunId,
        stepId: "retry",
        timerType: "retry",
        dueAt: 200,
        now: 100,
      });
      const appendEvent = store.appendEvent.bind(store);
      store.appendEvent = () => {
        throw new Error("injected timer event failure");
      };
      try {
        expect(() =>
          reconcileDueDurableTimers({ store, processInstanceId: "process-4", now: 200 }),
        ).toThrow("injected timer event failure");
      } finally {
        store.appendEvent = appendEvent;
      }
      expect(store.listTimers(run.runtimeRunId)).toEqual([
        expect.objectContaining({ timerId: timer.timerId, status: "pending" }),
      ]);
      expect(store.getRun(run.runtimeRunId)).toMatchObject({
        status: "retry_scheduled",
        recoveryState: "retry_scheduled",
      });
      expect(
        reconcileDueDurableTimers({ store, processInstanceId: "process-4", now: 200 }),
      ).toMatchObject({ firedTimers: 1, queuedRuns: 1 });
      expect(store.getRun(run.runtimeRunId)).toMatchObject({
        status: "queued",
        recoveryState: "runnable",
      });
      expect(store.listTimers(run.runtimeRunId)).toEqual([
        expect.objectContaining({ timerId: timer.timerId, status: "fired" }),
      ]);
    } finally {
      store.close();
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it("fires stale timers without requeueing nonmatching steps or live runs", () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "openclaw-durable-recovery-timer-race-"));
    const store = openDurableRuntimeSqliteStore({ path: path.join(dir, "openclaw.sqlite") });
    try {
      const nonmatchingStepRun = store.createRun({
        operationKind: "test.timer-stale-step",
        rootOperationReason: "recovery_test_timer_stale_step",
        status: "waiting_timer",
        recoveryState: "waiting_timer",
        now: 100,
      });
      store.createStep({
        runtimeRunId: nonmatchingStepRun.runtimeRunId,
        stepId: "wait",
        stepType: "timer",
        status: "waiting",
        recoveryState: "waiting_timer",
        now: 100,
      });
      store.createTimer({
        runtimeRunId: nonmatchingStepRun.runtimeRunId,
        stepId: "wait",
        timerType: "sleep",
        dueAt: 200,
        now: 100,
      });
      store.updateStep({
        runtimeRunId: nonmatchingStepRun.runtimeRunId,
        stepId: "wait",
        status: "queued",
        recoveryState: "runnable",
        now: 150,
      });

      const liveRun = store.createRun({
        operationKind: "test.timer-live-run",
        rootOperationReason: "recovery_test_timer_live_run",
        status: "waiting_timer",
        recoveryState: "waiting_timer",
        now: 100,
      });
      store.createStep({
        runtimeRunId: liveRun.runtimeRunId,
        stepId: "wait",
        stepType: "timer",
        status: "waiting",
        recoveryState: "waiting_timer",
        now: 100,
      });
      store.createTimer({
        runtimeRunId: liveRun.runtimeRunId,
        stepId: "wait",
        timerType: "sleep",
        dueAt: 200,
        now: 100,
      });
      store.updateRun({
        runtimeRunId: liveRun.runtimeRunId,
        status: "running",
        recoveryState: "running",
        now: 150,
      });

      expect(
        reconcileDueDurableTimers({ store, processInstanceId: "process-stale-timer", now: 200 }),
      ).toMatchObject({ firedTimers: 2, queuedRuns: 0 });
      expect(store.getRun(nonmatchingStepRun.runtimeRunId)).toMatchObject({
        status: "waiting_timer",
        recoveryState: "waiting_timer",
      });
      expect(store.listSteps(nonmatchingStepRun.runtimeRunId)).toEqual([
        expect.objectContaining({ status: "queued", recoveryState: "runnable" }),
      ]);
      expect(store.getRun(liveRun.runtimeRunId)).toMatchObject({
        status: "running",
        recoveryState: "running",
      });
      expect(store.listSteps(liveRun.runtimeRunId)).toEqual([
        expect.objectContaining({ status: "waiting", recoveryState: "waiting_timer" }),
      ]);
    } finally {
      store.close();
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it("consumes resume signals once and requeues their waiting run", () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "openclaw-durable-recovery-"));
    const store = openDurableRuntimeSqliteStore({ path: path.join(dir, "openclaw.sqlite") });
    try {
      const run = store.createRun({
        operationKind: "test.signal",
        rootOperationReason: "recovery_test_signal",
        status: "waiting_signal",
        recoveryState: "waiting_signal",
        now: 100,
      });
      store.createStep({
        runtimeRunId: run.runtimeRunId,
        stepId: "wait",
        stepType: "signal",
        status: "waiting",
        recoveryState: "waiting_signal",
        now: 100,
      });
      const signal = store.createSignal({
        runtimeRunId: run.runtimeRunId,
        signalType: "resume",
        now: 150,
      });
      const appendEvent = store.appendEvent.bind(store);
      store.appendEvent = () => {
        throw new Error("injected signal event failure");
      };
      try {
        expect(() =>
          reconcilePendingDurableSignals({ store, processInstanceId: "process-5", now: 200 }),
        ).toThrow("injected signal event failure");
      } finally {
        store.appendEvent = appendEvent;
      }
      expect(store.listPendingSignals()).toEqual([
        expect.objectContaining({ signalId: signal.signalId }),
      ]);
      expect(store.listPendingSignals()[0]?.consumedAt).toBeUndefined();
      expect(store.getRun(run.runtimeRunId)).toMatchObject({
        status: "waiting_signal",
        recoveryState: "waiting_signal",
      });
      expect(
        reconcilePendingDurableSignals({ store, processInstanceId: "process-5", now: 200 }),
      ).toMatchObject({ consumedSignals: 1, queuedRuns: 1 });
      expect(store.getRun(run.runtimeRunId)).toMatchObject({
        status: "queued",
        recoveryState: "runnable",
      });
      expect(store.listSignals(run.runtimeRunId)).toEqual([
        expect.objectContaining({ signalId: signal.signalId, consumedAt: 200 }),
      ]);
    } finally {
      store.close();
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it("consumes stale resume signals without requeueing a nonmatching step", () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "openclaw-durable-recovery-signal-race-"));
    const store = openDurableRuntimeSqliteStore({ path: path.join(dir, "openclaw.sqlite") });
    try {
      const run = store.createRun({
        operationKind: "test.signal-stale-step",
        rootOperationReason: "recovery_test_signal_stale_step",
        status: "waiting_signal",
        recoveryState: "waiting_signal",
        now: 100,
      });
      store.createStep({
        runtimeRunId: run.runtimeRunId,
        stepId: "wait",
        stepType: "signal",
        status: "waiting",
        recoveryState: "waiting_signal",
        now: 100,
      });
      const signal = store.createSignal({
        runtimeRunId: run.runtimeRunId,
        stepId: "wait",
        signalType: "resume",
        now: 120,
      });
      store.updateStep({
        runtimeRunId: run.runtimeRunId,
        stepId: "wait",
        status: "queued",
        recoveryState: "runnable",
        now: 150,
      });

      expect(
        reconcilePendingDurableSignals({
          store,
          processInstanceId: "process-stale-signal",
          now: 200,
        }),
      ).toMatchObject({ consumedSignals: 1, queuedRuns: 0 });
      expect(store.getRun(run.runtimeRunId)).toMatchObject({
        status: "waiting_signal",
        recoveryState: "waiting_signal",
      });
      expect(store.listSteps(run.runtimeRunId)).toEqual([
        expect.objectContaining({ status: "queued", recoveryState: "runnable" }),
      ]);
      expect(store.listSignals(run.runtimeRunId)).toEqual([
        expect.objectContaining({ signalId: signal.signalId, consumedAt: 200 }),
      ]);
    } finally {
      store.close();
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });
});
