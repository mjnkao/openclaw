import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { resolveStorePath } from "../config/sessions/paths.js";
import { updateSessionStore } from "../config/sessions/store.js";
import { peekSystemEvents, resetSystemEventsForTest } from "../infra/system-events.js";
import {
  createTaskRecord,
  getTaskById,
  markTaskTerminalById,
  resetTaskRegistryForTests,
  setTaskRegistryDeliveryRuntimeForTests,
} from "../tasks/runtime-internal.js";
import {
  reconcileDurableOwnerAttentionFact,
  sessionStoreOwnerAdapter,
  taskRunsOwnerAdapter,
} from "./owner-adapters.js";
import { openDurableRuntimeStore } from "./store-factory.js";

describe("durable canonical owner adapters", () => {
  let stateDir: string;
  let previousStateDir: string | undefined;

  beforeEach(() => {
    stateDir = fs.mkdtempSync(path.join(os.tmpdir(), "openclaw-owner-adapter-"));
    previousStateDir = process.env.OPENCLAW_STATE_DIR;
    process.env.OPENCLAW_STATE_DIR = stateDir;
    resetTaskRegistryForTests({ persist: false });
    resetSystemEventsForTest();
  });

  afterEach(() => {
    resetTaskRegistryForTests({ persist: false });
    resetSystemEventsForTest();
    if (previousStateDir === undefined) {
      delete process.env.OPENCLAW_STATE_DIR;
    } else {
      process.env.OPENCLAW_STATE_DIR = previousStateDir;
    }
    fs.rmSync(stateDir, { recursive: true, force: true });
  });

  it("projects an overdue official task without mirroring its lifecycle", () => {
    const task = createTaskRecord({
      runtime: "cli",
      requesterSessionKey: "agent:test:main",
      ownerKey: "agent:test:main",
      task: "long command",
      status: "running",
      deliveryStatus: "pending",
      notifyPolicy: "done_only",
      startedAt: 100,
      lastEventAt: 100,
      progressSummary: "Still processing",
    });
    expect(task).not.toBeNull();
    const fact = taskRunsOwnerAdapter.listAttentionFacts({ now: 1_000_000, limit: 10 })[0];
    expect(fact).toMatchObject({
      sourceOwner: "task_runs",
      sourceRef: task!.taskId,
      reason: "child_overdue",
      targetRef: "agent:test:main",
    });

    const store = openDurableRuntimeStore();
    try {
      reconcileDurableOwnerAttentionFact({ store, fact: fact!, now: 1_000_000 });
      expect(store.listRuns()).toHaveLength(0);
      expect(store.listWakeObligations({ sourceOwner: "task_runs" })).toEqual([
        expect.objectContaining({ sourceRef: task!.taskId, status: "pending" }),
      ]);
      const revisedFact = {
        ...fact!,
        sourceRevision: "task-revision-2",
        metadata: { ...fact!.metadata, sourceRevision: "task-revision-2" },
      };
      const revised = reconcileDurableOwnerAttentionFact({
        store,
        fact: revisedFact,
        now: 1_000_100,
      }).wake;
      expect(revised.metadata).toMatchObject({ sourceRevision: "task-revision-2" });
      expect(
        store.acknowledgeWakeObligation({
          wakeId: revised.wakeId,
          actorKind: "operator",
          actorRef: "test",
          expectedSourceRevision: fact!.sourceRevision,
        }),
      ).toBeUndefined();
    } finally {
      store.close();
    }
  });

  it("retries terminal delivery through the official task owner", async () => {
    const sendMessage = vi.fn(async () => ({ messageId: "message-1" }));
    setTaskRegistryDeliveryRuntimeForTests({ sendMessage } as never);
    const created = createTaskRecord({
      runtime: "cli",
      requesterSessionKey: "agent:test:main",
      ownerKey: "agent:test:main",
      requesterOrigin: { channel: "telegram", to: "user-1" },
      task: "long command",
      status: "running",
      deliveryStatus: "failed",
      notifyPolicy: "done_only",
      startedAt: 100,
      lastEventAt: 200,
    });
    const task = markTaskTerminalById({
      taskId: created!.taskId,
      status: "failed",
      endedAt: 200,
      terminalSummary: "command failed",
    });
    expect(task).not.toBeNull();
    const fact = taskRunsOwnerAdapter.inspect(task!.taskId);
    expect(fact).toMatchObject({ sourceOwner: "task_runs", reason: "child_terminal" });

    const result = await taskRunsOwnerAdapter.dispatchAttention({
      wake: {
        wakeId: "wake-task",
        sourceOwner: "task_runs",
        sourceRef: task!.taskId,
        reason: "child_terminal",
      } as never,
      claimToken: "claim-task",
    });

    expect(result).toMatchObject({ kind: "delivered" });
    expect(sendMessage).toHaveBeenCalledOnce();
    expect(getTaskById(task!.taskId)?.deliveryStatus).toBe("delivered");
  });

  it("hands restart uncertainty to the canonical session owner without replaying work", async () => {
    const sessionKey = "agent:test:durable-restart";
    const storePath = resolveStorePath(undefined, { agentId: "test", env: process.env });
    await updateSessionStore(storePath, (store) => {
      store[sessionKey] = {
        sessionId: "session-restart",
        updatedAt: Date.now(),
        totalTokens: 0,
        totalTokensFresh: true,
      };
    });

    const result = await sessionStoreOwnerAdapter.dispatchAttention({
      wake: {
        wakeId: "wake-restart",
        sourceOwner: "session_store",
        sourceRef: sessionKey,
        sourceRunId: "run-restart",
        targetKind: "agent_session",
        targetRef: sessionKey,
        reason: "restart_interrupted",
      } as never,
      claimToken: "claim-restart",
    });

    expect(result).toMatchObject({
      kind: "delivered",
      evidence: {
        proofBoundary: "target_session_handoff",
        ownerResult: "system_event_queued",
        sessionKey,
        sessionId: "session-restart",
        userDeliveryProven: false,
      },
    });
    expect(peekSystemEvents(sessionKey)).toEqual([
      expect.stringContaining("Do not repeat tools or external side effects automatically"),
    ]);
  });

  it("suspends session attention when the canonical session no longer exists", async () => {
    const result = await sessionStoreOwnerAdapter.dispatchAttention({
      wake: {
        wakeId: "wake-missing",
        sourceOwner: "session_store",
        sourceRef: "agent:test:missing",
        targetKind: "agent_session",
        targetRef: "agent:test:missing",
        reason: "restart_interrupted",
      } as never,
      claimToken: "claim-missing",
    });

    expect(result).toEqual({ kind: "suspended", reason: "canonical_session_missing" });
  });
});
