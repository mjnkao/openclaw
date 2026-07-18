import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { saveSubagentRegistryToSqlite } from "../agents/subagent-registry.store.sqlite.js";
import type { SubagentRunRecord } from "../agents/subagent-registry.types.js";
import { openDurableRuntimeStore } from "./store-factory.js";
import { runDurableWakeDispatcherOnce } from "./wake-dispatcher.js";

describe("durable wake dispatcher", () => {
  let stateDir: string;
  let previousStateDir: string | undefined;

  beforeEach(() => {
    stateDir = fs.mkdtempSync(path.join(os.tmpdir(), "openclaw-wake-dispatcher-"));
    previousStateDir = process.env.OPENCLAW_STATE_DIR;
    process.env.OPENCLAW_STATE_DIR = stateDir;
  });

  afterEach(() => {
    if (previousStateDir === undefined) {
      delete process.env.OPENCLAW_STATE_DIR;
    } else {
      process.env.OPENCLAW_STATE_DIR = previousStateDir;
    }
    fs.rmSync(stateDir, { recursive: true, force: true });
  });

  it("suspends a resolved obligation when its canonical owner adapter is unavailable", async () => {
    const store = openDurableRuntimeStore();
    const wake = store.createWakeObligation({
      sourceOwner: "plugin_jobs",
      sourceRef: "job-1",
      targetKind: "agent_session",
      targetRef: "agent:test:main",
      targetResolutionStatus: "resolved",
      reason: "operator_requested",
      dedupeKey: "plugin-job-1",
      now: 100,
    });

    const result = await runDurableWakeDispatcherOnce({
      store,
      workerId: "worker-1",
      now: 100,
      claimTtlMs: 50,
    });

    expect(result).toMatchObject({ claimed: 1, suspended: 1 });
    expect(store.getWakeObligation(wake.wakeId)).toMatchObject({
      status: "suspended",
      failedReason: "owner_adapter_not_registered",
    });
    expect(store.listDeliveryAttemptEvidence({ wakeId: wake.wakeId })).toEqual([
      expect.objectContaining({ status: "failed", error: "owner_adapter_not_registered" }),
    ]);
    store.close();
  });

  it("quarantines an expired in-flight side effect as unknown instead of replaying it", async () => {
    const store = openDurableRuntimeStore();
    const wake = store.createWakeObligation({
      sourceOwner: "plugin_jobs",
      sourceRef: "job-unknown",
      targetKind: "agent_session",
      targetRef: "agent:test:main",
      targetResolutionStatus: "resolved",
      reason: "operator_requested",
      dedupeKey: "plugin-job-unknown",
      now: 100,
    });
    const firstClaim = store.claimNextWakeObligation({
      workerId: "worker-before-crash",
      claimTtlMs: 50,
      retryBaseMs: 1,
      retryMaxMs: 1,
      now: 100,
    });
    expect(firstClaim).toBeDefined();

    const result = await runDurableWakeDispatcherOnce({
      store,
      workerId: "worker-after-restart",
      now: 151,
      claimTtlMs: 50,
      retryBaseMs: 1,
      retryMaxMs: 1,
    });

    expect(result.claimed).toBe(0);
    expect(store.getWakeObligation(wake.wakeId)).toMatchObject({ status: "suspended" });
    expect(store.listDeliveryAttemptEvidence({ wakeId: wake.wakeId })).toEqual([
      expect.objectContaining({ status: "unknown", unknownAt: 151 }),
    ]);
    expect(store.listUnresolvedUncertaintyFacts()).toEqual([
      expect.objectContaining({ kind: "delivery_unknown", sourceRef: "job-unknown" }),
    ]);
    store.close();
  });

  it("projects a canonical owner suspension without creating a mirrored runtime", async () => {
    const entry: SubagentRunRecord = {
      runId: "child-suspended",
      taskRunId: "child-suspended",
      childSessionKey: "agent:test:subagent:child",
      requesterSessionKey: "agent:test:main",
      requesterDisplayKey: "test",
      task: "delegated task",
      cleanup: "keep",
      expectsCompletionMessage: true,
      generation: 1,
      createdAt: 100,
      startedAt: 100,
      endedAt: 200,
      outcome: { status: "ok" },
      execution: { status: "terminal", startedAt: 100, endedAt: 200, outcome: { status: "ok" } },
      completion: { required: true, resultText: "done", capturedAt: 200 },
      delivery: { status: "suspended", suspendedAt: 250, suspendedReason: "retry-limit" },
    };
    saveSubagentRegistryToSqlite(new Map([[entry.runId, entry]]));
    const store = openDurableRuntimeStore();

    const result = await runDurableWakeDispatcherOnce({ store, workerId: "worker-1", now: 300 });

    expect(result).toMatchObject({ ownerFactsScanned: 1, obligationsCreated: 1, suspended: 1 });
    expect(store.listRuns()).toHaveLength(0);
    expect(store.listWakeObligations()).toEqual([
      expect.objectContaining({
        sourceOwner: "subagent_runs",
        sourceRef: "child-suspended",
        status: "suspended",
      }),
    ]);
    store.close();
  });
});
