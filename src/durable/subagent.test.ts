import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { saveSubagentRegistryToSqlite } from "../agents/subagent-registry.store.sqlite.js";
import type { SubagentRunRecord } from "../agents/subagent-registry.types.js";
import { openDurableRuntimeStore } from "./store-factory.js";
import {
  recordDurableSubagentAnnounceDelivery,
  recordDurableSubagentInterrupted,
  recordDurableSubagentTerminal,
} from "./subagent.js";

describe("durable subagent owner projection", () => {
  let stateDir: string;
  let previousStateDir: string | undefined;
  let previousEnabled: string | undefined;

  beforeEach(() => {
    stateDir = fs.mkdtempSync(path.join(os.tmpdir(), "openclaw-durable-subagent-"));
    previousStateDir = process.env.OPENCLAW_STATE_DIR;
    previousEnabled = process.env.OPENCLAW_DURABLE_RUNTIME;
    process.env.OPENCLAW_STATE_DIR = stateDir;
    process.env.OPENCLAW_DURABLE_RUNTIME = "1";
  });

  afterEach(() => {
    if (previousStateDir === undefined) {
      delete process.env.OPENCLAW_STATE_DIR;
    } else {
      process.env.OPENCLAW_STATE_DIR = previousStateDir;
    }
    if (previousEnabled === undefined) {
      delete process.env.OPENCLAW_DURABLE_RUNTIME;
    } else {
      process.env.OPENCLAW_DURABLE_RUNTIME = previousEnabled;
    }
    fs.rmSync(stateDir, { recursive: true, force: true });
  });

  function saveRun(overrides: Partial<SubagentRunRecord> = {}): SubagentRunRecord {
    const run: SubagentRunRecord = {
      runId: "child-run",
      taskRunId: "child-run",
      childSessionKey: "agent:worker:subagent:child",
      requesterSessionKey: "agent:operator:main",
      requesterRunId: "parent-agent-run",
      requesterDisplayKey: "operator",
      task: "long delegated task",
      cleanup: "keep",
      expectsCompletionMessage: true,
      generation: 1,
      createdAt: 100,
      startedAt: 100,
      endedAt: 200,
      outcome: { status: "ok" },
      execution: { status: "terminal", startedAt: 100, endedAt: 200, outcome: { status: "ok" } },
      completion: { required: true, resultText: "done", capturedAt: 200 },
      delivery: { status: "pending", createdAt: 200 },
      ...overrides,
    };
    saveSubagentRegistryToSqlite(new Map([[run.runId, run]]));
    return run;
  }

  it("creates one wake from the canonical terminal owner and correlates the exact parent run", () => {
    const store = openDurableRuntimeStore();
    const parent = store.createRun({
      operationKind: "openclaw.agent.turn",
      idempotencyKey: "parent-agent-run",
      sourceOwner: "session_store",
      sourceRef: "agent:operator:main",
      status: "waiting_child",
      recoveryState: "waiting_child",
      now: 90,
    });
    store.close();
    saveRun();

    recordDurableSubagentTerminal({ runId: "child-run" });
    recordDurableSubagentTerminal({ runId: "child-run" });

    const verify = openDurableRuntimeStore();
    try {
      expect(verify.listRuns()).toHaveLength(1);
      expect(verify.listWakeObligations()).toEqual([
        expect.objectContaining({
          sourceOwner: "subagent_runs",
          sourceRef: "child-run",
          parentRunId: parent.runtimeRunId,
          parentSessionKey: "agent:operator:main",
          reason: "child_terminal",
          status: "pending",
        }),
      ]);
    } finally {
      verify.close();
    }
  });

  it("records restart interruption as uncertainty without inventing a mirrored child run", () => {
    saveRun({
      endedAt: undefined,
      outcome: undefined,
      execution: {
        status: "interrupted",
        interruptedAt: 300,
        interruptionReason: "gateway-restart",
      },
    });

    recordDurableSubagentInterrupted({
      runId: "child-run",
      childSessionKey: "agent:worker:subagent:child",
      requesterSessionKey: "agent:operator:main",
      reason: "gateway-restart",
      interruptedAt: 300,
    });

    const verify = openDurableRuntimeStore();
    try {
      expect(verify.listRuns()).toHaveLength(0);
      expect(verify.listUnresolvedUncertaintyFacts()).toEqual([
        expect.objectContaining({
          sourceOwner: "subagent_runs",
          sourceRef: "child-run",
          kind: "requires_owner_decision",
        }),
      ]);
      expect(verify.listWakeObligations()).toEqual([
        expect.objectContaining({ reason: "restart_interrupted", status: "pending" }),
      ]);
    } finally {
      verify.close();
    }
  });

  it("does not infer requester acknowledgement from route delivery", () => {
    saveRun();
    recordDurableSubagentTerminal({ runId: "child-run" });
    saveRun({
      delivery: { status: "delivered", deliveredAt: 250, announcedAt: 250, attemptCount: 1 },
    });

    recordDurableSubagentAnnounceDelivery({
      runId: "child-run",
      delivered: true,
      path: "direct",
      directIdempotencyKey: "announce-turn",
    });

    const verify = openDurableRuntimeStore();
    try {
      const [wake] = verify.listWakeObligations();
      expect(wake?.status).not.toBe("acked");
      expect(verify.listDeliveryAttemptEvidence({ wakeId: wake!.wakeId })).toEqual([]);
    } finally {
      verify.close();
    }
  });
});
