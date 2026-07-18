import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { describe, expect, it } from "vitest";
import { openDurableRuntimeSqliteStore } from "../../durable/sqlite-store.js";
import { durableHandlers } from "./durable.js";

describe("durable gateway methods", () => {
  it("exposes health and explicit operator decisions for suspended work", () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "openclaw-durable-control-"));
    const previousEnabled = process.env.OPENCLAW_DURABLE_RUNTIME;
    const previousStateDir = process.env.OPENCLAW_STATE_DIR;
    process.env.OPENCLAW_DURABLE_RUNTIME = "1";
    process.env.OPENCLAW_STATE_DIR = dir;
    const store = openDurableRuntimeSqliteStore({
      path: path.join(dir, "state", "openclaw.sqlite"),
    });
    let storeClosed = false;
    try {
      const acknowledged = store.createWakeObligation({
        sourceOwner: "subagent_runs",
        sourceRef: "child-ack",
        reason: "child_terminal",
        dedupeKey: "child-ack",
        targetResolutionStatus: "inspect_only",
        now: 100,
      });
      const resumed = store.createWakeObligation({
        sourceOwner: "subagent_runs",
        sourceRef: "child-resume",
        reason: "delivery_unknown",
        dedupeKey: "child-resume",
        targetResolutionStatus: "inspect_only",
        now: 100,
      });
      store.suspendWakeObligation({
        wakeId: resumed.wakeId,
        failedReason: "operator inspection required",
        now: 110,
      });
      const superseded = store.createWakeObligation({
        sourceOwner: "subagent_runs",
        sourceRef: "child-supersede",
        reason: "child_terminal",
        dedupeKey: "child-supersede",
        targetResolutionStatus: "inspect_only",
        now: 100,
      });
      const fact = store.recordUncertaintyFact({
        sourceOwner: "subagent_runs",
        sourceRef: "child-resolve",
        kind: "requires_owner_decision",
        now: 100,
      });
      store.close();
      storeClosed = true;

      const invoke = (method: keyof typeof durableHandlers, params: Record<string, unknown>) => {
        const calls: unknown[][] = [];
        durableHandlers[method]?.({
          params,
          respond: (...args: unknown[]) => calls.push(args),
        } as never);
        expect(calls).toHaveLength(1);
        expect(calls[0]?.[0]).toBe(true);
        return calls[0]?.[1];
      };

      expect(invoke("durable.health.get", {})).toMatchObject({ enabled: true, authority: false });
      expect(
        invoke("durable.wakes.acknowledge", {
          wakeId: acknowledged.wakeId,
          reason: "result consumed",
        }),
      ).toMatchObject({ wake: { status: "acked" } });
      expect(invoke("durable.wakes.resume", { wakeId: resumed.wakeId })).toMatchObject({
        wake: { status: "pending" },
      });
      expect(
        invoke("durable.wakes.supersede", {
          wakeId: superseded.wakeId,
          reason: "newer owner revision",
        }),
      ).toMatchObject({ wake: { status: "superseded" } });
      expect(
        invoke("durable.uncertainty.resolve", {
          factId: fact.factId,
          status: "resolved",
          resolutionKind: "owner_inspected",
        }),
      ).toMatchObject({ uncertaintyFact: { status: "resolved" } });
    } finally {
      if (!storeClosed) {
        store.close();
      }
      if (previousEnabled === undefined) {
        delete process.env.OPENCLAW_DURABLE_RUNTIME;
      } else {
        process.env.OPENCLAW_DURABLE_RUNTIME = previousEnabled;
      }
      if (previousStateDir === undefined) {
        delete process.env.OPENCLAW_STATE_DIR;
      } else {
        process.env.OPENCLAW_STATE_DIR = previousStateDir;
      }
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it("exposes bounded source-backed obligation inspection", () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "openclaw-durable-gateway-"));
    const dbPath = path.join(dir, "state", "openclaw.sqlite");
    const previousEnabled = process.env.OPENCLAW_DURABLE_RUNTIME;
    const previousStateDir = process.env.OPENCLAW_STATE_DIR;
    process.env.OPENCLAW_DURABLE_RUNTIME = "1";
    process.env.OPENCLAW_STATE_DIR = dir;
    const store = openDurableRuntimeSqliteStore({ path: dbPath });
    let storeClosed = false;
    try {
      const wake = store.createWakeObligation({
        sourceOwner: "subagent_runs",
        sourceRef: "subagent-1",
        targetKind: "agent_session",
        targetRef: "agent:test:main",
        ownerKind: "agent_session",
        ownerRef: "agent:test:main",
        targetResolutionStatus: "resolved",
        reason: "child_terminal",
        dedupeKey: "subagent-terminal:subagent-1:agent:test:main",
        now: 100,
      });
      const fact = store.recordUncertaintyFact({
        sourceOwner: "subagent_runs",
        sourceRef: "subagent-1",
        kind: "lost_after_dispatch",
        dedupeKey: "lost:subagent-1",
        now: 110,
      });
      const claim = store.claimNextWakeObligation({
        workerId: "gateway-test",
        claimTtlMs: 1_000,
        retryBaseMs: 1_000,
        retryMaxMs: 60_000,
        now: 120,
      });
      expect(claim).toBeDefined();
      const attempt = store.completeWakeObligationClaim({
        wakeId: wake.wakeId,
        deliveryAttemptId: claim!.deliveryAttempt.deliveryAttemptId,
        claimToken: claim!.claimToken,
        attemptStatus: "failed",
        wakeStatus: "failed",
        error: "requester unavailable",
        now: 120,
      });
      expect(attempt).toBeDefined();
      store.close();
      storeClosed = true;

      const invoke = (method: keyof typeof durableHandlers, params: Record<string, unknown>) => {
        const calls: unknown[][] = [];
        durableHandlers[method]?.({
          params,
          respond: (...args: unknown[]) => calls.push(args),
        } as never);
        expect(calls).toHaveLength(1);
        expect(calls[0]?.[0]).toBe(true);
        return calls[0]?.[1];
      };

      expect(invoke("durable.obligations.list", { limit: 10 })).toMatchObject({
        obligations: expect.arrayContaining([
          expect.objectContaining({ wakeId: wake.wakeId, sourceOwner: "subagent_runs" }),
          expect.objectContaining({
            uncertaintyFactId: fact.factId,
            sourceRef: "subagent-1",
          }),
        ]),
      });
      expect(invoke("durable.wakes.list", { limit: 10 })).toMatchObject({
        wakes: [expect.objectContaining({ wakeId: wake.wakeId })],
      });
      expect(invoke("durable.wakes.inspect", { wakeId: wake.wakeId })).toMatchObject({
        inspection: {
          wake: { wakeId: wake.wakeId },
          unresolvedUncertaintyFacts: [expect.objectContaining({ factId: fact.factId })],
        },
      });
      expect(invoke("durable.uncertainty.list", { limit: 10 })).toMatchObject({
        uncertaintyFacts: [expect.objectContaining({ factId: fact.factId })],
      });
      expect(
        invoke("durable.delivery-attempts.list", { wakeId: wake.wakeId, limit: 10 }),
      ).toMatchObject({
        deliveryAttemptEvidence: [
          expect.objectContaining({ deliveryAttemptId: attempt!.deliveryAttemptId }),
        ],
      });
    } finally {
      if (!storeClosed) {
        store.close();
      }
      if (previousEnabled === undefined) {
        delete process.env.OPENCLAW_DURABLE_RUNTIME;
      } else {
        process.env.OPENCLAW_DURABLE_RUNTIME = previousEnabled;
      }
      if (previousStateDir === undefined) {
        delete process.env.OPENCLAW_STATE_DIR;
      } else {
        process.env.OPENCLAW_STATE_DIR = previousStateDir;
      }
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it("returns coordination projection for a durable runtime run", () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "openclaw-durable-gateway-"));
    const dbPath = path.join(dir, "state", "openclaw.sqlite");
    const previousEnabled = process.env.OPENCLAW_DURABLE_RUNTIME;
    const previousStateDir = process.env.OPENCLAW_STATE_DIR;
    process.env.OPENCLAW_DURABLE_RUNTIME = "1";
    process.env.OPENCLAW_STATE_DIR = dir;
    const store = openDurableRuntimeSqliteStore({ path: dbPath });
    let storeClosed = false;
    try {
      const parent = store.createRun({
        operationKind: "test.parent",
        rootOperationReason: "test-root",
        status: "waiting_child",
        recoveryState: "waiting_child",
        metadata: {
          taskId: "task-parent",
          taskFlowId: "flow-parent",
          sessionKey: "agent:bo:main",
        },
        now: 100,
      });
      store.createStep({
        runtimeRunId: parent.runtimeRunId,
        stepId: "subagents",
        stepType: "fan_in",
        status: "waiting",
        recoveryState: "waiting_child",
        now: 110,
      });
      const child = store.createRun({
        operationKind: "test.child",
        rootOperationReason: "test-root",
        status: "succeeded",
        recoveryState: "terminal",
        now: 120,
      });
      store.createLink({
        parentRuntimeRunId: parent.runtimeRunId,
        parentStepId: "subagents",
        childRuntimeRunId: child.runtimeRunId,
        linkType: "subagent",
        status: "succeeded",
        now: 130,
      });
      store.close();
      storeClosed = true;

      const calls: unknown[][] = [];
      durableHandlers["durable.coordination.get"]?.({
        params: { runtimeRunId: parent.runtimeRunId },
        respond: (...args: unknown[]) => calls.push(args),
      } as never);

      expect(calls).toHaveLength(1);
      expect(calls[0]?.[0]).toBe(true);
      expect(calls[0]?.[1]).toMatchObject({
        projection: {
          runtimeRunId: parent.runtimeRunId,
          waitingReason: "child",
          currentStepId: "subagents",
          external: {
            taskId: "task-parent",
            taskFlowId: "flow-parent",
            sessionKey: "agent:bo:main",
          },
          children: {
            total: 1,
            succeeded: 1,
            terminal: 1,
            open: 0,
          },
        },
      });
    } finally {
      if (!storeClosed) {
        store.close();
      }
      if (previousEnabled === undefined) {
        delete process.env.OPENCLAW_DURABLE_RUNTIME;
      } else {
        process.env.OPENCLAW_DURABLE_RUNTIME = previousEnabled;
      }
      if (previousStateDir === undefined) {
        delete process.env.OPENCLAW_STATE_DIR;
      } else {
        process.env.OPENCLAW_STATE_DIR = previousStateDir;
      }
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it("does not create durable state when the feature is disabled", () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "openclaw-durable-disabled-"));
    const previousEnabled = process.env.OPENCLAW_DURABLE_RUNTIME;
    const previousStateDir = process.env.OPENCLAW_STATE_DIR;
    delete process.env.OPENCLAW_DURABLE_RUNTIME;
    process.env.OPENCLAW_STATE_DIR = dir;
    try {
      const calls: unknown[][] = [];
      durableHandlers["durable.coordination.get"]?.({
        params: { runtimeRunId: "rt_disabled" },
        respond: (...args: unknown[]) => calls.push(args),
      } as never);

      expect(calls).toHaveLength(1);
      expect(calls[0]?.[0]).toBe(false);
      expect(fs.existsSync(path.join(dir, "state", "openclaw.sqlite"))).toBe(false);
    } finally {
      if (previousEnabled === undefined) {
        delete process.env.OPENCLAW_DURABLE_RUNTIME;
      } else {
        process.env.OPENCLAW_DURABLE_RUNTIME = previousEnabled;
      }
      if (previousStateDir === undefined) {
        delete process.env.OPENCLAW_STATE_DIR;
      } else {
        process.env.OPENCLAW_STATE_DIR = previousStateDir;
      }
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it("rejects invalid coordination params before opening durable state", () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "openclaw-durable-invalid-"));
    const previousEnabled = process.env.OPENCLAW_DURABLE_RUNTIME;
    const previousStateDir = process.env.OPENCLAW_STATE_DIR;
    process.env.OPENCLAW_DURABLE_RUNTIME = "1";
    process.env.OPENCLAW_STATE_DIR = dir;
    try {
      const calls: unknown[][] = [];
      durableHandlers["durable.coordination.get"]?.({
        params: { runtimeRunId: "", includeSteps: true },
        respond: (...args: unknown[]) => calls.push(args),
      } as never);

      expect(calls).toHaveLength(1);
      expect(calls[0]?.[0]).toBe(false);
      expect(fs.existsSync(path.join(dir, "state", "openclaw.sqlite"))).toBe(false);
    } finally {
      if (previousEnabled === undefined) {
        delete process.env.OPENCLAW_DURABLE_RUNTIME;
      } else {
        process.env.OPENCLAW_DURABLE_RUNTIME = previousEnabled;
      }
      if (previousStateDir === undefined) {
        delete process.env.OPENCLAW_STATE_DIR;
      } else {
        process.env.OPENCLAW_STATE_DIR = previousStateDir;
      }
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });
});
