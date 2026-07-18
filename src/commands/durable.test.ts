import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { describe, expect, it, vi } from "vitest";
import { resolveDurableRuntimeSqlitePath } from "../durable/config.js";
import { openDurableRuntimeStore } from "../durable/store-factory.js";
import { resolveSqliteDatabaseFilePaths } from "../infra/sqlite-files.js";
import { durableCommand } from "./durable.js";

function createRuntimeCapture() {
  const logs: string[] = [];
  const errors: string[] = [];
  const runtime = {
    log: (message: unknown) => logs.push(String(message)),
    error: (message: unknown) => errors.push(String(message)),
    exit: vi.fn(),
  };
  return { errors, logs, runtime };
}

describe("durableCommand", () => {
  it("does not create or migrate durable state when the runtime is disabled", async () => {
    const stateDir = fs.mkdtempSync(path.join(os.tmpdir(), "openclaw-durable-cli-disabled-"));
    const env = { OPENCLAW_STATE_DIR: stateDir };
    const sqlitePath = resolveDurableRuntimeSqlitePath(env);
    const candidates = resolveSqliteDatabaseFilePaths(sqlitePath);
    const { errors, logs, runtime } = createRuntimeCapture();

    try {
      await durableCommand({ action: "stats", env }, runtime);

      expect(errors).toEqual([]);
      expect(runtime.exit).not.toHaveBeenCalled();
      expect(logs).toEqual([
        "Durable runtime is disabled. Set OPENCLAW_DURABLE_RUNTIME=1 to inspect durable runtime state.",
      ]);
      for (const candidate of candidates) {
        expect(fs.existsSync(candidate)).toBe(false);
      }
    } finally {
      fs.rmSync(stateDir, { recursive: true, force: true });
    }
  });

  it("reports disabled status as JSON without creating durable state", async () => {
    const stateDir = fs.mkdtempSync(path.join(os.tmpdir(), "openclaw-durable-cli-disabled-"));
    const env = { OPENCLAW_STATE_DIR: stateDir };
    const sqlitePath = resolveDurableRuntimeSqlitePath(env);
    const { logs, runtime } = createRuntimeCapture();

    try {
      await durableCommand({ action: "stats", env, json: true }, runtime);

      expect(JSON.parse(logs[0] ?? "{}")).toEqual({ enabled: false });
      for (const candidate of resolveSqliteDatabaseFilePaths(sqlitePath)) {
        expect(fs.existsSync(candidate)).toBe(false);
      }
    } finally {
      fs.rmSync(stateDir, { recursive: true, force: true });
    }
  });

  it("lists and inspects source-backed obligations", async () => {
    const stateDir = fs.mkdtempSync(path.join(os.tmpdir(), "openclaw-durable-cli-obligations-"));
    const env = {
      OPENCLAW_STATE_DIR: stateDir,
      OPENCLAW_DURABLE_RUNTIME: "1",
    };
    const wakeId = (() => {
      const store = openDurableRuntimeStore({ env });
      try {
        return store.createWakeObligation({
          sourceOwner: "subagent_runs",
          sourceRef: "subagent-cli-1",
          targetKind: "agent_session",
          targetRef: "agent:test:main",
          ownerKind: "agent_session",
          ownerRef: "agent:test:main",
          targetResolutionStatus: "resolved",
          reason: "child_terminal",
          dedupeKey: "subagent-terminal:subagent-cli-1:agent:test:main",
          now: 100,
        }).wakeId;
      } finally {
        store.close();
      }
    })();

    try {
      const listCapture = createRuntimeCapture();
      await durableCommand(
        { action: "obligations", env, json: true, limit: 10 },
        listCapture.runtime,
      );
      expect(JSON.parse(listCapture.logs[0] ?? "[]")).toEqual([
        expect.objectContaining({
          wakeId,
          sourceOwner: "subagent_runs",
          sourceRef: "subagent-cli-1",
        }),
      ]);

      const inspectCapture = createRuntimeCapture();
      await durableCommand(
        { action: "wake", runtimeRunId: wakeId, env, json: true },
        inspectCapture.runtime,
      );
      expect(JSON.parse(inspectCapture.logs[0] ?? "{}")).toMatchObject({
        wake: { wakeId, sourceOwner: "subagent_runs", sourceRef: "subagent-cli-1" },
      });
    } finally {
      fs.rmSync(stateDir, { recursive: true, force: true });
    }
  });

  it("explains why a run is waiting on child work", async () => {
    const stateDir = fs.mkdtempSync(path.join(os.tmpdir(), "openclaw-durable-cli-why-"));
    const env = {
      OPENCLAW_STATE_DIR: stateDir,
      OPENCLAW_DURABLE_RUNTIME: "1",
    };
    const runtimeRunId = (() => {
      const store = openDurableRuntimeStore({ env });
      try {
        const parent = store.createRun({
          operationKind: "test.parent",
          status: "waiting_child",
          recoveryState: "waiting_child",
          sourceOwner: "test",
          sourceRef: "agent:test:main",
          now: 100,
        });
        store.createStep({
          runtimeRunId: parent.runtimeRunId,
          stepId: "children",
          stepType: "fan_in",
          status: "waiting",
          recoveryState: "waiting_child",
          now: 100,
        });
        const child = store.createRun({
          operationKind: "test.child",
          rootOperationReason: "test-root",
          status: "running",
          recoveryState: "running",
          parentRuntimeRunId: parent.runtimeRunId,
          parentStepId: "children",
          now: 110,
        });
        store.createLink({
          parentRuntimeRunId: parent.runtimeRunId,
          parentStepId: "children",
          childRuntimeRunId: child.runtimeRunId,
          linkType: "subagent",
          status: "running",
          now: 110,
        });
        return parent.runtimeRunId;
      } finally {
        store.close();
      }
    })();

    try {
      const { logs, runtime } = createRuntimeCapture();

      await durableCommand({ action: "why", runtimeRunId, env }, runtime);

      expect(logs[0]).toContain("Summary: Run is waiting for child work: 1 open of 1 children.");
      expect(logs[0]).toContain("Waiting reason: child");
      expect(logs[0]).toContain("Children: total=1 open=1 terminal=0");
      expect(logs[0]).toContain(`- openclaw durable children ${runtimeRunId}`);
    } finally {
      fs.rmSync(stateDir, { recursive: true, force: true });
    }
  });

  it("renders fan-in group diagnostics in show output", async () => {
    const stateDir = fs.mkdtempSync(path.join(os.tmpdir(), "openclaw-durable-cli-show-"));
    const env = {
      OPENCLAW_STATE_DIR: stateDir,
      OPENCLAW_DURABLE_RUNTIME: "1",
    };
    const runtimeRunId = (() => {
      const store = openDurableRuntimeStore({ env });
      try {
        const parent = store.createRun({
          operationKind: "test.parent",
          status: "waiting_child",
          recoveryState: "waiting_child",
          sourceOwner: "test",
          sourceRef: "agent:test:main",
          now: 100,
        });
        const parentStepId = "children";
        const fanInGroupId = `${parent.runtimeRunId}:${parentStepId}`;
        store.createStep({
          runtimeRunId: parent.runtimeRunId,
          stepId: parentStepId,
          stepType: "fan_in",
          status: "waiting",
          recoveryState: "waiting_child",
          metadata: { fanInGroupId },
          now: 100,
        });
        const child = store.createRun({
          operationKind: "test.child",
          rootOperationReason: "test-root",
          status: "succeeded",
          recoveryState: "terminal",
          parentRuntimeRunId: parent.runtimeRunId,
          parentStepId,
          now: 110,
        });
        store.createLink({
          parentRuntimeRunId: parent.runtimeRunId,
          parentStepId,
          childRuntimeRunId: child.runtimeRunId,
          linkType: "subagent",
          status: "succeeded",
          metadata: { fanInGroupId, childSessionKey: "agent:test:subagent:child" },
          now: 110,
        });
        return parent.runtimeRunId;
      } finally {
        store.close();
      }
    })();

    try {
      const { logs, runtime } = createRuntimeCapture();

      await durableCommand({ action: "show", runtimeRunId, env }, runtime);

      expect(logs[0]).toContain("fan_in=");
      expect(logs[0]).toContain("session=agent:test:subagent:child");
    } finally {
      fs.rmSync(stateDir, { recursive: true, force: true });
    }
  });

  it("surfaces recovery diagnostics in the why command", async () => {
    const stateDir = fs.mkdtempSync(path.join(os.tmpdir(), "openclaw-durable-cli-why-"));
    const env = {
      OPENCLAW_STATE_DIR: stateDir,
      OPENCLAW_DURABLE_RUNTIME: "1",
    };
    const runtimeRunId = (() => {
      const store = openDurableRuntimeStore({ env });
      try {
        return store.createRun({
          operationKind: "test.agent_turn",
          rootOperationReason: "test-root",
          status: "lost",
          recoveryState: "lost",
          completedAt: 200,
          metadata: {
            recoveryDiagnostic: {
              state: "lost",
              severity: "error",
              reason: "stale_heartbeat",
              message: "Agent turn was marked lost during durable recovery.",
              nextAction: "inspect_timeline_then_retry_or_resume",
              safeRecoveryActions: ["inspect_timeline", "retry_request"],
            },
          },
          now: 200,
        }).runtimeRunId;
      } finally {
        store.close();
      }
    })();

    try {
      const { logs, runtime } = createRuntimeCapture();

      await durableCommand({ action: "why", runtimeRunId, env }, runtime);

      expect(logs[0]).toContain("Summary: Agent turn was marked lost during durable recovery.");
      expect(logs[0]).toContain("Recovery: lost/error next=inspect_timeline_then_retry_or_resume");
      expect(logs[0]).toContain("Reason: stale_heartbeat");
      expect(logs[0]).toContain("Safe actions: inspect_timeline, retry_request");
    } finally {
      fs.rmSync(stateDir, { recursive: true, force: true });
    }
  });
});
