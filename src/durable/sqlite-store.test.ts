import { spawn, type ChildProcess } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { describe, expect, it } from "vitest";
import { requireNodeSqlite } from "../infra/node-sqlite.js";
import { resolveSqliteDatabaseFilePaths } from "../infra/sqlite-files.js";
import {
  OPENCLAW_STATE_SCHEMA_VERSION,
  closeOpenClawStateDatabaseForPath,
  openOpenClawStateDatabase,
} from "../state/openclaw-state-db.js";
import { resolveOpenClawStateSqlitePath } from "../state/openclaw-state-db.paths.js";
import { openDurableRuntimeSqliteStore } from "./sqlite-store.js";
import type {
  ReconcileWakeObligationAppliedResult,
  ReconcileWakeObligationResult,
} from "./types.js";

const DURABLE_TABLES = [
  "delivery_attempt_evidence",
  "durable_event_evidence",
  "durable_execution_records",
  "durable_execution_steps",
  "durable_payload_refs",
  "durable_run_correlations",
  "durable_signal_evidence",
  "durable_timer_obligations",
  "uncertainty_facts",
  "wake_obligation_occurrences",
  "wake_obligations",
] as const;

function requireAppliedWakeResult(
  result: ReconcileWakeObligationResult,
): ReconcileWakeObligationAppliedResult {
  if (result.disposition === "conflict") {
    throw new Error(`Expected applied wake reconciliation, received ${result.reason}`);
  }
  return result;
}

const CONCURRENT_WAKE_WRITER_SCRIPT = String.raw`
const [moduleUrl, dbPath, occurrenceKey] = process.argv.slice(1);
const { openDurableRuntimeSqliteStore } = await import(moduleUrl);
const store = openDurableRuntimeSqliteStore({ path: dbPath });
process.send?.({ type: "ready" });
process.once("message", (message) => {
  if (message !== "go") {
    process.exitCode = 2;
    store.close();
    process.disconnect?.();
    return;
  }
  try {
    const result = store.reconcileWakeObligation({
      candidate: {
        sourceOwner: "test-owner",
        sourceRef: "test-source:concurrent",
        targetKind: "agent_session",
        targetRef: "agent:test:main",
        ownerKind: "agent_session",
        ownerRef: "agent:test:main",
        reportRouteRef: "agent:test:main",
        targetResolutionStatus: "resolved",
        reason: "operator_requested",
        factsRef: "test-facts:concurrent",
        sourceRevision: occurrenceKey,
        occurrenceKey,
        now: 100,
      },
      policy: { mode: "while_unresolved", recurrence: "after_terminal" },
    });
    process.send?.({ type: "result", result });
  } catch (error) {
    process.send?.({
      type: "error",
      error: error instanceof Error ? error.stack ?? error.message : String(error),
    });
    process.exitCode = 1;
  } finally {
    store.close();
    process.disconnect?.();
  }
});
`;

type ConcurrentWakeWriter = {
  child: ChildProcess;
  ready: Promise<void>;
  run: () => Promise<ReconcileWakeObligationResult>;
};

function startConcurrentWakeWriter(params: {
  dbPath: string;
  occurrenceKey: string;
}): ConcurrentWakeWriter {
  const moduleUrl = pathToFileURL(path.resolve("src/durable/sqlite-store.ts")).href;
  const child = spawn(
    process.execPath,
    [
      "--import",
      "tsx",
      "--input-type=module",
      "-e",
      CONCURRENT_WAKE_WRITER_SCRIPT,
      moduleUrl,
      params.dbPath,
      params.occurrenceKey,
    ],
    { cwd: process.cwd(), stdio: ["ignore", "pipe", "pipe", "ipc"] },
  );
  let stderr = "";
  child.stderr?.setEncoding("utf8");
  child.stderr?.on("data", (chunk: string) => {
    stderr += chunk;
  });
  let resolveReady!: () => void;
  let rejectReady!: (error: Error) => void;
  const ready = new Promise<void>((resolve, reject) => {
    resolveReady = resolve;
    rejectReady = reject;
  });
  let resolveResult!: (result: ReconcileWakeObligationResult) => void;
  let rejectResult!: (error: Error) => void;
  const result = new Promise<ReconcileWakeObligationResult>((resolve, reject) => {
    resolveResult = resolve;
    rejectResult = reject;
  });
  let receivedReady = false;
  let receivedResult = false;
  child.on("message", (message: unknown) => {
    if (!message || typeof message !== "object") {
      return;
    }
    const envelope = message as {
      type?: string;
      result?: ReconcileWakeObligationResult;
      error?: string;
    };
    if (envelope.type === "ready") {
      receivedReady = true;
      resolveReady();
    } else if (envelope.type === "result" && envelope.result) {
      receivedResult = true;
      resolveResult(envelope.result);
    } else if (envelope.type === "error") {
      const error = new Error(envelope.error ?? "Concurrent wake writer failed");
      rejectReady(error);
      rejectResult(error);
    }
  });
  child.once("error", (error) => {
    rejectReady(error);
    rejectResult(error);
  });
  child.once("exit", (code) => {
    if (!receivedReady) {
      rejectReady(new Error(`Concurrent wake writer exited ${code}: ${stderr}`));
    }
    if (!receivedResult) {
      rejectResult(new Error(`Concurrent wake writer exited ${code}: ${stderr}`));
    }
  });
  return {
    child,
    ready,
    run: () => {
      child.send("go");
      return result;
    },
  };
}

describe("durable runtime sqlite store", () => {
  it("rolls back a caller-composed durable transaction as one unit", () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "openclaw-durable-transaction-"));
    const store = openDurableRuntimeSqliteStore({ path: path.join(dir, "openclaw.sqlite") });
    try {
      expect(() =>
        store.withTransaction(() => {
          store.createRun({
            operationKind: "test.atomic-admission",
            rootOperationReason: "transaction_rollback_test",
          });
          throw new Error("fault-injected transaction rollback");
        }),
      ).toThrow(/fault-injected transaction rollback/);
      expect(store.listRuns()).toEqual([]);
      expect(store.getStats()).toMatchObject({ runs: 0, events: 0, steps: 0 });
    } finally {
      store.close();
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it("does not install durable tables during normal shared-state bootstrap", () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "openclaw-shared-state-"));
    const dbPath = path.join(dir, "openclaw.sqlite");
    const state = openOpenClawStateDatabase({ path: dbPath });
    try {
      const runtimeTables = state.db
        .prepare(
          `SELECT name FROM sqlite_master
             WHERE type = 'table'
               AND name IN (${DURABLE_TABLES.map(() => "?").join(", ")})
             ORDER BY name`,
        )
        .all(...DURABLE_TABLES);
      expect(runtimeTables).toEqual([]);
      expect(
        state.db.prepare("SELECT 1 FROM schema_meta WHERE meta_key = ?").get("durable_runtime"),
      ).toBeUndefined();
    } finally {
      closeOpenClawStateDatabaseForPath({ path: dbPath });
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it("does not close a shared-state handle owned by another caller", () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "openclaw-durable-shared-owner-"));
    const dbPath = path.join(dir, "openclaw.sqlite");
    const ownerDatabase = openOpenClawStateDatabase({ path: dbPath });
    const store = openDurableRuntimeSqliteStore({ path: dbPath });
    try {
      store.close();
      expect(ownerDatabase.db.isOpen).toBe(true);
      expect(openOpenClawStateDatabase({ path: dbPath })).toBe(ownerDatabase);
    } finally {
      closeOpenClawStateDatabaseForPath({ path: dbPath });
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it("installs additive durable tables without a separate schema marker", () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "openclaw-durable-store-"));
    const dbPath = path.join(dir, "openclaw.sqlite");
    const store = openDurableRuntimeSqliteStore({
      path: dbPath,
    });
    try {
      expect(store.getStats()).toMatchObject({ runs: 0, events: 0, steps: 0 });
    } finally {
      store.close();
    }
    const { DatabaseSync } = requireNodeSqlite();
    const db = new DatabaseSync(dbPath, { readOnly: true });
    try {
      expect(db.prepare("SELECT 1 FROM schema_meta WHERE meta_key = ?").get("primary")).toEqual({
        1: 1,
      });
      expect(
        db.prepare("SELECT 1 FROM schema_meta WHERE meta_key = ?").get("durable_runtime"),
      ).toBeUndefined();
      expect(db.prepare("PRAGMA user_version").get()).toEqual({
        user_version: OPENCLAW_STATE_SCHEMA_VERSION,
      });
    } finally {
      db.close();
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it("rejects durable stores from a newer schema version", () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "openclaw-durable-store-"));
    const dbPath = path.join(dir, "openclaw.sqlite");
    const { DatabaseSync } = requireNodeSqlite();
    const db = new DatabaseSync(dbPath);
    try {
      db.exec(`PRAGMA user_version = ${OPENCLAW_STATE_SCHEMA_VERSION + 1};`);
    } finally {
      db.close();
    }

    try {
      expect(() => openDurableRuntimeSqliteStore({ path: dbPath })).toThrow(
        /uses newer schema version .* supports/,
      );
      const verifyDb = new DatabaseSync(dbPath);
      try {
        const runtimeTables = verifyDb
          .prepare(
            `SELECT name FROM sqlite_master
               WHERE type = 'table'
                 AND name IN (${DURABLE_TABLES.map(() => "?").join(", ")})
               ORDER BY name`,
          )
          .all(...DURABLE_TABLES);
        expect(runtimeTables).toEqual([]);
      } finally {
        verifyDb.close();
      }
    } finally {
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it("upgrades a canonical pre-durable shared state database without touching owner rows", () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "openclaw-durable-upgrade-"));
    const dbPath = path.join(dir, "openclaw.sqlite");
    const { DatabaseSync } = requireNodeSqlite();
    const state = openOpenClawStateDatabase({ path: dbPath });
    try {
      state.db
        .prepare(
          `INSERT INTO state_leases (
             scope, lease_key, owner, expires_at, heartbeat_at, payload_json, created_at, updated_at
           ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
        )
        .run("test-owner", "existing-row", "official-7.1", null, 100, null, 100, 100);
      const runtimeTables = state.db
        .prepare(
          `SELECT name FROM sqlite_master
             WHERE type = 'table'
               AND name IN (${DURABLE_TABLES.map(() => "?").join(", ")})`,
        )
        .all(...DURABLE_TABLES);
      expect(runtimeTables).toEqual([]);
    } finally {
      closeOpenClawStateDatabaseForPath({ path: dbPath });
    }

    const store = openDurableRuntimeSqliteStore({ path: dbPath });
    try {
      expect(store.getStats()).toMatchObject({ runs: 0, events: 0, steps: 0 });
      const run = store.createRun({
        operationKind: "openclaw.chat.send",
        idempotencyKey: "upgrade-smoke",
        status: "succeeded",
        recoveryState: "terminal",
        sourceOwner: "session_store",
        sourceRef: "agent:upgrade:test",
        now: 100,
      });
      store.appendEvent({
        runtimeRunId: run.runtimeRunId,
        eventType: "upgrade.smoke",
        eventTime: 100,
      });
    } finally {
      store.close();
    }

    const verifyDb = new DatabaseSync(dbPath);
    try {
      expect(
        verifyDb
          .prepare("SELECT meta_key, role, schema_version FROM schema_meta WHERE meta_key = ?")
          .get("primary"),
      ).toEqual({ meta_key: "primary", role: "global", schema_version: 1 });
      expect(
        verifyDb
          .prepare("SELECT meta_key, role, schema_version FROM schema_meta WHERE meta_key = ?")
          .get("durable_runtime"),
      ).toBeUndefined();
      expect(
        verifyDb
          .prepare(
            "SELECT scope, lease_key, owner, heartbeat_at FROM state_leases WHERE scope = ? AND lease_key = ?",
          )
          .all("test-owner", "existing-row"),
      ).toEqual([
        {
          scope: "test-owner",
          lease_key: "existing-row",
          owner: "official-7.1",
          heartbeat_at: 100,
        },
      ]);
      expect(verifyDb.prepare("PRAGMA user_version").get()).toEqual({
        user_version: OPENCLAW_STATE_SCHEMA_VERSION,
      });
      const runtimeTables = verifyDb
        .prepare(
          `SELECT name FROM sqlite_master
             WHERE type = 'table'
               AND name IN (${DURABLE_TABLES.map(() => "?").join(", ")})
             ORDER BY name`,
        )
        .all(...DURABLE_TABLES) as Array<{ name: string }>;
      expect(runtimeTables.map((row) => row.name)).toEqual([...DURABLE_TABLES]);
    } finally {
      verifyDb.close();
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it("uses shared state private-mode hardening when it creates the state database", () => {
    const stateDir = fs.mkdtempSync(path.join(os.tmpdir(), "openclaw-durable-state-mode-"));
    fs.chmodSync(stateDir, 0o755);
    const env = { OPENCLAW_STATE_DIR: stateDir };
    const pathname = resolveOpenClawStateSqlitePath(env);
    const store = openDurableRuntimeSqliteStore({ env });
    try {
      expect(fs.statSync(path.dirname(pathname)).mode & 0o777).toBe(0o700);
      for (const candidate of resolveSqliteDatabaseFilePaths(pathname)) {
        if (fs.existsSync(candidate)) {
          expect(fs.statSync(candidate).mode & 0o777).toBe(0o600);
        }
      }
    } finally {
      store.close();
      fs.rmSync(stateDir, { recursive: true, force: true });
    }
  });

  it("creates runs, dedupes idempotency keys, and appends ordered events", () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "openclaw-durable-store-"));
    const store = openDurableRuntimeSqliteStore({
      path: path.join(dir, "openclaw.sqlite"),
    });
    try {
      expect(() =>
        store.createRun({
          operationKind: "test.invalid-source",
          sourceOwner: "session_store",
          sourceRef: "agent:test:main",
          rootOperationReason: "not-a-root-operation",
        }),
      ).toThrow(/not both/);
      const first = store.createRun({
        operationKind: "test.runtime",
        rootOperationReason: "test-root",
        idempotencyKey: "request-1",
        requestHash: "hash-1",
        workUnitId: "wu:test:card-1",
        reportRouteRef: "route:test:main",
        metadata: { surface: "test" },
        now: 100,
      });
      const duplicate = store.createRun({
        operationKind: "test.runtime",
        rootOperationReason: "test-root",
        idempotencyKey: "request-1",
        requestHash: "hash-1",
        now: 200,
      });
      expect(duplicate.runtimeRunId).toBe(first.runtimeRunId);
      expect(store.getRunByIdempotencyKey("test.runtime", "request-1")?.runtimeRunId).toBe(
        first.runtimeRunId,
      );

      const started = store.appendEvent({
        runtimeRunId: first.runtimeRunId,
        eventType: "runtime.started",
        payload: { ok: true },
      });
      const completed = store.appendEvent({
        runtimeRunId: first.runtimeRunId,
        eventType: "runtime.completed",
        idempotencyKey: "request-1:completed",
      });
      const duplicateCompleted = store.appendEvent({
        runtimeRunId: first.runtimeRunId,
        eventType: "runtime.completed",
        idempotencyKey: "request-1:completed",
      });
      const explicit = store.appendEvent({
        runtimeRunId: first.runtimeRunId,
        eventId: "event-explicit",
        eventType: "runtime.proof",
        idempotencyKey: "request-1:proof",
        payload: { result: "stable" },
        payloadHash: "proof-hash",
      });
      expect(
        store.appendEvent({
          runtimeRunId: first.runtimeRunId,
          eventId: "event-explicit",
          eventType: "runtime.proof",
          idempotencyKey: "request-1:proof",
          payload: { result: "stable" },
          payloadHash: "proof-hash",
        }).eventId,
      ).toBe(explicit.eventId);
      expect(() =>
        store.appendEvent({
          runtimeRunId: first.runtimeRunId,
          eventId: "event-explicit",
          eventType: "runtime.proof",
          idempotencyKey: "request-1:proof",
          payload: { result: "changed" },
          payloadHash: "different-hash",
        }),
      ).toThrow(/event replay conflict/);
      expect(() =>
        store.appendEvent({
          runtimeRunId: first.runtimeRunId,
          eventId: "different-event-id",
          eventType: "runtime.proof",
          idempotencyKey: "request-1:proof",
        }),
      ).toThrow(/event replay conflict/);
      expect(store.listOpenRuns({ operationKind: "test.runtime" })).toMatchObject([
        {
          runtimeRunId: first.runtimeRunId,
          operationKind: "test.runtime",
          status: "received",
          workUnitId: "wu:test:card-1",
          reportRouteRef: "route:test:main",
        },
      ]);
      const terminal = store.updateRun({
        runtimeRunId: first.runtimeRunId,
        status: "succeeded",
        recoveryState: "terminal",
        workUnitId: "wu:test:card-1-updated",
        completedAt: 300,
        now: 300,
      });

      expect(started.eventSeq).toBe(1);
      expect(completed.eventSeq).toBe(2);
      expect(duplicateCompleted.eventId).toBe(completed.eventId);
      expect(terminal).toMatchObject({
        runtimeRunId: first.runtimeRunId,
        status: "succeeded",
        recoveryState: "terminal",
        workUnitId: "wu:test:card-1-updated",
        reportRouteRef: "route:test:main",
        completedAt: 300,
      });
      expect(store.getTimeline(first.runtimeRunId).map((event) => event.eventType)).toEqual([
        "runtime.started",
        "runtime.completed",
        "runtime.proof",
      ]);
      expect(store.listOpenRuns({ operationKind: "test.runtime" })).toEqual([]);
      expect(store.getStats()).toMatchObject({ runs: 1, events: 3, steps: 0, openRuns: 0 });
    } finally {
      store.close();
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it("stores core runtime primitives for steps, refs, links, timers, and signals", () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "openclaw-durable-store-"));
    const store = openDurableRuntimeSqliteStore({
      path: path.join(dir, "openclaw.sqlite"),
    });
    try {
      const parent = store.createRun({
        operationKind: "test.runtime",
        rootOperationReason: "test-root",
        status: "queued",
        recoveryState: "runnable",
        idempotencyKey: "parent",
        now: 100,
      });
      const child = store.createRun({
        operationKind: "test.runtime",
        rootOperationReason: "test-root",
        status: "queued",
        recoveryState: "runnable",
        idempotencyKey: "child",
        parentRuntimeRunId: parent.runtimeRunId,
        now: 110,
      });
      const inputRef = store.createRef({
        runtimeRunId: parent.runtimeRunId,
        refKind: "input",
        mediaType: "application/json",
        hash: "input-hash",
        storageKind: "inline",
        storageUri: "inline:test",
        now: 140,
      });
      expect(store.getRef(inputRef.refId)).toMatchObject({
        refKind: "input",
        storageKind: "inline",
        hash: "input-hash",
      });

      const step = store.createStep({
        runtimeRunId: parent.runtimeRunId,
        stepType: "fan_in",
        status: "waiting",
        recoveryState: "waiting_child",
        inputRef: inputRef.refId,
        idempotencyKey: "fan-in-1",
        metadata: { policy: "all_terminal" },
        now: 150,
      });
      const duplicateStep = store.createStep({
        runtimeRunId: parent.runtimeRunId,
        stepType: "fan_in",
        idempotencyKey: "fan-in-1",
        now: 160,
      });
      expect(duplicateStep.stepId).toBe(step.stepId);

      const updatedStep = store.updateStep({
        runtimeRunId: parent.runtimeRunId,
        stepId: step.stepId,
        status: "succeeded",
        recoveryState: "terminal",
        outputRef: "output-ref",
        completedAt: 170,
        now: 170,
      });
      expect(updatedStep).toMatchObject({
        stepId: step.stepId,
        status: "succeeded",
        outputRef: "output-ref",
        completedAt: 170,
      });
      expect(
        store.updateStep({
          runtimeRunId: parent.runtimeRunId,
          stepId: step.stepId,
          status: "queued",
          recoveryState: "runnable",
          completedAt: null,
          now: 171,
        }),
      ).toBeUndefined();
      expect(store.listSteps(parent.runtimeRunId)).toHaveLength(1);

      const executableStep = store.createStep({
        runtimeRunId: parent.runtimeRunId,
        stepType: "tool",
        status: "queued",
        recoveryState: "runnable",
        idempotencyKey: "tool-1",
        now: 175,
      });
      const claimedStep = store.claimNextRunnableStep({
        operationKind: "test.runtime",
        stepType: "tool",
        workerId: "worker-1",
        claimTtlMs: 1_000,
        now: 176,
      });
      expect(claimedStep).toMatchObject({
        claimToken: expect.stringMatching(/^claim_/),
        claimExpiresAt: 1_176,
        step: {
          runtimeRunId: parent.runtimeRunId,
          stepId: executableStep.stepId,
          status: "queued",
          recoveryState: "claimed",
          claimedBy: expect.stringMatching(/^claim_/),
        },
      });
      expect(
        store.renewStepClaim({
          runtimeRunId: parent.runtimeRunId,
          stepId: executableStep.stepId,
          claimToken: claimedStep!.claimToken,
          claimTtlMs: 2_000,
          now: 177,
        }),
      ).toMatchObject({ claimExpiresAt: 2_177, claimedBy: claimedStep!.claimToken });
      expect(
        store.releaseStepClaim({
          runtimeRunId: parent.runtimeRunId,
          stepId: executableStep.stepId,
          claimToken: claimedStep!.claimToken,
          now: 178,
        }),
      ).toMatchObject({
        stepId: executableStep.stepId,
        recoveryState: "runnable",
      });

      const link = store.createLink({
        parentRuntimeRunId: parent.runtimeRunId,
        parentStepId: step.stepId,
        childRuntimeRunId: child.runtimeRunId,
        linkType: "child_runtime",
        status: "running",
        now: 180,
      });
      expect(link.status).toBe("running");
      expect(
        store.updateLink({
          parentRuntimeRunId: parent.runtimeRunId,
          parentStepId: step.stepId,
          childRuntimeRunId: child.runtimeRunId,
          status: "succeeded",
          now: 190,
        }),
      ).toMatchObject({ status: "succeeded" });
      expect(store.listChildLinks(parent.runtimeRunId)).toHaveLength(1);

      const timer = store.createTimer({
        runtimeRunId: parent.runtimeRunId,
        stepId: step.stepId,
        timerType: "retry",
        dueAt: 200,
        now: 195,
      });
      expect(store.listDueTimers(199)).toEqual([]);
      expect(store.listDueTimers(200)).toMatchObject([{ timerId: timer.timerId }]);
      expect(
        store.updateTimer({ timerId: timer.timerId, status: "fired", now: 201 }),
      ).toMatchObject({
        status: "fired",
        firedAt: 201,
      });

      const signal = store.createSignal({
        runtimeRunId: parent.runtimeRunId,
        stepId: step.stepId,
        signalType: "human_input",
        idempotencyKey: "signal-1",
        payloadRef: inputRef.refId,
        now: 210,
      });
      const duplicateSignal = store.createSignal({
        runtimeRunId: parent.runtimeRunId,
        stepId: step.stepId,
        signalType: "human_input",
        idempotencyKey: "signal-1",
        payloadRef: inputRef.refId,
        now: 211,
      });
      expect(duplicateSignal.signalId).toBe(signal.signalId);
      expect(store.consumeSignal({ signalId: signal.signalId, now: 220 })).toMatchObject({
        signalId: signal.signalId,
        consumedAt: 220,
      });
      expect(store.listSignals(parent.runtimeRunId)).toHaveLength(1);
      expect(store.listPendingSignals()).toEqual([]);
      expect(store.getStats()).toMatchObject({ runs: 2, steps: 2 });
    } finally {
      store.close();
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it("validates stable-identity replays without rolling lifecycle state back", () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "openclaw-durable-replay-"));
    const store = openDurableRuntimeSqliteStore({
      path: path.join(dir, "openclaw.sqlite"),
    });
    try {
      const runInput = {
        runtimeRunId: "run-stable",
        operationKind: "test.replay",
        rootOperationReason: "test-root",
        idempotencyKey: "run-replay",
        requestHash: "request-hash",
        inputRef: "input:stable",
        now: 100,
      } as const;
      const run = store.createRun(runInput);
      store.updateRun({
        runtimeRunId: run.runtimeRunId,
        status: "succeeded",
        recoveryState: "terminal",
        completedAt: 110,
        now: 110,
      });
      expect(store.createRun({ ...runInput, now: 120 })).toMatchObject({
        runtimeRunId: run.runtimeRunId,
        status: "succeeded",
        completedAt: 110,
      });
      expect(() =>
        store.createRun({ ...runInput, requestHash: "changed-request-hash", now: 121 }),
      ).toThrow(/run replay conflict/);

      const stepInput = {
        runtimeRunId: run.runtimeRunId,
        stepId: "step-stable",
        stepType: "tool",
        idempotencyKey: "step-replay",
        now: 130,
      } as const;
      const step = store.createStep(stepInput);
      store.updateStep({
        runtimeRunId: run.runtimeRunId,
        stepId: step.stepId,
        status: "succeeded",
        recoveryState: "terminal",
        completedAt: 140,
        now: 140,
      });
      expect(store.createStep({ ...stepInput, now: 150 })).toMatchObject({
        stepId: step.stepId,
        status: "succeeded",
        completedAt: 140,
      });
      expect(() => store.createStep({ ...stepInput, stepType: "agent", now: 151 })).toThrow(
        /step replay conflict/,
      );

      const refInput = {
        refId: "ref-stable",
        runtimeRunId: run.runtimeRunId,
        stepId: step.stepId,
        refKind: "artifact",
        mediaType: "application/json",
        hash: "artifact-hash",
        storageKind: "external",
        storageUri: "artifact:stable",
        metadata: { nested: { ok: true }, purpose: "proof" },
        now: 160,
      } as const;
      const ref = store.createRef(refInput);
      expect(
        store.createRef({
          ...refInput,
          metadata: { purpose: "proof", nested: { ok: true } },
          now: 161,
        }).refId,
      ).toBe(ref.refId);
      expect(() => store.createRef({ ...refInput, hash: "changed-hash", now: 162 })).toThrow(
        /ref replay conflict/,
      );

      const child = store.createRun({
        runtimeRunId: "run-child",
        operationKind: "test.replay",
        rootOperationReason: "test-root",
        now: 170,
      });
      const linkInput = {
        parentRuntimeRunId: run.runtimeRunId,
        parentStepId: step.stepId,
        childRuntimeRunId: child.runtimeRunId,
        linkType: "child_runtime",
        now: 180,
      } as const;
      store.createLink(linkInput);
      store.updateLink({ ...linkInput, status: "succeeded", now: 181 });
      expect(store.createLink({ ...linkInput, now: 182 }).status).toBe("succeeded");
      expect(() => store.createLink({ ...linkInput, linkType: "handoff", now: 183 })).toThrow(
        /link replay conflict/,
      );

      const timerInput = {
        timerId: "timer-stable",
        runtimeRunId: run.runtimeRunId,
        stepId: step.stepId,
        timerType: "retry",
        dueAt: 200,
        metadata: { policy: "bounded" },
        now: 190,
      } as const;
      store.createTimer(timerInput);
      store.updateTimer({ timerId: timerInput.timerId, status: "fired", now: 201 });
      expect(store.createTimer({ ...timerInput, now: 202 }).status).toBe("fired");
      expect(() => store.createTimer({ ...timerInput, dueAt: 201, now: 203 })).toThrow(
        /timer replay conflict/,
      );

      const signalInput = {
        signalId: "signal-stable",
        runtimeRunId: run.runtimeRunId,
        stepId: step.stepId,
        signalType: "human_input",
        idempotencyKey: "signal-replay",
        payloadRef: ref.refId,
        correlationId: "correlation:stable",
        metadata: { source: "test" },
        now: 210,
      } as const;
      store.createSignal(signalInput);
      store.consumeSignal({ signalId: signalInput.signalId, now: 220 });
      expect(store.createSignal({ ...signalInput, now: 221 }).consumedAt).toBe(220);
      expect(() =>
        store.createSignal({ ...signalInput, payloadRef: "ref:changed", now: 222 }),
      ).toThrow(/signal replay conflict/);
    } finally {
      store.close();
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it("does not claim retry-scheduled runs or steps before recovery queues them", () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "openclaw-durable-store-"));
    const store = openDurableRuntimeSqliteStore({
      path: path.join(dir, "openclaw.sqlite"),
    });
    try {
      const run = store.createRun({
        operationKind: "test.runtime",
        rootOperationReason: "test-root",
        status: "retry_scheduled",
        recoveryState: "retry_scheduled",
        now: 100,
      });
      store.createStep({
        runtimeRunId: run.runtimeRunId,
        stepType: "tool",
        status: "retry_scheduled",
        recoveryState: "retry_scheduled",
        now: 110,
      });

      expect(
        store.claimNextRunnableStep({
          operationKind: "test.runtime",
          workerId: "worker-1",
          claimTtlMs: 1_000,
          now: 120,
        }),
      ).toBeUndefined();
    } finally {
      store.close();
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it("reclaims expired step leases without accepting stale owner writes", () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "openclaw-durable-store-"));
    const store = openDurableRuntimeSqliteStore({
      path: path.join(dir, "openclaw.sqlite"),
    });
    try {
      const run = store.createRun({
        operationKind: "test.runtime",
        rootOperationReason: "test-root",
        status: "queued",
        recoveryState: "runnable",
        now: 100,
      });
      const step = store.createStep({
        runtimeRunId: run.runtimeRunId,
        stepType: "tool",
        status: "queued",
        recoveryState: "runnable",
        now: 110,
      });

      expect(() =>
        store.claimNextRunnableStep({
          operationKind: "test.runtime",
          workerId: "worker-invalid",
          claimTtlMs: 0,
          now: 139,
        }),
      ).toThrow(/claimTtlMs must be a positive safe integer/);

      const firstClaim = store.claimNextRunnableStep({
        operationKind: "test.runtime",
        workerId: "worker-1",
        claimTtlMs: 10,
        now: 140,
      });
      expect(firstClaim).toMatchObject({
        claimToken: expect.stringMatching(/^claim_/),
        claimExpiresAt: 150,
        step: { stepId: step.stepId, claimedBy: expect.stringMatching(/^claim_/) },
      });
      expect(
        store.claimNextRunnableStep({
          operationKind: "test.runtime",
          workerId: "worker-2",
          claimTtlMs: 10,
          now: 145,
        }),
      ).toBeUndefined();
      const secondClaim = store.claimNextRunnableStep({
        operationKind: "test.runtime",
        workerId: "worker-2",
        claimTtlMs: 10,
        now: 151,
      });
      expect(secondClaim).toMatchObject({
        claimToken: expect.stringMatching(/^claim_/),
        claimExpiresAt: 161,
        step: { stepId: step.stepId, claimedBy: expect.stringMatching(/^claim_/) },
      });
      expect(secondClaim?.claimToken).not.toBe(firstClaim?.claimToken);
      expect(
        store.releaseStepClaim({
          runtimeRunId: run.runtimeRunId,
          stepId: step.stepId,
          claimToken: firstClaim!.claimToken,
          now: 152,
        }),
      ).toBeUndefined();
      expect(
        store.updateStep({
          runtimeRunId: run.runtimeRunId,
          stepId: step.stepId,
          expectedClaimToken: firstClaim!.claimToken,
          status: "succeeded",
          recoveryState: "terminal",
          now: 155,
        }),
      ).toBeUndefined();
      expect(
        store.updateStep({
          runtimeRunId: run.runtimeRunId,
          stepId: step.stepId,
          status: "succeeded",
          recoveryState: "terminal",
          now: 156,
        }),
      ).toBeUndefined();
      expect(
        store.updateStep({
          runtimeRunId: run.runtimeRunId,
          stepId: step.stepId,
          claimedBy: null,
          now: 157,
        }),
      ).toBeUndefined();
      const completedStep = store.updateStep({
        runtimeRunId: run.runtimeRunId,
        stepId: step.stepId,
        expectedClaimToken: secondClaim!.claimToken,
        status: "succeeded",
        recoveryState: "terminal",
        now: 160,
      });
      expect(completedStep).toMatchObject({
        stepId: step.stepId,
        status: "succeeded",
        recoveryState: "terminal",
      });
      expect(completedStep?.claimedBy).toBeUndefined();
      expect(completedStep?.claimExpiresAt).toBeUndefined();
      expect(completedStep?.heartbeatAt).toBeUndefined();
    } finally {
      store.close();
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it("rejects incoherent lifecycle tuples and never claims malformed rows", () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "openclaw-durable-lifecycle-tuple-"));
    const dbPath = path.join(dir, "openclaw.sqlite");
    const store = openDurableRuntimeSqliteStore({ path: dbPath });
    try {
      const run = store.createRun({
        operationKind: "test.runtime",
        rootOperationReason: "test-root",
        status: "queued",
        recoveryState: "runnable",
        now: 100,
      });
      const step = store.createStep({
        runtimeRunId: run.runtimeRunId,
        stepType: "tool",
        status: "queued",
        recoveryState: "runnable",
        now: 100,
      });
      expect(() =>
        store.updateRun({ runtimeRunId: run.runtimeRunId, completedAt: 110, now: 110 }),
      ).toThrow(/lifecycle requires terminal status/);
      expect(() =>
        store.updateStep({
          runtimeRunId: run.runtimeRunId,
          stepId: step.stepId,
          completedAt: 110,
          now: 110,
        }),
      ).toThrow(/lifecycle requires terminal status/);

      const { DatabaseSync } = requireNodeSqlite();
      const corruptDb = new DatabaseSync(dbPath);
      try {
        corruptDb
          .prepare("UPDATE durable_execution_records SET completed_at = ? WHERE runtime_run_id = ?")
          .run(120, run.runtimeRunId);
        corruptDb
          .prepare(
            `UPDATE durable_execution_steps
                SET completed_at = ?
              WHERE runtime_run_id = ? AND step_id = ?`,
          )
          .run(120, run.runtimeRunId, step.stepId);
      } finally {
        corruptDb.close();
      }

      expect(store.listOpenRuns()).toEqual([]);
      expect(
        store.claimNextRunnableStep({ workerId: "worker", claimTtlMs: 100, now: 130 }),
      ).toBeUndefined();

      expect(
        store.createRun({
          operationKind: "test.terminal",
          rootOperationReason: "test-root",
          status: "succeeded",
          now: 140,
        }),
      ).toMatchObject({ status: "succeeded", recoveryState: "terminal", completedAt: 140 });
    } finally {
      store.close();
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it("uses unambiguous tuple encoding for step lease keys", () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "openclaw-durable-step-lease-key-"));
    const store = openDurableRuntimeSqliteStore({ path: path.join(dir, "openclaw.sqlite") });
    try {
      const firstRun = store.createRun({
        runtimeRunId: "run:a:b",
        operationKind: "test.runtime",
        rootOperationReason: "test-root",
        status: "queued",
        recoveryState: "runnable",
        now: 100,
      });
      const secondRun = store.createRun({
        runtimeRunId: "run:a",
        operationKind: "test.runtime",
        rootOperationReason: "test-root",
        status: "queued",
        recoveryState: "runnable",
        now: 100,
      });
      store.createStep({
        runtimeRunId: firstRun.runtimeRunId,
        stepId: "c",
        stepType: "tool",
        status: "queued",
        recoveryState: "runnable",
        now: 100,
      });
      store.createStep({
        runtimeRunId: secondRun.runtimeRunId,
        stepId: "b:c",
        stepType: "tool",
        status: "queued",
        recoveryState: "runnable",
        now: 100,
      });

      const firstClaim = store.claimNextRunnableStep({
        workerId: "worker-1",
        claimTtlMs: 1_000,
        now: 110,
      });
      const secondClaim = store.claimNextRunnableStep({
        workerId: "worker-2",
        claimTtlMs: 1_000,
        now: 110,
      });

      expect(firstClaim).toBeDefined();
      expect(secondClaim).toBeDefined();
      expect(secondClaim?.step.runtimeRunId).not.toBe(firstClaim?.step.runtimeRunId);
    } finally {
      store.close();
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it("paginates timelines and compacts only terminal run history", () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "openclaw-durable-store-"));
    const store = openDurableRuntimeSqliteStore({
      path: path.join(dir, "openclaw.sqlite"),
    });
    try {
      const active = store.createRun({
        operationKind: "test.runtime",
        rootOperationReason: "test-root",
        status: "running",
        recoveryState: "running",
      });
      store.appendEvent({ runtimeRunId: active.runtimeRunId, eventType: "active.one" });
      expect(store.compactTerminalRun({ runtimeRunId: active.runtimeRunId })).toEqual({
        runtimeRunId: active.runtimeRunId,
        compacted: false,
        redactedEventPayloads: 0,
        hasMore: false,
      });

      const terminal = store.createRun({
        operationKind: "test.runtime",
        rootOperationReason: "test-root",
        status: "succeeded",
        recoveryState: "terminal",
        completedAt: 200,
      });
      for (let index = 1; index <= 5; index += 1) {
        store.appendEvent({
          eventId: `terminal-event-${index}`,
          runtimeRunId: terminal.runtimeRunId,
          eventType: `terminal.${index}`,
          idempotencyKey: `terminal:${index}`,
          payload: { index },
        });
      }
      expect(
        store
          .getTimeline(terminal.runtimeRunId, { afterEventSeq: 2, limit: 2 })
          .map((event) => [event.eventSeq, event.eventType]),
      ).toEqual([
        [3, "terminal.3"],
        [4, "terminal.4"],
      ]);

      expect(
        store.compactTerminalRun({
          runtimeRunId: terminal.runtimeRunId,
          keepLastEvents: 2,
          now: 500,
        }),
      ).toEqual({
        runtimeRunId: terminal.runtimeRunId,
        compacted: true,
        redactedEventPayloads: 3,
        hasMore: false,
      });
      const compactedTimeline = store.getTimeline(terminal.runtimeRunId);
      expect(compactedTimeline.map((event) => event.eventType)).toEqual([
        "terminal.1",
        "terminal.2",
        "terminal.3",
        "terminal.4",
        "terminal.5",
      ]);
      for (const event of compactedTimeline.slice(0, 3)) {
        expect(event.payload).toBeUndefined();
        expect(event.payloadHash).toEqual(expect.any(String));
      }
      expect(
        store.appendEvent({
          eventId: "terminal-event-1",
          runtimeRunId: terminal.runtimeRunId,
          eventType: "terminal.1",
          idempotencyKey: "terminal:1",
          payload: { index: 1 },
        }),
      ).toMatchObject({ eventId: "terminal-event-1", eventSeq: 1 });
      expect(() =>
        store.appendEvent({
          eventId: "terminal-event-1",
          runtimeRunId: terminal.runtimeRunId,
          eventType: "terminal.1",
          idempotencyKey: "terminal:1",
          payload: { index: 999 },
        }),
      ).toThrow(/event replay conflict/);
      expect(
        store.compactTerminalRun({ runtimeRunId: terminal.runtimeRunId, keepLastEvents: 2 }),
      ).toEqual({
        runtimeRunId: terminal.runtimeRunId,
        compacted: false,
        redactedEventPayloads: 0,
        hasMore: false,
      });
    } finally {
      store.close();
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it("adds durable tables to an existing shared state database without rewriting existing rows", () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "openclaw-durable-store-"));
    const dbPath = path.join(dir, "openclaw.sqlite");
    const { DatabaseSync } = requireNodeSqlite();
    const existingDb = new DatabaseSync(dbPath);
    try {
      existingDb.exec(`
        CREATE TABLE diagnostic_events (
          scope TEXT NOT NULL,
          event_key TEXT NOT NULL,
          payload_json TEXT NOT NULL,
          created_at INTEGER NOT NULL,
          PRIMARY KEY (scope, event_key)
        );
      `);
      existingDb
        .prepare(
          `INSERT INTO diagnostic_events (scope, event_key, payload_json, created_at)
           VALUES (?, ?, ?, ?)`,
        )
        .run("state", "startup", '{"ok":true}', 123);
      const durableTablesBefore = existingDb
        .prepare(
          `SELECT name
             FROM sqlite_master
            WHERE type = 'table'
              AND name IN (${DURABLE_TABLES.map(() => "?").join(", ")})
            ORDER BY name`,
        )
        .all(...DURABLE_TABLES) as Array<{ name: string }>;
      expect(durableTablesBefore).toEqual([]);
    } finally {
      existingDb.close();
    }

    const store = openDurableRuntimeSqliteStore({ path: dbPath });
    store.close();

    const upgradedDb = new DatabaseSync(dbPath);
    try {
      const durableTablesAfter = upgradedDb
        .prepare(
          `SELECT name
             FROM sqlite_master
            WHERE type = 'table'
              AND name IN (${DURABLE_TABLES.map(() => "?").join(", ")})
            ORDER BY name`,
        )
        .all(...DURABLE_TABLES) as Array<{ name: string }>;
      expect(durableTablesAfter.map((row) => row.name)).toEqual([...DURABLE_TABLES].toSorted());
      expect(
        upgradedDb
          .prepare(
            `SELECT scope, event_key, payload_json, created_at
               FROM diagnostic_events
              WHERE scope = ?
                AND event_key = ?`,
          )
          .get("state", "startup"),
      ).toEqual({
        scope: "state",
        event_key: "startup",
        payload_json: '{"ok":true}',
        created_at: 123,
      });
      expect(upgradedDb.prepare("PRAGMA integrity_check").get()).toEqual({
        integrity_check: "ok",
      });
    } finally {
      upgradedDb.close();
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it("preserves wake coalescing and lifecycle across store connections", () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "openclaw-durable-wake-coalescing-"));
    const dbPath = path.join(dir, "openclaw.sqlite");
    const firstStore = openDurableRuntimeSqliteStore({ path: dbPath });
    const secondStore = openDurableRuntimeSqliteStore({ path: dbPath });
    const candidate = (occurrenceKey: string, revision: string, now: number) => ({
      sourceOwner: "subagent_runs",
      sourceRef: "child-run-1",
      targetKind: "agent_session" as const,
      targetRef: "agent:test:main",
      ownerKind: "agent_session" as const,
      ownerRef: "agent:test:main",
      reportRouteRef: "agent:test:main",
      targetResolutionStatus: "resolved" as const,
      targetResolutionReason: "canonical_owner_resolved",
      reason: "child_overdue" as const,
      factsRef: `subagent_runs:child-run-1:${revision}`,
      occurrenceKey,
      sourceRevision: revision,
      now,
    });
    try {
      const first = requireAppliedWakeResult(
        firstStore.reconcileWakeObligation({
          candidate: candidate("child-progress:1", "revision-1", 100),
          policy: { mode: "while_unresolved", recurrence: "after_terminal" },
        }),
      );
      expect(first).toMatchObject({ disposition: "created", duplicateWakeIds: [] });

      const claim = firstStore.claimNextWakeObligation({
        workerId: "worker-a",
        claimTtlMs: 1_000,
        retryBaseMs: 1,
        retryMaxMs: 1,
        now: 110,
      });
      expect(claim?.wake.wakeId).toBe(first.wake.wakeId);
      firstStore.completeWakeObligationClaim({
        wakeId: claim!.wake.wakeId,
        deliveryAttemptId: claim!.deliveryAttempt.deliveryAttemptId,
        claimToken: claim!.claimToken,
        attemptStatus: "handoff_accepted",
        wakeStatus: "handoff_accepted",
        now: 120,
      });

      const coalesced = secondStore.reconcileWakeObligation({
        candidate: candidate("child-progress:2", "revision-2", 200),
        policy: { mode: "while_unresolved", recurrence: "after_terminal" },
      });
      expect(coalesced).toMatchObject({
        disposition: "coalesced",
        duplicateWakeIds: [],
        wake: {
          wakeId: first.wake.wakeId,
          factsRef: "subagent_runs:child-run-1:revision-2",
          status: "handoff_accepted",
          metadata: {
            sourceRevision: "revision-2",
            wakeReconciliation: {
              mode: "while_unresolved",
              recurrence: "after_terminal",
              latestOccurrenceKey: "child-progress:2",
            },
          },
        },
      });
      expect(firstStore.listWakeObligations({ sourceRef: "child-run-1" })).toHaveLength(1);
      expect(firstStore.listDeliveryAttemptEvidence({ wakeId: first.wake.wakeId })).toHaveLength(1);

      expect(
        firstStore.reconcileWakeObligation({
          candidate: candidate("child-progress:2", "revision-2", 210),
          policy: { mode: "while_unresolved", recurrence: "after_terminal" },
        }),
      ).toMatchObject({ disposition: "exact_match", wake: { wakeId: first.wake.wakeId } });

      firstStore.acknowledgeWakeObligation({
        wakeId: first.wake.wakeId,
        actorKind: "system_worker",
        actorRef: "test-consumer",
        now: 220,
      });
      expect(
        secondStore.reconcileWakeObligation({
          candidate: candidate("child-progress:2", "revision-2", 230),
          policy: { mode: "while_unresolved", recurrence: "after_terminal" },
        }),
      ).toMatchObject({
        disposition: "exact_match",
        duplicateScan: "not_scanned",
        wake: { wakeId: first.wake.wakeId, status: "acked" },
      });
      expect(firstStore.listWakeObligations({ sourceRef: "child-run-1" })).toHaveLength(1);
      const recurring = requireAppliedWakeResult(
        secondStore.reconcileWakeObligation({
          candidate: candidate("child-progress:3", "revision-3", 300),
          policy: { mode: "while_unresolved", recurrence: "after_terminal" },
        }),
      );
      expect(recurring).toMatchObject({ disposition: "created" });
      expect(recurring.wake.wakeId).not.toBe(first.wake.wakeId);

      const distinctRoute = firstStore.reconcileWakeObligation({
        candidate: {
          ...candidate("child-progress:route-2", "revision-3", 310),
          targetRef: "agent:test:other",
          ownerRef: "agent:test:other",
          reportRouteRef: "agent:test:other",
        },
        policy: { mode: "while_unresolved", recurrence: "after_terminal" },
      });
      expect(distinctRoute).toMatchObject({ disposition: "created" });
      expect(firstStore.listWakeObligations({ sourceRef: "child-run-1" })).toHaveLength(3);
      expect(firstStore.getWakeObligationInspection(first.wake.wakeId)).toMatchObject({
        sourceRefs: {
          occurrenceKeys: ["child-progress:1", "child-progress:2"],
          occurrenceCount: 2,
          occurrenceKeysTruncated: false,
        },
      });
    } finally {
      secondStore.close();
      firstStore.close();
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it("serializes concurrent process writers into one canonical wake", async () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "openclaw-durable-wake-process-race-"));
    const dbPath = path.join(dir, "openclaw.sqlite");
    const initializer = openDurableRuntimeSqliteStore({ path: dbPath });
    initializer.close();
    const firstWriter = startConcurrentWakeWriter({
      dbPath,
      occurrenceKey: "process-occurrence:1",
    });
    const secondWriter = startConcurrentWakeWriter({
      dbPath,
      occurrenceKey: "process-occurrence:2",
    });
    try {
      await Promise.all([firstWriter.ready, secondWriter.ready]);
      const [firstResult, secondResult] = await Promise.all([
        firstWriter.run(),
        secondWriter.run(),
      ]);
      expect([firstResult.disposition, secondResult.disposition].toSorted()).toEqual([
        "coalesced",
        "created",
      ]);

      const store = openDurableRuntimeSqliteStore({ path: dbPath });
      try {
        const wakes = store.listWakeObligations({ sourceRef: "test-source:concurrent" });
        expect(wakes).toHaveLength(1);
        const firstOccurrence = store.getWakeObligationByOccurrenceKey({
          sourceOwner: "test-owner",
          sourceRef: "test-source:concurrent",
          occurrenceKey: "process-occurrence:1",
        });
        const secondOccurrence = store.getWakeObligationByOccurrenceKey({
          sourceOwner: "test-owner",
          sourceRef: "test-source:concurrent",
          occurrenceKey: "process-occurrence:2",
        });
        expect(firstOccurrence?.wakeId).toBe(wakes[0]?.wakeId);
        expect(secondOccurrence?.wakeId).toBe(wakes[0]?.wakeId);
        expect(store.getWakeObligationInspection(wakes[0]!.wakeId)).toMatchObject({
          sourceRefs: { occurrenceCount: 2, occurrenceKeysTruncated: false },
        });
      } finally {
        store.close();
      }
    } finally {
      firstWriter.child.kill();
      secondWriter.child.kill();
      fs.rmSync(dir, { recursive: true, force: true });
    }
  }, 20_000);

  it("keeps parent targets in the logical wake identity", () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "openclaw-durable-parent-target-"));
    const store = openDurableRuntimeSqliteStore({ path: path.join(dir, "openclaw.sqlite") });
    const base = {
      sourceOwner: "subagent_runs",
      sourceRef: "child-parent-target",
      reason: "child_terminal" as const,
    };
    try {
      const first = requireAppliedWakeResult(
        store.reconcileWakeObligation({
          candidate: {
            ...base,
            parentSessionKey: "agent:test:parent-a",
            occurrenceKey: "parent-target:1",
            now: 100,
          },
          policy: { mode: "while_unresolved", recurrence: "after_terminal" },
        }),
      );
      const second = requireAppliedWakeResult(
        store.reconcileWakeObligation({
          candidate: {
            ...base,
            parentSessionKey: "agent:test:parent-b",
            occurrenceKey: "parent-target:2",
            now: 110,
          },
          policy: { mode: "while_unresolved", recurrence: "after_terminal" },
        }),
      );

      expect(first.disposition).toBe("created");
      expect(second.disposition).toBe("created");
      expect(second.wake.wakeId).not.toBe(first.wake.wakeId);
      expect(store.getWakeObligation(first.wake.wakeId)?.parentSessionKey).toBe(
        "agent:test:parent-a",
      );
      expect(store.listWakeObligations({ sourceRef: "child-parent-target" })).toHaveLength(2);
    } finally {
      store.close();
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it("repairs duplicate unresolved wakes atomically without deleting history", () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "openclaw-durable-wake-conflict-"));
    const dbPath = path.join(dir, "openclaw.sqlite");
    const store = openDurableRuntimeSqliteStore({ path: dbPath });
    const base = {
      sourceOwner: "flow_runs",
      sourceRef: "flow-1",
      targetKind: "agent_session" as const,
      targetRef: "agent:test:main",
      reason: "fan_in_incomplete" as const,
    };
    try {
      const first = store.createWakeObligation({
        ...base,
        occurrenceKey: "flow-progress:1",
        now: 100,
      });
      const duplicate = store.createWakeObligation({
        ...base,
        occurrenceKey: "flow-progress:2",
        now: 200,
      });
      const { DatabaseSync } = requireNodeSqlite();
      const seedDb = new DatabaseSync(dbPath);
      try {
        seedDb
          .prepare(
            `UPDATE wake_obligations
                SET coalescing_mode = 'while_unresolved', recurrence_policy = 'after_terminal'
              WHERE wake_id IN (?, ?)`,
          )
          .run(first.wakeId, duplicate.wakeId);
      } finally {
        seedDb.close();
      }

      const result = store.reconcileWakeObligation({
        candidate: { ...base, occurrenceKey: "flow-progress:3", now: 300 },
        policy: { mode: "while_unresolved", recurrence: "after_terminal" },
      });

      expect(result).toMatchObject({
        disposition: "coalesced",
        wake: { wakeId: first.wakeId },
        duplicateWakeIds: [duplicate.wakeId],
      });
      expect(store.getWakeObligation(duplicate.wakeId)).toMatchObject({
        status: "superseded",
        metadata: {
          wakeReconciliation: { supersededByWakeId: first.wakeId },
        },
      });
      expect(
        store.getWakeObligationByOccurrenceKey({
          sourceOwner: "flow_runs",
          sourceRef: "flow-1",
          occurrenceKey: "flow-progress:2",
        })?.wakeId,
      ).toBe(first.wakeId);
    } finally {
      store.close();
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it("returns a non-mutating conflict when duplicate wakes both carry delivery evidence", () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "openclaw-durable-wake-evidence-conflict-"));
    const dbPath = path.join(dir, "openclaw.sqlite");
    const store = openDurableRuntimeSqliteStore({ path: dbPath });
    const base = {
      sourceOwner: "task_runs",
      sourceRef: "task-evidence-conflict",
      targetKind: "agent_session" as const,
      targetRef: "agent:test:main",
      reason: "child_overdue" as const,
    };
    try {
      const first = store.createWakeObligation({
        ...base,
        occurrenceKey: "evidence-conflict:1",
        now: 100,
      });
      const second = store.createWakeObligation({
        ...base,
        occurrenceKey: "evidence-conflict:2",
        now: 200,
      });
      const { DatabaseSync } = requireNodeSqlite();
      const seedDb = new DatabaseSync(dbPath);
      try {
        seedDb.exec(
          `UPDATE wake_obligations
              SET coalescing_mode = 'while_unresolved', recurrence_policy = 'after_terminal'`,
        );
      } finally {
        seedDb.close();
      }

      const firstClaim = store.claimNextWakeObligation({
        workerId: "worker-evidence-1",
        claimTtlMs: 1_000,
        retryBaseMs: 1,
        retryMaxMs: 1,
        now: 210,
      });
      store.completeWakeObligationClaim({
        wakeId: firstClaim!.wake.wakeId,
        deliveryAttemptId: firstClaim!.deliveryAttempt.deliveryAttemptId,
        claimToken: firstClaim!.claimToken,
        attemptStatus: "handoff_accepted",
        wakeStatus: "handoff_accepted",
        now: 220,
      });
      const secondClaim = store.claimNextWakeObligation({
        workerId: "worker-evidence-2",
        claimTtlMs: 1_000,
        retryBaseMs: 1,
        retryMaxMs: 1,
        now: 230,
      });
      expect(new Set([firstClaim!.wake.wakeId, secondClaim!.wake.wakeId])).toEqual(
        new Set([first.wakeId, second.wakeId]),
      );

      expect(
        store.reconcileWakeObligation({
          candidate: { ...base, occurrenceKey: "evidence-conflict:3", now: 240 },
          policy: { mode: "while_unresolved", recurrence: "after_terminal" },
        }),
      ).toMatchObject({
        disposition: "conflict",
        reason: "multiple_evidence_bearing_wakes",
        candidatePersisted: false,
        duplicateScan: "complete",
        conflictingWakeIds: expect.arrayContaining([first.wakeId, second.wakeId]),
      });
      expect(
        store.getWakeObligationByOccurrenceKey({
          sourceOwner: "task_runs",
          sourceRef: "task-evidence-conflict",
          occurrenceKey: "evidence-conflict:3",
        }),
      ).toBeUndefined();
      expect(store.getWakeObligation(second.wakeId)?.status).toBe("pending");
    } finally {
      store.close();
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it("does not repair duplicate wakes that both carry owner-control evidence", () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "openclaw-durable-wake-control-conflict-"));
    const dbPath = path.join(dir, "openclaw.sqlite");
    const store = openDurableRuntimeSqliteStore({ path: dbPath });
    const base = {
      sourceOwner: "test-owner",
      sourceRef: "test-source:control-conflict",
      targetKind: "agent_session" as const,
      targetRef: "agent:test:main",
      reason: "operator_requested" as const,
    };
    try {
      const first = store.createWakeObligation({
        ...base,
        occurrenceKey: "control-conflict:1",
        now: 100,
      });
      const second = store.createWakeObligation({
        ...base,
        occurrenceKey: "control-conflict:2",
        now: 110,
      });
      const { DatabaseSync } = requireNodeSqlite();
      const seedDb = new DatabaseSync(dbPath);
      try {
        seedDb.exec(
          `UPDATE wake_obligations
              SET coalescing_mode = 'while_unresolved', recurrence_policy = 'after_terminal'`,
        );
      } finally {
        seedDb.close();
      }
      for (const [index, wakeId] of [first.wakeId, second.wakeId].entries()) {
        expect(
          store.markWakeObligationDecisionRequired({
            wakeId,
            actorKind: "operator",
            actorRef: "test-operator",
            decisionKind: "inspected",
            idempotencyKey: `control-conflict:${index}`,
            now: 120 + index,
          }),
        ).toBeDefined();
      }

      expect(
        store.reconcileWakeObligation({
          candidate: { ...base, occurrenceKey: "control-conflict:3", now: 130 },
          policy: { mode: "while_unresolved", recurrence: "after_terminal" },
        }),
      ).toMatchObject({
        disposition: "conflict",
        reason: "multiple_evidence_bearing_wakes",
        candidatePersisted: false,
        conflictingWakeIds: expect.arrayContaining([first.wakeId, second.wakeId]),
      });
      expect(
        store.getWakeObligationByOccurrenceKey({
          sourceOwner: base.sourceOwner,
          sourceRef: base.sourceRef,
          occurrenceKey: "control-conflict:3",
        }),
      ).toBeUndefined();
      expect(store.getWakeObligation(first.wakeId)?.status).toBe("pending");
      expect(store.getWakeObligation(second.wakeId)?.status).toBe("pending");
    } finally {
      store.close();
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it("uses the unresolved identity index for bounded wake reconciliation", () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "openclaw-durable-wake-index-"));
    const dbPath = path.join(dir, "openclaw.sqlite");
    const { DatabaseSync } = requireNodeSqlite();
    const initialStore = openDurableRuntimeSqliteStore({ path: dbPath });
    initialStore.close();
    const legacyDb = new DatabaseSync(dbPath);
    legacyDb.exec("DROP INDEX idx_wake_obligations_unresolved_identity");
    legacyDb.close();

    const upgradedStore = openDurableRuntimeSqliteStore({ path: dbPath });
    upgradedStore.close();
    const db = new DatabaseSync(dbPath, { readOnly: true });
    try {
      const plan = db
        .prepare(
          `EXPLAIN QUERY PLAN
           SELECT *
             FROM wake_obligations
            WHERE source_owner = ?
              AND source_ref = ?
              AND coalescing_mode = 'while_unresolved'
              AND reason = ?
              AND target_kind IS NULL
              AND target_ref IS NULL
              AND owner_kind IS NULL
              AND owner_ref IS NULL
              AND report_route_ref IS NULL
              AND status NOT IN ('acked', 'superseded')
              AND (
                created_at > ?
                OR (created_at = ? AND wake_id > ?)
              )
            ORDER BY created_at, wake_id
            LIMIT ?`,
        )
        .all(
          "session_store",
          "agent:test:main",
          "operator_requested",
          100,
          100,
          "wake_cursor_0064",
          65,
        ) as Array<{ detail: string }>;

      expect(
        plan.some((row) => row.detail.includes("idx_wake_obligations_unresolved_identity")),
      ).toBe(true);
    } finally {
      db.close();
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it("fails closed before mutating an incomplete logical wake scan", () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "openclaw-durable-wake-bounded-scan-"));
    const store = openDurableRuntimeSqliteStore({ path: path.join(dir, "openclaw.sqlite") });
    const base = {
      sourceOwner: "subagent_runs",
      sourceRef: "child-run-bounded",
      targetKind: "agent_session" as const,
      targetRef: "agent:test:main",
      ownerKind: "agent_session" as const,
      ownerRef: "agent:test:main",
      reportRouteRef: "agent:test:main",
      reason: "child_overdue" as const,
    };
    try {
      store.withTransaction(() => {
        for (let index = 0; index < 513; index += 1) {
          store.createWakeObligation({
            ...base,
            wakeId: `wake_bounded_${String(index).padStart(4, "0")}`,
            occurrenceKey: `child-bounded:${index}`,
            factsRef: "unchanged",
            now: index,
          });
        }
      });
      const { DatabaseSync } = requireNodeSqlite();
      const seedDb = new DatabaseSync(path.join(dir, "openclaw.sqlite"));
      try {
        seedDb.exec(
          `UPDATE wake_obligations
              SET coalescing_mode = 'while_unresolved', recurrence_policy = 'after_terminal'`,
        );
      } finally {
        seedDb.close();
      }

      expect(
        store.reconcileWakeObligation({
          candidate: {
            ...base,
            occurrenceKey: "child-bounded:next",
            factsRef: "must-not-commit",
            now: 1_000,
          },
          policy: { mode: "while_unresolved", recurrence: "after_terminal" },
        }),
      ).toMatchObject({
        disposition: "conflict",
        reason: "scan_truncated",
        candidatePersisted: false,
        duplicateScan: "truncated",
      });
      expect(
        store.getWakeObligationByOccurrenceKey({
          sourceOwner: "subagent_runs",
          sourceRef: "child-run-bounded",
          occurrenceKey: "child-bounded:next",
        }),
      ).toBeUndefined();
      const unchanged = store.getWakeObligation("wake_bounded_0000");
      expect(unchanged).toMatchObject({ factsRef: "unchanged" });
      expect(unchanged).toMatchObject({
        metadata: { wakeReconciliation: { latestOccurrenceKey: "child-bounded:0" } },
      });
    } finally {
      store.close();
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it("does not apply the coalescing scan bound when reconciliation is disabled", () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "openclaw-durable-wake-no-coalescing-"));
    const store = openDurableRuntimeSqliteStore({ path: path.join(dir, "openclaw.sqlite") });
    const base = {
      sourceOwner: "subagent_runs",
      sourceRef: "child-run-independent",
      targetKind: "agent_session" as const,
      targetRef: "agent:test:main",
      reason: "child_overdue" as const,
    };
    try {
      store.withTransaction(() => {
        for (let index = 0; index < 513; index += 1) {
          store.createWakeObligation({
            ...base,
            wakeId: `wake_independent_${String(index).padStart(4, "0")}`,
            occurrenceKey: `child-independent:${index}`,
            now: index,
          });
        }
      });

      expect(
        store.reconcileWakeObligation({
          candidate: { ...base, occurrenceKey: "child-independent:next", now: 1_000 },
          policy: { mode: "none" },
        }),
      ).toMatchObject({ disposition: "created", wake: { status: "pending" } });
      expect(
        store.reconcileWakeObligation({
          candidate: { ...base, occurrenceKey: "child-independent:next", now: 1_001 },
          policy: { mode: "none" },
        }),
      ).toMatchObject({ disposition: "exact_match" });
      expect(
        store.reconcileWakeObligation({
          candidate: { ...base, occurrenceKey: "child-independent:coalesced", now: 1_002 },
          policy: { mode: "while_unresolved", recurrence: "after_terminal" },
        }),
      ).toMatchObject({ disposition: "created", duplicateScan: "complete" });
    } finally {
      store.close();
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it("uses exact occurrence lookup before the bounded logical wake scan", () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "openclaw-durable-wake-exact-scan-"));
    const store = openDurableRuntimeSqliteStore({ path: path.join(dir, "openclaw.sqlite") });
    const base = {
      sourceOwner: "subagent_runs",
      sourceRef: "child-run-exact",
      targetKind: "agent_session" as const,
      targetRef: "agent:test:main",
      ownerKind: "agent_session" as const,
      ownerRef: "agent:test:main",
      reportRouteRef: "agent:test:main",
      reason: "child_overdue" as const,
    };
    try {
      const exactCandidate = {
        ...base,
        occurrenceKey: "child-exact:512",
        factsRef: "subagent_runs:child-run-exact:latest",
        now: 1_000,
      };
      store.reconcileWakeObligation({
        candidate: exactCandidate,
        policy: { mode: "while_unresolved", recurrence: "after_terminal" },
      });
      store.withTransaction(() => {
        for (let index = 0; index < 512; index += 1) {
          store.createWakeObligation({
            ...base,
            wakeId: `wake_exact_${String(index).padStart(4, "0")}`,
            occurrenceKey: `child-exact:${index}`,
            now: index,
          });
        }
      });
      const { DatabaseSync } = requireNodeSqlite();
      const seedDb = new DatabaseSync(path.join(dir, "openclaw.sqlite"));
      try {
        seedDb
          .prepare(
            `UPDATE wake_obligations
                SET coalescing_mode = 'while_unresolved', recurrence_policy = 'after_terminal'
              WHERE wake_id LIKE 'wake_exact_%'`,
          )
          .run();
      } finally {
        seedDb.close();
      }

      expect(
        store.reconcileWakeObligation({
          candidate: { ...exactCandidate, now: 1_001 },
          policy: { mode: "while_unresolved", recurrence: "after_terminal" },
        }),
      ).toMatchObject({
        disposition: "exact_match",
        duplicateWakeIds: [],
        wake: {
          wakeId: expect.stringMatching(/^wake_/),
          factsRef: "subagent_runs:child-run-exact:latest",
        },
      });
    } finally {
      store.close();
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it("keeps terminal wake history outside the unresolved reconciliation bound", () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "openclaw-durable-wake-history-"));
    const store = openDurableRuntimeSqliteStore({ path: path.join(dir, "openclaw.sqlite") });
    const base = {
      sourceOwner: "subagent_runs",
      sourceRef: "child-run-history",
      targetKind: "agent_session" as const,
      targetRef: "agent:test:main",
      reason: "child_overdue" as const,
    };
    try {
      store.withTransaction(() => {
        for (let index = 0; index < 513; index += 1) {
          const wake = store.createWakeObligation({
            ...base,
            wakeId: `wake_history_${String(index).padStart(4, "0")}`,
            occurrenceKey: `child-history:${index}`,
            now: index,
          });
          store.acknowledgeWakeObligation({
            wakeId: wake.wakeId,
            actorKind: "system_worker",
            actorRef: "test-consumer",
            now: index + 1,
          });
        }
      });
      const { DatabaseSync } = requireNodeSqlite();
      const seedDb = new DatabaseSync(path.join(dir, "openclaw.sqlite"));
      try {
        seedDb.exec(
          `UPDATE wake_obligations
              SET coalescing_mode = 'while_unresolved', recurrence_policy = 'never'`,
        );
      } finally {
        seedDb.close();
      }

      expect(
        store.reconcileWakeObligation({
          candidate: { ...base, occurrenceKey: "child-history:blocked", now: 1_000 },
          policy: { mode: "while_unresolved", recurrence: "never" },
        }),
      ).toMatchObject({
        disposition: "terminal_match",
        wake: { wakeId: "wake_history_0512", status: "acked" },
      });

      expect(
        store.reconcileWakeObligation({
          candidate: { ...base, occurrenceKey: "child-history:recurring", now: 1_001 },
          policy: { mode: "while_unresolved", recurrence: "after_terminal" },
        }),
      ).toMatchObject({
        disposition: "conflict",
        reason: "policy_conflict",
        candidatePersisted: false,
      });
    } finally {
      store.close();
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it("paginates a logical wake identity without skipping equal timestamps", () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "openclaw-durable-wake-cursor-"));
    const store = openDurableRuntimeSqliteStore({ path: path.join(dir, "openclaw.sqlite") });
    const base = {
      sourceOwner: "subagent_runs",
      sourceRef: "child-run-cursor",
      targetKind: "agent_session" as const,
      targetRef: "agent:test:main",
      ownerKind: "agent_session" as const,
      ownerRef: "agent:test:main",
      reportRouteRef: "agent:test:main",
      reason: "child_overdue" as const,
    };
    try {
      store.withTransaction(() => {
        for (let index = 0; index < 70; index += 1) {
          store.createWakeObligation({
            ...base,
            wakeId: `wake_cursor_${String(index).padStart(4, "0")}`,
            occurrenceKey: `child-cursor:${index}`,
            now: 100,
          });
        }
      });
      const { DatabaseSync } = requireNodeSqlite();
      const seedDb = new DatabaseSync(path.join(dir, "openclaw.sqlite"));
      try {
        seedDb.exec(
          `UPDATE wake_obligations
              SET coalescing_mode = 'while_unresolved', recurrence_policy = 'after_terminal'`,
        );
      } finally {
        seedDb.close();
      }

      const result = requireAppliedWakeResult(
        store.reconcileWakeObligation({
          candidate: {
            ...base,
            occurrenceKey: "child-cursor:next",
            factsRef: "subagent_runs:child-run-cursor:latest",
            now: 200,
          },
          policy: { mode: "while_unresolved", recurrence: "after_terminal" },
        }),
      );

      expect(result).toMatchObject({
        disposition: "coalesced",
        wake: {
          wakeId: "wake_cursor_0000",
          factsRef: "subagent_runs:child-run-cursor:latest",
        },
      });
      expect(result.duplicateWakeIds).toHaveLength(69);
      expect(new Set(result.duplicateWakeIds).size).toBe(69);
      expect(result.duplicateWakeIds).toContain("wake_cursor_0069");
    } finally {
      store.close();
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it("reports an idempotency conflict when an occurrence key changes projection", () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "openclaw-durable-wake-key-owner-"));
    const store = openDurableRuntimeSqliteStore({ path: path.join(dir, "openclaw.sqlite") });
    try {
      store.createWakeObligation({
        sourceOwner: "subagent_runs",
        sourceRef: "child-run-first",
        targetKind: "agent_session",
        targetRef: "agent:test:main",
        reason: "child_overdue",
        occurrenceKey: "shared-occurrence-key",
        now: 100,
      });

      expect(
        store.reconcileWakeObligation({
          candidate: {
            sourceOwner: "subagent_runs",
            sourceRef: "child-run-first",
            targetKind: "agent_session",
            targetRef: "agent:test:other",
            reason: "child_overdue",
            occurrenceKey: "shared-occurrence-key",
            now: 200,
          },
          policy: { mode: "while_unresolved", recurrence: "after_terminal" },
        }),
      ).toMatchObject({
        disposition: "conflict",
        reason: "idempotency_conflict",
        candidatePersisted: false,
      });
      expect(store.listWakeObligations({ sourceRef: "child-run-first" })).toHaveLength(1);
    } finally {
      store.close();
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it("scopes occurrence idempotency to the canonical source", () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "openclaw-durable-wake-source-scope-"));
    const store = openDurableRuntimeSqliteStore({ path: path.join(dir, "openclaw.sqlite") });
    try {
      const first = store.createWakeObligation({
        sourceOwner: "task_runs",
        sourceRef: "task-a",
        targetKind: "agent_session",
        targetRef: "agent:test:main",
        reason: "operator_requested",
        occurrenceKey: "owner-local-occurrence",
        now: 100,
      });
      const second = store.createWakeObligation({
        sourceOwner: "task_runs",
        sourceRef: "task-b",
        targetKind: "agent_session",
        targetRef: "agent:test:main",
        reason: "operator_requested",
        occurrenceKey: "owner-local-occurrence",
        now: 110,
      });

      expect(second.wakeId).not.toBe(first.wakeId);
      expect(
        store.getWakeObligationByOccurrenceKey({
          sourceOwner: "task_runs",
          sourceRef: "task-a",
          occurrenceKey: "owner-local-occurrence",
        })?.wakeId,
      ).toBe(first.wakeId);
      expect(
        store.getWakeObligationByOccurrenceKey({
          sourceOwner: "task_runs",
          sourceRef: "task-b",
          occurrenceKey: "owner-local-occurrence",
        })?.wakeId,
      ).toBe(second.wakeId);
    } finally {
      store.close();
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it("rejects recurrence-policy changes on an unresolved logical wake", () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "openclaw-durable-wake-policy-"));
    const store = openDurableRuntimeSqliteStore({ path: path.join(dir, "openclaw.sqlite") });
    const candidate = (occurrenceKey: string, now: number) => ({
      sourceOwner: "task_runs",
      sourceRef: "task-policy",
      targetKind: "agent_session" as const,
      targetRef: "agent:test:main",
      reason: "operator_requested" as const,
      occurrenceKey,
      now,
    });
    try {
      const first = requireAppliedWakeResult(
        store.reconcileWakeObligation({
          candidate: candidate("task-policy:1", 100),
          policy: { mode: "while_unresolved", recurrence: "never" },
        }),
      );
      expect(
        store.reconcileWakeObligation({
          candidate: candidate("task-policy:2", 110),
          policy: { mode: "while_unresolved", recurrence: "after_terminal" },
        }),
      ).toMatchObject({
        disposition: "conflict",
        reason: "policy_conflict",
        candidatePersisted: false,
        conflictingWakeIds: [first.wake.wakeId],
      });
      expect(
        store.getWakeObligationByOccurrenceKey({
          sourceOwner: "task_runs",
          sourceRef: "task-policy",
          occurrenceKey: "task-policy:2",
        }),
      ).toBeUndefined();
      expect(store.getWakeObligation(first.wake.wakeId)?.coalescingPolicy).toEqual({
        mode: "while_unresolved",
        recurrence: "never",
      });
    } finally {
      store.close();
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it("bounds occurrence inspection while retaining exact lookup", () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "openclaw-durable-wake-occurrences-"));
    const store = openDurableRuntimeSqliteStore({ path: path.join(dir, "openclaw.sqlite") });
    try {
      let wakeId: string | undefined;
      for (let index = 0; index < 105; index += 1) {
        const occurrenceKey = `task-occurrence:${String(index).padStart(3, "0")}`;
        const result = requireAppliedWakeResult(
          store.reconcileWakeObligation({
            candidate: {
              sourceOwner: "task_runs",
              sourceRef: "task-occurrence-window",
              targetKind: "agent_session",
              targetRef: "agent:test:main",
              reason: "operator_requested",
              occurrenceKey,
              sourceRevision: `revision-${index}`,
              now: 100 + index,
            },
            policy: { mode: "while_unresolved", recurrence: "never" },
          }),
        );
        wakeId ??= result.wake.wakeId;
        expect(result.wake.wakeId).toBe(wakeId);
      }

      const inspection = store.getWakeObligationInspection(wakeId!);
      expect(inspection?.sourceRefs).toMatchObject({
        occurrenceCount: 105,
        occurrenceKeysTruncated: true,
      });
      expect(inspection?.sourceRefs.occurrenceKeys).toHaveLength(100);
      expect(inspection?.sourceRefs.occurrenceKeys[0]).toBe("task-occurrence:005");
      expect(inspection?.sourceRefs.occurrenceKeys.at(-1)).toBe("task-occurrence:104");
      expect(
        store.getWakeObligationByOccurrenceKey({
          sourceOwner: "task_runs",
          sourceRef: "task-occurrence-window",
          occurrenceKey: "task-occurrence:000",
        })?.wakeId,
      ).toBe(wakeId);
    } finally {
      store.close();
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it("marks bounded delivery and uncertainty inspection collections as truncated", () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "openclaw-durable-inspection-bounds-"));
    const dbPath = path.join(dir, "openclaw.sqlite");
    const store = openDurableRuntimeSqliteStore({ path: dbPath });
    try {
      const wake = store.createWakeObligation({
        sourceOwner: "test-owner",
        sourceRef: "test-source:inspection-bounds",
        targetKind: "agent_session",
        targetRef: "agent:test:main",
        reason: "operator_requested",
        occurrenceKey: "inspection-bounds:1",
        now: 100,
      });
      const { DatabaseSync } = requireNodeSqlite();
      const seedDb = new DatabaseSync(dbPath);
      try {
        const insertAttempt = seedDb.prepare(
          `INSERT INTO delivery_attempt_evidence (
             delivery_attempt_id, wake_id, source_owner, source_ref, dedupe_key,
             status, scheduled_at, created_at, updated_at
           ) VALUES (?, ?, ?, ?, ?, 'scheduled', ?, ?, ?)`,
        );
        for (let index = 0; index < 105; index += 1) {
          insertAttempt.run(
            `attempt-inspection-${index}`,
            wake.wakeId,
            "test-owner",
            "test-source:inspection-bounds",
            `attempt-dedupe-${index}`,
            200 + index,
            200 + index,
            200 + index,
          );
        }
      } finally {
        seedDb.close();
      }
      for (let index = 0; index < 105; index += 1) {
        store.recordUncertaintyFact({
          factId: `fact-inspection-${index}`,
          sourceOwner: "test-owner",
          sourceRef: "test-source:inspection-bounds",
          kind: "requires_owner_decision",
          dedupeKey: `fact-dedupe-${index}`,
          now: 400 + index,
        });
      }

      const inspection = store.getWakeObligationInspection(wake.wakeId);
      expect(inspection?.deliveryAttemptEvidence).toHaveLength(100);
      expect(inspection).toMatchObject({
        deliveryAttemptEvidenceCount: 105,
        deliveryAttemptEvidenceTruncated: true,
        unresolvedUncertaintyFactCount: 105,
        unresolvedUncertaintyFactsTruncated: true,
      });
      expect(inspection?.unresolvedUncertaintyFacts).toHaveLength(100);
    } finally {
      store.close();
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it("paginates every unresolved wake for owner reconciliation", () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "openclaw-durable-owner-wake-scan-"));
    const store = openDurableRuntimeSqliteStore({ path: path.join(dir, "openclaw.sqlite") });
    try {
      for (let index = 0; index < 503; index += 1) {
        store.createWakeObligation({
          wakeId: `wake_${String(index).padStart(4, "0")}`,
          sourceOwner: "subagent_runs",
          sourceRef: `child-${index}`,
          targetKind: "agent_session",
          targetRef: "agent:test:main",
          reason: "child_overdue",
          occurrenceKey: `child-progress:${index}`,
          now: index,
        });
      }
      const terminal = store.createWakeObligation({
        wakeId: "wake_terminal",
        sourceOwner: "subagent_runs",
        sourceRef: "terminal-child",
        targetKind: "agent_session",
        targetRef: "agent:test:main",
        reason: "child_terminal",
        occurrenceKey: "child-terminal",
        now: 1_000,
      });
      store.acknowledgeWakeObligation({
        wakeId: terminal.wakeId,
        actorKind: "system_worker",
        actorRef: "test",
        now: 1_001,
      });

      const wakeIds: string[] = [];
      let afterWakeId: string | undefined;
      for (;;) {
        const page = store.listOwnerWakeObligationsForReconciliation({
          sourceOwner: "subagent_runs",
          afterWakeId,
          limit: 100,
        });
        wakeIds.push(...page.map((wake) => wake.wakeId));
        if (page.length < 100) {
          break;
        }
        afterWakeId = page.at(-1)!.wakeId;
      }

      expect(wakeIds).toHaveLength(503);
      expect(new Set(wakeIds).size).toBe(503);
      expect(wakeIds).not.toContain(terminal.wakeId);
    } finally {
      store.close();
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it("does not recur after terminal when coalescing policy forbids it", () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "openclaw-durable-wake-single-"));
    const store = openDurableRuntimeSqliteStore({ path: path.join(dir, "openclaw.sqlite") });
    const candidate = (occurrenceKey: string, now: number) => ({
      sourceOwner: "session_store",
      sourceRef: "agent:test:main",
      targetKind: "agent_session" as const,
      targetRef: "agent:test:main",
      reason: "operator_requested" as const,
      occurrenceKey,
      now,
    });
    try {
      const first = requireAppliedWakeResult(
        store.reconcileWakeObligation({
          candidate: candidate("single-attention:1", 100),
          policy: { mode: "while_unresolved", recurrence: "never" },
        }),
      );
      store.acknowledgeWakeObligation({
        wakeId: first.wake.wakeId,
        actorKind: "system_worker",
        actorRef: "test-consumer",
        now: 150,
      });

      expect(
        store.reconcileWakeObligation({
          candidate: candidate("single-attention:2", 200),
          policy: { mode: "while_unresolved", recurrence: "never" },
        }),
      ).toMatchObject({
        disposition: "terminal_match",
        wake: { wakeId: first.wake.wakeId, status: "acked" },
      });
      expect(store.listWakeObligations({ sourceRef: "agent:test:main" })).toHaveLength(1);
    } finally {
      store.close();
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it("selects a runnable wake beyond an older backoff prefix", () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "openclaw-durable-wake-backoff-"));
    const dbPath = path.join(dir, "openclaw.sqlite");
    const store = openDurableRuntimeSqliteStore({ path: dbPath });
    try {
      store.withTransaction(() => {
        for (let index = 0; index < 101; index += 1) {
          store.createWakeObligation({
            wakeId: `wake_backoff_${String(index).padStart(3, "0")}`,
            sourceOwner: "task_runs",
            sourceRef: `task-backoff-${index}`,
            targetKind: "agent_session",
            targetRef: "agent:test:main",
            reason: "operator_requested",
            occurrenceKey: `task-backoff:${index}`,
            now: index === 100 ? 1 : 0,
          });
        }
      });
      const { DatabaseSync } = requireNodeSqlite();
      const seedDb = new DatabaseSync(dbPath);
      try {
        seedDb
          .prepare(
            `UPDATE wake_obligations
                SET status = 'failed', attempt_count = 1, last_attempt_at = 100,
                    next_attempt_at = 10000, updated_at = 0
              WHERE wake_id < 'wake_backoff_100'`,
          )
          .run();
      } finally {
        seedDb.close();
      }

      expect(
        store.claimNextWakeObligation({
          workerId: "worker-ready",
          claimTtlMs: 100,
          retryBaseMs: 1_000,
          retryMaxMs: 1_000,
          now: 500,
        }),
      ).toMatchObject({ wake: { wakeId: "wake_backoff_100" } });
    } finally {
      store.close();
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it("fences wake dispatch so only the active lease can complete it", () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "openclaw-durable-wake-claim-"));
    const dbPath = path.join(dir, "openclaw.sqlite");
    const firstStore = openDurableRuntimeSqliteStore({ path: dbPath });
    const secondStore = openDurableRuntimeSqliteStore({ path: dbPath });
    try {
      const wake = firstStore.createWakeObligation({
        sourceOwner: "session_store",
        sourceRef: "agent:test:main",
        ownerKind: "agent_session",
        ownerRef: "agent:test:main",
        targetKind: "agent_session",
        targetRef: "agent:test:main",
        reason: "operator_requested",
        occurrenceKey: "session:agent:test:main:dispatch",
        now: 100,
      });
      const claim = firstStore.claimNextWakeObligation({
        workerId: "worker-a",
        claimTtlMs: 100,
        retryBaseMs: 10,
        retryMaxMs: 100,
        now: 100,
      });
      expect(claim).toBeDefined();
      expect(
        secondStore.claimNextWakeObligation({
          workerId: "worker-b",
          claimTtlMs: 100,
          retryBaseMs: 10,
          retryMaxMs: 100,
          now: 101,
        }),
      ).toBeUndefined();
      expect(
        secondStore.completeWakeObligationClaim({
          wakeId: wake.wakeId,
          deliveryAttemptId: claim!.deliveryAttempt.deliveryAttemptId,
          claimToken: "stale-token",
          attemptStatus: "handoff_accepted",
          wakeStatus: "handoff_accepted",
          now: 110,
        }),
      ).toBeUndefined();
      expect(
        firstStore.renewWakeObligationClaim({
          wakeId: wake.wakeId,
          deliveryAttemptId: claim!.deliveryAttempt.deliveryAttemptId,
          claimToken: claim!.claimToken,
          claimTtlMs: 100,
          now: 150,
        }),
      ).toBe(true);
      expect(
        firstStore.completeWakeObligationClaim({
          wakeId: wake.wakeId,
          deliveryAttemptId: claim!.deliveryAttempt.deliveryAttemptId,
          claimToken: claim!.claimToken,
          attemptStatus: "handoff_accepted",
          wakeStatus: "acked",
          evidence: { accepted: true },
          now: 225,
        }),
      ).toMatchObject({ status: "handoff_accepted", handoffAcceptedAt: 225 });
      expect(firstStore.getWakeObligation(wake.wakeId)).toMatchObject({
        status: "acked",
        ackedAt: 225,
        attemptCount: 1,
      });
      const attempts = firstStore.listDeliveryAttemptEvidence({ wakeId: wake.wakeId });
      expect(attempts).toEqual([expect.objectContaining({ status: "handoff_accepted" })]);
      expect(attempts[0]?.deliveryClaimedBy).toBeUndefined();
    } finally {
      secondStore.close();
      firstStore.close();
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it("closes active delivery claims when terminal wake control wins the race", () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "openclaw-durable-wake-control-race-"));
    const store = openDurableRuntimeSqliteStore({ path: path.join(dir, "openclaw.sqlite") });
    try {
      const acknowledgedWake = store.createWakeObligation({
        sourceOwner: "session_store",
        sourceRef: "agent:test:ack-race",
        targetKind: "agent_session",
        targetRef: "agent:test:ack-race",
        reason: "operator_requested",
        occurrenceKey: "wake-control-ack-race",
        now: 100,
      });
      const acknowledgedClaim = store.claimNextWakeObligation({
        workerId: "worker-ack-race",
        claimTtlMs: 1_000,
        retryBaseMs: 1,
        retryMaxMs: 1,
        now: 110,
      });
      expect(acknowledgedClaim).toBeDefined();

      expect(
        store.acknowledgeWakeObligation({
          wakeId: acknowledgedWake.wakeId,
          actorKind: "system_worker",
          actorRef: "session_attention_consumer",
          now: 120,
        }),
      ).toMatchObject({ status: "acked", ackedAt: 120 });
      const acknowledgedAttempt = store.getDeliveryAttemptEvidence(
        acknowledgedClaim!.deliveryAttempt.deliveryAttemptId,
      );
      expect(acknowledgedAttempt).toMatchObject({
        status: "handoff_accepted",
        handoffAcceptedAt: 120,
      });
      expect(acknowledgedAttempt).not.toHaveProperty("deliveryClaimedBy");
      expect(acknowledgedAttempt).not.toHaveProperty("deliveryClaimExpiresAt");
      expect(
        store.renewWakeObligationClaim({
          wakeId: acknowledgedWake.wakeId,
          deliveryAttemptId: acknowledgedClaim!.deliveryAttempt.deliveryAttemptId,
          claimToken: acknowledgedClaim!.claimToken,
          claimTtlMs: 1_000,
          now: 121,
        }),
      ).toBe(false);

      const supersededWake = store.createWakeObligation({
        sourceOwner: "session_store",
        sourceRef: "agent:test:supersede-race",
        targetKind: "agent_session",
        targetRef: "agent:test:supersede-race",
        reason: "operator_requested",
        occurrenceKey: "wake-control-supersede-race",
        now: 200,
      });
      const supersededClaim = store.claimNextWakeObligation({
        workerId: "worker-supersede-race",
        claimTtlMs: 1_000,
        retryBaseMs: 1,
        retryMaxMs: 1,
        now: 210,
      });
      expect(supersededClaim).toBeDefined();

      expect(
        store.supersedeWakeObligation({
          wakeId: supersededWake.wakeId,
          actorKind: "system_worker",
          actorRef: "session_delivery_recovery",
          reason: "session generation changed",
          now: 220,
        }),
      ).toMatchObject({ status: "superseded" });
      const supersededAttempt = store.getDeliveryAttemptEvidence(
        supersededClaim!.deliveryAttempt.deliveryAttemptId,
      );
      expect(supersededAttempt).toMatchObject({
        status: "superseded",
        error: "session generation changed",
      });
      expect(supersededAttempt).not.toHaveProperty("deliveryClaimedBy");
      expect(supersededAttempt).not.toHaveProperty("deliveryClaimExpiresAt");
    } finally {
      store.close();
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it("rolls back terminal wake control when active-claim finalization fails", () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "openclaw-durable-wake-control-rollback-"));
    const dbPath = path.join(dir, "openclaw.sqlite");
    const store = openDurableRuntimeSqliteStore({ path: dbPath });
    try {
      const wake = store.createWakeObligation({
        wakeId: "wake-control-atomic-rollback",
        sourceOwner: "session_store",
        sourceRef: "agent:test:control-rollback",
        targetKind: "agent_session",
        targetRef: "agent:test:control-rollback",
        reason: "operator_requested",
        occurrenceKey: "wake-control-atomic-rollback",
        now: 100,
      });
      const claim = store.claimNextWakeObligation({
        workerId: "worker-control-rollback",
        claimTtlMs: 1_000,
        retryBaseMs: 1,
        retryMaxMs: 1,
        now: 110,
      });
      const { DatabaseSync } = requireNodeSqlite();
      const faultDb = new DatabaseSync(dbPath);
      try {
        faultDb.exec(`
          CREATE TRIGGER abort_wake_control_claim_finalization
          BEFORE UPDATE OF status ON delivery_attempt_evidence
          WHEN OLD.wake_id = 'wake-control-atomic-rollback'
            AND NEW.status = 'handoff_accepted'
          BEGIN
            SELECT RAISE(ABORT, 'fault-injected control claim finalization');
          END;
        `);
      } finally {
        faultDb.close();
      }

      expect(() =>
        store.acknowledgeWakeObligation({
          wakeId: wake.wakeId,
          actorKind: "system_worker",
          actorRef: "session_attention_consumer",
          now: 120,
        }),
      ).toThrow(/fault-injected control claim finalization/);
      expect(store.getWakeObligation(wake.wakeId)).toMatchObject({ status: "pending" });
      expect(
        store.getDeliveryAttemptEvidence(claim!.deliveryAttempt.deliveryAttemptId),
      ).toMatchObject({
        status: "attempted",
        deliveryClaimedBy: claim!.claimToken,
      });
    } finally {
      store.close();
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it("rolls back attempt completion when the wake update aborts", () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "openclaw-durable-wake-rollback-"));
    const dbPath = path.join(dir, "openclaw.sqlite");
    const store = openDurableRuntimeSqliteStore({ path: dbPath });
    try {
      const wake = store.createWakeObligation({
        wakeId: "wake-atomic-rollback",
        sourceOwner: "session_store",
        sourceRef: "agent:test:rollback",
        targetKind: "agent_session",
        targetRef: "agent:test:rollback",
        reason: "operator_requested",
        occurrenceKey: "wake-atomic-rollback",
        now: 100,
      });
      const claim = store.claimNextWakeObligation({
        workerId: "worker-rollback",
        claimTtlMs: 1_000,
        retryBaseMs: 1,
        retryMaxMs: 1,
        now: 110,
      });
      const { DatabaseSync } = requireNodeSqlite();
      const faultDb = new DatabaseSync(dbPath);
      try {
        faultDb.exec(`
          CREATE TRIGGER abort_wake_atomic_rollback
          BEFORE UPDATE OF status ON wake_obligations
          WHEN OLD.wake_id = 'wake-atomic-rollback' AND NEW.status = 'acked'
          BEGIN
            SELECT RAISE(ABORT, 'fault-injected wake update');
          END;
        `);
      } finally {
        faultDb.close();
      }

      expect(() =>
        store.completeWakeObligationClaim({
          wakeId: wake.wakeId,
          deliveryAttemptId: claim!.deliveryAttempt.deliveryAttemptId,
          claimToken: claim!.claimToken,
          attemptStatus: "handoff_accepted",
          wakeStatus: "acked",
          evidence: { accepted: true },
          now: 120,
        }),
      ).toThrow(/fault-injected wake update/);
      expect(store.getWakeObligation(wake.wakeId)).toMatchObject({
        status: "pending",
        attemptCount: 1,
      });
      expect(
        store.getDeliveryAttemptEvidence(claim!.deliveryAttempt.deliveryAttemptId),
      ).toMatchObject({
        status: "attempted",
        deliveryClaimedBy: claim!.claimToken,
      });
    } finally {
      store.close();
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it("persists unresolved obligations across an explicit close and reopen", () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "openclaw-durable-wake-reopen-"));
    const dbPath = path.join(dir, "openclaw.sqlite");
    const firstStore = openDurableRuntimeSqliteStore({ path: dbPath });
    const wake = firstStore.createWakeObligation({
      sourceOwner: "session_store",
      sourceRef: "agent:test:reopen",
      targetKind: "agent_session",
      targetRef: "agent:test:reopen",
      reason: "restart_interrupted",
      occurrenceKey: "wake-explicit-reopen",
      now: 100,
    });
    firstStore.close();

    const reopenedStore = openDurableRuntimeSqliteStore({ path: dbPath });
    try {
      expect(reopenedStore.getWakeObligation(wake.wakeId)).toMatchObject({
        sourceOwner: "session_store",
        sourceRef: "agent:test:reopen",
        status: "pending",
      });
      expect(reopenedStore.listUnresolvedObligations()).toEqual(
        expect.arrayContaining([
          expect.objectContaining({ wakeId: wake.wakeId, kind: "pending_wake" }),
        ]),
      );
    } finally {
      reopenedStore.close();
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it("keeps suspended wakes visible as unresolved owner attention", () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "openclaw-durable-wake-suspended-"));
    const store = openDurableRuntimeSqliteStore({ path: path.join(dir, "openclaw.sqlite") });
    try {
      const wake = store.createWakeObligation({
        sourceOwner: "task_runs",
        sourceRef: "task-suspended",
        targetKind: "agent_session",
        targetRef: "agent:test:main",
        reason: "side_effect_uncertain",
        occurrenceKey: "task-suspended:1",
        now: 100,
      });
      expect(
        store.suspendWakeObligation({
          wakeId: wake.wakeId,
          failedReason: "owner_decision_required",
          now: 110,
        }),
      ).toMatchObject({ status: "suspended" });
      expect(store.listUnresolvedObligations()).toEqual(
        expect.arrayContaining([
          expect.objectContaining({
            wakeId: wake.wakeId,
            kind: "pending_wake",
            status: "suspended",
          }),
        ]),
      );
      expect(store.getStats().pendingWakes).toBe(1);
    } finally {
      store.close();
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it("finds overdue wakes whose no-silence diagnostic is missing or stale", () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "openclaw-durable-wake-diagnostic-"));
    const store = openDurableRuntimeSqliteStore({ path: path.join(dir, "openclaw.sqlite") });
    try {
      const missing = store.createWakeObligation({
        wakeId: "wake-diagnostic-missing",
        sourceOwner: "task_runs",
        sourceRef: "task-diagnostic-missing",
        targetKind: "agent_session",
        targetRef: "agent:test:main",
        reason: "operator_requested",
        occurrenceKey: "wake-diagnostic-missing",
        now: 100,
      });
      const current = store.createWakeObligation({
        wakeId: "wake-diagnostic-current",
        sourceOwner: "task_runs",
        sourceRef: "task-diagnostic-current",
        targetKind: "agent_session",
        targetRef: "agent:test:main",
        reason: "operator_requested",
        occurrenceKey: "wake-diagnostic-current",
        metadata: { diagnostics: { noSilenceSla: { overdue: true, slaMs: 500 } } },
        now: 110,
      });
      const stale = store.createWakeObligation({
        wakeId: "wake-diagnostic-stale",
        sourceOwner: "task_runs",
        sourceRef: "task-diagnostic-stale",
        targetKind: "agent_session",
        targetRef: "agent:test:main",
        reason: "operator_requested",
        occurrenceKey: "wake-diagnostic-stale",
        metadata: { diagnostics: { noSilenceSla: { overdue: true, slaMs: 250 } } },
        now: 120,
      });

      expect(
        store
          .listWakeObligationsNeedingNoSilenceDiagnostic({
            overdueBefore: 200,
            slaMs: 500,
          })
          .map((wake) => wake.wakeId),
      ).toEqual([missing.wakeId, stale.wakeId]);
      expect(store.getWakeObligation(current.wakeId)).toMatchObject({ status: "pending" });
    } finally {
      store.close();
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it("rejects stale wake revisions across store connections and preserves metadata", () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "openclaw-durable-control-revision-"));
    const dbPath = path.join(dir, "openclaw.sqlite");
    const firstStore = openDurableRuntimeSqliteStore({ path: dbPath });
    const secondStore = openDurableRuntimeSqliteStore({ path: dbPath });
    try {
      const wake = firstStore.createWakeObligation({
        sourceOwner: "task_runs",
        sourceRef: "task-1",
        targetKind: "agent_session",
        targetRef: "agent:test:main",
        reason: "child_terminal",
        occurrenceKey: "task-terminal:task-1",
        sourceRevision: "revision-1",
        metadata: { ownerField: "preserved" },
        now: 100,
      });
      expect(
        secondStore.updateWakeObligationProjection({
          wakeId: wake.wakeId,
          sourceRevision: "revision-2",
          metadata: { projectionField: "latest" },
          factsRef: "task_runs:task-1:revision-2",
          now: 105,
        }),
      ).toMatchObject({
        sourceRevision: "revision-2",
        metadata: { ownerField: "preserved", projectionField: "latest" },
      });
      const control = {
        wakeId: wake.wakeId,
        actorKind: "operator" as const,
        actorRef: "test",
        now: 110,
      };
      expect(
        firstStore.acknowledgeWakeObligation({
          ...control,
          expectedSourceRevision: "revision-1",
        }),
      ).toBeUndefined();
      expect(firstStore.getWakeObligation(wake.wakeId)?.status).toBe("pending");
      expect(
        firstStore.acknowledgeWakeObligation({
          ...control,
          expectedSourceRevision: "revision-2",
        }),
      ).toMatchObject({ status: "acked" });

      const suspendedWake = firstStore.createWakeObligation({
        sourceOwner: "task_runs",
        sourceRef: "task-projection-status",
        targetKind: "agent_session",
        targetRef: "agent:test:main",
        reason: "side_effect_uncertain",
        occurrenceKey: "task-projection-status:1",
        now: 115,
      });
      firstStore.suspendWakeObligation({
        wakeId: suspendedWake.wakeId,
        failedReason: "owner_decision_required",
        now: 116,
      });
      expect(
        secondStore.updateWakeObligationProjection({
          wakeId: suspendedWake.wakeId,
          metadata: { projectionField: "updated" },
          now: 117,
        }),
      ).toMatchObject({ status: "suspended", metadata: { projectionField: "updated" } });

      const fact = firstStore.recordUncertaintyFact({
        sourceOwner: "task_runs",
        sourceRef: "task-1",
        kind: "requires_owner_decision",
        now: 120,
      });
      expect(
        firstStore.resolveUncertaintyFact({
          factId: fact.factId,
          status: "resolved",
          resolutionKind: "owner_inspected",
          expectedUpdatedAt: 119,
          now: 130,
        }),
      ).toBeUndefined();
      expect(
        firstStore.resolveUncertaintyFact({
          factId: fact.factId,
          status: "resolved",
          resolutionKind: "owner_inspected",
          expectedUpdatedAt: 120,
          now: 130,
        }),
      ).toMatchObject({ status: "resolved", updatedAt: 130 });
    } finally {
      secondStore.close();
      firstStore.close();
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it("scopes uncertainty idempotency to the source and rejects changed replays", () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "openclaw-durable-uncertainty-scope-"));
    const store = openDurableRuntimeSqliteStore({ path: path.join(dir, "openclaw.sqlite") });
    const firstInput = {
      factId: "fact-source-a",
      sourceOwner: "test-owner",
      sourceRef: "test-source:a",
      kind: "requires_owner_decision",
      dedupeKey: "shared-dedupe-key",
      facts: { outcome: "unknown" },
      metadata: { boundary: "test" },
      now: 100,
    } as const;
    try {
      const first = store.recordUncertaintyFact(firstInput);
      expect(store.recordUncertaintyFact({ ...firstInput, now: 110 }).factId).toBe(first.factId);
      expect(() =>
        store.recordUncertaintyFact({
          ...firstInput,
          facts: { outcome: "changed" },
          now: 120,
        }),
      ).toThrow(/uncertainty replay conflict/);

      const second = store.recordUncertaintyFact({
        ...firstInput,
        factId: "fact-source-b",
        sourceRef: "test-source:b",
        now: 130,
      });
      expect(second.factId).not.toBe(first.factId);
      expect(
        store.listUncertaintyFacts({ sourceOwner: "test-owner", sourceRef: "test-source:a" }),
      ).toMatchObject([{ factId: first.factId }]);
      expect(
        store.listUncertaintyFacts({ sourceOwner: "test-owner", sourceRef: "test-source:b" }),
      ).toMatchObject([{ factId: second.factId }]);
      expect(() =>
        store.recordUncertaintyFact({
          ...firstInput,
          sourceRef: "test-source:b",
          dedupeKey: "different-key",
          now: 140,
        }),
      ).toThrow(/uncertainty replay conflict/);
    } finally {
      store.close();
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it("bounds durable JSON payloads and wake control history", () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "openclaw-durable-json-bounds-"));
    const store = openDurableRuntimeSqliteStore({ path: path.join(dir, "openclaw.sqlite") });
    try {
      expect(() =>
        store.createRun({
          operationKind: "test.oversized",
          rootOperationReason: "test-root",
          metadata: { payload: "x".repeat(64 * 1024) },
        }),
      ).toThrow(/exceeds 65536 bytes/);

      const wake = store.createWakeObligation({
        sourceOwner: "task_runs",
        sourceRef: "task-json-bounds",
        targetKind: "agent_session",
        targetRef: "agent:test:main",
        reason: "operator_requested",
        occurrenceKey: "task-json-bounds:1",
        metadata: { ownerField: "preserved" },
        now: 100,
      });
      expect(() =>
        store.updateWakeObligationProjection({
          wakeId: wake.wakeId,
          metadata: { payload: "x".repeat(64 * 1024) },
          now: 101,
        }),
      ).toThrow(/exceeds 65536 bytes/);
      expect(store.getWakeObligation(wake.wakeId)).toMatchObject({
        metadata: { ownerField: "preserved" },
      });

      for (let index = 0; index < 40; index += 1) {
        expect(
          store.markWakeObligationDecisionRequired({
            wakeId: wake.wakeId,
            actorKind: "operator",
            actorRef: "test-operator",
            decisionKind: "inspected",
            idempotencyKey: `inspection:${index}`,
            now: 200 + index,
          }),
        ).toBeDefined();
      }
      const metadata = store.getWakeObligation(wake.wakeId)?.metadata;
      const controls = metadata?.durableWakeControls as
        | Array<{ idempotencyKey?: string }>
        | undefined;
      expect(controls).toHaveLength(32);
      expect(controls?.[0]?.idempotencyKey).toBe("inspection:8");
      expect(controls?.at(-1)?.idempotencyKey).toBe("inspection:39");
    } finally {
      store.close();
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it("fails closed when persisted durable evidence JSON is malformed", () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "openclaw-durable-json-corruption-"));
    const dbPath = path.join(dir, "openclaw.sqlite");
    const store = openDurableRuntimeSqliteStore({ path: dbPath });
    try {
      const run = store.createRun({
        operationKind: "test.corruption",
        rootOperationReason: "test-root",
        now: 100,
      });
      const event = store.appendEvent({
        runtimeRunId: run.runtimeRunId,
        eventType: "test.evidence",
        payload: { ok: true },
      });
      const fact = store.recordUncertaintyFact({
        sourceOwner: "test-owner",
        sourceRef: "test-source:corrupt-fact",
        kind: "requires_owner_decision",
        facts: { ok: true },
        now: 110,
      });
      const wake = store.createWakeObligation({
        sourceOwner: "test-owner",
        sourceRef: "test-source:corrupt-delivery",
        targetKind: "agent_session",
        targetRef: "agent:test:main",
        reason: "operator_requested",
        occurrenceKey: "corrupt-delivery:1",
        now: 120,
      });
      const claim = store.claimNextWakeObligation({
        workerId: "test-worker",
        claimTtlMs: 1_000,
        retryBaseMs: 1,
        retryMaxMs: 1,
        now: 130,
      });
      expect(claim?.wake.wakeId).toBe(wake.wakeId);

      const { DatabaseSync } = requireNodeSqlite();
      const corruptDb = new DatabaseSync(dbPath);
      try {
        corruptDb
          .prepare("UPDATE durable_event_evidence SET payload_json = ? WHERE event_id = ?")
          .run("{", event.eventId);
        corruptDb
          .prepare("UPDATE uncertainty_facts SET facts_json = ? WHERE fact_id = ?")
          .run("[]", fact.factId);
        corruptDb
          .prepare(
            "UPDATE delivery_attempt_evidence SET evidence_json = ? WHERE delivery_attempt_id = ?",
          )
          .run("not-json", claim!.deliveryAttempt.deliveryAttemptId);
      } finally {
        corruptDb.close();
      }

      expect(() => store.getTimeline(run.runtimeRunId)).toThrow(
        /Durable event payload is malformed/,
      );
      expect(() =>
        store.listUncertaintyFacts({
          sourceOwner: "test-owner",
          sourceRef: "test-source:corrupt-fact",
        }),
      ).toThrow(/Durable uncertainty facts is malformed/);
      expect(() => store.getWakeObligationInspection(wake.wakeId)).toThrow(
        /Durable delivery evidence is malformed/,
      );
    } finally {
      store.close();
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it("keeps owner projections outside the durable storage boundary", () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "openclaw-durable-owners-"));
    const dbPath = path.join(dir, "openclaw.sqlite");
    const store = openDurableRuntimeSqliteStore({ path: dbPath });
    try {
      const run = store.createRun({
        operationKind: "test.runtime",
        rootOperationReason: "test-root",
        status: "queued",
        recoveryState: "runnable",
        now: 100,
      });
      const step = store.createStep({
        runtimeRunId: run.runtimeRunId,
        stepType: "tool",
        status: "queued",
        recoveryState: "runnable",
        now: 105,
      });
      expect(
        store.claimNextRunnableStep({
          workerId: "attempt-1",
          claimTtlMs: 10,
          now: 110,
        }),
      ).toMatchObject({
        step: { runtimeRunId: run.runtimeRunId, stepId: step.stepId },
        claimToken: expect.stringMatching(/^claim_/),
      });

      const { DatabaseSync } = requireNodeSqlite();
      const db = new DatabaseSync(dbPath);
      try {
        db.prepare(
          `INSERT INTO subagent_runs (
             run_id, child_session_key, requester_session_key, requester_display_key,
             task, cleanup, created_at, pending_final_delivery,
             pending_final_delivery_created_at, pending_final_delivery_attempt_count,
             pending_final_delivery_last_error
           ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        ).run(
          "subagent-1",
          "agent:test:subagent:1",
          "agent:test:main",
          "agent:test:main",
          "test task",
          "keep",
          100,
          1,
          130,
          2,
          "requester unavailable",
        );
        db.prepare(
          `INSERT INTO delivery_queue_entries (
             queue_name, id, status, session_key, channel, target, retry_count,
             last_error, recovery_state, entry_json, enqueued_at, updated_at
           ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        ).run(
          "outbound",
          "delivery-1",
          "failed",
          "agent:test:main",
          "test-channel",
          "channel:1",
          3,
          "send failed",
          "needs_retry",
          "{}",
          140,
          150,
        );
      } finally {
        db.close();
      }

      const unresolved = store.listUnresolvedObligations({ now: 200 });
      expect(unresolved).toEqual([
        expect.objectContaining({
          sourceOwner: "state_leases",
          sourceRef: `durable_execution_step:${JSON.stringify([run.runtimeRunId, step.stepId])}`,
          kind: "expired_state_lease",
          subjectRef: expect.stringMatching(/^claim_/),
        }),
      ]);
      expect(unresolved.some((item) => item.sourceOwner === "subagent_runs")).toBe(false);
      expect(unresolved.some((item) => item.sourceOwner === "delivery_queue_entries")).toBe(false);
    } finally {
      store.close();
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });
});
