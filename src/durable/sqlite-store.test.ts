import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { describe, expect, it } from "vitest";
import { requireNodeSqlite } from "../infra/node-sqlite.js";
import { resolveSqliteDatabaseFilePaths } from "../infra/sqlite-files.js";
import { resolveDurableRuntimeSqlitePath } from "./config.js";
import {
  DURABLE_RUNTIME_SQLITE_SCHEMA_VERSION,
  openDurableRuntimeSqliteStore,
} from "./sqlite-store.js";

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
  "wake_obligations",
] as const;

describe("durable runtime sqlite store", () => {
  it("records the supported durable schema version", () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "openclaw-durable-store-"));
    const store = openDurableRuntimeSqliteStore({
      path: path.join(dir, "openclaw.sqlite"),
    });
    try {
      expect(store.getStats()).toMatchObject({
        schemaVersion: DURABLE_RUNTIME_SQLITE_SCHEMA_VERSION,
      });
    } finally {
      store.close();
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it("rejects durable stores from a newer schema version", () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "openclaw-durable-store-"));
    const dbPath = path.join(dir, "openclaw.sqlite");
    const { DatabaseSync } = requireNodeSqlite();
    const db = new DatabaseSync(dbPath);
    try {
      db.exec(`PRAGMA user_version = ${Number(DURABLE_RUNTIME_SQLITE_SCHEMA_VERSION) + 1};`);
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
          .all();
        expect(runtimeTables).toEqual([]);
      } finally {
        verifyDb.close();
      }
    } finally {
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it("upgrades a pre-durable shared state database without touching existing rows", () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "openclaw-durable-upgrade-"));
    const dbPath = path.join(dir, "openclaw.sqlite");
    const { DatabaseSync } = requireNodeSqlite();
    const db = new DatabaseSync(dbPath);
    try {
      db.exec(`
        CREATE TABLE schema_meta (
          meta_key TEXT NOT NULL PRIMARY KEY,
          role TEXT NOT NULL,
          schema_version INTEGER NOT NULL,
          agent_id TEXT,
          app_version TEXT,
          created_at INTEGER NOT NULL,
          updated_at INTEGER NOT NULL
        );
        CREATE TABLE cron_jobs (
          store_key TEXT NOT NULL PRIMARY KEY,
          job_id TEXT NOT NULL,
          definition_json TEXT NOT NULL,
          enabled INTEGER NOT NULL
        );
      `);
      db.prepare(
        "INSERT INTO schema_meta (meta_key, role, schema_version, created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
      ).run("primary", "global", 1, 100, 100);
      db.prepare(
        "INSERT INTO cron_jobs (store_key, job_id, definition_json, enabled) VALUES (?, ?, ?, ?)",
      ).run(
        "legacy-job",
        "legacy-job",
        JSON.stringify({ schedule: "*/5 * * * *", task: "legacy" }),
        1,
      );
    } finally {
      db.close();
    }

    const store = openDurableRuntimeSqliteStore({ path: dbPath });
    try {
      expect(store.getStats()).toMatchObject({
        schemaVersion: DURABLE_RUNTIME_SQLITE_SCHEMA_VERSION,
      });
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
      expect(verifyDb.prepare("SELECT job_id, enabled FROM cron_jobs").all()).toEqual([
        { job_id: "legacy-job", enabled: 1 },
      ]);
      expect(verifyDb.prepare("PRAGMA user_version").get()).toEqual({
        user_version: DURABLE_RUNTIME_SQLITE_SCHEMA_VERSION,
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

  it("opens partial early durable schemas before creating indexes on added columns", () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "openclaw-durable-upgrade-"));
    const dbPath = path.join(dir, "openclaw.sqlite");
    const { DatabaseSync } = requireNodeSqlite();
    const db = new DatabaseSync(dbPath);
    try {
      db.exec(`
        CREATE TABLE durable_execution_records (
          runtime_run_id TEXT NOT NULL PRIMARY KEY,
          operation_kind TEXT NOT NULL,
          operation_version TEXT NOT NULL DEFAULT '1',
          idempotency_key TEXT,
          request_hash TEXT,
          status TEXT NOT NULL,
          source_owner TEXT,
          source_ref TEXT,
          input_ref TEXT,
          created_at INTEGER NOT NULL,
          updated_at INTEGER NOT NULL,
          completed_at INTEGER,
          recovery_state TEXT NOT NULL DEFAULT 'runnable',
          checkpoint_ref TEXT,
          metadata_json TEXT
        );
        INSERT INTO durable_execution_records (
          runtime_run_id, operation_kind, operation_version, idempotency_key, request_hash,
          status, source_owner, source_ref, input_ref, created_at, updated_at, completed_at,
          recovery_state, checkpoint_ref, metadata_json
        ) VALUES (
          'run_legacy_partial', 'openclaw.agent.turn', '1', 'legacy-partial', NULL,
          'running', 'agent', 'agent:legacy:main', NULL, 10, 10, NULL,
          'running', NULL, '{"legacy":true}'
        );
      `);
    } finally {
      db.close();
    }

    const store = openDurableRuntimeSqliteStore({ path: dbPath });
    try {
      expect(store.getRun("run_legacy_partial")).toMatchObject({
        runtimeRunId: "run_legacy_partial",
        operationKind: "openclaw.agent.turn",
        status: "running",
        recoveryState: "running",
        sourceOwner: "agent",
        sourceRef: "agent:legacy:main",
        metadata: { legacy: true },
      });
      expect(
        store.updateRun({
          runtimeRunId: "run_legacy_partial",
          workUnitId: "wu:legacy",
          reportRouteId: "discord:legacy-main",
          now: 20,
        }),
      ).toMatchObject({
        runtimeRunId: "run_legacy_partial",
        workUnitId: "wu:legacy",
        reportRouteId: "discord:legacy-main",
      });
      expect(store.getStats()).toMatchObject({ runs: 1, schemaVersion: 1 });
    } finally {
      store.close();
    }

    const verifyDb = new DatabaseSync(dbPath);
    try {
      const columns = verifyDb
        .prepare("PRAGMA table_info(durable_execution_records)")
        .all() as Array<{
        name: string;
      }>;
      expect(columns.map((column) => column.name)).toEqual(
        expect.arrayContaining(["work_unit_id", "report_route_id", "heartbeat_at"]),
      );
      const indexes = verifyDb
        .prepare(
          `SELECT name FROM sqlite_master
             WHERE type = 'index'
               AND name IN ('idx_durable_execution_records_work_unit', 'idx_durable_execution_records_report_route')
             ORDER BY name`,
        )
        .all();
      expect(indexes).toEqual([
        { name: "idx_durable_execution_records_report_route" },
        { name: "idx_durable_execution_records_work_unit" },
      ]);
    } finally {
      verifyDb.close();
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it("uses shared state private-mode hardening when it creates the state database", () => {
    const stateDir = fs.mkdtempSync(path.join(os.tmpdir(), "openclaw-durable-state-mode-"));
    fs.chmodSync(stateDir, 0o755);
    const env = { OPENCLAW_STATE_DIR: stateDir };
    const pathname = resolveDurableRuntimeSqlitePath(env);
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
        reportRouteId: "discord:bo-main",
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

      const started = store.appendEvent({
        runtimeRunId: first.runtimeRunId,
        eventType: "runtime.started",
        payload: { ok: true },
      });
      const completed = store.appendEvent({
        runtimeRunId: first.runtimeRunId,
        eventType: "runtime.completed",
      });
      expect(store.listOpenRuns({ operationKind: "test.runtime" })).toMatchObject([
        {
          runtimeRunId: first.runtimeRunId,
          operationKind: "test.runtime",
          status: "received",
          workUnitId: "wu:test:card-1",
          reportRouteId: "discord:bo-main",
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
      expect(terminal).toMatchObject({
        runtimeRunId: first.runtimeRunId,
        status: "succeeded",
        recoveryState: "terminal",
        workUnitId: "wu:test:card-1-updated",
        reportRouteId: "discord:bo-main",
        completedAt: 300,
      });
      expect(store.getTimeline(first.runtimeRunId).map((event) => event.eventType)).toEqual([
        "runtime.started",
        "runtime.completed",
      ]);
      expect(store.listOpenRuns({ operationKind: "test.runtime" })).toEqual([]);
      expect(store.getStats()).toMatchObject({ runs: 1, events: 2, steps: 0, openRuns: 0 });
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
        runtimeRunId: parent.runtimeRunId,
        stepId: executableStep.stepId,
        status: "queued",
        recoveryState: "claimed",
        claimedBy: expect.stringMatching(/^claim_/),
        claimExpiresAt: 1_176,
      });
      expect(
        store.releaseStepClaim({
          runtimeRunId: parent.runtimeRunId,
          stepId: executableStep.stepId,
          workerId: claimedStep!.claimedBy!,
          now: 177,
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
        signalType: "human_input",
        idempotencyKey: "signal-1",
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

      const firstClaim = store.claimNextRunnableStep({
        operationKind: "test.runtime",
        workerId: "worker-1",
        claimTtlMs: 10,
        now: 140,
      });
      expect(firstClaim).toMatchObject({
        stepId: step.stepId,
        claimedBy: expect.stringMatching(/^claim_/),
        claimExpiresAt: 150,
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
        stepId: step.stepId,
        claimedBy: expect.stringMatching(/^claim_/),
        claimExpiresAt: 161,
      });
      expect(secondClaim?.claimedBy).not.toBe(firstClaim?.claimedBy);
      expect(
        store.releaseStepClaim({
          runtimeRunId: run.runtimeRunId,
          stepId: step.stepId,
          workerId: firstClaim!.claimedBy!,
          now: 152,
        }),
      ).toBeUndefined();
      expect(
        store.updateStep({
          runtimeRunId: run.runtimeRunId,
          stepId: step.stepId,
          expectedClaimedBy: firstClaim!.claimedBy!,
          status: "succeeded",
          recoveryState: "terminal",
          now: 155,
        }),
      ).toBeUndefined();
      const completedStep = store.updateStep({
        runtimeRunId: run.runtimeRunId,
        stepId: step.stepId,
        expectedClaimedBy: secondClaim!.claimedBy!,
        status: "succeeded",
        recoveryState: "terminal",
        claimedBy: null,
        claimExpiresAt: null,
        now: 160,
      });
      expect(completedStep).toMatchObject({
        stepId: step.stepId,
        status: "succeeded",
        recoveryState: "terminal",
      });
      expect(completedStep?.claimedBy).toBeUndefined();
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
        removedEvents: 0,
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
          runtimeRunId: terminal.runtimeRunId,
          eventType: `terminal.${index}`,
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
        removedEvents: 3,
      });
      expect(store.getTimeline(terminal.runtimeRunId).map((event) => event.eventType)).toEqual([
        "terminal.4",
        "terminal.5",
        "runtime.history.compacted",
      ]);
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
        dedupeKey: "session:agent:test:main:dispatch",
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
          attemptStatus: "delivered",
          wakeStatus: "delivered",
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
          attemptStatus: "delivered",
          wakeStatus: "delivered",
          evidence: { accepted: true },
          now: 225,
        }),
      ).toMatchObject({ status: "delivered", deliveredAt: 225 });
      expect(firstStore.getWakeObligation(wake.wakeId)).toMatchObject({
        status: "delivered",
        attemptCount: 1,
      });
      const attempts = firstStore.listDeliveryAttemptEvidence({ wakeId: wake.wakeId });
      expect(attempts).toEqual([expect.objectContaining({ status: "delivered" })]);
      expect(attempts[0]?.deliveryClaimedBy).toBeUndefined();
    } finally {
      secondStore.close();
      firstStore.close();
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it("rejects stale wake and uncertainty control revisions", () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "openclaw-durable-control-revision-"));
    const store = openDurableRuntimeSqliteStore({ path: path.join(dir, "openclaw.sqlite") });
    try {
      const wake = store.createWakeObligation({
        sourceOwner: "task_runs",
        sourceRef: "task-1",
        targetKind: "agent_session",
        targetRef: "agent:test:main",
        reason: "child_terminal",
        dedupeKey: "task-terminal:task-1",
        metadata: { sourceRevision: "revision-2" },
        now: 100,
      });
      const control = {
        wakeId: wake.wakeId,
        actorKind: "operator" as const,
        actorRef: "test",
        now: 110,
      };
      expect(
        store.acknowledgeWakeObligation({
          ...control,
          expectedSourceRevision: "revision-1",
        }),
      ).toBeUndefined();
      expect(store.getWakeObligation(wake.wakeId)?.status).toBe("pending");
      expect(
        store.acknowledgeWakeObligation({
          ...control,
          expectedSourceRevision: "revision-2",
        }),
      ).toMatchObject({ status: "acked" });

      const fact = store.recordUncertaintyFact({
        sourceOwner: "task_runs",
        sourceRef: "task-1",
        kind: "requires_owner_decision",
        now: 120,
      });
      expect(
        store.resolveUncertaintyFact({
          factId: fact.factId,
          status: "resolved",
          resolutionKind: "owner_inspected",
          expectedUpdatedAt: 119,
          now: 130,
        }),
      ).toBeUndefined();
      expect(
        store.resolveUncertaintyFact({
          factId: fact.factId,
          status: "resolved",
          resolutionKind: "owner_inspected",
          expectedUpdatedAt: 120,
          now: 130,
        }),
      ).toMatchObject({ status: "resolved", updatedAt: 130 });
    } finally {
      store.close();
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it("projects unresolved obligations from canonical upstream owners", () => {
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
      ).toMatchObject({ runtimeRunId: run.runtimeRunId, stepId: step.stepId });

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
          "discord",
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

      expect(store.listUnresolvedObligations({ now: 200 })).toEqual(
        expect.arrayContaining([
          expect.objectContaining({
            sourceOwner: "subagent_runs",
            sourceRef: "subagent-1",
            kind: "pending_subagent_delivery",
            subjectRef: "agent:test:main",
          }),
          expect.objectContaining({
            sourceOwner: "delivery_queue_entries",
            sourceRef: "outbound:delivery-1",
            kind: "pending_delivery_queue",
            status: "failed",
          }),
          expect.objectContaining({
            sourceOwner: "state_leases",
            sourceRef: `durable_execution_step:${run.runtimeRunId}:${step.stepId}`,
            kind: "expired_state_lease",
            subjectRef: expect.stringMatching(/^claim_/),
          }),
        ]),
      );
    } finally {
      store.close();
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });
});
