import {
  fs,
  os,
  path,
  describe,
  expect,
  it,
  requireNodeSqlite,
  resolveSqliteDatabaseFilePaths,
  OPENCLAW_STATE_SCHEMA_VERSION,
  closeOpenClawStateDatabaseForPathForTest,
  openOpenClawStateDatabase,
  resolveOpenClawStateSqlitePath,
  openDurableRuntimeSqliteStore,
  DURABLE_TABLES,
} from "./sqlite-store.test-harness.js";

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
      closeOpenClawStateDatabaseForPathForTest({ path: dbPath });
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
      closeOpenClawStateDatabaseForPathForTest({ path: dbPath });
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
      closeOpenClawStateDatabaseForPathForTest({ path: dbPath });
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
      ).toEqual({
        meta_key: "primary",
        role: "global",
        schema_version: OPENCLAW_STATE_SCHEMA_VERSION,
      });
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
      expect(store.listOpenRuns({ operationKind: "test.runtime" })).toMatchObject({
        complete: true,
        runs: [
          {
            runtimeRunId: first.runtimeRunId,
            operationKind: "test.runtime",
            status: "received",
            workUnitId: "wu:test:card-1",
            reportRouteRef: "route:test:main",
          },
        ],
      });
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
      expect(store.listOpenRuns({ operationKind: "test.runtime" })).toEqual({
        complete: true,
        runs: [],
      });
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
      const atomicallyFiredTimer = store.createTimer({
        runtimeRunId: parent.runtimeRunId,
        stepId: step.stepId,
        timerType: "sleep",
        dueAt: 205,
        now: 202,
      });
      expect(
        store.fireDueTimer({ timerId: atomicallyFiredTimer.timerId, now: 204 }),
      ).toBeUndefined();
      expect(store.fireDueTimer({ timerId: atomicallyFiredTimer.timerId, now: 205 })).toMatchObject(
        {
          status: "fired",
          firedAt: 205,
        },
      );
      expect(
        store.fireDueTimer({ timerId: atomicallyFiredTimer.timerId, now: 206 }),
      ).toBeUndefined();

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
      const atomicallyConsumedSignal = store.createSignal({
        runtimeRunId: parent.runtimeRunId,
        stepId: step.stepId,
        signalType: "resume",
        idempotencyKey: "signal-atomic-consume",
        now: 221,
      });
      expect(
        store.consumePendingSignal({ signalId: atomicallyConsumedSignal.signalId, now: 222 }),
      ).toMatchObject({ signalId: atomicallyConsumedSignal.signalId, consumedAt: 222 });
      expect(
        store.consumePendingSignal({ signalId: atomicallyConsumedSignal.signalId, now: 223 }),
      ).toBeUndefined();
      expect(store.listSignals(parent.runtimeRunId)).toHaveLength(2);
      expect(store.listPendingSignals()).toEqual([]);
      expect(store.getStats()).toMatchObject({ runs: 2, steps: 2 });
    } finally {
      store.close();
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it("paginates open runs with an opaque stable cursor", () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "openclaw-durable-open-run-page-"));
    const store = openDurableRuntimeSqliteStore({ path: path.join(dir, "openclaw.sqlite") });
    try {
      store.withTransaction(() => {
        for (let index = 0; index < 503; index += 1) {
          store.createRun({
            runtimeRunId: `run_page_${String(index).padStart(4, "0")}`,
            operationKind: "openclaw.agent.turn",
            rootOperationReason: "bounded open-run pagination test",
            status: "running",
            recoveryState: "running",
            now: Math.floor(index / 2),
          });
        }
        store.createRun({
          runtimeRunId: "run_other_operation",
          operationKind: "openclaw.chat.send",
          rootOperationReason: "bounded open-run pagination test",
          status: "running",
          recoveryState: "running",
          now: 100,
        });
      });

      const runIds: string[] = [];
      let cursor: string | undefined;
      for (;;) {
        const page = store.listOpenRuns({
          operationKind: "openclaw.agent.turn",
          updatedAtOrBefore: 1_000,
          cursor,
          limit: 100,
        });
        runIds.push(...page.runs.map((run) => run.runtimeRunId));
        if (page.complete) {
          expect(page.nextCursor).toBeUndefined();
          break;
        }
        expect(page.nextCursor).toBeDefined();
        cursor = page.nextCursor;
      }

      expect(runIds).toHaveLength(503);
      expect(new Set(runIds).size).toBe(503);
      expect(runIds).not.toContain("run_other_operation");
      expect(() => store.listOpenRuns({ limit: 501 })).toThrow(/limit must not exceed 500/);
      const firstPage = store.listOpenRuns({
        operationKind: "openclaw.agent.turn",
        limit: 1,
      });
      expect(() =>
        store.listOpenRuns({
          operationKind: "openclaw.chat.send",
          cursor: firstPage.nextCursor,
          limit: 1,
        }),
      ).toThrow(/cursor does not match operationKind/);
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
});
