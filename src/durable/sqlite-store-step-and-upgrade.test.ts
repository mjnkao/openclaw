import {
  fs,
  os,
  path,
  describe,
  expect,
  it,
  requireNodeSqlite,
  openDurableRuntimeSqliteStore,
  DURABLE_TABLES,
  requireAppliedWakeResult,
} from "./sqlite-store.test-harness.js";

describe("durable runtime sqlite store", () => {
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

      expect(store.listOpenRuns()).toEqual({ complete: true, runs: [] });
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
      expect(claim?.wake.deliveryRevision).toBe(1);
      expect(claim?.deliveryAttempt.claimedWakeDeliveryRevision).toBe(1);
      expect(claim?.deliveryAttempt).not.toHaveProperty("metadata");
      firstStore.completeWakeObligationClaim({
        wakeId: claim!.wake.wakeId,
        deliveryAttemptId: claim!.deliveryAttempt.deliveryAttemptId,
        claimToken: claim!.claimToken,
        attemptStatus: "handoff_accepted",
        wakeStatus: "handoff_accepted",
        now: 120,
      });

      const coalesced = requireAppliedWakeResult(
        secondStore.reconcileWakeObligation({
          candidate: candidate("child-progress:2", "revision-2", 200),
          policy: { mode: "while_unresolved", recurrence: "after_terminal" },
        }),
      );
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
      expect(coalesced.wake.deliveryRevision).not.toBe(claim!.wake.deliveryRevision);
      expect(firstStore.listWakeObligations({ sourceRef: "child-run-1" })).toHaveLength(1);
      expect(firstStore.listDeliveryAttemptEvidence({ wakeId: first.wake.wakeId })).toHaveLength(1);

      expect(
        firstStore.reconcileWakeObligation({
          candidate: candidate("child-progress:2", "revision-2", 210),
          policy: { mode: "while_unresolved", recurrence: "after_terminal" },
        }),
      ).toMatchObject({ disposition: "exact_match", wake: { wakeId: first.wake.wakeId } });

      expect(
        firstStore.acknowledgeWakeObligation({
          wakeId: first.wake.wakeId,
          actorKind: "system_worker",
          actorRef: "test-consumer",
          now: 220,
        }),
      ).toBeUndefined();
      expect(
        firstStore.acknowledgeWakeObligation({
          wakeId: first.wake.wakeId,
          actorKind: "system_worker",
          actorRef: "test-consumer",
          expectedDeliveryRevision: claim!.wake.deliveryRevision,
          now: 220,
        }),
      ).toBeUndefined();
      expect(
        firstStore.acknowledgeWakeObligation({
          wakeId: first.wake.wakeId,
          actorKind: "system_worker",
          actorRef: "test-consumer",
          expectedDeliveryRevision: coalesced.wake.deliveryRevision,
          now: 220,
        }),
      ).toMatchObject({ status: "acked" });
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

  it("fences an active delivery claim when coalescing advances the source revision", () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "openclaw-durable-wake-revision-fence-"));
    const dbPath = path.join(dir, "openclaw.sqlite");
    const firstStore = openDurableRuntimeSqliteStore({ path: dbPath });
    const secondStore = openDurableRuntimeSqliteStore({ path: dbPath });
    const candidate = (occurrenceKey: string, sourceRevision: string, now: number) => ({
      sourceOwner: "subagent_runs",
      sourceRef: "child-run-revision-fence",
      targetKind: "agent_session" as const,
      targetRef: "agent:test:main",
      ownerKind: "agent_session" as const,
      ownerRef: "agent:test:main",
      reportRouteRef: "agent:test:main",
      targetResolutionStatus: "resolved" as const,
      reason: "child_overdue" as const,
      factsRef: `subagent_runs:child-run-revision-fence:${sourceRevision}`,
      occurrenceKey,
      sourceRevision,
      now,
    });
    try {
      const first = requireAppliedWakeResult(
        firstStore.reconcileWakeObligation({
          candidate: candidate("child-progress:1", "revision-1", 100),
          policy: { mode: "while_unresolved", recurrence: "after_terminal" },
        }),
      );
      const claim = firstStore.claimNextWakeObligation({
        workerId: "worker-revision-1",
        claimTtlMs: 1_000,
        retryBaseMs: 1,
        retryMaxMs: 1,
        now: 110,
      });
      expect(claim?.wake.wakeId).toBe(first.wake.wakeId);

      expect(
        secondStore.reconcileWakeObligation({
          candidate: candidate("child-progress:2", "revision-2", 120),
          policy: { mode: "while_unresolved", recurrence: "after_terminal" },
        }),
      ).toMatchObject({
        disposition: "coalesced",
        wake: {
          wakeId: first.wake.wakeId,
          status: "pending",
          metadata: { sourceRevision: "revision-2" },
        },
      });

      expect(
        firstStore.renewWakeObligationClaim({
          wakeId: first.wake.wakeId,
          deliveryAttemptId: claim!.deliveryAttempt.deliveryAttemptId,
          claimToken: claim!.claimToken,
          claimTtlMs: 1_000,
          now: 130,
        }),
      ).toBe(false);
      expect(
        firstStore.completeWakeObligationClaim({
          wakeId: first.wake.wakeId,
          deliveryAttemptId: claim!.deliveryAttempt.deliveryAttemptId,
          claimToken: claim!.claimToken,
          attemptStatus: "handoff_accepted",
          wakeStatus: "acked",
          now: 140,
        }),
      ).toBeUndefined();
      expect(firstStore.getWakeObligation(first.wake.wakeId)).toMatchObject({
        status: "pending",
        metadata: { sourceRevision: "revision-2" },
      });
      expect(
        firstStore.getDeliveryAttemptEvidence(claim!.deliveryAttempt.deliveryAttemptId),
      ).toMatchObject({ status: "attempted" });

      const withoutRevision = (occurrenceKey: string, now: number) => ({
        ...candidate(occurrenceKey, "unused", now),
        sourceRef: "child-run-occurrence-fence",
        factsRef: `subagent_runs:child-run-occurrence-fence:${occurrenceKey}`,
        sourceRevision: undefined,
      });
      const occurrenceFenced = requireAppliedWakeResult(
        firstStore.reconcileWakeObligation({
          candidate: withoutRevision("child-progress:without-revision:1", 200),
          policy: { mode: "while_unresolved", recurrence: "after_terminal" },
        }),
      );
      const occurrenceClaim = firstStore.claimNextWakeObligation({
        workerId: "worker-occurrence-1",
        claimTtlMs: 1_000,
        retryBaseMs: 1,
        retryMaxMs: 1,
        now: 210,
      });
      expect(occurrenceClaim?.wake.wakeId).toBe(occurrenceFenced.wake.wakeId);

      expect(
        secondStore.reconcileWakeObligation({
          candidate: withoutRevision("child-progress:without-revision:2", 220),
          policy: { mode: "while_unresolved", recurrence: "after_terminal" },
        }),
      ).toMatchObject({
        disposition: "coalesced",
        wake: { wakeId: occurrenceFenced.wake.wakeId, status: "pending" },
      });
      expect(
        firstStore.renewWakeObligationClaim({
          wakeId: occurrenceFenced.wake.wakeId,
          deliveryAttemptId: occurrenceClaim!.deliveryAttempt.deliveryAttemptId,
          claimToken: occurrenceClaim!.claimToken,
          claimTtlMs: 1_000,
          now: 230,
        }),
      ).toBe(false);
      expect(
        firstStore.completeWakeObligationClaim({
          wakeId: occurrenceFenced.wake.wakeId,
          deliveryAttemptId: occurrenceClaim!.deliveryAttempt.deliveryAttemptId,
          claimToken: occurrenceClaim!.claimToken,
          attemptStatus: "handoff_accepted",
          wakeStatus: "acked",
          now: 240,
        }),
      ).toBeUndefined();
      expect(firstStore.getWakeObligation(occurrenceFenced.wake.wakeId)).toMatchObject({
        status: "pending",
        metadata: {
          wakeReconciliation: { latestOccurrenceKey: "child-progress:without-revision:2" },
        },
      });
    } finally {
      secondStore.close();
      firstStore.close();
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it("fences claims and automated controls across direct projection updates", () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "openclaw-durable-projection-fence-"));
    const dbPath = path.join(dir, "openclaw.sqlite");
    const firstStore = openDurableRuntimeSqliteStore({ path: dbPath });
    const secondStore = openDurableRuntimeSqliteStore({ path: dbPath });
    try {
      const wake = firstStore.createWakeObligation({
        sourceOwner: "task_runs",
        sourceRef: "task-projection-fence",
        targetKind: "agent_session",
        targetRef: "agent:test:main",
        reason: "child_overdue",
        factsRef: "task_runs:task-projection-fence:1",
        occurrenceKey: "task-projection-fence:1",
        metadata: { progress: "first" },
        now: 100,
      });
      const claim = firstStore.claimNextWakeObligation({
        workerId: "worker-projection-1",
        claimTtlMs: 1_000,
        retryBaseMs: 1,
        retryMaxMs: 1,
        now: 110,
      });
      expect(claim?.wake.wakeId).toBe(wake.wakeId);

      const diagnosticOnly = secondStore.updateWakeObligationProjection({
        wakeId: wake.wakeId,
        metadata: { diagnostics: { wakeOverdue: { overdue: true, thresholdMs: 1_000 } } },
        now: 115,
      });
      expect(diagnosticOnly?.deliveryRevision).toBe(claim!.wake.deliveryRevision);

      const updated = secondStore.updateWakeObligationProjection({
        wakeId: wake.wakeId,
        factsRef: "task_runs:task-projection-fence:2",
        metadata: { progress: "second" },
        now: 120,
      });
      expect(updated).toMatchObject({
        wakeId: wake.wakeId,
        factsRef: "task_runs:task-projection-fence:2",
        metadata: { progress: "second" },
      });
      expect(updated?.deliveryRevision).not.toBe(claim!.wake.deliveryRevision);

      expect(
        firstStore.renewWakeObligationClaim({
          wakeId: wake.wakeId,
          deliveryAttemptId: claim!.deliveryAttempt.deliveryAttemptId,
          claimToken: claim!.claimToken,
          claimTtlMs: 1_000,
          now: 130,
        }),
      ).toBe(false);
      expect(
        firstStore.completeWakeObligationClaim({
          wakeId: wake.wakeId,
          deliveryAttemptId: claim!.deliveryAttempt.deliveryAttemptId,
          claimToken: claim!.claimToken,
          attemptStatus: "handoff_accepted",
          wakeStatus: "acked",
          now: 140,
        }),
      ).toBeUndefined();
      expect(
        firstStore.acknowledgeWakeObligation({
          wakeId: wake.wakeId,
          actorKind: "system_worker",
          actorRef: "projection-consumer",
          expectedDeliveryRevision: claim!.wake.deliveryRevision,
          now: 150,
        }),
      ).toBeUndefined();
      expect(
        firstStore.acknowledgeWakeObligation({
          wakeId: wake.wakeId,
          actorKind: "system_worker",
          actorRef: "projection-consumer",
          expectedDeliveryRevision: updated!.deliveryRevision,
          now: 160,
        }),
      ).toMatchObject({ status: "acked" });
    } finally {
      secondStore.close();
      firstStore.close();
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });
});
