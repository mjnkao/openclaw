import {
  fs,
  os,
  path,
  describe,
  expect,
  it,
  requireNodeSqlite,
  openDurableRuntimeSqliteStore,
  requireAppliedWakeResult,
  startConcurrentWakeWriter,
} from "./sqlite-store.test-harness.js";

describe("durable runtime sqlite store", () => {
  it("attributes an expired coalesced claim to the projection that was attempted", () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "openclaw-durable-coalesced-unknown-"));
    const dbPath = path.join(dir, "openclaw.sqlite");
    const firstStore = openDurableRuntimeSqliteStore({ path: dbPath });
    const secondStore = openDurableRuntimeSqliteStore({ path: dbPath });
    const candidate = (
      occurrenceKey: string,
      sourceRevision: string,
      sourceRunId: string,
      now: number,
    ) => ({
      sourceOwner: "subagent_runs",
      sourceRef: "child-coalesced-unknown",
      targetKind: "agent_session" as const,
      targetRef: "agent:test:main",
      reason: "child_overdue" as const,
      factsRef: `subagent_runs:child-coalesced-unknown:${sourceRevision}`,
      sourceRunId,
      occurrenceKey,
      sourceRevision,
      now,
    });
    try {
      const first = requireAppliedWakeResult(
        firstStore.reconcileWakeObligation({
          candidate: candidate("child-progress:1", "revision-1", "run-attempted", 100),
          policy: { mode: "while_unresolved", recurrence: "after_terminal" },
        }),
      );
      const claim = firstStore.claimNextWakeObligation({
        workerId: "worker-coalesced-unknown",
        claimTtlMs: 100,
        retryBaseMs: 1,
        retryMaxMs: 1,
        now: 100,
      });
      expect(claim?.wake.wakeId).toBe(first.wake.wakeId);

      expect(
        secondStore.reconcileWakeObligation({
          candidate: candidate("child-progress:2", "revision-2", "run-coalesced", 150),
          policy: { mode: "while_unresolved", recurrence: "after_terminal" },
        }),
      ).toMatchObject({
        disposition: "coalesced",
        wake: {
          factsRef: "subagent_runs:child-coalesced-unknown:revision-2",
          sourceRunId: "run-coalesced",
        },
      });
      expect(
        firstStore.claimNextWakeObligation({
          workerId: "worker-after-coalesce",
          claimTtlMs: 100,
          retryBaseMs: 1,
          retryMaxMs: 1,
          now: 201,
        }),
      ).toBeUndefined();
      expect(firstStore.getWakeObligation(first.wake.wakeId)).toMatchObject({
        status: "suspended",
        factsRef: "subagent_runs:child-coalesced-unknown:revision-2",
      });
      expect(firstStore.listUncertaintyFacts({ sourceRef: "child-coalesced-unknown" })).toEqual([
        expect.objectContaining({
          kind: "delivery_unknown",
          factsRef: "subagent_runs:child-coalesced-unknown:revision-1",
          sourceRunId: "run-attempted",
          facts: expect.objectContaining({
            claimedWakeDeliveryRevision: claim!.wake.deliveryRevision,
            claimedFactsRef: "subagent_runs:child-coalesced-unknown:revision-1",
            claimedSourceRunId: "run-attempted",
          }),
        }),
      ]);
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
              AND parent_run_id IS NULL
              AND parent_session_key IS NULL
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

  it("uses bounded indexes for recovery and owner scan queries", () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "openclaw-durable-query-plans-"));
    const dbPath = path.join(dir, "openclaw.sqlite");
    const store = openDurableRuntimeSqliteStore({ path: dbPath });
    store.close();
    const { DatabaseSync } = requireNodeSqlite();
    const db = new DatabaseSync(dbPath, { readOnly: true });
    const plan = (sql: string, ...params: Array<string | number>) =>
      db.prepare(`EXPLAIN QUERY PLAN ${sql}`).all(...params) as Array<{ detail: string }>;
    const expectIndexWithoutSort = (rows: Array<{ detail: string }>, indexName: string) => {
      expect(rows.some((row) => row.detail.includes(indexName))).toBe(true);
      expect(rows.some((row) => row.detail.includes("USE TEMP B-TREE"))).toBe(false);
    };
    try {
      expectIndexWithoutSort(
        plan(
          `SELECT * FROM wake_obligations
            WHERE source_owner = ?
              AND status NOT IN ('acked', 'superseded')
              AND created_at <= ?
              AND (created_at > ? OR (created_at = ? AND wake_id > ?))
            ORDER BY created_at, wake_id
            LIMIT ?`,
          "subagent_runs",
          1_000,
          100,
          100,
          "wake_cursor",
          101,
        ),
        "idx_wake_obligations_unresolved_owner_scan",
      );
      expectIndexWithoutSort(
        plan(
          `SELECT * FROM wake_obligations
            WHERE status NOT IN ('acked', 'superseded')
              AND created_at <= ?
              AND (created_at > ? OR (created_at = ? AND wake_id > ?))
            ORDER BY created_at, wake_id
            LIMIT ?`,
          1_000,
          100,
          100,
          "wake_cursor",
          101,
        ),
        "idx_wake_obligations_unresolved_scan",
      );
      expectIndexWithoutSort(
        plan(
          `SELECT d.*
             FROM delivery_attempt_evidence AS d
             JOIN wake_obligations AS w ON w.wake_id = d.wake_id
            WHERE d.status = 'attempted'
              AND d.delivery_claim_expires_at IS NOT NULL
              AND d.delivery_claim_expires_at <= ?
              AND w.status IN ('pending', 'failed')
            ORDER BY d.delivery_claim_expires_at, d.delivery_attempt_id
            LIMIT 100`,
          1_000,
        ),
        "idx_delivery_attempt_evidence_expired_claim",
      );
      expectIndexWithoutSort(
        plan(
          `SELECT w.*
             FROM wake_obligations AS w INDEXED BY idx_wake_obligations_claimable
             LEFT JOIN state_leases AS l
               ON l.lease_key = w.wake_id AND l.scope = 'wake_obligation'
            WHERE w.status IN ('pending', 'failed')
              AND (w.next_attempt_at IS NULL OR w.next_attempt_at <= ?)
              AND l.lease_key IS NULL
            ORDER BY w.next_attempt_at, w.updated_at, w.wake_id
            LIMIT 100`,
          1_000,
        ),
        "idx_wake_obligations_claimable",
      );
      expectIndexWithoutSort(
        plan(
          `SELECT * FROM durable_execution_records
            WHERE status NOT IN ('succeeded', 'failed', 'cancelled', 'lost')
              AND recovery_state != 'terminal'
              AND completed_at IS NULL
              AND operation_kind = ?
              AND updated_at <= ?
              AND (updated_at > ? OR (updated_at = ? AND runtime_run_id > ?))
            ORDER BY updated_at, runtime_run_id
            LIMIT ?`,
          "openclaw.agent.turn",
          1_000,
          100,
          100,
          "run_cursor",
          101,
        ),
        "idx_durable_execution_records_open_operation",
      );
      expect(
        plan(
          `SELECT * FROM wake_obligation_occurrences
            WHERE source_owner = ? AND source_ref = ? AND occurrence_key = ?`,
          "subagent_runs",
          "child-run",
          "child-run:1",
        ).some((row) => row.detail.includes("sqlite_autoindex_wake_obligation_occurrences_1")),
      ).toBe(true);
      expect(
        plan(
          `SELECT * FROM uncertainty_facts
            WHERE source_owner = ? AND source_ref = ? AND dedupe_key = ?`,
          "subagent_runs",
          "child-run",
          "child-run:unknown",
        ).some((row) => row.detail.includes("idx_uncertainty_facts_source_dedupe")),
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

  it("rolls back duplicate repair when occurrence persistence fails", () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "openclaw-durable-reconcile-rollback-"));
    const dbPath = path.join(dir, "openclaw.sqlite");
    const store = openDurableRuntimeSqliteStore({ path: dbPath });
    const base = {
      sourceOwner: "subagent_runs",
      sourceRef: "child-reconcile-rollback",
      targetKind: "agent_session" as const,
      targetRef: "agent:test:main",
      reason: "child_overdue" as const,
    };
    try {
      const first = store.createWakeObligation({
        ...base,
        wakeId: "wake_reconcile_rollback_a",
        occurrenceKey: "rollback:a",
        factsRef: "before-a",
        now: 100,
      });
      const second = store.createWakeObligation({
        ...base,
        wakeId: "wake_reconcile_rollback_b",
        occurrenceKey: "rollback:b",
        factsRef: "before-b",
        now: 110,
      });
      const { DatabaseSync } = requireNodeSqlite();
      const faultDb = new DatabaseSync(dbPath);
      try {
        faultDb.exec(`
          UPDATE wake_obligations
             SET coalescing_mode = 'while_unresolved', recurrence_policy = 'after_terminal';
          CREATE TRIGGER abort_reconcile_occurrence
          BEFORE INSERT ON wake_obligation_occurrences
          WHEN NEW.occurrence_key = 'rollback:next'
          BEGIN
            SELECT RAISE(ABORT, 'fault-injected occurrence insert');
          END;
        `);
      } finally {
        faultDb.close();
      }

      expect(() =>
        store.reconcileWakeObligation({
          candidate: {
            ...base,
            occurrenceKey: "rollback:next",
            factsRef: "must-not-commit",
            now: 120,
          },
          policy: { mode: "while_unresolved", recurrence: "after_terminal" },
        }),
      ).toThrow(/fault-injected occurrence insert/);

      expect(store.getWakeObligation(first.wakeId)).toMatchObject({
        status: "pending",
        factsRef: "before-a",
      });
      expect(store.getWakeObligation(second.wakeId)).toMatchObject({
        status: "pending",
        factsRef: "before-b",
      });
      expect(
        store.getWakeObligationByOccurrenceKey({
          sourceOwner: base.sourceOwner,
          sourceRef: base.sourceRef,
          occurrenceKey: "rollback:a",
        })?.wakeId,
      ).toBe(first.wakeId);
      expect(
        store.getWakeObligationByOccurrenceKey({
          sourceOwner: base.sourceOwner,
          sourceRef: base.sourceRef,
          occurrenceKey: "rollback:b",
        })?.wakeId,
      ).toBe(second.wakeId);
      expect(
        store.getWakeObligationByOccurrenceKey({
          sourceOwner: base.sourceOwner,
          sourceRef: base.sourceRef,
          occurrenceKey: "rollback:next",
        }),
      ).toBeUndefined();
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
});
