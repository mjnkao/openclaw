import {
  fs,
  os,
  path,
  describe,
  expect,
  it,
  vi,
  requireNodeSqlite,
  openDurableRuntimeSqliteStore,
  requireAppliedWakeResult,
} from "./sqlite-store.test-harness.js";

describe("durable runtime sqlite store", () => {
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
            expectedDeliveryRevision: wake.deliveryRevision,
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

  it("hashes projection metadata without ambient locale collation", () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "openclaw-durable-stable-hash-"));
    const store = openDurableRuntimeSqliteStore({ path: path.join(dir, "openclaw.sqlite") });
    const localeCompare = vi.spyOn(String.prototype, "localeCompare").mockImplementation(() => {
      throw new Error("projection hashing must not use ambient locale collation");
    });
    try {
      const base = {
        sourceOwner: "task_runs",
        sourceRef: "task-stable-hash",
        targetKind: "agent_session" as const,
        targetRef: "agent:test:main",
        reason: "operator_requested" as const,
        occurrenceKey: "task-stable-hash:1",
        now: 100,
      };
      const first = store.reconcileWakeObligation({
        candidate: { ...base, metadata: { "\u00e4": 1, z: 2 } },
        policy: { mode: "while_unresolved", recurrence: "after_terminal" },
      });
      expect(first).toMatchObject({ disposition: "created" });
      expect(
        store.reconcileWakeObligation({
          candidate: { ...base, metadata: { z: 2, "\u00e4": 1 }, now: 110 },
          policy: { mode: "while_unresolved", recurrence: "after_terminal" },
        }),
      ).toMatchObject({ disposition: "exact_match" });
    } finally {
      localeCompare.mockRestore();
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
             status, claimed_wake_delivery_revision, scheduled_at, created_at, updated_at
           ) VALUES (?, ?, ?, ?, ?, 'scheduled', ?, ?, ?, ?)`,
        );
        for (let index = 0; index < 105; index += 1) {
          insertAttempt.run(
            `attempt-inspection-${index}`,
            wake.wakeId,
            "test-owner",
            "test-source:inspection-bounds",
            `attempt-dedupe-${index}`,
            wake.deliveryRevision,
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

  it("paginates unresolved wakes with optional owner and creation cutoff filters", () => {
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
          now: Math.floor(index / 2),
        });
      }
      const otherOwner = store.createWakeObligation({
        wakeId: "wake_other_owner",
        sourceOwner: "task_runs",
        sourceRef: "task-other-owner",
        targetKind: "agent_session",
        targetRef: "agent:test:main",
        reason: "operator_requested",
        occurrenceKey: "task-other-owner:1",
        now: 100,
      });
      const future = store.createWakeObligation({
        wakeId: "wake_future",
        sourceOwner: "subagent_runs",
        sourceRef: "child-future",
        targetKind: "agent_session",
        targetRef: "agent:test:main",
        reason: "child_overdue",
        occurrenceKey: "child-progress:future",
        now: 2_000,
      });
      const forwardStatus = store.createWakeObligation({
        wakeId: "wake_forward_status",
        sourceOwner: "subagent_runs",
        sourceRef: "child-forward-status",
        targetKind: "agent_session",
        targetRef: "agent:test:main",
        reason: "child_overdue",
        occurrenceKey: "child-progress:forward-status",
        now: 200,
      });
      const { DatabaseSync } = requireNodeSqlite();
      const forwardDb = new DatabaseSync(path.join(dir, "openclaw.sqlite"));
      try {
        forwardDb.exec("PRAGMA ignore_check_constraints = ON");
        forwardDb
          .prepare("UPDATE wake_obligations SET status = 'paused' WHERE wake_id = ?")
          .run(forwardStatus.wakeId);
      } finally {
        forwardDb.close();
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
        expectedDeliveryRevision: terminal.deliveryRevision,
        now: 1_001,
      });

      const wakeIds: string[] = [];
      let cursor: string | undefined;
      for (;;) {
        const page = store.listUnresolvedWakeObligationsPage({
          sourceOwner: "subagent_runs",
          createdAtOrBefore: 1_000,
          cursor,
          limit: 100,
        });
        wakeIds.push(...page.wakes.map((wake) => wake.wakeId));
        if (page.complete) {
          expect(page.nextCursor).toBeUndefined();
          break;
        }
        expect(page.nextCursor).toBeDefined();
        cursor = page.nextCursor;
      }

      expect(wakeIds).toHaveLength(504);
      expect(new Set(wakeIds).size).toBe(504);
      expect(wakeIds).not.toContain(terminal.wakeId);
      expect(wakeIds).not.toContain(otherOwner.wakeId);
      expect(wakeIds).not.toContain(future.wakeId);
      expect(wakeIds).toContain(forwardStatus.wakeId);
      const allOwners: string[] = [];
      cursor = undefined;
      for (;;) {
        const page = store.listUnresolvedWakeObligationsPage({
          createdAtOrBefore: 1_000,
          cursor,
          limit: 500,
        });
        allOwners.push(...page.wakes.map((wake) => wake.wakeId));
        if (page.complete) {
          break;
        }
        cursor = page.nextCursor;
      }
      expect(allOwners).toHaveLength(505);
      expect(allOwners).toContain(otherOwner.wakeId);
      expect(allOwners).not.toContain(terminal.wakeId);
      expect(allOwners).not.toContain(future.wakeId);
      expect(() =>
        store.listUnresolvedWakeObligationsPage({ createdAtOrBefore: -1, limit: 100 }),
      ).toThrow(/createdAtOrBefore must be a non-negative safe integer/);
      expect(() =>
        store.listUnresolvedWakeObligationsPage({ createdAtOrBefore: 1_000, limit: 10_000 }),
      ).toThrow(/limit must not exceed 500/);
      const ownerPage = store.listUnresolvedWakeObligationsPage({
        sourceOwner: "subagent_runs",
        createdAtOrBefore: 1_000,
        limit: 1,
      });
      expect(ownerPage.complete).toBe(false);
      expect(() =>
        store.listUnresolvedWakeObligationsPage({
          sourceOwner: "task_runs",
          cursor: ownerPage.nextCursor,
          limit: 1,
        }),
      ).toThrow(/cursor does not match sourceOwner/);
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
        expectedDeliveryRevision: first.wake.deliveryRevision,
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
});
