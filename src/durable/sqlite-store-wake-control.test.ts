import {
  fs,
  os,
  path,
  describe,
  expect,
  it,
  requireNodeSqlite,
  openDurableRuntimeSqliteStore,
} from "./sqlite-store.test-harness.js";

describe("durable runtime sqlite store", () => {
  it("rejects completion when attempt status or expiry disagrees with the lease", () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "openclaw-durable-claim-consistency-"));
    const dbPath = path.join(dir, "openclaw.sqlite");
    const store = openDurableRuntimeSqliteStore({ path: dbPath });
    try {
      const wake = store.createWakeObligation({
        sourceOwner: "session_store",
        sourceRef: "agent:test:claim-consistency",
        targetKind: "agent_session",
        targetRef: "agent:test:claim-consistency",
        reason: "operator_requested",
        occurrenceKey: "session:agent:test:claim-consistency",
        now: 100,
      });
      const claim = store.claimNextWakeObligation({
        workerId: "worker-consistency",
        claimTtlMs: 100,
        retryBaseMs: 1,
        retryMaxMs: 1,
        now: 100,
      });
      expect(claim?.wake.wakeId).toBe(wake.wakeId);

      const { DatabaseSync } = requireNodeSqlite();
      const seedDb = new DatabaseSync(dbPath);
      try {
        seedDb
          .prepare(
            `UPDATE state_leases
                SET expires_at = 1000
              WHERE scope = 'wake_obligation'
                AND lease_key = ?`,
          )
          .run(wake.wakeId);
      } finally {
        seedDb.close();
      }
      expect(
        store.completeWakeObligationClaim({
          wakeId: wake.wakeId,
          deliveryAttemptId: claim!.deliveryAttempt.deliveryAttemptId,
          claimToken: claim!.claimToken,
          attemptStatus: "handoff_accepted",
          wakeStatus: "acked",
          now: 201,
        }),
      ).toBeUndefined();

      const statusDb = new DatabaseSync(dbPath);
      try {
        statusDb
          .prepare(
            `UPDATE delivery_attempt_evidence
                SET status = 'failed', delivery_claim_expires_at = 1000
              WHERE delivery_attempt_id = ?`,
          )
          .run(claim!.deliveryAttempt.deliveryAttemptId);
      } finally {
        statusDb.close();
      }
      expect(
        store.completeWakeObligationClaim({
          wakeId: wake.wakeId,
          deliveryAttemptId: claim!.deliveryAttempt.deliveryAttemptId,
          claimToken: claim!.claimToken,
          attemptStatus: "handoff_accepted",
          wakeStatus: "acked",
          now: 202,
        }),
      ).toBeUndefined();
      expect(store.getWakeObligation(wake.wakeId)?.status).toBe("pending");
    } finally {
      store.close();
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it("fails closed and records uncertainty for a revision-mismatched active attempt", () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "openclaw-durable-prefence-claim-"));
    const dbPath = path.join(dir, "openclaw.sqlite");
    const initialStore = openDurableRuntimeSqliteStore({ path: dbPath });
    const wake = initialStore.createWakeObligation({
      sourceOwner: "session_store",
      sourceRef: "agent:test:prefence",
      targetKind: "agent_session",
      targetRef: "agent:test:prefence",
      reason: "operator_requested",
      occurrenceKey: "session:agent:test:prefence",
      now: 100,
    });
    const claim = initialStore.claimNextWakeObligation({
      workerId: "worker-prefence",
      claimTtlMs: 100,
      retryBaseMs: 1,
      retryMaxMs: 1,
      now: 100,
    });
    expect(claim?.wake.wakeId).toBe(wake.wakeId);
    initialStore.close();

    const { DatabaseSync } = requireNodeSqlite();
    const seedDb = new DatabaseSync(dbPath);
    try {
      seedDb
        .prepare(
          `UPDATE delivery_attempt_evidence
              SET claimed_wake_delivery_revision = ?, metadata_json = ?
            WHERE delivery_attempt_id = ?`,
        )
        .run(
          claim!.wake.deliveryRevision + 1,
          JSON.stringify({
            claimedWakeSourceRevision: null,
            claimedWakeOccurrenceKey: "session:agent:test:prefence",
          }),
          claim!.deliveryAttempt.deliveryAttemptId,
        );
    } finally {
      seedDb.close();
    }

    const reopenedStore = openDurableRuntimeSqliteStore({ path: dbPath });
    try {
      expect(
        reopenedStore.getDeliveryAttemptEvidence(claim!.deliveryAttempt.deliveryAttemptId),
      ).not.toHaveProperty("metadata");
      expect(
        reopenedStore.renewWakeObligationClaim({
          wakeId: wake.wakeId,
          deliveryAttemptId: claim!.deliveryAttempt.deliveryAttemptId,
          claimToken: claim!.claimToken,
          claimTtlMs: 100,
          now: 150,
        }),
      ).toBe(false);
      expect(
        reopenedStore.completeWakeObligationClaim({
          wakeId: wake.wakeId,
          deliveryAttemptId: claim!.deliveryAttempt.deliveryAttemptId,
          claimToken: claim!.claimToken,
          attemptStatus: "handoff_accepted",
          wakeStatus: "acked",
          now: 150,
        }),
      ).toBeUndefined();
      expect(
        reopenedStore.claimNextWakeObligation({
          workerId: "worker-after-upgrade",
          claimTtlMs: 100,
          retryBaseMs: 1,
          retryMaxMs: 1,
          now: 201,
        }),
      ).toBeUndefined();
      expect(reopenedStore.getWakeObligation(wake.wakeId)).toMatchObject({
        status: "suspended",
        failedReason: "dispatch_outcome_unknown",
      });
      expect(
        reopenedStore.getDeliveryAttemptEvidence(claim!.deliveryAttempt.deliveryAttemptId),
      ).toMatchObject({ status: "unknown", unknownAt: 201 });
      expect(reopenedStore.listUncertaintyFacts({ sourceRef: "agent:test:prefence" })).toEqual([
        expect.objectContaining({
          kind: "delivery_unknown",
          refId: claim!.deliveryAttempt.deliveryAttemptId,
          status: "open",
        }),
      ]);
    } finally {
      reopenedStore.close();
    }

    const verifyDb = new DatabaseSync(dbPath);
    try {
      expect(
        verifyDb
          .prepare(
            `SELECT COUNT(*) AS count
               FROM state_leases
              WHERE scope = 'wake_obligation'
                AND lease_key = ?`,
          )
          .get(wake.wakeId),
      ).toEqual({ count: 0 });
    } finally {
      verifyDb.close();
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it("atomically records an active dispatch as unknown when its wake is suspended", () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "openclaw-durable-suspend-claim-"));
    const store = openDurableRuntimeSqliteStore({ path: path.join(dir, "openclaw.sqlite") });
    try {
      const wake = store.createWakeObligation({
        sourceOwner: "task_runs",
        sourceRef: "task-suspend-claim",
        targetKind: "agent_session",
        targetRef: "agent:test:main",
        reason: "side_effect_uncertain",
        factsRef: "task_runs:task-suspend-claim:1",
        occurrenceKey: "task-suspend-claim:1",
        now: 100,
      });
      const claim = store.claimNextWakeObligation({
        workerId: "worker-suspend-claim",
        claimTtlMs: 1_000,
        retryBaseMs: 1,
        retryMaxMs: 1,
        now: 110,
      });
      expect(claim?.wake.wakeId).toBe(wake.wakeId);

      expect(
        store.suspendWakeObligation({
          wakeId: wake.wakeId,
          suspensionClass: "delivery_outcome_unknown",
          failedReason: "owner_decision_required",
          now: 120,
        }),
      ).toMatchObject({
        status: "suspended",
        suspensionClass: "delivery_outcome_unknown",
        failedReason: "owner_decision_required",
      });
      expect(
        store.getDeliveryAttemptEvidence(claim!.deliveryAttempt.deliveryAttemptId),
      ).toMatchObject({
        status: "unknown",
        unknownAt: 120,
      });
      expect(store.listUncertaintyFacts({ sourceRef: "task-suspend-claim" })).toEqual([
        expect.objectContaining({
          kind: "delivery_unknown",
          factsRef: "task_runs:task-suspend-claim:1",
          refId: claim!.deliveryAttempt.deliveryAttemptId,
        }),
      ]);
      expect(
        store.renewWakeObligationClaim({
          wakeId: wake.wakeId,
          deliveryAttemptId: claim!.deliveryAttempt.deliveryAttemptId,
          claimToken: claim!.claimToken,
          claimTtlMs: 1_000,
          now: 130,
        }),
      ).toBe(false);
      expect(
        store.completeWakeObligationClaim({
          wakeId: wake.wakeId,
          deliveryAttemptId: claim!.deliveryAttempt.deliveryAttemptId,
          claimToken: claim!.claimToken,
          attemptStatus: "handoff_accepted",
          wakeStatus: "acked",
          now: 130,
        }),
      ).toBeUndefined();
    } finally {
      store.close();
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it("completes an active dispatch with an explicit unknown outcome", () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "openclaw-durable-complete-unknown-"));
    const dbPath = path.join(dir, "openclaw.sqlite");
    const store = openDurableRuntimeSqliteStore({ path: dbPath });
    try {
      const wake = store.createWakeObligation({
        sourceOwner: "task_runs",
        sourceRef: "task-complete-unknown",
        targetKind: "agent_session",
        targetRef: "agent:test:main",
        reason: "side_effect_uncertain",
        factsRef: "task_runs:task-complete-unknown:1",
        sourceRunId: "run-complete-unknown",
        occurrenceKey: "task-complete-unknown:1",
        now: 100,
      });
      const claim = store.claimNextWakeObligation({
        workerId: "worker-complete-unknown",
        claimTtlMs: 1_000,
        retryBaseMs: 1,
        retryMaxMs: 1,
        now: 110,
      });

      expect(
        store.completeWakeObligationClaim({
          wakeId: wake.wakeId,
          deliveryAttemptId: claim!.deliveryAttempt.deliveryAttemptId,
          claimToken: claim!.claimToken,
          attemptStatus: "unknown",
          wakeStatus: "suspended",
          error: "transport disconnected after dispatch",
          evidence: { transport: "disconnected" },
          now: 120,
        }),
      ).toMatchObject({
        status: "unknown",
        error: "transport disconnected after dispatch",
        evidence: { transport: "disconnected" },
        unknownAt: 120,
      });
      expect(store.getWakeObligation(wake.wakeId)).toMatchObject({
        status: "suspended",
        failedReason: "transport disconnected after dispatch",
      });
      expect(store.listUncertaintyFacts({ sourceRef: "task-complete-unknown" })).toEqual([
        expect.objectContaining({
          kind: "delivery_unknown",
          sourceRunId: "run-complete-unknown",
          factsRef: "task_runs:task-complete-unknown:1",
          refId: claim!.deliveryAttempt.deliveryAttemptId,
        }),
      ]);
      expect(
        store.renewWakeObligationClaim({
          wakeId: wake.wakeId,
          deliveryAttemptId: claim!.deliveryAttempt.deliveryAttemptId,
          claimToken: claim!.claimToken,
          claimTtlMs: 1_000,
          now: 130,
        }),
      ).toBe(false);
    } finally {
      store.close();
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it("rolls back wake suspension when unknown-attempt finalization fails", () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "openclaw-durable-suspend-rollback-"));
    const dbPath = path.join(dir, "openclaw.sqlite");
    const store = openDurableRuntimeSqliteStore({ path: dbPath });
    try {
      const wake = store.createWakeObligation({
        wakeId: "wake-suspend-atomic-rollback",
        sourceOwner: "task_runs",
        sourceRef: "task-suspend-rollback",
        targetKind: "agent_session",
        targetRef: "agent:test:main",
        reason: "side_effect_uncertain",
        occurrenceKey: "task-suspend-rollback:1",
        now: 100,
      });
      const claim = store.claimNextWakeObligation({
        workerId: "worker-suspend-rollback",
        claimTtlMs: 1_000,
        retryBaseMs: 1,
        retryMaxMs: 1,
        now: 110,
      });
      const { DatabaseSync } = requireNodeSqlite();
      const faultDb = new DatabaseSync(dbPath);
      try {
        faultDb.exec(`
          CREATE TRIGGER abort_suspend_unknown_fact
          BEFORE INSERT ON uncertainty_facts
          WHEN NEW.ref_id = '${claim!.deliveryAttempt.deliveryAttemptId}'
          BEGIN
            SELECT RAISE(ABORT, 'fault-injected unknown fact insert');
          END;
        `);
      } finally {
        faultDb.close();
      }

      expect(() =>
        store.suspendWakeObligation({
          wakeId: wake.wakeId,
          suspensionClass: "delivery_outcome_unknown",
          failedReason: "owner_decision_required",
          now: 120,
        }),
      ).toThrow(/fault-injected unknown fact insert/);
      const rolledBackWake = store.getWakeObligation(wake.wakeId);
      expect(rolledBackWake).toMatchObject({ status: "pending" });
      expect(rolledBackWake).not.toHaveProperty("failedReason");
      expect(
        store.getDeliveryAttemptEvidence(claim!.deliveryAttempt.deliveryAttemptId),
      ).toMatchObject({
        status: "attempted",
        deliveryClaimedBy: claim!.claimToken,
        deliveryClaimExpiresAt: 1_110,
      });
      expect(store.listUncertaintyFacts({ sourceRef: "task-suspend-rollback" })).toEqual([]);
      expect(
        store.renewWakeObligationClaim({
          wakeId: wake.wakeId,
          deliveryAttemptId: claim!.deliveryAttempt.deliveryAttemptId,
          claimToken: claim!.claimToken,
          claimTtlMs: 1_000,
          now: 130,
        }),
      ).toBe(true);
    } finally {
      store.close();
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
          expectedDeliveryRevision: acknowledgedWake.deliveryRevision,
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
          expectedDeliveryRevision: supersededWake.deliveryRevision,
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
          expectedDeliveryRevision: wake.deliveryRevision,
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
          suspensionClass: "owner_decision_required",
          failedReason: "owner_decision_required",
          now: 110,
        }),
      ).toMatchObject({
        status: "suspended",
        suspensionClass: "owner_decision_required",
      });
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

  it("resumes only revision-matched pre-attempt suspension classes", () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "openclaw-durable-safe-resume-"));
    const store = openDurableRuntimeSqliteStore({ path: path.join(dir, "openclaw.sqlite") });
    const createWake = (sourceRef: string, now: number) =>
      store.createWakeObligation({
        sourceOwner: "task_runs",
        sourceRef,
        targetKind: "agent_session",
        targetRef: "agent:test:main",
        reason: "operator_requested",
        occurrenceKey: `${sourceRef}:1`,
        now,
      });
    try {
      const safeWake = createWake("task-safe-resume", 100);
      const suspended = store.suspendWakeObligation({
        wakeId: safeWake.wakeId,
        suspensionClass: "capability_unavailable",
        failedReason: "owner capability unavailable",
        now: 110,
      });
      expect(suspended).toMatchObject({
        status: "suspended",
        suspensionClass: "capability_unavailable",
      });
      expect(
        store.resumeWakeObligation({
          wakeId: safeWake.wakeId,
          actorKind: "system_worker",
          actorRef: "owner_reconciliation",
          expectedDeliveryRevision: safeWake.deliveryRevision,
          expectedSuspensionClass: "capability_unavailable",
          now: 120,
        }),
      ).toBeUndefined();
      const resumed = store.resumeWakeObligation({
        wakeId: safeWake.wakeId,
        actorKind: "system_worker",
        actorRef: "owner_reconciliation",
        expectedDeliveryRevision: suspended!.deliveryRevision,
        expectedSuspensionClass: "capability_unavailable",
        now: 121,
      });
      expect(resumed).toMatchObject({ status: "pending" });
      expect(resumed).not.toHaveProperty("suspensionClass");
      store.acknowledgeWakeObligation({
        wakeId: safeWake.wakeId,
        actorKind: "system_worker",
        actorRef: "test",
        expectedDeliveryRevision: resumed!.deliveryRevision,
        now: 130,
      });

      const attemptedWake = createWake("task-attempted-resume", 200);
      const claim = store.claimNextWakeObligation({
        workerId: "worker-safe-resume",
        claimTtlMs: 1_000,
        retryBaseMs: 1,
        retryMaxMs: 1,
        now: 210,
      });
      expect(claim?.wake.wakeId).toBe(attemptedWake.wakeId);
      const attemptedSuspension = store.suspendWakeObligation({
        wakeId: attemptedWake.wakeId,
        suspensionClass: "capability_unavailable",
        failedReason: "capability disappeared after claim",
        now: 220,
      });
      expect(attemptedSuspension).toMatchObject({ status: "suspended" });
      expect(
        store.resumeWakeObligation({
          wakeId: attemptedWake.wakeId,
          actorKind: "system_worker",
          actorRef: "owner_reconciliation",
          expectedDeliveryRevision: attemptedSuspension!.deliveryRevision,
          expectedSuspensionClass: "capability_unavailable",
          now: 230,
        }),
      ).toBeUndefined();
      expect(store.getWakeObligation(attemptedWake.wakeId)?.status).toBe("suspended");
      expect(
        store.getDeliveryAttemptEvidence(claim!.deliveryAttempt.deliveryAttemptId)?.status,
      ).toBe("unknown");
    } finally {
      store.close();
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });
});
