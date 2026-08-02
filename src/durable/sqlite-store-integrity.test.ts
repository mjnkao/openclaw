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
        suspensionClass: "owner_decision_required",
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
