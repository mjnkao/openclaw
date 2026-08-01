import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { resetConfigRuntimeState, setRuntimeConfigSnapshot } from "../config/runtime-snapshot.js";
import { upsertSessionEntry } from "../config/sessions/session-accessor.js";
import { openDurableRuntimeStore } from "../durable/store-factory.js";
import {
  enqueueSessionDelivery,
  loadPendingSessionDeliveries,
} from "../infra/session-delivery-queue.js";
import { peekConsumedSystemEventDeliveryQueueIds } from "../infra/system-event-delivery-state.js";
import {
  consumeSelectedSystemEventEntries,
  enqueueSystemEventEntry,
  peekSystemEventEntries,
  resetSystemEventsForTest,
} from "../infra/system-events.js";
import { withTempDir } from "../test-helpers/temp-dir.js";
import {
  acknowledgeConsumedSessionAttentionDeliveries,
  requestSessionAttentionDelivery,
  releaseConsumedSessionAttentionDeliveries,
} from "./session-attention.js";

describe("session attention delivery", () => {
  let previousStateDir: string | undefined;

  beforeEach(() => {
    previousStateDir = process.env.OPENCLAW_STATE_DIR;
    setRuntimeConfigSnapshot({ durable: { mode: "observe" } });
    resetSystemEventsForTest();
  });

  afterEach(() => {
    resetSystemEventsForTest();
    resetConfigRuntimeState();
    if (previousStateDir === undefined) {
      delete process.env.OPENCLAW_STATE_DIR;
    } else {
      process.env.OPENCLAW_STATE_DIR = previousStateDir;
    }
  });

  it("acks the persistent outbox only after attached-session consumption", async () => {
    await withTempDir({ prefix: "openclaw-session-attention-" }, async (stateDir) => {
      process.env.OPENCLAW_STATE_DIR = stateDir;
      const sessionKey = "agent:test:session-attention";
      const deliveryQueueId = await enqueueSessionDelivery({
        kind: "systemEvent",
        sessionKey,
        text: "inspect interrupted durable work",
        expectedSessionId: "session-1",
        idempotencyKey: "durable-wake:test",
      });
      enqueueSystemEventEntry("inspect interrupted durable work", {
        sessionKey,
        contextKey: "durable-wake:test",
        deliveryQueueId,
      });

      expect(await loadPendingSessionDeliveries()).toHaveLength(1);
      consumeSelectedSystemEventEntries(sessionKey, peekSystemEventEntries(sessionKey));
      expect(peekConsumedSystemEventDeliveryQueueIds(sessionKey)).toEqual([deliveryQueueId]);

      await expect(acknowledgeConsumedSessionAttentionDeliveries(sessionKey)).resolves.toEqual({
        acknowledgedIds: [deliveryQueueId],
        failed: [],
      });
      expect(await loadPendingSessionDeliveries()).toEqual([]);
      expect(peekConsumedSystemEventDeliveryQueueIds(sessionKey)).toEqual([]);
    });
  });

  it("releases in-flight consumption after an agent run fails so recovery can retry", async () => {
    await withTempDir({ prefix: "openclaw-session-attention-" }, async (stateDir) => {
      process.env.OPENCLAW_STATE_DIR = stateDir;
      const sessionKey = "agent:test:session-attention-failure";
      const deliveryQueueId = await enqueueSessionDelivery({
        kind: "systemEvent",
        sessionKey,
        text: "inspect interrupted durable work",
        expectedSessionId: "session-1",
        idempotencyKey: "durable-wake:retry",
      });
      enqueueSystemEventEntry("inspect interrupted durable work", {
        sessionKey,
        contextKey: "durable-wake:retry",
        deliveryQueueId,
      });
      consumeSelectedSystemEventEntries(sessionKey, peekSystemEventEntries(sessionKey));
      expect(peekConsumedSystemEventDeliveryQueueIds(sessionKey)).toEqual([deliveryQueueId]);

      releaseConsumedSessionAttentionDeliveries(sessionKey);

      expect(peekConsumedSystemEventDeliveryQueueIds(sessionKey)).toEqual([]);
      expect(await loadPendingSessionDeliveries()).toEqual([
        expect.objectContaining({ id: deliveryQueueId }),
      ]);
    });
  });

  it("reports RAM admission as deferred when persistent attention survives a full durable queue", async () => {
    await withTempDir({ prefix: "openclaw-session-attention-" }, async (stateDir) => {
      process.env.OPENCLAW_STATE_DIR = stateDir;
      const sessionKey = "agent:test:session-attention-overflow";
      await upsertSessionEntry({ sessionKey }, { sessionId: "session-overflow", updatedAt: 1 });
      const store = openDurableRuntimeStore();
      const wake = store.createWakeObligation({
        sourceOwner: "session_store",
        sourceRef: sessionKey,
        targetKind: "agent_session",
        targetRef: sessionKey,
        reason: "restart_interrupted",
        occurrenceKey: "session-attention-overflow",
        now: 1,
      });
      store.close();
      for (let index = 0; index < 20; index += 1) {
        enqueueSystemEventEntry(`existing durable attention ${index}`, {
          sessionKey,
          contextKey: `durable-existing:${index}`,
          deliveryQueueId: `existing-queue-${index}`,
        });
      }

      const result = await requestSessionAttentionDelivery({
        sessionKey,
        text: "new durable attention",
        idempotencyKey: "durable-wake:overflow",
        wakeId: wake.wakeId,
        deliveryRevision: wake.deliveryRevision,
      });

      expect(result).toMatchObject({
        status: "handoff_accepted",
        duplicate: false,
        immediateAdmission: "deferred",
      });
      expect(peekSystemEventEntries(sessionKey)).toHaveLength(20);
      expect(await loadPendingSessionDeliveries()).toEqual([
        expect.objectContaining({
          sessionKey,
          text: "new durable attention",
          idempotencyKey: "durable-wake:overflow",
          source: {
            owner: "durable_wake",
            ref: wake.wakeId,
            deliveryRevision: wake.deliveryRevision,
          },
        }),
      ]);
    });
  });
});
