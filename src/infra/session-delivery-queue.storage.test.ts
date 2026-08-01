// Covers session delivery queue persistence state transitions.
import { describe, expect, it } from "vitest";
import { openOpenClawStateDatabase } from "../state/openclaw-state-db.js";
import { withTempDir } from "../test-helpers/temp-dir.js";
import {
  ackSessionDelivery,
  enqueueSessionDelivery,
  failSessionDelivery,
  loadPendingSessionDeliveries,
} from "./session-delivery-queue.js";

describe("session-delivery queue storage", () => {
  function readSessionQueueStatus(tempDir: string, id: string): string | undefined {
    const { db } = openOpenClawStateDatabase({
      env: { ...process.env, OPENCLAW_STATE_DIR: tempDir },
    });
    const row = db
      .prepare("SELECT status FROM delivery_queue_entries WHERE queue_name = 'session' AND id = ?")
      .get(id) as { status?: string } | undefined;
    return row?.status;
  }

  it("dedupes entries when an idempotency key is reused", async () => {
    await withTempDir({ prefix: "openclaw-session-delivery-" }, async (tempDir) => {
      const firstId = await enqueueSessionDelivery(
        {
          kind: "agentTurn",
          sessionKey: "agent:main:main",
          message: "continue after restart",
          messageId: "restart-sentinel:agent:main:main:agentTurn:123",
          idempotencyKey: "restart-sentinel:agent:main:main:agentTurn:123",
        },
        tempDir,
      );
      const secondId = await enqueueSessionDelivery(
        {
          kind: "agentTurn",
          sessionKey: "agent:main:main",
          message: "continue after restart",
          messageId: "restart-sentinel:agent:main:main:agentTurn:123",
          idempotencyKey: "restart-sentinel:agent:main:main:agentTurn:123",
        },
        tempDir,
      );

      expect(secondId).toBe(firstId);
      expect(await loadPendingSessionDeliveries(tempDir)).toHaveLength(1);
    });
  });

  it("persists retry metadata and removes acked entries", async () => {
    await withTempDir({ prefix: "openclaw-session-delivery-" }, async (tempDir) => {
      const id = await enqueueSessionDelivery(
        {
          kind: "systemEvent",
          sessionKey: "agent:main:main",
          text: "restart complete",
        },
        tempDir,
      );

      await failSessionDelivery(id, "dispatch failed", tempDir);
      const [failedEntry] = await loadPendingSessionDeliveries(tempDir);
      expect(failedEntry?.retryCount).toBe(1);
      expect(failedEntry?.lastError).toBe("dispatch failed");

      await ackSessionDelivery(id, tempDir);
      expect(await loadPendingSessionDeliveries(tempDir)).toStrictEqual([]);
    });
  });

  it("persists and scopes idempotency by durable wake delivery revision", async () => {
    await withTempDir({ prefix: "openclaw-session-delivery-" }, async (tempDir) => {
      const firstId = await enqueueSessionDelivery(
        {
          kind: "systemEvent",
          sessionKey: "agent:main:main",
          text: "inspect revision one",
          idempotencyKey: "durable-wake:wake-bound",
          source: {
            owner: "durable_wake",
            ref: "wake-bound",
            deliveryRevision: 1,
          },
        },
        tempDir,
      );
      const secondId = await enqueueSessionDelivery(
        {
          kind: "systemEvent",
          sessionKey: "agent:main:main",
          text: "inspect revision two",
          idempotencyKey: "durable-wake:wake-bound",
          source: {
            owner: "durable_wake",
            ref: "wake-bound",
            deliveryRevision: 2,
          },
        },
        tempDir,
      );

      expect(secondId).not.toBe(firstId);
      const pending = await loadPendingSessionDeliveries(tempDir);
      expect(pending).toHaveLength(2);
      expect(pending).toEqual(
        expect.arrayContaining([
          expect.objectContaining({
            id: firstId,
            source: {
              owner: "durable_wake",
              ref: "wake-bound",
              deliveryRevision: 1,
            },
          }),
          expect.objectContaining({
            id: secondId,
            source: {
              owner: "durable_wake",
              ref: "wake-bound",
              deliveryRevision: 2,
            },
          }),
        ]),
      );
    });
  });

  it("moves entries out of pending retry state", async () => {
    await withTempDir({ prefix: "openclaw-session-delivery-" }, async (tempDir) => {
      const id = await enqueueSessionDelivery(
        {
          kind: "systemEvent",
          sessionKey: "agent:main:main",
          text: "restart complete",
        },
        tempDir,
      );

      await ackSessionDelivery(id, tempDir);

      expect(readSessionQueueStatus(tempDir, id)).toBeUndefined();
    });
  });
});
