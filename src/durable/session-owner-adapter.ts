import { loadSessionEntry } from "../config/sessions/session-accessor.js";
import { formatErrorMessage } from "../infra/errors.js";
import { requestHeartbeat } from "../infra/heartbeat-wake.js";
import {
  inspectSessionDeliveryForPrompt,
  rejectSessionDeliveryBeforePrompt,
  retireSessionDeliveryBeforePrompt,
} from "../infra/session-delivery-queue-storage.js";
import {
  drainPendingSessionDeliveries,
  type QueuedSessionDelivery,
  type SessionDeliveryRecoveryLogger,
} from "../infra/session-delivery-queue.js";
import {
  peekConsumedSystemEventDeliveryQueueIds,
  registerSystemEventDeliveryInspector,
  rejectSystemEventDeliveryQueueId,
} from "../infra/system-event-delivery-state.js";
import { enqueueSystemEventEntry, peekSystemEventEntries } from "../infra/system-events.js";
import { requestSessionAttentionDelivery } from "../sessions/session-attention.js";
import { normalizeSessionKeyPreservingOpaquePeerIds } from "../sessions/session-key-utils.js";
import { isDurableRuntimeEnabled } from "./config.js";
import type {
  DurableOwnerAdapter,
  DurableOwnerAttentionFact,
  DurableOwnerDispatchResult,
} from "./owner-adapter-contract.js";
import { openDurableRuntimeStore } from "./store-factory.js";
import type { WakeObligation } from "./types.js";

type DurableSessionWakeBinding = {
  wakeId: string;
  deliveryRevision?: number;
  deliveryQueueId?: string;
  sessionKey: string;
  queuedSessionKey?: string;
};

function requireDurableSessionWakeBinding(
  store: ReturnType<typeof openDurableRuntimeStore>,
  params: DurableSessionWakeBinding,
): {
  wake: WakeObligation;
  deliveryRevision: number;
  controlDeliveryRevision: number;
  staleRevision: boolean;
} {
  const deliveryRevision = params.deliveryRevision;
  if (
    deliveryRevision === undefined ||
    !Number.isSafeInteger(deliveryRevision) ||
    deliveryRevision <= 0
  ) {
    throw new Error(`durable session delivery is missing its wake revision: ${params.wakeId}`);
  }
  const wake = store.getWakeObligation(params.wakeId);
  if (!wake) {
    throw new Error(`durable session delivery references a missing wake: ${params.wakeId}`);
  }
  if (wake.targetKind !== "agent_session") {
    throw new Error(`durable session delivery references a non-session wake: ${params.wakeId}`);
  }
  const targetRef = normalizeSessionKeyPreservingOpaquePeerIds(wake.targetRef);
  const sessionKey = normalizeSessionKeyPreservingOpaquePeerIds(params.sessionKey);
  const queuedSessionKey = normalizeSessionKeyPreservingOpaquePeerIds(
    params.queuedSessionKey ?? params.sessionKey,
  );
  if (!targetRef || !sessionKey || !queuedSessionKey) {
    throw new Error(`durable session delivery has an incomplete session binding: ${params.wakeId}`);
  }
  if (targetRef !== sessionKey || targetRef !== queuedSessionKey) {
    throw new Error(`durable session delivery target does not match its session: ${params.wakeId}`);
  }
  const settledAcceptedAttempt =
    wake.status === "handoff_accepted" &&
    wake.deliveryRevision === deliveryRevision + 1 &&
    store
      .listDeliveryAttemptEvidence({
        wakeId: wake.wakeId,
        status: "handoff_accepted",
        limit: 1,
      })
      .some((attempt) => attempt.claimedWakeDeliveryRevision === deliveryRevision);
  return {
    wake,
    deliveryRevision,
    controlDeliveryRevision: wake.deliveryRevision,
    staleRevision: wake.deliveryRevision !== deliveryRevision && !settledAcceptedAttempt,
  };
}

function requireCurrentDurableSessionWakeBinding(
  store: ReturnType<typeof openDurableRuntimeStore>,
  params: DurableSessionWakeBinding,
): {
  wake: WakeObligation;
  deliveryRevision: number;
  controlDeliveryRevision: number;
} {
  const binding = requireDurableSessionWakeBinding(store, params);
  if (binding.staleRevision) {
    throw new Error(`durable session delivery wake revision changed: ${params.wakeId}`);
  }
  return binding;
}

function inspectDurableSessionDeliveryForPrompt(params: {
  sessionKey: string;
  deliveryQueueId: string;
}): "allow" | "reject" | undefined {
  const entry = inspectSessionDeliveryForPrompt(params.deliveryQueueId);
  if (entry?.kind !== "systemEvent" || entry.source?.owner !== "durable_wake") {
    return undefined;
  }
  try {
    const store = openDurableRuntimeStore();
    let binding: ReturnType<typeof requireDurableSessionWakeBinding>;
    try {
      binding = requireDurableSessionWakeBinding(store, {
        wakeId: entry.source.ref,
        deliveryRevision: entry.source.deliveryRevision,
        sessionKey: params.sessionKey,
        queuedSessionKey: entry.sessionKey,
      });
    } finally {
      store.close();
    }
    if (
      binding.staleRevision ||
      binding.wake.status === "acked" ||
      binding.wake.status === "superseded"
    ) {
      retireSessionDeliveryBeforePrompt(entry.id);
      return "reject";
    }
    const currentSession = loadSessionEntry({
      sessionKey: params.sessionKey,
      readConsistency: "latest",
    });
    if (
      !currentSession ||
      (entry.expectedSessionId !== undefined &&
        currentSession.sessionId !== entry.expectedSessionId)
    ) {
      supersedeDurableSessionWakeForGenerationChange({
        wakeId: entry.source.ref,
        deliveryRevision: entry.source.deliveryRevision,
        deliveryQueueId: entry.id,
        sessionKey: params.sessionKey,
        queuedSessionKey: entry.sessionKey,
        expectedSessionId: entry.expectedSessionId,
        actualSessionId: currentSession?.sessionId,
      });
      retireSessionDeliveryBeforePrompt(entry.id);
      return "reject";
    }
    return "allow";
  } catch (error) {
    rejectSessionDeliveryBeforePrompt(entry.id, formatErrorMessage(error));
    return "reject";
  }
}

registerSystemEventDeliveryInspector(
  "durable-session-wake",
  inspectDurableSessionDeliveryForPrompt,
);

export function supersedeDurableSessionWakeForGenerationChange(params: {
  wakeId: string;
  deliveryRevision?: number;
  deliveryQueueId: string;
  sessionKey: string;
  queuedSessionKey?: string;
  expectedSessionId?: string;
  actualSessionId?: string;
}): void {
  if (!isDurableRuntimeEnabled()) {
    throw new Error("durable runtime is disabled while a durable session wake is pending");
  }
  const store = openDurableRuntimeStore();
  try {
    const binding = requireCurrentDurableSessionWakeBinding(store, params);
    const wake = store.supersedeWakeObligation({
      wakeId: params.wakeId,
      actorKind: "system_worker",
      actorRef: "session_delivery_recovery",
      reason: "target session generation changed before attached-session consumption",
      decisionRef: `session-delivery:${params.deliveryQueueId}`,
      idempotencyKey: `session-delivery-generation:${params.deliveryQueueId}`,
      expectedDeliveryRevision: binding.controlDeliveryRevision,
      evidence: {
        sessionKey: params.sessionKey,
        queuedSessionKey: params.queuedSessionKey ?? params.sessionKey,
        deliveryRevision: binding.deliveryRevision,
        expectedSessionId: params.expectedSessionId,
        actualSessionId: params.actualSessionId,
        deliveryQueueId: params.deliveryQueueId,
      },
      supersededByRef: params.actualSessionId,
    });
    if (!wake) {
      const current = store.getWakeObligation(params.wakeId);
      if (current?.status !== "acked" && current?.status !== "superseded") {
        throw new Error(`durable wake could not be superseded: ${params.wakeId}`);
      }
    }
  } finally {
    store.close();
  }
}

export function acknowledgeDurableSessionWakeConsumption(params: {
  wakeId: string;
  deliveryRevision?: number;
  deliveryQueueId: string;
  sessionKey: string;
  queuedSessionKey?: string;
  expectedSessionId?: string;
}): void {
  if (!isDurableRuntimeEnabled()) {
    throw new Error("durable runtime is disabled while a durable session wake is pending");
  }
  const store = openDurableRuntimeStore();
  try {
    const binding = requireCurrentDurableSessionWakeBinding(store, params);
    const wake = store.acknowledgeWakeObligation({
      wakeId: params.wakeId,
      actorKind: "system_worker",
      actorRef: "session_attention_consumer",
      reason: "target session completed an agent run containing the durable attention event",
      decisionRef: `session-delivery:${params.deliveryQueueId}`,
      idempotencyKey: `session-delivery-consumed:${params.deliveryQueueId}`,
      expectedDeliveryRevision: binding.controlDeliveryRevision,
      evidence: {
        sessionKey: params.sessionKey,
        queuedSessionKey: params.queuedSessionKey ?? params.sessionKey,
        deliveryRevision: binding.deliveryRevision,
        expectedSessionId: params.expectedSessionId,
        deliveryQueueId: params.deliveryQueueId,
        attachedSessionConsumptionProven: true,
      },
    });
    if (!wake) {
      const current = store.getWakeObligation(params.wakeId);
      if (current?.status !== "acked" && current?.status !== "superseded") {
        throw new Error(`durable wake could not be acknowledged: ${params.wakeId}`);
      }
    }
  } finally {
    store.close();
  }
}

type QueuedDurableSessionAttention = Extract<QueuedSessionDelivery, { kind: "systemEvent" }> & {
  source: { owner: "durable_wake"; ref: string };
};

function isQueuedDurableSessionAttention(
  entry: QueuedSessionDelivery,
): entry is QueuedDurableSessionAttention {
  return entry.kind === "systemEvent" && entry.source?.owner === "durable_wake";
}

/** Fail closed for unbound queue rows; return false for already-terminal wakes. */
export function isDurableSessionWakeActiveForDelivery(params: DurableSessionWakeBinding): boolean {
  if (!isDurableRuntimeEnabled()) {
    throw new Error("durable runtime is disabled while a durable session wake is pending");
  }
  const store = openDurableRuntimeStore();
  try {
    const { wake, staleRevision } = requireDurableSessionWakeBinding(store, params);
    const active = !staleRevision && wake.status !== "acked" && wake.status !== "superseded";
    if (!active && params.deliveryQueueId) {
      rejectSystemEventDeliveryQueueId(params.deliveryQueueId);
    }
    return active;
  } catch (error) {
    if (params.deliveryQueueId) {
      rejectSystemEventDeliveryQueueId(params.deliveryQueueId);
    }
    throw error;
  } finally {
    store.close();
  }
}

/** Replay only durable session attention entries during the existing recovery tick. */
export async function recoverDurableSessionAttentionDeliveries(params: {
  log: SessionDeliveryRecoveryLogger;
}): Promise<void> {
  await drainPendingSessionDeliveries({
    drainKey: "durable-session-attention",
    logLabel: "durable session attention",
    log: params.log,
    selectEntry: (entry) => ({ match: isQueuedDurableSessionAttention(entry) }),
    deliver: async (entry) => {
      if (!isQueuedDurableSessionAttention(entry)) {
        return undefined;
      }
      if (
        !isDurableSessionWakeActiveForDelivery({
          wakeId: entry.source.ref,
          deliveryRevision: entry.source.deliveryRevision,
          deliveryQueueId: entry.id,
          sessionKey: entry.sessionKey,
          queuedSessionKey: entry.sessionKey,
        })
      ) {
        return undefined;
      }
      const session = loadSessionEntry({ sessionKey: entry.sessionKey, readConsistency: "latest" });
      if (
        !session ||
        (entry.expectedSessionId !== undefined && session.sessionId !== entry.expectedSessionId)
      ) {
        supersedeDurableSessionWakeForGenerationChange({
          wakeId: entry.source.ref,
          deliveryRevision: entry.source.deliveryRevision,
          deliveryQueueId: entry.id,
          sessionKey: entry.sessionKey,
          queuedSessionKey: entry.sessionKey,
          expectedSessionId: entry.expectedSessionId,
          actualSessionId: session?.sessionId,
        });
        rejectSystemEventDeliveryQueueId(entry.id);
        return undefined;
      }
      if (peekConsumedSystemEventDeliveryQueueIds(entry.sessionKey).includes(entry.id)) {
        return { acknowledgement: "deferred" as const };
      }
      const alreadyAdmitted = peekSystemEventEntries(entry.sessionKey).some((event) =>
        event.deliveryQueueIds?.includes(entry.id),
      );
      if (!alreadyAdmitted) {
        enqueueSystemEventEntry(entry.text, {
          sessionKey: entry.sessionKey,
          contextKey: entry.idempotencyKey,
          deliveryContext: entry.deliveryContext,
          deliveryQueueId: entry.id,
        });
        requestHeartbeat({
          source: "other",
          intent: "immediate",
          reason: "durable-attention-recovery",
          sessionKey: entry.sessionKey,
        });
      }
      return { acknowledgement: "deferred" as const };
    },
  });
}

function formatInterruptedSessionAttention(wake: WakeObligation): string {
  const sourceRun = wake.sourceRunId?.trim();
  if (
    wake.reason === "child_terminal" ||
    wake.reason === "child_overdue" ||
    wake.reason === "fan_in_incomplete"
  ) {
    return [
      `Durable owner attention is required for ${wake.sourceOwner}:${wake.sourceRef}.`,
      `Reason: ${wake.reason}.`,
      `Inspect durable wake ${wake.wakeId}${sourceRun ? ` and execution ${sourceRun}` : ""} before choosing the next step.`,
      "Send a concise progress or terminal update; do not repeat an uncertain external side effect without reconciliation.",
    ].join(" ");
  }
  return [
    "A previously accepted agent operation was interrupted by a runtime restart and did not reach a proven terminal result.",
    "Its outcome is uncertain. Do not repeat tools or external side effects automatically.",
    `Inspect durable wake ${wake.wakeId}${sourceRun ? ` and execution ${sourceRun}` : ""} before deciding whether to retry.`,
    "Send a concise status update, or request explicit retry approval when the prior side effect cannot be reconciled.",
  ].join(" ");
}

export const sessionStoreOwnerAdapter: DurableOwnerAdapter = {
  sourceOwner: "session_store",

  inspect(): DurableOwnerAttentionFact | undefined {
    return undefined;
  },

  listAttentionFacts(): DurableOwnerAttentionFact[] {
    return [];
  },

  async dispatchAttention({ wake }): Promise<DurableOwnerDispatchResult> {
    const sessionKey = wake.targetRef?.trim() || wake.ownerRef?.trim() || wake.sourceRef.trim();
    const result = await requestSessionAttentionDelivery({
      sessionKey,
      text: formatInterruptedSessionAttention(wake),
      idempotencyKey: `durable-wake:${wake.wakeId}`,
      wakeId: wake.wakeId,
      deliveryRevision: wake.deliveryRevision,
    });
    if (result.status === "missing") {
      return {
        kind: "suspended",
        reason: result.reason === "session_not_found" ? "canonical_session_missing" : result.reason,
      };
    }
    return {
      kind: "handoff_accepted",
      evidence: {
        proofBoundary: "persistent_session_queue_acceptance",
        ownerResult: "session_delivery_enqueued",
        sessionKey: result.sessionKey,
        sessionId: result.sessionId,
        deliveryQueueId: result.deliveryQueueId,
        duplicate: result.duplicate,
        immediateAdmission: result.immediateAdmission,
        queuedAt: result.queuedAt,
        generationFenced: Boolean(result.sessionId),
        attachedSessionConsumptionProven: false,
        userDeliveryProven: false,
      },
    };
  },
};
