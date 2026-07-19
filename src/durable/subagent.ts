import { createSubsystemLogger } from "../logging/subsystem.js";
import { isDurableRuntimeEnabled } from "./config.js";
import { recordDurableRuntimeHealthFailure, recordDurableRuntimeHealthSuccess } from "./health.js";
import { reconcileDurableOwnerAttentionFact, subagentRunsOwnerAdapter } from "./owner-adapters.js";
import { openDurableRuntimeStore } from "./store-factory.js";

const log = createSubsystemLogger("durable/subagent-owner");

function recordSubagentOwnerProjection(params: { operation: string; action: () => void }): void {
  try {
    params.action();
    recordDurableRuntimeHealthSuccess();
  } catch (error) {
    recordDurableRuntimeHealthFailure({
      component: "subagent_owner",
      operation: params.operation,
      error,
    });
    log.error(`durable subagent owner projection ${params.operation} failed: ${String(error)}`);
  }
}

export function recordDurableSubagentTerminal(params: {
  runId: string;
  childSessionKey?: string;
  status?: string;
  error?: string;
  summary?: string;
  env?: NodeJS.ProcessEnv;
}): void {
  const env = params.env ?? process.env;
  if (!isDurableRuntimeEnabled(env)) {
    return;
  }
  recordSubagentOwnerProjection({
    operation: "terminal",
    action: () => {
      const fact = subagentRunsOwnerAdapter.inspect(params.runId);
      if (!fact) {
        return;
      }
      const store = openDurableRuntimeStore({ env });
      try {
        reconcileDurableOwnerAttentionFact({ store, fact, now: Date.now() });
      } finally {
        store.close();
      }
    },
  });
}

export function recordDurableSubagentAnnounceDelivery(params: {
  runId: string;
  childSessionKey?: string;
  directIdempotencyKey?: string;
  delivered: boolean;
  path?: string;
  error?: string;
  reason?: string;
  env?: NodeJS.ProcessEnv;
}): void {
  const env = params.env ?? process.env;
  if (!isDurableRuntimeEnabled(env)) {
    return;
  }
  recordSubagentOwnerProjection({
    operation: "announce_delivery",
    action: () => {
      const now = Date.now();
      const store = openDurableRuntimeStore({ env });
      try {
        const fact = subagentRunsOwnerAdapter.inspect(params.runId);
        if (fact) {
          reconcileDurableOwnerAttentionFact({ store, fact, now });
        }
        // Canonical subagent delivery state is the evidence boundary. The leased
        // dispatcher observes it on the next pass; this callback must not create
        // an unleased competing delivery attempt or infer requester consumption.
      } finally {
        store.close();
      }
    },
  });
}

export function recordDurableSubagentInterrupted(params: {
  runId: string;
  childSessionKey: string;
  requesterSessionKey: string;
  reason: "gateway-restart" | "lost-execution-context";
  interruptedAt?: number;
  env?: NodeJS.ProcessEnv;
}): void {
  const env = params.env ?? process.env;
  if (!isDurableRuntimeEnabled(env)) {
    return;
  }
  recordSubagentOwnerProjection({
    operation: "interrupted",
    action: () => {
      const now = params.interruptedAt ?? Date.now();
      const store = openDurableRuntimeStore({ env });
      try {
        const fact = store.recordUncertaintyFact({
          sourceOwner: "subagent_runs",
          sourceRef: params.runId,
          kind: "requires_owner_decision",
          refId: params.childSessionKey,
          dedupeKey: `subagent-interrupted:${params.runId}:${params.reason}`,
          facts: {
            childSessionKey: params.childSessionKey,
            requesterSessionKey: params.requesterSessionKey,
            interruptionReason: params.reason,
            interruptedAt: now,
          },
          now,
        });
        store.createWakeObligation({
          sourceOwner: "subagent_runs",
          sourceRef: params.runId,
          parentSessionKey: params.requesterSessionKey,
          targetKind: "agent_session",
          targetRef: params.requesterSessionKey,
          ownerKind: "agent_session",
          ownerRef: params.requesterSessionKey,
          reportRouteRef: params.requesterSessionKey,
          targetResolutionStatus: "resolved",
          targetResolutionReason: "requester session resolved from canonical subagent owner",
          reason: "restart_interrupted",
          factsRef: `uncertainty_facts:${fact.factId}`,
          dedupeKey: `subagent-interrupted-wake:${params.runId}:${params.requesterSessionKey}`,
          metadata: {
            childSessionKey: params.childSessionKey,
            interruptionReason: params.reason,
            interruptedAt: now,
          },
          now,
        });
      } finally {
        store.close();
      }
    },
  });
}
