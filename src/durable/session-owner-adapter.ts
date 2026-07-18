import { requestSessionAttentionDelivery } from "../sessions/session-attention.js";
import type {
  DurableOwnerAdapter,
  DurableOwnerAttentionFact,
  DurableOwnerDispatchResult,
} from "./owner-adapter-contract.js";
import type { WakeObligation } from "./types.js";

function formatInterruptedSessionAttention(wake: WakeObligation): string {
  const sourceRun = wake.sourceRunId?.trim();
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
    const result = requestSessionAttentionDelivery({
      sessionKey,
      text: formatInterruptedSessionAttention(wake),
      idempotencyKey: `durable-wake:${wake.wakeId}`,
    });
    if (result.status === "missing") {
      return {
        kind: "suspended",
        reason: result.reason === "session_not_found" ? "canonical_session_missing" : result.reason,
      };
    }
    return {
      kind: "delivered",
      evidence: {
        proofBoundary: "target_session_handoff",
        ownerResult: "system_event_queued",
        sessionKey: result.sessionKey,
        sessionId: result.sessionId,
        duplicate: result.duplicate,
        queuedAt: result.queuedAt,
        userDeliveryProven: false,
      },
    };
  },
};
