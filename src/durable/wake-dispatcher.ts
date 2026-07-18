import { createSubsystemLogger } from "../logging/subsystem.js";
import { recordDurableRuntimeHealthFailure, recordDurableRuntimeHealthSuccess } from "./health.js";
import { getDurableOwnerAdapter, reconcileDurableOwnerAttentionFacts } from "./owner-adapters.js";
import type { DurableRuntimeStore, WakeObligation } from "./types.js";

const log = createSubsystemLogger("durable/wake-dispatcher");

const DEFAULT_WAKE_RETRY_BASE_MS = 15_000;
const DEFAULT_WAKE_RETRY_MAX_MS = 5 * 60_000;
const DEFAULT_WAKE_CLAIM_TTL_MS = 60_000;
const DEFAULT_WAKE_MAX_ATTEMPTS = 6;
const DEFAULT_NO_SILENCE_SLA_MS = 2 * 60_000;

export type DurableWakeDispatcherResult = {
  ownerFactsScanned: number;
  obligationsCreated: number;
  claimed: number;
  delivered: number;
  failed: number;
  suspended: number;
  superseded: number;
  overdue: number;
};

function markOverdueWakes(params: {
  store: DurableRuntimeStore;
  now: number;
  slaMs: number;
  limit: number;
}): number {
  const wakes = params.store
    .listWakeObligations({ limit: params.limit })
    .filter(
      (wake) =>
        wake.status !== "acked" &&
        wake.status !== "superseded" &&
        params.now - wake.createdAt >= params.slaMs,
    );
  for (const wake of wakes) {
    const existingMetadata = wake.metadata ?? {};
    params.store.updateWakeObligationProjection({
      wakeId: wake.wakeId,
      metadata: {
        ...existingMetadata,
        diagnostics: {
          ...(typeof existingMetadata.diagnostics === "object" && existingMetadata.diagnostics
            ? existingMetadata.diagnostics
            : {}),
          noSilenceSla: {
            overdue: true,
            ageMs: params.now - wake.createdAt,
            slaMs: params.slaMs,
            detectedAt: params.now,
            nextAction:
              wake.status === "suspended"
                ? "inspect_then_authorize_owner_decision"
                : "inspect_target_and_delivery_attempts",
          },
        },
      },
      now: params.now,
    });
  }
  return wakes.length;
}

function completeDispatch(params: {
  store: DurableRuntimeStore;
  claim: NonNullable<ReturnType<DurableRuntimeStore["claimNextWakeObligation"]>>;
  result:
    | { kind: "delivered"; evidence: Record<string, unknown> }
    | { kind: "failed"; reason: string; evidence?: Record<string, unknown> }
    | { kind: "suspended"; reason: string; evidence?: Record<string, unknown>; unknown?: boolean }
    | { kind: "superseded"; reason: string; evidence?: Record<string, unknown> };
  now: number;
}): boolean {
  const { claim, result } = params;
  const attemptStatus =
    result.kind === "delivered"
      ? "delivered"
      : result.kind === "superseded"
        ? "superseded"
        : result.kind === "suspended" && result.unknown
          ? "unknown"
          : "failed";
  const wakeStatus =
    result.kind === "delivered"
      ? "delivered"
      : result.kind === "superseded"
        ? "superseded"
        : result.kind === "suspended"
          ? "suspended"
          : "failed";
  return Boolean(
    params.store.completeWakeObligationClaim({
      wakeId: claim.wake.wakeId,
      deliveryAttemptId: claim.deliveryAttempt.deliveryAttemptId,
      claimToken: claim.claimToken,
      attemptStatus,
      wakeStatus,
      evidence: result.evidence,
      error: "reason" in result ? result.reason : undefined,
      now: params.now,
    }),
  );
}

function unavailableTargetReason(wake: WakeObligation): string | undefined {
  if (
    wake.targetResolutionStatus === "missing" ||
    wake.targetResolutionStatus === "ambiguous" ||
    wake.targetResolutionStatus === "unauthorized" ||
    wake.targetResolutionStatus === "inspect_only"
  ) {
    return wake.targetResolutionReason ?? `target_${wake.targetResolutionStatus}`;
  }
  return undefined;
}

export async function runDurableWakeDispatcherOnce(params: {
  store: DurableRuntimeStore;
  workerId: string;
  now?: number;
  limit?: number;
  claimTtlMs?: number;
  retryBaseMs?: number;
  retryMaxMs?: number;
  maxAttempts?: number;
  noSilenceSlaMs?: number;
}): Promise<DurableWakeDispatcherResult> {
  const currentTime = () => params.now ?? Date.now();
  const now = currentTime();
  const limit = Math.max(1, Math.min(100, Math.trunc(params.limit ?? 25)));
  const claimTtlMs = params.claimTtlMs ?? DEFAULT_WAKE_CLAIM_TTL_MS;
  const retryBaseMs = params.retryBaseMs ?? DEFAULT_WAKE_RETRY_BASE_MS;
  const retryMaxMs = params.retryMaxMs ?? DEFAULT_WAKE_RETRY_MAX_MS;
  const maxAttempts = params.maxAttempts ?? DEFAULT_WAKE_MAX_ATTEMPTS;
  const reconciliation = reconcileDurableOwnerAttentionFacts({
    store: params.store,
    now,
    limit: 500,
  });
  const result: DurableWakeDispatcherResult = {
    ownerFactsScanned: reconciliation.scanned,
    obligationsCreated: reconciliation.created,
    claimed: 0,
    delivered: 0,
    failed: 0,
    suspended: reconciliation.suspended,
    superseded: 0,
    overdue: markOverdueWakes({
      store: params.store,
      now,
      slaMs: params.noSilenceSlaMs ?? DEFAULT_NO_SILENCE_SLA_MS,
      limit: 500,
    }),
  };

  for (let index = 0; index < limit; index += 1) {
    const claimTime = currentTime();
    const claim = params.store.claimNextWakeObligation({
      workerId: params.workerId,
      claimTtlMs,
      retryBaseMs,
      retryMaxMs,
      now: claimTime,
    });
    if (!claim) {
      break;
    }
    result.claimed += 1;

    if (claim.wake.attemptCount > maxAttempts) {
      if (
        completeDispatch({
          store: params.store,
          claim,
          result: { kind: "suspended", reason: "wake_retry_limit" },
          now: currentTime(),
        })
      ) {
        result.suspended += 1;
      }
      continue;
    }

    const targetError = unavailableTargetReason(claim.wake);
    if (targetError) {
      if (
        completeDispatch({
          store: params.store,
          claim,
          result: { kind: "suspended", reason: targetError },
          now: currentTime(),
        })
      ) {
        result.suspended += 1;
      }
      continue;
    }

    const adapter = getDurableOwnerAdapter(claim.wake.sourceOwner);
    if (!adapter) {
      if (
        completeDispatch({
          store: params.store,
          claim,
          result: { kind: "suspended", reason: "owner_adapter_not_registered" },
          now: currentTime(),
        })
      ) {
        result.suspended += 1;
      }
      continue;
    }

    let claimLost = false;
    const renewalIntervalMs = Math.max(100, Math.floor(claimTtlMs / 3));
    const renewalTimer =
      params.now === undefined
        ? setInterval(() => {
            if (
              !params.store.renewWakeObligationClaim({
                wakeId: claim.wake.wakeId,
                deliveryAttemptId: claim.deliveryAttempt.deliveryAttemptId,
                claimToken: claim.claimToken,
                claimTtlMs,
                now: currentTime(),
              })
            ) {
              claimLost = true;
            }
          }, renewalIntervalMs)
        : undefined;
    renewalTimer?.unref();

    try {
      const dispatch = await adapter.dispatchAttention({
        wake: claim.wake,
        claimToken: claim.claimToken,
      });
      if (claimLost) {
        throw new Error(`wake claim lost during dispatch: ${claim.wake.wakeId}`);
      }
      const completion =
        dispatch.kind === "delivered"
          ? ({ kind: "delivered", evidence: dispatch.evidence } as const)
          : dispatch.kind === "superseded"
            ? ({
                kind: "superseded",
                reason: dispatch.reason,
                evidence: dispatch.evidence,
              } as const)
            : dispatch.kind === "suspended"
              ? ({
                  kind: "suspended",
                  reason: dispatch.reason,
                  evidence: dispatch.evidence,
                } as const)
              : ({
                  kind: "failed",
                  reason: dispatch.reason,
                  evidence: dispatch.evidence,
                } as const);
      if (
        !completeDispatch({ store: params.store, claim, result: completion, now: currentTime() })
      ) {
        throw new Error(`wake claim became stale before completion: ${claim.wake.wakeId}`);
      }
      if (completion.kind === "delivered") {
        result.delivered += 1;
      } else if (completion.kind === "superseded") {
        result.superseded += 1;
      } else if (completion.kind === "suspended") {
        result.suspended += 1;
      } else {
        result.failed += 1;
      }
      recordDurableRuntimeHealthSuccess(currentTime());
    } catch (error) {
      recordDurableRuntimeHealthFailure({
        component: "wake_dispatcher",
        operation: "dispatch_attention",
        error,
        now: currentTime(),
      });
      log.warn(`wake dispatch failed with unknown outcome: ${String(error)}`);
      if (
        completeDispatch({
          store: params.store,
          claim,
          result: {
            kind: "suspended",
            reason: String(error),
            unknown: true,
          },
          now: currentTime(),
        })
      ) {
        result.suspended += 1;
      }
    } finally {
      if (renewalTimer) {
        clearInterval(renewalTimer);
      }
    }
  }

  return result;
}
