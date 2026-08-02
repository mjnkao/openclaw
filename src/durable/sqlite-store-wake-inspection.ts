import type { DurableSqliteStoreContext } from "./sqlite-store-context.js";
import {
  MAX_WAKE_INSPECTION_RELATED_ITEMS,
  MAX_WAKE_INSPECTION_OCCURRENCE_KEYS,
  isRecordValue,
} from "./sqlite-store-support-core.js";
import { queryFirst, queryRows } from "./sqlite-store-support-query.js";
import type { WakeClaimOperations } from "./sqlite-store-wake-claims.js";
import type { WakeControlOperations } from "./sqlite-store-wake-control.js";
import type { WakeObligationInspection } from "./types.js";

export function createWakeInspectionOperations(
  context: DurableSqliteStoreContext,
  wakeControlOperations: WakeControlOperations,
  wakeClaimOperations: WakeClaimOperations,
) {
  const { db, durableDb } = context;
  const { getWakeObligationRecord, storeListUncertaintyFacts } = wakeControlOperations;
  const { listDeliveryAttemptEvidenceRecords } = wakeClaimOperations;
  const getWakeObligationInspectionRecord = (
    wakeId: string,
  ): WakeObligationInspection | undefined => {
    const wake = getWakeObligationRecord(wakeId);
    if (!wake) {
      return undefined;
    }
    const metadata = wake.metadata ?? {};
    const diagnostics = isRecordValue(metadata.diagnostics) ? metadata.diagnostics : undefined;
    const evidence = isRecordValue(metadata.evidence) ? metadata.evidence : undefined;
    const unresolvedUncertaintyFacts = storeListUncertaintyFacts({
      sourceOwner: wake.sourceOwner,
      sourceRef: wake.sourceRef,
      status: "open",
      limit: MAX_WAKE_INSPECTION_RELATED_ITEMS,
    });
    const unresolvedUncertaintyFactCount = Number(
      queryFirst<{ count: number | bigint }>(
        db,
        durableDb
          .selectFrom("uncertainty_facts")
          .select((eb) => eb.fn.countAll<number>().as("count"))
          .where("source_owner", "=", wake.sourceOwner)
          .where("source_ref", "=", wake.sourceRef)
          .where("status", "=", "open"),
      )?.count ?? 0,
    );
    const deliveryAttemptEvidence = listDeliveryAttemptEvidenceRecords({
      wakeId,
      limit: MAX_WAKE_INSPECTION_RELATED_ITEMS,
    });
    const deliveryAttemptEvidenceCount = Number(
      queryFirst<{ count: number | bigint }>(
        db,
        durableDb
          .selectFrom("delivery_attempt_evidence")
          .select((eb) => eb.fn.countAll<number>().as("count"))
          .where("wake_id", "=", wakeId),
      )?.count ?? 0,
    );
    const occurrenceRows = queryRows<{ occurrence_key: string }>(
      db,
      durableDb
        .selectFrom("wake_obligation_occurrences")
        .select("occurrence_key")
        .where("wake_id", "=", wakeId)
        .orderBy("observed_at", "desc")
        .orderBy("occurrence_key", "desc")
        .limit(MAX_WAKE_INSPECTION_OCCURRENCE_KEYS),
    );
    const occurrenceKeys = occurrenceRows.toReversed().map((row) => row.occurrence_key);
    const occurrenceCount = Number(
      queryFirst<{ count: number | bigint }>(
        db,
        durableDb
          .selectFrom("wake_obligation_occurrences")
          .select((eb) => eb.fn.countAll<number>().as("count"))
          .where("wake_id", "=", wakeId),
      )?.count ?? 0,
    );
    return {
      wake,
      targetResolution: {
        ...(wake.targetResolutionStatus ? { status: wake.targetResolutionStatus } : {}),
        ...(wake.targetResolutionReason ? { reason: wake.targetResolutionReason } : {}),
        ...(wake.targetKind ? { targetKind: wake.targetKind } : {}),
        ...(wake.targetRef ? { targetRef: wake.targetRef } : {}),
        ...(wake.ownerKind ? { ownerKind: wake.ownerKind } : {}),
        ...(wake.ownerRef ? { ownerRef: wake.ownerRef } : {}),
        ...(wake.reportRouteRef ? { reportRouteRef: wake.reportRouteRef } : {}),
        ...(wake.factsRef ? { factsRef: wake.factsRef } : {}),
        ...(wake.sourceRunId ? { sourceRunId: wake.sourceRunId } : {}),
        ...(diagnostics ? { diagnostics } : {}),
        ...(evidence ? { evidence } : {}),
      },
      deliveryAttemptEvidence,
      deliveryAttemptEvidenceCount,
      deliveryAttemptEvidenceTruncated:
        deliveryAttemptEvidenceCount > deliveryAttemptEvidence.length,
      unresolvedUncertaintyFacts,
      unresolvedUncertaintyFactCount,
      unresolvedUncertaintyFactsTruncated:
        unresolvedUncertaintyFactCount > unresolvedUncertaintyFacts.length,
      sourceRefs: {
        sourceOwner: wake.sourceOwner,
        sourceRef: wake.sourceRef,
        ...(wake.factsRef ? { factsRef: wake.factsRef } : {}),
        ...(wake.sourceRunId ? { sourceRunId: wake.sourceRunId } : {}),
        ...(wake.sourceRevision ? { sourceRevision: wake.sourceRevision } : {}),
        occurrenceKeys,
        occurrenceCount,
        occurrenceKeysTruncated: occurrenceCount > occurrenceKeys.length,
        ...(wake.parentRunId ? { parentRunId: wake.parentRunId } : {}),
        ...(wake.parentSessionKey ? { parentSessionKey: wake.parentSessionKey } : {}),
      },
    };
  };

  return {
    getWakeObligationInspectionRecord,
  };
}

export type WakeInspectionOperations = ReturnType<typeof createWakeInspectionOperations>;
