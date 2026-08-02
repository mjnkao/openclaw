import { randomUUID } from "node:crypto";
import { runSqliteImmediateTransactionSync } from "../infra/sqlite-transaction.js";
import type { DurableSqliteStoreContext } from "./sqlite-store-context.js";
import {
  optionalText,
  requireSourceRef,
  serializeJson,
  assertCompatibleUncertaintyReplay,
  mergeMetadataJson,
} from "./sqlite-store-support-core.js";
import type { UncertaintyFactRow } from "./sqlite-store-support-core.js";
import {
  rowToUncertaintyFact,
  queryFirst,
  executeQuery,
  isSameSqlValue,
} from "./sqlite-store-support-query.js";
import type { WakeClaimOperations } from "./sqlite-store-wake-claims.js";
import type { WakeControlOperations } from "./sqlite-store-wake-control.js";
import type { WakeInspectionOperations } from "./sqlite-store-wake-inspection.js";
import type { WakeReconciliationOperations } from "./sqlite-store-wake-reconciliation.js";
import type {
  WakeObligationStatus,
  WakeObligationTargetKind,
  WakeObligationOwnerKind,
  WakeObligationTargetResolutionStatus,
  UncertaintyFactStatus,
  DeliveryAttemptEvidenceStatus,
  WakeObligation,
  UncertaintyFact,
  DeliveryAttemptEvidence,
  WakeObligationInspection,
  WakeObligationPage,
  CreateWakeObligationInput,
  ReconcileWakeObligationInput,
  ReconcileWakeObligationResult,
  UpdateWakeObligationProjectionInput,
  SuspendWakeObligationInput,
  WakeObligationControlInput,
  SupersedeWakeObligationInput,
  MarkWakeObligationDecisionRequiredInput,
  ResumeWakeObligationInput,
  CreateUncertaintyFactInput,
  ResolveUncertaintyFactInput,
  WakeObligationClaim,
  ClaimNextWakeObligationInput,
  RenewWakeObligationClaimInput,
  CompleteWakeObligationClaimInput,
} from "./types.js";

export function createWakeMethods(
  context: DurableSqliteStoreContext,
  wakeReconciliationOperations: WakeReconciliationOperations,
  wakeControlOperations: WakeControlOperations,
  wakeClaimOperations: WakeClaimOperations,
  wakeInspectionOperations: WakeInspectionOperations,
) {
  const { db, durableDb } = context;
  const { createWakeObligationRecord, reconcileWakeObligationRecord } =
    wakeReconciliationOperations;
  const {
    updateWakeObligationProjectionRecord,
    suspendWakeObligationRecord,
    getWakeObligationRecord,
    getWakeObligationByOccurrenceKeyRecord,
    listWakeObligationRecords,
    listUnresolvedWakeObligationsPageRecords,
    storeListUncertaintyFacts,
    acknowledgeWakeObligationRecord,
    supersedeWakeObligationRecord,
    resumeWakeObligationRecord,
    markWakeObligationDecisionRequiredRecord,
  } = wakeControlOperations;
  const {
    claimNextWakeObligationRecord,
    completeWakeObligationClaimRecord,
    renewWakeObligationClaimRecord,
    getDeliveryAttemptEvidenceRecord,
    listDeliveryAttemptEvidenceRecords,
  } = wakeClaimOperations;
  const { getWakeObligationInspectionRecord } = wakeInspectionOperations;
  return {
    reconcileWakeObligation(input: ReconcileWakeObligationInput): ReconcileWakeObligationResult {
      return reconcileWakeObligationRecord(input);
    },

    createWakeObligation(input: CreateWakeObligationInput): WakeObligation {
      return createWakeObligationRecord(input);
    },

    updateWakeObligationProjection(
      input: UpdateWakeObligationProjectionInput,
    ): WakeObligation | undefined {
      return updateWakeObligationProjectionRecord(input);
    },

    suspendWakeObligation(input: SuspendWakeObligationInput): WakeObligation | undefined {
      return suspendWakeObligationRecord(input);
    },

    acknowledgeWakeObligation(input: WakeObligationControlInput): WakeObligation | undefined {
      return acknowledgeWakeObligationRecord(input);
    },

    supersedeWakeObligation(input: SupersedeWakeObligationInput): WakeObligation | undefined {
      return supersedeWakeObligationRecord(input);
    },

    resumeWakeObligation(input: ResumeWakeObligationInput): WakeObligation | undefined {
      return resumeWakeObligationRecord(input);
    },

    markWakeObligationDecisionRequired(
      input: MarkWakeObligationDecisionRequiredInput,
    ): WakeObligation | undefined {
      return markWakeObligationDecisionRequiredRecord(input);
    },

    getWakeObligation(wakeId: string): WakeObligation | undefined {
      return getWakeObligationRecord(wakeId);
    },

    getWakeObligationByOccurrenceKey(input: {
      sourceOwner: string;
      sourceRef: string;
      occurrenceKey: string;
    }): WakeObligation | undefined {
      return getWakeObligationByOccurrenceKeyRecord(input);
    },

    getWakeObligationInspection(wakeId: string): WakeObligationInspection | undefined {
      return getWakeObligationInspectionRecord(wakeId);
    },

    listWakeObligations(options?: {
      sourceOwner?: string;
      sourceRef?: string;
      parentRunId?: string;
      parentSessionKey?: string;
      targetKind?: WakeObligationTargetKind;
      targetRef?: string;
      ownerKind?: WakeObligationOwnerKind;
      ownerRef?: string;
      reportRouteRef?: string;
      targetResolutionStatus?: WakeObligationTargetResolutionStatus;
      status?: WakeObligationStatus;
      limit?: number;
    }): WakeObligation[] {
      return listWakeObligationRecords(options);
    },

    listUnresolvedWakeObligationsPage(input: {
      sourceOwner?: string;
      createdAtOrBefore?: number;
      cursor?: string;
      limit: number;
    }): WakeObligationPage {
      return listUnresolvedWakeObligationsPageRecords(input);
    },

    recordUncertaintyFact(input: CreateUncertaintyFactInput): UncertaintyFact {
      const { sourceOwner, sourceRef } = requireSourceRef(input, "Durable uncertainty fact");
      const now = input.now ?? Date.now();
      const factId = input.factId ?? `uncertain_${randomUUID()}`;
      const dedupeKey = optionalText(input.dedupeKey);
      return runSqliteImmediateTransactionSync(db, () => {
        const existingById = input.factId
          ? queryFirst<UncertaintyFactRow>(
              db,
              durableDb
                .selectFrom("uncertainty_facts")
                .selectAll()
                .where("fact_id", "=", input.factId),
            )
          : undefined;
        const existingByDedupe = dedupeKey
          ? queryFirst<UncertaintyFactRow>(
              db,
              durableDb
                .selectFrom("uncertainty_facts")
                .selectAll()
                .where("source_owner", "=", sourceOwner)
                .where("source_ref", "=", sourceRef)
                .where("dedupe_key", "=", dedupeKey),
            )
          : undefined;
        if (existingById && existingByDedupe && existingById.fact_id !== existingByDedupe.fact_id) {
          throw new Error(`Durable uncertainty replay conflict for fact id ${input.factId}`);
        }
        const existing = existingById ?? existingByDedupe;
        if (existing) {
          assertCompatibleUncertaintyReplay(
            existing,
            input,
            existingById
              ? `fact id ${existing.fact_id}`
              : `${sourceOwner}:${sourceRef}:${dedupeKey}`,
          );
          return rowToUncertaintyFact(existing);
        }
        executeQuery(
          db,
          durableDb.insertInto("uncertainty_facts").values({
            fact_id: factId,
            source_owner: sourceOwner,
            source_ref: sourceRef,
            kind: input.kind,
            source_run_id: optionalText(input.sourceRunId),
            step_id: optionalText(input.stepId),
            event_id: optionalText(input.eventId),
            ref_id: optionalText(input.refId),
            facts_ref: optionalText(input.factsRef),
            dedupe_key: dedupeKey,
            facts_json: serializeJson(input.facts),
            status: "open",
            resolution_kind: null,
            resolution_ref: null,
            resolved_at: null,
            created_at: now,
            updated_at: now,
            metadata_json: serializeJson(input.metadata),
          }),
        );
        const row = queryFirst<UncertaintyFactRow>(
          db,
          durableDb.selectFrom("uncertainty_facts").selectAll().where("fact_id", "=", factId),
        );
        return rowToUncertaintyFact(row!);
      });
    },

    resolveUncertaintyFact(input: ResolveUncertaintyFactInput): UncertaintyFact | undefined {
      const now = input.now ?? Date.now();
      return runSqliteImmediateTransactionSync(db, () => {
        const current = queryFirst<UncertaintyFactRow>(
          db,
          durableDb.selectFrom("uncertainty_facts").selectAll().where("fact_id", "=", input.factId),
        );
        if (!current) {
          return undefined;
        }
        if (
          input.expectedUpdatedAt !== undefined &&
          current.updated_at !== input.expectedUpdatedAt
        ) {
          return undefined;
        }
        if (current.status !== "open") {
          const isNoOp =
            input.status === current.status &&
            isSameSqlValue(optionalText(input.resolutionKind), current.resolution_kind) &&
            isSameSqlValue(optionalText(input.resolutionRef), current.resolution_ref) &&
            isSameSqlValue(
              mergeMetadataJson(current.metadata_json, input.metadata),
              current.metadata_json,
            );
          return isNoOp ? rowToUncertaintyFact(current) : undefined;
        }
        executeQuery(
          db,
          durableDb
            .updateTable("uncertainty_facts")
            .set({
              status: input.status,
              resolution_kind: optionalText(input.resolutionKind),
              resolution_ref: optionalText(input.resolutionRef),
              resolved_at: now,
              updated_at: now,
              metadata_json: mergeMetadataJson(current.metadata_json, input.metadata),
            })
            .where("fact_id", "=", input.factId),
        );
        const row = queryFirst<UncertaintyFactRow>(
          db,
          durableDb.selectFrom("uncertainty_facts").selectAll().where("fact_id", "=", input.factId),
        );
        return rowToUncertaintyFact(row!);
      });
    },

    listUncertaintyFacts(options?: {
      sourceOwner?: string;
      sourceRef?: string;
      sourceRunId?: string;
      status?: UncertaintyFactStatus;
      limit?: number;
    }): UncertaintyFact[] {
      return storeListUncertaintyFacts(options);
    },

    claimNextWakeObligation(input: ClaimNextWakeObligationInput): WakeObligationClaim | undefined {
      return claimNextWakeObligationRecord(input);
    },

    renewWakeObligationClaim(input: RenewWakeObligationClaimInput): boolean {
      return renewWakeObligationClaimRecord(input);
    },

    completeWakeObligationClaim(
      input: CompleteWakeObligationClaimInput,
    ): DeliveryAttemptEvidence | undefined {
      return completeWakeObligationClaimRecord(input);
    },

    getDeliveryAttemptEvidence(deliveryAttemptId: string): DeliveryAttemptEvidence | undefined {
      return getDeliveryAttemptEvidenceRecord(deliveryAttemptId);
    },

    listDeliveryAttemptEvidence(options?: {
      wakeId?: string;
      dedupeKey?: string;
      status?: DeliveryAttemptEvidenceStatus;
      limit?: number;
    }): DeliveryAttemptEvidence[] {
      return listDeliveryAttemptEvidenceRecords(options);
    },

    listPendingWakeObligations(options?: { limit?: number }): WakeObligation[] {
      return listWakeObligationRecords({ status: "pending", limit: options?.limit });
    },

    listUnresolvedUncertaintyFacts(options?: {
      sourceRunId?: string;
      limit?: number;
    }): UncertaintyFact[] {
      return storeListUncertaintyFacts({
        sourceRunId: options?.sourceRunId,
        status: "open",
        limit: options?.limit,
      });
    },
  };
}
