import { runSqliteImmediateTransactionSync } from "../infra/sqlite-transaction.js";
import type { DurableSqliteStoreContext } from "./sqlite-store-context.js";
import {
  optionalText,
  metadataText,
  WAKE_OBLIGATION_LEASE_SCOPE,
  MAX_UNRESOLVED_WAKE_PAGE_SIZE,
  UNRESOLVED_WAKE_CURSOR_VERSION,
  requirePositiveSafeInteger,
  requireSourceRef,
  serializeJson,
  sanitizeWakeProjectionMetadata,
  parseMetadata,
  WAKE_SUSPENSION_METADATA_KEY,
  wakeProjectionFingerprint,
  buildWakeControlDecision,
  mergeWakeControlMetadata,
  matchesExpectedWakeRevision,
  isMatchingControlNoop,
} from "./sqlite-store-support-core.js";
import type {
  WakeObligationRow,
  UncertaintyFactRow,
  DeliveryAttemptEvidenceRow,
} from "./sqlite-store-support-core.js";
import {
  rowToWakeObligation,
  rowToUncertaintyFact,
  queryRows,
  queryFirst,
  executeQuery,
  normalizeQueryLimit,
  encodeUnresolvedWakePageCursor,
  decodeUnresolvedWakePageCursor,
  isTerminalWakeStatus,
  unresolvedWakeStatusSql,
  isAllowedWakeStatusTransition,
  isSameSqlValue,
} from "./sqlite-store-support-query.js";
import type { WakeReconciliationOperations } from "./sqlite-store-wake-reconciliation.js";
import type {
  WakeObligationStatus,
  WakeObligationTargetKind,
  WakeObligationOwnerKind,
  WakeObligationTargetResolutionStatus,
  UncertaintyFactStatus,
  DeliveryAttemptEvidenceStatus,
  WakeObligationSuspensionClass,
  WakeObligation,
  UncertaintyFact,
  WakeObligationPage,
  UpdateWakeObligationProjectionInput,
  SuspendWakeObligationInput,
  WakeObligationControlInput,
  SupersedeWakeObligationInput,
  MarkWakeObligationDecisionRequiredInput,
  ResumeWakeObligationInput,
} from "./types.js";

export function createWakeControlOperations(
  context: DurableSqliteStoreContext,
  wakeReconciliationOperations: WakeReconciliationOperations,
) {
  const { db, durableDb } = context;
  const { finalizeUnknownDeliveryAttempt, findWakeByOccurrence } = wakeReconciliationOperations;
  const updateWakeObligationRecord = (input: {
    wakeId: string;
    status?: WakeObligationStatus;
    attemptCount?: number;
    lastAttemptAt?: number | null;
    nextAttemptAt?: number | null;
    ackedAt?: number | null;
    failedReason?: string | null;
    suspensionClass?: WakeObligationSuspensionClass | null;
    metadata?: Record<string, unknown>;
    factsRef?: string;
    now?: number;
    finalizeActiveClaim?: {
      attemptStatus: Extract<
        DeliveryAttemptEvidenceStatus,
        "handoff_accepted" | "superseded" | "unknown"
      >;
      error?: string;
    };
  }): WakeObligation | undefined => {
    const now = input.now ?? Date.now();
    return runSqliteImmediateTransactionSync(db, () => {
      const finalizeActiveClaim = (current: WakeObligationRow) => {
        if (!input.finalizeActiveClaim) {
          return;
        }
        if (input.finalizeActiveClaim.attemptStatus === "unknown") {
          const activeAttempts = queryRows<DeliveryAttemptEvidenceRow>(
            db,
            durableDb
              .selectFrom("delivery_attempt_evidence")
              .selectAll()
              .where("wake_id", "=", input.wakeId)
              .where("status", "=", "attempted")
              .where("delivery_claimed_by", "is not", null),
          );
          for (const attempt of activeAttempts) {
            finalizeUnknownDeliveryAttempt({
              wake: current,
              attempt,
              now,
              error: input.finalizeActiveClaim.error ?? "wake dispatch outcome is unknown",
            });
          }
          return;
        }
        executeQuery(
          db,
          durableDb
            .updateTable("delivery_attempt_evidence")
            .set({
              status: input.finalizeActiveClaim.attemptStatus,
              ...(input.finalizeActiveClaim.attemptStatus === "handoff_accepted"
                ? { handoff_accepted_at: now }
                : {}),
              ...(input.finalizeActiveClaim.error
                ? { error_message: input.finalizeActiveClaim.error }
                : {}),
              delivery_claimed_by: null,
              delivery_claim_expires_at: null,
              updated_at: now,
            })
            .where("wake_id", "=", input.wakeId)
            .where("delivery_claimed_by", "is not", null),
        );
        executeQuery(
          db,
          durableDb
            .deleteFrom("state_leases")
            .where("scope", "=", WAKE_OBLIGATION_LEASE_SCOPE)
            .where("lease_key", "=", input.wakeId),
        );
      };
      const current = queryFirst<WakeObligationRow>(
        db,
        durableDb.selectFrom("wake_obligations").selectAll().where("wake_id", "=", input.wakeId),
      );
      if (!current) {
        return undefined;
      }
      const nextStatus = input.status ?? current.status;
      const nextAttemptCount = input.attemptCount ?? current.attempt_count;
      const nextLastAttemptAt =
        input.lastAttemptAt === undefined ? current.last_attempt_at : input.lastAttemptAt;
      const nextNextAttemptAt =
        input.nextAttemptAt === undefined ? current.next_attempt_at : input.nextAttemptAt;
      const nextAckedAt = input.ackedAt === undefined ? current.acked_at : input.ackedAt;
      const nextFailedReason =
        input.failedReason === undefined
          ? current.failed_reason
          : optionalText(input.failedReason ?? undefined);
      const nextSuspensionClass =
        input.suspensionClass === undefined ? current.suspension_class : input.suspensionClass;
      const nextMetadataJson =
        input.metadata === undefined
          ? current.metadata_json
          : serializeJson({ ...parseMetadata(current.metadata_json), ...input.metadata });
      const nextFactsRef =
        input.factsRef === undefined ? current.facts_ref : optionalText(input.factsRef);
      const projectionChanged =
        wakeProjectionFingerprint(current) !==
        wakeProjectionFingerprint(current, nextMetadataJson, nextFactsRef);
      const authorityChanged =
        projectionChanged ||
        nextStatus !== current.status ||
        nextSuspensionClass !== current.suspension_class;
      if (isTerminalWakeStatus(current.status)) {
        const isNoOp =
          nextStatus === current.status &&
          isSameSqlValue(nextAttemptCount, current.attempt_count) &&
          isSameSqlValue(nextLastAttemptAt, current.last_attempt_at) &&
          isSameSqlValue(nextNextAttemptAt, current.next_attempt_at) &&
          isSameSqlValue(nextAckedAt, current.acked_at) &&
          isSameSqlValue(nextFailedReason, current.failed_reason) &&
          isSameSqlValue(nextSuspensionClass, current.suspension_class) &&
          isSameSqlValue(nextFactsRef, current.facts_ref) &&
          isSameSqlValue(nextMetadataJson, current.metadata_json);
        if (!isNoOp) {
          return undefined;
        }
        finalizeActiveClaim(current);
        return rowToWakeObligation(current);
      }
      if (!isAllowedWakeStatusTransition(current.status, nextStatus)) {
        return undefined;
      }
      executeQuery(
        db,
        durableDb
          .updateTable("wake_obligations")
          .set({
            status: nextStatus,
            attempt_count: nextAttemptCount,
            last_attempt_at: nextLastAttemptAt,
            next_attempt_at: nextNextAttemptAt,
            acked_at: nextAckedAt,
            failed_reason: nextFailedReason,
            suspension_class: nextSuspensionClass,
            delivery_revision: current.delivery_revision + (authorityChanged ? 1 : 0),
            facts_ref: nextFactsRef,
            updated_at: now,
            metadata_json: nextMetadataJson,
          })
          .where("wake_id", "=", input.wakeId),
      );
      finalizeActiveClaim(current);
      const row = queryFirst<WakeObligationRow>(
        db,
        durableDb.selectFrom("wake_obligations").selectAll().where("wake_id", "=", input.wakeId),
      );
      return rowToWakeObligation(row!);
    });
  };

  const updateWakeObligationProjectionRecord = (
    input: UpdateWakeObligationProjectionInput,
  ): WakeObligation | undefined => {
    const sourceRevision =
      optionalText(input.sourceRevision) ?? metadataText(input.metadata.sourceRevision);
    return updateWakeObligationRecord({
      wakeId: input.wakeId,
      metadata: {
        ...sanitizeWakeProjectionMetadata(input.metadata),
        ...(sourceRevision ? { sourceRevision } : {}),
      },
      factsRef: input.factsRef,
      now: input.now,
    });
  };

  const suspendWakeObligationRecord = (
    input: SuspendWakeObligationInput,
  ): WakeObligation | undefined => {
    const now = input.now ?? Date.now();
    return runSqliteImmediateTransactionSync(db, () => {
      const current = queryFirst<WakeObligationRow>(
        db,
        durableDb.selectFrom("wake_obligations").selectAll().where("wake_id", "=", input.wakeId),
      );
      if (!current || isTerminalWakeStatus(current.status)) {
        return undefined;
      }
      return updateWakeObligationRecord({
        wakeId: input.wakeId,
        status: "suspended",
        failedReason: input.failedReason,
        metadata: {
          ...sanitizeWakeProjectionMetadata(input.metadata),
          [WAKE_SUSPENSION_METADATA_KEY]: {
            suspendedAt: now,
            deliveryRevisionBeforeSuspension: current.delivery_revision,
          },
        },
        suspensionClass: input.suspensionClass,
        finalizeActiveClaim: {
          attemptStatus: "unknown",
          error: "wake suspended while dispatch outcome was unresolved",
        },
        now,
      });
    });
  };

  const getWakeObligationRecord = (wakeId: string): WakeObligation | undefined => {
    const row = queryFirst<WakeObligationRow>(
      db,
      durableDb.selectFrom("wake_obligations").selectAll().where("wake_id", "=", wakeId),
    );
    return row ? rowToWakeObligation(row) : undefined;
  };

  const getWakeObligationByOccurrenceKeyRecord = (input: {
    sourceOwner: string;
    sourceRef: string;
    occurrenceKey: string;
  }): WakeObligation | undefined => {
    const { sourceOwner, sourceRef } = requireSourceRef(input, "Wake obligation occurrence");
    const normalizedOccurrenceKey = input.occurrenceKey.trim();
    if (!normalizedOccurrenceKey) {
      throw new Error("Wake obligation occurrence key is required");
    }
    const exact = findWakeByOccurrence({
      sourceOwner,
      sourceRef,
      occurrenceKey: normalizedOccurrenceKey,
    });
    return exact ? rowToWakeObligation(exact.wake) : undefined;
  };

  const listWakeObligationRecords = (options?: {
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
  }): WakeObligation[] => {
    const sourceOwner = optionalText(options?.sourceOwner);
    const sourceRef = optionalText(options?.sourceRef);
    const parentRunId = optionalText(options?.parentRunId);
    const parentSessionKey = optionalText(options?.parentSessionKey);
    const targetRef = optionalText(options?.targetRef);
    const ownerRef = optionalText(options?.ownerRef);
    const reportRouteRef = optionalText(options?.reportRouteRef);
    const rows = queryRows<WakeObligationRow>(
      db,
      durableDb
        .selectFrom("wake_obligations")
        .selectAll()
        .$if(Boolean(sourceOwner), (qb) => qb.where("source_owner", "=", sourceOwner!))
        .$if(Boolean(sourceRef), (qb) => qb.where("source_ref", "=", sourceRef!))
        .$if(Boolean(parentRunId), (qb) => qb.where("parent_run_id", "=", parentRunId!))
        .$if(Boolean(parentSessionKey), (qb) =>
          qb.where("parent_session_key", "=", parentSessionKey!),
        )
        .$if(Boolean(options?.targetKind), (qb) =>
          qb.where("target_kind", "=", options!.targetKind!),
        )
        .$if(Boolean(targetRef), (qb) => qb.where("target_ref", "=", targetRef!))
        .$if(Boolean(options?.ownerKind), (qb) => qb.where("owner_kind", "=", options!.ownerKind!))
        .$if(Boolean(ownerRef), (qb) => qb.where("owner_ref", "=", ownerRef!))
        .$if(Boolean(reportRouteRef), (qb) => qb.where("report_route_ref", "=", reportRouteRef!))
        .$if(Boolean(options?.targetResolutionStatus), (qb) =>
          qb.where("target_resolution_status", "=", options!.targetResolutionStatus!),
        )
        .$if(Boolean(options?.status), (qb) => qb.where("status", "=", options!.status!))
        .orderBy("updated_at", "desc")
        .orderBy("wake_id", "desc")
        .limit(normalizeQueryLimit(options?.limit, 500)),
    );
    return rows.map(rowToWakeObligation);
  };

  const listUnresolvedWakeObligationsPageRecords = (input: {
    sourceOwner?: string;
    createdAtOrBefore?: number;
    cursor?: string;
    limit: number;
  }): WakeObligationPage => {
    requirePositiveSafeInteger(input.limit, "Unresolved wake page limit");
    if (input.limit > MAX_UNRESOLVED_WAKE_PAGE_SIZE) {
      throw new Error(
        `Unresolved wake page limit must not exceed ${MAX_UNRESOLVED_WAKE_PAGE_SIZE}`,
      );
    }
    if (
      input.createdAtOrBefore !== undefined &&
      (!Number.isSafeInteger(input.createdAtOrBefore) || input.createdAtOrBefore < 0)
    ) {
      throw new Error("Unresolved wake createdAtOrBefore must be a non-negative safe integer");
    }
    const requestedSourceOwner = optionalText(input.sourceOwner);
    const requestedCreatedAtOrBefore = input.createdAtOrBefore ?? null;
    const cursor = input.cursor ? decodeUnresolvedWakePageCursor(input.cursor) : undefined;
    if (cursor && requestedSourceOwner !== null && requestedSourceOwner !== cursor.sourceOwner) {
      throw new Error("Unresolved wake page cursor does not match sourceOwner");
    }
    if (
      cursor &&
      input.createdAtOrBefore !== undefined &&
      requestedCreatedAtOrBefore !== cursor.createdAtOrBefore
    ) {
      throw new Error("Unresolved wake page cursor does not match createdAtOrBefore");
    }
    const sourceOwner = cursor?.sourceOwner ?? requestedSourceOwner;
    const createdAtOrBefore = cursor?.createdAtOrBefore ?? requestedCreatedAtOrBefore;
    const rows = queryRows<WakeObligationRow>(
      db,
      durableDb
        .selectFrom("wake_obligations")
        .selectAll()
        .where(unresolvedWakeStatusSql())
        .$if(Boolean(sourceOwner), (qb) => qb.where("source_owner", "=", sourceOwner!))
        .$if(createdAtOrBefore !== null, (qb) => qb.where("created_at", "<=", createdAtOrBefore!))
        .$if(cursor !== undefined, (qb) =>
          qb.where((eb) =>
            eb.or([
              eb("created_at", ">", cursor!.createdAt),
              eb.and([
                eb("created_at", "=", cursor!.createdAt),
                eb("wake_id", ">", cursor!.wakeId),
              ]),
            ]),
          ),
        )
        .orderBy("created_at", "asc")
        .orderBy("wake_id", "asc")
        .limit(input.limit + 1),
    );
    const pageRows = rows.slice(0, input.limit);
    const complete = rows.length <= input.limit;
    const last = pageRows.at(-1);
    return {
      wakes: pageRows.map(rowToWakeObligation),
      complete,
      ...(!complete && last
        ? {
            nextCursor: encodeUnresolvedWakePageCursor({
              version: UNRESOLVED_WAKE_CURSOR_VERSION,
              sourceOwner,
              createdAtOrBefore,
              createdAt: last.created_at,
              wakeId: last.wake_id,
            }),
          }
        : {}),
    };
  };

  const storeListUncertaintyFacts = (options?: {
    sourceOwner?: string;
    sourceRef?: string;
    sourceRunId?: string;
    status?: UncertaintyFactStatus;
    limit?: number;
  }): UncertaintyFact[] => {
    const sourceOwner = optionalText(options?.sourceOwner);
    const sourceRef = optionalText(options?.sourceRef);
    const sourceRunId = optionalText(options?.sourceRunId);
    const rows = queryRows<UncertaintyFactRow>(
      db,
      durableDb
        .selectFrom("uncertainty_facts")
        .selectAll()
        .$if(Boolean(sourceOwner), (qb) => qb.where("source_owner", "=", sourceOwner!))
        .$if(Boolean(sourceRef), (qb) => qb.where("source_ref", "=", sourceRef!))
        .$if(Boolean(sourceRunId), (qb) => qb.where("source_run_id", "=", sourceRunId!))
        .$if(Boolean(options?.status), (qb) => qb.where("status", "=", options!.status!))
        .orderBy("updated_at", "desc")
        .orderBy("fact_id", "desc")
        .limit(normalizeQueryLimit(options?.limit, 500)),
    );
    return rows.map(rowToUncertaintyFact);
  };

  const acknowledgeWakeObligationRecord = (
    input: WakeObligationControlInput,
  ): WakeObligation | undefined => {
    const now = input.now ?? Date.now();
    return runSqliteImmediateTransactionSync(db, () => {
      const current = queryFirst<WakeObligationRow>(
        db,
        durableDb.selectFrom("wake_obligations").selectAll().where("wake_id", "=", input.wakeId),
      );
      if (!current) {
        return undefined;
      }
      if (!matchesExpectedWakeRevision(current, input)) {
        return undefined;
      }
      if (current.status === "acked") {
        return updateWakeObligationRecord({
          wakeId: input.wakeId,
          status: "acked",
          finalizeActiveClaim: { attemptStatus: "handoff_accepted" },
          now,
        });
      }
      if (isTerminalWakeStatus(current.status)) {
        return undefined;
      }
      const decision = buildWakeControlDecision(input, "acknowledged", now);
      return updateWakeObligationRecord({
        wakeId: input.wakeId,
        status: "acked",
        ackedAt: now,
        suspensionClass: null,
        metadata: mergeWakeControlMetadata(current.metadata_json, decision),
        finalizeActiveClaim: { attemptStatus: "handoff_accepted" },
        now,
      });
    });
  };

  const supersedeWakeObligationRecord = (
    input: SupersedeWakeObligationInput,
  ): WakeObligation | undefined => {
    const now = input.now ?? Date.now();
    return runSqliteImmediateTransactionSync(db, () => {
      const current = queryFirst<WakeObligationRow>(
        db,
        durableDb.selectFrom("wake_obligations").selectAll().where("wake_id", "=", input.wakeId),
      );
      if (!current) {
        return undefined;
      }
      if (!matchesExpectedWakeRevision(current, input)) {
        return undefined;
      }
      if (current.status === "superseded") {
        return updateWakeObligationRecord({
          wakeId: input.wakeId,
          status: "superseded",
          finalizeActiveClaim: {
            attemptStatus: "superseded",
            error: input.reason ?? "superseded",
          },
          now,
        });
      }
      if (isTerminalWakeStatus(current.status)) {
        return undefined;
      }
      const decision = buildWakeControlDecision(input, "superseded", now);
      return updateWakeObligationRecord({
        wakeId: input.wakeId,
        status: "superseded",
        failedReason: input.reason ?? "superseded",
        suspensionClass: null,
        metadata: mergeWakeControlMetadata(
          current.metadata_json,
          decision,
          input.supersededByRef ? { supersededByRef: input.supersededByRef } : undefined,
        ),
        finalizeActiveClaim: {
          attemptStatus: "superseded",
          error: input.reason ?? "superseded",
        },
        now,
      });
    });
  };

  const resumeWakeObligationRecord = (
    input: ResumeWakeObligationInput,
  ): WakeObligation | undefined => {
    const now = input.now ?? Date.now();
    return runSqliteImmediateTransactionSync(db, () => {
      const current = queryFirst<WakeObligationRow>(
        db,
        durableDb.selectFrom("wake_obligations").selectAll().where("wake_id", "=", input.wakeId),
      );
      if (!current) {
        return undefined;
      }
      if (!matchesExpectedWakeRevision(current, input)) {
        return undefined;
      }
      if (current.status !== "suspended") {
        return isMatchingControlNoop(current, "resumed", input.idempotencyKey)
          ? rowToWakeObligation(current)
          : undefined;
      }
      if (current.suspension_class !== input.expectedSuspensionClass) {
        return undefined;
      }
      const unsafeAttempt = queryFirst<{ delivery_attempt_id: string }>(
        db,
        durableDb
          .selectFrom("delivery_attempt_evidence")
          .select("delivery_attempt_id")
          .where("wake_id", "=", input.wakeId)
          .where("status", "in", ["attempted", "unknown", "handoff_accepted"])
          .limit(1),
      );
      if (unsafeAttempt) {
        return undefined;
      }
      const decision = buildWakeControlDecision(input, "resumed", now);
      return updateWakeObligationRecord({
        wakeId: input.wakeId,
        status: "pending",
        failedReason: null,
        nextAttemptAt: null,
        suspensionClass: null,
        metadata: mergeWakeControlMetadata(current.metadata_json, decision),
        now,
      });
    });
  };

  const markWakeObligationDecisionRequiredRecord = (
    input: MarkWakeObligationDecisionRequiredInput,
  ): WakeObligation | undefined => {
    const now = input.now ?? Date.now();
    return runSqliteImmediateTransactionSync(db, () => {
      const current = queryFirst<WakeObligationRow>(
        db,
        durableDb.selectFrom("wake_obligations").selectAll().where("wake_id", "=", input.wakeId),
      );
      if (!current) {
        return undefined;
      }
      if (!matchesExpectedWakeRevision(current, input)) {
        return undefined;
      }
      if (isTerminalWakeStatus(current.status)) {
        return isMatchingControlNoop(current, input.decisionKind, input.idempotencyKey)
          ? rowToWakeObligation(current)
          : undefined;
      }
      const decision = buildWakeControlDecision(input, input.decisionKind, now);
      return updateWakeObligationRecord({
        wakeId: input.wakeId,
        status: current.status,
        suspensionClass:
          current.status === "suspended" ? "owner_decision_required" : current.suspension_class,
        metadata: mergeWakeControlMetadata(current.metadata_json, decision),
        now,
      });
    });
  };

  return {
    updateWakeObligationRecord,
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
  };
}

export type WakeControlOperations = ReturnType<typeof createWakeControlOperations>;
