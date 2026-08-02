import { randomUUID } from "node:crypto";
import { runSqliteImmediateTransactionSync } from "../infra/sqlite-transaction.js";
import type { DurableSqliteStoreContext } from "./sqlite-store-context.js";
import {
  optionalText,
  WAKE_OBLIGATION_LEASE_SCOPE,
  WAKE_RECONCILIATION_PAGE_SIZE,
  WAKE_RECONCILIATION_MAX_PAGES,
  requireSourceRef,
  serializeJson,
  wakeProjectionMetadata,
  wakeProjectionHash,
  isRecordValue,
  parseMetadata,
  deliveryAttemptClaimedOptionalText,
  hasWakeControlEvidence,
} from "./sqlite-store-support-core.js";
import type {
  WakeObligationRow,
  WakeObligationOccurrenceRow,
  DeliveryAttemptEvidenceRow,
} from "./sqlite-store-support-core.js";
import {
  rowToWakeObligation,
  queryRows,
  queryFirst,
  executeQuery,
  unresolvedWakeStatusSql,
} from "./sqlite-store-support-query.js";
import type {
  WakeObligation,
  CreateWakeObligationInput,
  ReconcileWakeObligationInput,
  ReconcileWakeObligationResult,
} from "./types.js";

export function createWakeReconciliationOperations(context: DurableSqliteStoreContext) {
  const { db, durableDb } = context;
  const finalizeUnknownDeliveryAttempt = (input: {
    wake: WakeObligationRow;
    attempt: DeliveryAttemptEvidenceRow;
    now: number;
    error: string;
    evidence?: Record<string, unknown>;
  }): boolean => {
    const changed = executeQuery(
      db,
      durableDb
        .updateTable("delivery_attempt_evidence")
        .set({
          status: "unknown",
          evidence_json: serializeJson(input.evidence),
          error_message: input.error,
          unknown_at: input.now,
          delivery_claimed_by: null,
          delivery_claim_expires_at: null,
          updated_at: input.now,
        })
        .where("delivery_attempt_id", "=", input.attempt.delivery_attempt_id)
        .where("status", "=", "attempted"),
    );
    if (changed === 0) {
      return false;
    }
    executeQuery(
      db,
      durableDb
        .deleteFrom("state_leases")
        .where("scope", "=", WAKE_OBLIGATION_LEASE_SCOPE)
        .where("lease_key", "=", input.wake.wake_id),
    );
    const claimedFactsRef = deliveryAttemptClaimedOptionalText(input.attempt, "claimedFactsRef");
    const claimedSourceRunId = deliveryAttemptClaimedOptionalText(
      input.attempt,
      "claimedSourceRunId",
    );
    const claimedWakeDeliveryRevision = input.attempt.claimed_wake_delivery_revision;
    executeQuery(
      db,
      durableDb
        .insertInto("uncertainty_facts")
        .values({
          fact_id: `uncertainty_${randomUUID()}`,
          source_owner: input.wake.source_owner,
          source_ref: input.wake.source_ref,
          kind: "delivery_unknown",
          source_run_id:
            claimedSourceRunId === undefined ? input.wake.source_run_id : claimedSourceRunId,
          step_id: null,
          event_id: null,
          ref_id: input.attempt.delivery_attempt_id,
          facts_ref: claimedFactsRef === undefined ? input.wake.facts_ref : claimedFactsRef,
          dedupe_key: `wake-dispatch-unknown:${input.attempt.delivery_attempt_id}`,
          facts_json: serializeJson({
            wakeId: input.wake.wake_id,
            deliveryAttemptId: input.attempt.delivery_attempt_id,
            claimedWakeDeliveryRevision,
            ...(claimedFactsRef !== undefined ? { claimedFactsRef } : {}),
            ...(claimedSourceRunId !== undefined ? { claimedSourceRunId } : {}),
            ...(input.evidence ? { evidence: input.evidence } : {}),
          }),
          status: "open",
          resolution_kind: null,
          resolution_ref: null,
          resolved_at: null,
          created_at: input.now,
          updated_at: input.now,
          metadata_json: null,
        })
        .onConflict((conflict) => conflict.doNothing()),
    );
    return true;
  };

  const prepareWakeCandidate = (
    input: CreateWakeObligationInput,
    policy: ReconcileWakeObligationInput["policy"],
  ) => {
    const { sourceOwner, sourceRef } = requireSourceRef(input, "Durable wake obligation");
    const parentRunId = optionalText(input.parentRunId);
    const parentSessionKey = optionalText(input.parentSessionKey);
    const targetRef = optionalText(input.targetRef);
    const reportRouteRef = optionalText(input.reportRouteRef);
    const hasInspectableResolution =
      input.targetResolutionStatus === "ambiguous" ||
      input.targetResolutionStatus === "missing" ||
      input.targetResolutionStatus === "unauthorized" ||
      input.targetResolutionStatus === "inspect_only";
    if (
      !parentRunId &&
      !parentSessionKey &&
      !targetRef &&
      !reportRouteRef &&
      !hasInspectableResolution
    ) {
      throw new Error(
        "Durable wake requires a parent target, generalized target, report route, or inspect-only resolution",
      );
    }
    const occurrenceKey = optionalText(input.occurrenceKey);
    if (!occurrenceKey) {
      throw new Error("Durable wake requires an occurrenceKey");
    }
    return {
      sourceOwner,
      sourceRef,
      parentRunId,
      parentSessionKey,
      targetRef,
      reportRouteRef,
      occurrenceKey,
      projectionHash: wakeProjectionHash(input, policy),
      now: input.now ?? Date.now(),
      wakeId: optionalText(input.wakeId) ?? `wake_${randomUUID()}`,
    };
  };

  const insertWakeRow = (
    input: CreateWakeObligationInput,
    prepared: ReturnType<typeof prepareWakeCandidate>,
    policy: ReconcileWakeObligationInput["policy"],
  ): WakeObligationRow => {
    const metadata = wakeProjectionMetadata(input);
    metadata.wakeReconciliation = {
      mode: policy.mode,
      ...(policy.mode === "while_unresolved" ? { recurrence: policy.recurrence } : {}),
      latestOccurrenceKey: prepared.occurrenceKey,
      latestOccurrenceAt: prepared.now,
    };
    executeQuery(
      db,
      durableDb.insertInto("wake_obligations").values({
        wake_id: prepared.wakeId,
        source_owner: prepared.sourceOwner,
        source_ref: prepared.sourceRef,
        coalescing_mode: policy.mode,
        recurrence_policy: policy.mode === "while_unresolved" ? policy.recurrence : null,
        parent_run_id: prepared.parentRunId,
        parent_session_key: prepared.parentSessionKey,
        target_kind: optionalText(input.targetKind),
        target_ref: prepared.targetRef,
        owner_kind: optionalText(input.ownerKind),
        owner_ref: optionalText(input.ownerRef),
        report_route_ref: prepared.reportRouteRef,
        target_resolution_status: optionalText(input.targetResolutionStatus),
        target_resolution_reason: optionalText(input.targetResolutionReason),
        reason: input.reason,
        facts_ref: optionalText(input.factsRef),
        source_run_id: optionalText(input.sourceRunId),
        delivery_revision: 1,
        suspension_class: null,
        attempt_count: 0,
        last_attempt_at: null,
        next_attempt_at: null,
        acked_at: null,
        failed_reason: null,
        status: "pending",
        created_at: prepared.now,
        updated_at: prepared.now,
        metadata_json: serializeJson(metadata),
      }),
    );
    executeQuery(
      db,
      durableDb.insertInto("wake_obligation_occurrences").values({
        source_owner: prepared.sourceOwner,
        source_ref: prepared.sourceRef,
        occurrence_key: prepared.occurrenceKey,
        wake_id: prepared.wakeId,
        projection_hash: prepared.projectionHash,
        observed_at: prepared.now,
      }),
    );
    return queryFirst<WakeObligationRow>(
      db,
      durableDb.selectFrom("wake_obligations").selectAll().where("wake_id", "=", prepared.wakeId),
    )!;
  };

  const findWakeByOccurrence = (input: {
    sourceOwner: string;
    sourceRef: string;
    occurrenceKey: string;
  }): { occurrence: WakeObligationOccurrenceRow; wake: WakeObligationRow } | undefined => {
    const occurrence = queryFirst<WakeObligationOccurrenceRow>(
      db,
      durableDb
        .selectFrom("wake_obligation_occurrences")
        .selectAll()
        .where("source_owner", "=", input.sourceOwner)
        .where("source_ref", "=", input.sourceRef)
        .where("occurrence_key", "=", input.occurrenceKey),
    );
    if (!occurrence) {
      return undefined;
    }
    const wake = queryFirst<WakeObligationRow>(
      db,
      durableDb
        .selectFrom("wake_obligations")
        .selectAll()
        .where("wake_id", "=", occurrence.wake_id),
    );
    if (!wake) {
      throw new Error(`Durable wake occurrence ${input.occurrenceKey} has no canonical wake`);
    }
    return { occurrence, wake };
  };

  const createWakeObligationRecord = (input: CreateWakeObligationInput): WakeObligation => {
    const policy = { mode: "none" } as const;
    const prepared = prepareWakeCandidate(input, policy);
    return runSqliteImmediateTransactionSync(db, () => {
      const exact = findWakeByOccurrence(prepared);
      if (exact) {
        if (exact.occurrence.projection_hash !== prepared.projectionHash) {
          throw new Error(`Durable wake occurrence conflict for ${prepared.occurrenceKey}`);
        }
        return rowToWakeObligation(exact.wake);
      }
      return rowToWakeObligation(insertWakeRow(input, prepared, policy));
    });
  };

  const reconcileWakeObligationRecord = (
    input: ReconcileWakeObligationInput,
  ): ReconcileWakeObligationResult => {
    const candidate = input.candidate;
    const prepared = prepareWakeCandidate(candidate, input.policy);

    return runSqliteImmediateTransactionSync(db, () => {
      const exact = findWakeByOccurrence(prepared);
      if (exact) {
        if (exact.occurrence.projection_hash !== prepared.projectionHash) {
          return {
            disposition: "conflict",
            reason: "idempotency_conflict",
            candidatePersisted: false,
            duplicateScan: "not_scanned",
            conflictingWakeIds: [exact.wake.wake_id],
            representativeWake: rowToWakeObligation(exact.wake),
          };
        }
        return {
          wake: rowToWakeObligation(exact.wake),
          disposition: "exact_match",
          candidatePersisted: true,
          duplicateScan: "not_scanned",
          duplicateWakeIds: [],
        };
      }

      if (input.policy.mode === "none") {
        return {
          wake: rowToWakeObligation(insertWakeRow(candidate, prepared, input.policy)),
          disposition: "created",
          candidatePersisted: true,
          duplicateScan: "not_scanned",
          duplicateWakeIds: [],
        };
      }
      const recurrence = input.policy.recurrence;

      const targetKind = optionalText(candidate.targetKind);
      const ownerKind = optionalText(candidate.ownerKind);
      const ownerRef = optionalText(candidate.ownerRef);
      const selectIdentityRows = () =>
        durableDb
          .selectFrom("wake_obligations")
          .selectAll()
          .where("source_owner", "=", prepared.sourceOwner)
          .where("source_ref", "=", prepared.sourceRef)
          .where("coalescing_mode", "=", "while_unresolved")
          .where("reason", "=", candidate.reason)
          .$if(prepared.parentRunId === null, (qb) => qb.where("parent_run_id", "is", null))
          .$if(prepared.parentRunId !== null, (qb) =>
            qb.where("parent_run_id", "=", prepared.parentRunId!),
          )
          .$if(prepared.parentSessionKey === null, (qb) =>
            qb.where("parent_session_key", "is", null),
          )
          .$if(prepared.parentSessionKey !== null, (qb) =>
            qb.where("parent_session_key", "=", prepared.parentSessionKey!),
          )
          .$if(targetKind === null, (qb) => qb.where("target_kind", "is", null))
          .$if(targetKind !== null, (qb) => qb.where("target_kind", "=", targetKind!))
          .$if(prepared.targetRef === null, (qb) => qb.where("target_ref", "is", null))
          .$if(prepared.targetRef !== null, (qb) =>
            qb.where("target_ref", "=", prepared.targetRef!),
          )
          .$if(ownerKind === null, (qb) => qb.where("owner_kind", "is", null))
          .$if(ownerKind !== null, (qb) => qb.where("owner_kind", "=", ownerKind!))
          .$if(ownerRef === null, (qb) => qb.where("owner_ref", "is", null))
          .$if(ownerRef !== null, (qb) => qb.where("owner_ref", "=", ownerRef!))
          .$if(prepared.reportRouteRef === null, (qb) => qb.where("report_route_ref", "is", null))
          .$if(prepared.reportRouteRef !== null, (qb) =>
            qb.where("report_route_ref", "=", prepared.reportRouteRef!),
          );
      const identityRows: WakeObligationRow[] = [];
      let cursor: { createdAt: number; wakeId: string } | undefined;
      let scanComplete = false;

      for (let page = 0; page < WAKE_RECONCILIATION_MAX_PAGES; page += 1) {
        const pageRows = queryRows<WakeObligationRow>(
          db,
          selectIdentityRows()
            .where(unresolvedWakeStatusSql())
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
            .limit(WAKE_RECONCILIATION_PAGE_SIZE + 1),
        );
        const boundedRows = pageRows.slice(0, WAKE_RECONCILIATION_PAGE_SIZE);
        identityRows.push(...boundedRows);
        if (pageRows.length <= WAKE_RECONCILIATION_PAGE_SIZE) {
          scanComplete = true;
          break;
        }
        const last = boundedRows.at(-1)!;
        cursor = { createdAt: last.created_at, wakeId: last.wake_id };
      }

      if (!scanComplete) {
        return {
          disposition: "conflict",
          reason: "scan_truncated",
          candidatePersisted: false,
          duplicateScan: "truncated",
          conflictingWakeIds: identityRows.map((row) => row.wake_id),
          ...(identityRows[0] ? { representativeWake: rowToWakeObligation(identityRows[0]) } : {}),
        };
      }

      if (identityRows.length > 0) {
        const policyConflicts = identityRows.filter((row) => row.recurrence_policy !== recurrence);
        if (policyConflicts.length > 0) {
          return {
            disposition: "conflict",
            reason: "policy_conflict",
            candidatePersisted: false,
            duplicateScan: "complete",
            conflictingWakeIds: policyConflicts.map((row) => row.wake_id),
            representativeWake: rowToWakeObligation(policyConflicts[0]!),
          };
        }
        const wakeIds = identityRows.map((row) => row.wake_id);
        const deliveryEvidenceIds = new Set(
          queryRows<{ wake_id: string }>(
            db,
            durableDb
              .selectFrom("delivery_attempt_evidence")
              .select("wake_id")
              .distinct()
              .where("wake_id", "in", wakeIds),
          ).map((row) => row.wake_id),
        );
        const claimedWakeIds = new Set(
          queryRows<{ lease_key: string }>(
            db,
            durableDb
              .selectFrom("state_leases")
              .select("lease_key")
              .where("scope", "=", WAKE_OBLIGATION_LEASE_SCOPE)
              .where("lease_key", "in", wakeIds),
          ).map((row) => row.lease_key),
        );
        const evidenceBearing = identityRows.filter(
          (row) =>
            row.status !== "pending" ||
            row.attempt_count > 0 ||
            row.last_attempt_at !== null ||
            deliveryEvidenceIds.has(row.wake_id) ||
            claimedWakeIds.has(row.wake_id) ||
            hasWakeControlEvidence(row.metadata_json),
        );
        if (evidenceBearing.length > 1) {
          return {
            disposition: "conflict",
            reason: "multiple_evidence_bearing_wakes",
            candidatePersisted: false,
            duplicateScan: "complete",
            conflictingWakeIds: evidenceBearing.map((row) => row.wake_id),
            representativeWake: rowToWakeObligation(evidenceBearing[0]!),
          };
        }

        const canonical = evidenceBearing[0] ?? identityRows[0]!;
        const duplicates = identityRows.filter((row) => row.wake_id !== canonical.wake_id);
        for (const duplicate of duplicates) {
          const duplicateMetadata = parseMetadata(duplicate.metadata_json);
          const duplicateReconciliation = isRecordValue(duplicateMetadata.wakeReconciliation)
            ? duplicateMetadata.wakeReconciliation
            : {};
          executeQuery(
            db,
            durableDb
              .updateTable("wake_obligation_occurrences")
              .set({ wake_id: canonical.wake_id })
              .where("wake_id", "=", duplicate.wake_id),
          );
          executeQuery(
            db,
            durableDb
              .updateTable("wake_obligations")
              .set({
                status: "superseded",
                coalescing_mode: "none",
                recurrence_policy: null,
                delivery_revision: duplicate.delivery_revision + 1,
                suspension_class: null,
                failed_reason: "coalesced during durable wake reconciliation",
                updated_at: prepared.now,
                metadata_json: serializeJson({
                  ...duplicateMetadata,
                  wakeReconciliation: {
                    ...duplicateReconciliation,
                    supersededByWakeId: canonical.wake_id,
                    reconciledAt: prepared.now,
                  },
                }),
              })
              .where("wake_id", "=", duplicate.wake_id),
          );
        }

        const currentMetadata = parseMetadata(canonical.metadata_json);
        const currentReconciliation = isRecordValue(currentMetadata.wakeReconciliation)
          ? currentMetadata.wakeReconciliation
          : {};
        executeQuery(
          db,
          durableDb
            .updateTable("wake_obligations")
            .set({
              target_resolution_status:
                optionalText(candidate.targetResolutionStatus) ??
                canonical.target_resolution_status,
              target_resolution_reason:
                optionalText(candidate.targetResolutionReason) ??
                canonical.target_resolution_reason,
              facts_ref: optionalText(candidate.factsRef) ?? canonical.facts_ref,
              source_run_id: optionalText(candidate.sourceRunId) ?? canonical.source_run_id,
              recurrence_policy: recurrence,
              delivery_revision: canonical.delivery_revision + 1,
              metadata_json: serializeJson({
                ...currentMetadata,
                ...wakeProjectionMetadata(candidate),
                wakeReconciliation: {
                  ...currentReconciliation,
                  mode: input.policy.mode,
                  recurrence,
                  latestOccurrenceKey: prepared.occurrenceKey,
                  latestOccurrenceAt: prepared.now,
                },
              }),
              updated_at: prepared.now,
            })
            .where("wake_id", "=", canonical.wake_id),
        );
        executeQuery(
          db,
          durableDb.insertInto("wake_obligation_occurrences").values({
            source_owner: prepared.sourceOwner,
            source_ref: prepared.sourceRef,
            occurrence_key: prepared.occurrenceKey,
            wake_id: canonical.wake_id,
            projection_hash: prepared.projectionHash,
            observed_at: prepared.now,
          }),
        );
        const updated = queryFirst<WakeObligationRow>(
          db,
          durableDb
            .selectFrom("wake_obligations")
            .selectAll()
            .where("wake_id", "=", canonical.wake_id),
        )!;
        return {
          wake: rowToWakeObligation(updated),
          disposition: "coalesced",
          candidatePersisted: true,
          duplicateScan: "complete",
          duplicateWakeIds: duplicates.map((row) => row.wake_id),
        };
      }

      const terminal = queryFirst<WakeObligationRow>(
        db,
        selectIdentityRows()
          .where("status", "in", ["acked", "superseded"])
          .orderBy("created_at", "desc")
          .orderBy("wake_id", "desc")
          .limit(1),
      );
      if (terminal && terminal.recurrence_policy !== recurrence) {
        return {
          disposition: "conflict",
          reason: "policy_conflict",
          candidatePersisted: false,
          duplicateScan: "complete",
          conflictingWakeIds: [terminal.wake_id],
          representativeWake: rowToWakeObligation(terminal),
        };
      }
      if (terminal && recurrence === "never") {
        executeQuery(
          db,
          durableDb.insertInto("wake_obligation_occurrences").values({
            source_owner: prepared.sourceOwner,
            source_ref: prepared.sourceRef,
            occurrence_key: prepared.occurrenceKey,
            wake_id: terminal.wake_id,
            projection_hash: prepared.projectionHash,
            observed_at: prepared.now,
          }),
        );
        return {
          wake: rowToWakeObligation(terminal),
          disposition: "terminal_match",
          candidatePersisted: true,
          duplicateScan: "complete",
          duplicateWakeIds: [],
        };
      }

      return {
        wake: rowToWakeObligation(insertWakeRow(candidate, prepared, input.policy)),
        disposition: "created",
        candidatePersisted: true,
        duplicateScan: "complete",
        duplicateWakeIds: [],
      };
    });
  };

  return {
    finalizeUnknownDeliveryAttempt,
    prepareWakeCandidate,
    insertWakeRow,
    findWakeByOccurrence,
    createWakeObligationRecord,
    reconcileWakeObligationRecord,
  };
}

export type WakeReconciliationOperations = ReturnType<typeof createWakeReconciliationOperations>;
