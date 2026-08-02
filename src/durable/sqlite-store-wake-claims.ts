import { randomUUID } from "node:crypto";
import { sql } from "kysely";
import { executeSqliteQuerySync } from "../infra/kysely-sync.js";
import { runSqliteImmediateTransactionSync } from "../infra/sqlite-transaction.js";
import type { DurableSqliteStoreContext } from "./sqlite-store-context.js";
import {
  optionalText,
  WAKE_OBLIGATION_LEASE_SCOPE,
  requirePositiveSafeInteger,
  wakeRetryDelayMs,
  serializeJson,
  DELIVERY_ATTEMPT_INTERNAL_METADATA_KEY,
  DELIVERY_ATTEMPT_INTERNAL_METADATA_VERSION,
  attemptMatchesClaimedWakeRevision,
} from "./sqlite-store-support-core.js";
import type { WakeObligationRow, DeliveryAttemptEvidenceRow } from "./sqlite-store-support-core.js";
import {
  rowToWakeObligation,
  rowToDeliveryAttemptEvidence,
  queryRows,
  queryFirst,
  executeQuery,
  normalizeQueryLimit,
  isTerminalWakeStatus,
  isAllowedWakeStatusTransition,
} from "./sqlite-store-support-query.js";
import type { WakeReconciliationOperations } from "./sqlite-store-wake-reconciliation.js";
import type {
  DeliveryAttemptEvidenceStatus,
  DeliveryAttemptEvidence,
  WakeObligationClaim,
  ClaimNextWakeObligationInput,
  RenewWakeObligationClaimInput,
  CompleteWakeObligationClaimInput,
} from "./types.js";

export function createWakeClaimOperations(
  context: DurableSqliteStoreContext,
  wakeReconciliationOperations: WakeReconciliationOperations,
) {
  const { db, durableDb } = context;
  const { finalizeUnknownDeliveryAttempt } = wakeReconciliationOperations;
  const claimNextWakeObligationRecord = (
    input: ClaimNextWakeObligationInput,
  ): WakeObligationClaim | undefined => {
    requirePositiveSafeInteger(input.claimTtlMs, "Durable wake claimTtlMs");
    requirePositiveSafeInteger(input.retryBaseMs, "Durable wake retryBaseMs");
    requirePositiveSafeInteger(input.retryMaxMs, "Durable wake retryMaxMs");
    if (input.retryMaxMs < input.retryBaseMs) {
      throw new Error("Durable wake retryMaxMs must be greater than or equal to retryBaseMs");
    }
    const now = input.now ?? Date.now();
    const claimExpiresAt = now + input.claimTtlMs;
    return runSqliteImmediateTransactionSync(db, () => {
      const suspendAmbiguousDispatch = (
        candidate: WakeObligationRow,
        ambiguousAttempt: DeliveryAttemptEvidenceRow,
      ) => {
        finalizeUnknownDeliveryAttempt({
          wake: candidate,
          attempt: ambiguousAttempt,
          now,
          error: "wake dispatch claim expired before durable completion evidence",
        });
        executeQuery(
          db,
          durableDb
            .updateTable("wake_obligations")
            .set({
              status: "suspended",
              suspension_class: "delivery_outcome_unknown",
              delivery_revision: candidate.delivery_revision + 1,
              failed_reason: "dispatch_outcome_unknown",
              updated_at: now,
            })
            .where("wake_id", "=", candidate.wake_id),
        );
      };

      const expiredAttempts = queryRows<DeliveryAttemptEvidenceRow>(
        db,
        durableDb
          .selectFrom("delivery_attempt_evidence as d")
          .innerJoin("wake_obligations as w", "w.wake_id", "d.wake_id")
          .selectAll("d")
          .where("d.status", "=", "attempted")
          .where("d.delivery_claim_expires_at", "is not", null)
          .where("d.delivery_claim_expires_at", "<=", now)
          .where("w.status", "in", ["pending", "failed"])
          .orderBy("d.delivery_claim_expires_at", "asc")
          .orderBy("d.delivery_attempt_id", "asc")
          .limit(100),
      );
      for (const expiredAttempt of expiredAttempts) {
        const candidate = queryFirst<WakeObligationRow>(
          db,
          durableDb
            .selectFrom("wake_obligations")
            .selectAll()
            .where("wake_id", "=", expiredAttempt.wake_id),
        );
        if (candidate && (candidate.status === "pending" || candidate.status === "failed")) {
          suspendAmbiguousDispatch(candidate, expiredAttempt);
        }
      }

      executeQuery(
        db,
        durableDb
          .deleteFrom("state_leases")
          .where("scope", "=", WAKE_OBLIGATION_LEASE_SCOPE)
          .where("expires_at", "<=", now),
      );
      // SQLite's planner otherwise prefers the status index and sorts every matching row.
      const candidateQuery = sql<WakeObligationRow> /* kysely-allow-raw: bounded claim plan */ `
          SELECT w.*
            FROM wake_obligations AS w INDEXED BY idx_wake_obligations_claimable
            LEFT JOIN state_leases AS l
              ON l.lease_key = w.wake_id AND l.scope = ${WAKE_OBLIGATION_LEASE_SCOPE}
           WHERE w.status IN ('pending', 'failed')
             AND (w.next_attempt_at IS NULL OR w.next_attempt_at <= ${now})
             AND l.lease_key IS NULL
           ORDER BY w.next_attempt_at, w.updated_at, w.wake_id
           LIMIT 100
        `;
      const candidates = executeSqliteQuerySync(db, {
        compile: () => candidateQuery.compile(durableDb),
      }).rows;
      for (const candidate of candidates) {
        const ambiguousAttempt = queryFirst<DeliveryAttemptEvidenceRow>(
          db,
          durableDb
            .selectFrom("delivery_attempt_evidence")
            .selectAll()
            .where("wake_id", "=", candidate.wake_id)
            .where("status", "=", "attempted")
            .orderBy("scheduled_at", "desc")
            .limit(1),
        );
        if (ambiguousAttempt) {
          suspendAmbiguousDispatch(candidate, ambiguousAttempt);
          continue;
        }

        const claimToken = `wake_claim_${randomUUID()}`;
        const deliveryAttemptId = `wake_delivery_${randomUUID()}`;
        const attemptNumber = candidate.attempt_count + 1;
        const nextAttemptAt =
          now +
          wakeRetryDelayMs({
            wakeId: candidate.wake_id,
            attemptCount: attemptNumber,
            retryBaseMs: input.retryBaseMs,
            retryMaxMs: input.retryMaxMs,
          });
        executeQuery(
          db,
          durableDb.insertInto("state_leases").values({
            scope: WAKE_OBLIGATION_LEASE_SCOPE,
            lease_key: candidate.wake_id,
            owner: claimToken,
            expires_at: claimExpiresAt,
            heartbeat_at: now,
            payload_json: serializeJson({
              wakeId: candidate.wake_id,
              deliveryAttemptId,
              workerId: input.workerId,
            }),
            created_at: now,
            updated_at: now,
          }),
        );
        executeQuery(
          db,
          durableDb.insertInto("delivery_attempt_evidence").values({
            delivery_attempt_id: deliveryAttemptId,
            source_owner: candidate.source_owner,
            source_ref: candidate.source_ref,
            wake_id: candidate.wake_id,
            dedupe_key: `${candidate.wake_id}:dispatch:${attemptNumber}`,
            replay_pass_id: claimToken,
            target_kind: candidate.target_kind,
            target_ref: candidate.target_ref,
            route_kind: candidate.target_kind,
            route_ref: candidate.report_route_ref ?? candidate.target_ref,
            status: "attempted",
            claimed_wake_delivery_revision: candidate.delivery_revision,
            evidence_json: serializeJson({ workerId: input.workerId }),
            error_message: null,
            scheduled_at: now,
            attempted_at: now,
            handoff_accepted_at: null,
            failed_at: null,
            unknown_at: null,
            delivery_claimed_by: claimToken,
            delivery_claim_expires_at: claimExpiresAt,
            created_at: now,
            updated_at: now,
            metadata_json: serializeJson({
              [DELIVERY_ATTEMPT_INTERNAL_METADATA_KEY]: {
                version: DELIVERY_ATTEMPT_INTERNAL_METADATA_VERSION,
                claimedFactsRef: candidate.facts_ref,
                claimedSourceRunId: candidate.source_run_id,
              },
            }),
          }),
        );
        executeQuery(
          db,
          durableDb
            .updateTable("wake_obligations")
            .set({
              attempt_count: attemptNumber,
              last_attempt_at: now,
              next_attempt_at: nextAttemptAt,
              updated_at: now,
            })
            .where("wake_id", "=", candidate.wake_id),
        );
        const wake = queryFirst<WakeObligationRow>(
          db,
          durableDb
            .selectFrom("wake_obligations")
            .selectAll()
            .where("wake_id", "=", candidate.wake_id),
        );
        const attempt = queryFirst<DeliveryAttemptEvidenceRow>(
          db,
          durableDb
            .selectFrom("delivery_attempt_evidence")
            .selectAll()
            .where("delivery_attempt_id", "=", deliveryAttemptId),
        );
        return {
          wake: rowToWakeObligation(wake!),
          deliveryAttempt: rowToDeliveryAttemptEvidence(attempt!),
          claimToken,
          claimExpiresAt,
        };
      }
      return undefined;
    });
  };

  const completeWakeObligationClaimRecord = (
    input: CompleteWakeObligationClaimInput,
  ): DeliveryAttemptEvidence | undefined => {
    const now = input.now ?? Date.now();
    const validPair =
      (input.attemptStatus === "handoff_accepted" &&
        (input.wakeStatus === "handoff_accepted" || input.wakeStatus === "acked")) ||
      (input.attemptStatus === "failed" &&
        (input.wakeStatus === "failed" || input.wakeStatus === "suspended")) ||
      (input.attemptStatus === "unknown" && input.wakeStatus === "suspended") ||
      (input.attemptStatus === "superseded" && input.wakeStatus === "superseded");
    if (!validPair) {
      return undefined;
    }
    return runSqliteImmediateTransactionSync(db, () => {
      const lease = queryFirst<{ expires_at: number | bigint | null }>(
        db,
        durableDb
          .selectFrom("state_leases")
          .select("expires_at")
          .where("scope", "=", WAKE_OBLIGATION_LEASE_SCOPE)
          .where("lease_key", "=", input.wakeId)
          .where("owner", "=", input.claimToken),
      );
      if (!lease || lease.expires_at === null || Number(lease.expires_at) <= now) {
        return undefined;
      }
      const current = queryFirst<DeliveryAttemptEvidenceRow>(
        db,
        durableDb
          .selectFrom("delivery_attempt_evidence")
          .selectAll()
          .where("delivery_attempt_id", "=", input.deliveryAttemptId)
          .where("wake_id", "=", input.wakeId)
          .where("status", "=", "attempted")
          .where("delivery_claimed_by", "=", input.claimToken),
      );
      const wake = queryFirst<WakeObligationRow>(
        db,
        durableDb.selectFrom("wake_obligations").selectAll().where("wake_id", "=", input.wakeId),
      );
      if (
        !current ||
        current.delivery_claim_expires_at === null ||
        current.delivery_claim_expires_at <= now ||
        !wake ||
        isTerminalWakeStatus(wake.status) ||
        !attemptMatchesClaimedWakeRevision(current, wake)
      ) {
        return undefined;
      }
      if (!isAllowedWakeStatusTransition(wake.status, input.wakeStatus)) {
        return undefined;
      }
      if (input.attemptStatus === "unknown") {
        finalizeUnknownDeliveryAttempt({
          wake,
          attempt: current,
          now,
          error: optionalText(input.error) ?? "wake dispatch outcome is unknown",
          evidence: input.evidence,
        });
      } else {
        executeQuery(
          db,
          durableDb
            .updateTable("delivery_attempt_evidence")
            .set({
              status: input.attemptStatus,
              evidence_json: serializeJson(input.evidence),
              error_message: optionalText(input.error),
              handoff_accepted_at: input.attemptStatus === "handoff_accepted" ? now : null,
              failed_at: input.attemptStatus === "failed" ? now : null,
              delivery_claimed_by: null,
              delivery_claim_expires_at: null,
              updated_at: now,
            })
            .where("delivery_attempt_id", "=", input.deliveryAttemptId),
        );
      }
      executeQuery(
        db,
        durableDb
          .updateTable("wake_obligations")
          .set({
            status: input.wakeStatus,
            suspension_class:
              input.wakeStatus === "suspended"
                ? input.attemptStatus === "unknown"
                  ? "delivery_outcome_unknown"
                  : "owner_decision_required"
                : null,
            delivery_revision: wake.delivery_revision + 1,
            acked_at: input.wakeStatus === "acked" ? now : null,
            failed_reason:
              input.wakeStatus === "failed" || input.wakeStatus === "suspended"
                ? optionalText(input.error)
                : null,
            updated_at: now,
          })
          .where("wake_id", "=", input.wakeId),
      );
      if (input.attemptStatus !== "unknown") {
        executeQuery(
          db,
          durableDb
            .deleteFrom("state_leases")
            .where("scope", "=", WAKE_OBLIGATION_LEASE_SCOPE)
            .where("lease_key", "=", input.wakeId)
            .where("owner", "=", input.claimToken),
        );
      }
      const row = queryFirst<DeliveryAttemptEvidenceRow>(
        db,
        durableDb
          .selectFrom("delivery_attempt_evidence")
          .selectAll()
          .where("delivery_attempt_id", "=", input.deliveryAttemptId),
      );
      return row ? rowToDeliveryAttemptEvidence(row) : undefined;
    });
  };

  const renewWakeObligationClaimRecord = (input: RenewWakeObligationClaimInput): boolean => {
    requirePositiveSafeInteger(input.claimTtlMs, "Durable wake claimTtlMs");
    const now = input.now ?? Date.now();
    const claimExpiresAt = now + input.claimTtlMs;
    return runSqliteImmediateTransactionSync(db, () => {
      const lease = queryFirst<{ expires_at: number | bigint | null }>(
        db,
        durableDb
          .selectFrom("state_leases")
          .select("expires_at")
          .where("scope", "=", WAKE_OBLIGATION_LEASE_SCOPE)
          .where("lease_key", "=", input.wakeId)
          .where("owner", "=", input.claimToken),
      );
      const attempt = queryFirst<DeliveryAttemptEvidenceRow>(
        db,
        durableDb
          .selectFrom("delivery_attempt_evidence")
          .selectAll()
          .where("delivery_attempt_id", "=", input.deliveryAttemptId)
          .where("wake_id", "=", input.wakeId)
          .where("status", "=", "attempted")
          .where("delivery_claimed_by", "=", input.claimToken),
      );
      const wake = queryFirst<WakeObligationRow>(
        db,
        durableDb.selectFrom("wake_obligations").selectAll().where("wake_id", "=", input.wakeId),
      );
      if (
        !lease ||
        lease.expires_at === null ||
        Number(lease.expires_at) <= now ||
        !attempt ||
        attempt.delivery_claim_expires_at === null ||
        attempt.delivery_claim_expires_at <= now ||
        !wake ||
        !attemptMatchesClaimedWakeRevision(attempt, wake)
      ) {
        return false;
      }
      const renewedLease = executeQuery(
        db,
        durableDb
          .updateTable("state_leases")
          .set({ expires_at: claimExpiresAt, heartbeat_at: now, updated_at: now })
          .where("scope", "=", WAKE_OBLIGATION_LEASE_SCOPE)
          .where("lease_key", "=", input.wakeId)
          .where("owner", "=", input.claimToken),
      );
      const renewedAttempt = executeQuery(
        db,
        durableDb
          .updateTable("delivery_attempt_evidence")
          .set({ delivery_claim_expires_at: claimExpiresAt, updated_at: now })
          .where("delivery_attempt_id", "=", input.deliveryAttemptId)
          .where("wake_id", "=", input.wakeId)
          .where("status", "=", "attempted")
          .where("delivery_claimed_by", "=", input.claimToken),
      );
      return renewedLease === 1 && renewedAttempt === 1;
    });
  };

  const getDeliveryAttemptEvidenceRecord = (
    deliveryAttemptId: string,
  ): DeliveryAttemptEvidence | undefined => {
    const row = queryFirst<DeliveryAttemptEvidenceRow>(
      db,
      durableDb
        .selectFrom("delivery_attempt_evidence")
        .selectAll()
        .where("delivery_attempt_id", "=", deliveryAttemptId),
    );
    return row ? rowToDeliveryAttemptEvidence(row) : undefined;
  };

  const listDeliveryAttemptEvidenceRecords = (options?: {
    wakeId?: string;
    dedupeKey?: string;
    status?: DeliveryAttemptEvidenceStatus;
    limit?: number;
  }): DeliveryAttemptEvidence[] => {
    const wakeId = optionalText(options?.wakeId);
    const dedupeKey = optionalText(options?.dedupeKey);
    const rows = queryRows<DeliveryAttemptEvidenceRow>(
      db,
      durableDb
        .selectFrom("delivery_attempt_evidence")
        .selectAll()
        .$if(Boolean(wakeId), (qb) => qb.where("wake_id", "=", wakeId!))
        .$if(Boolean(dedupeKey), (qb) => qb.where("dedupe_key", "=", dedupeKey!))
        .$if(Boolean(options?.status), (qb) => qb.where("status", "=", options!.status!))
        .orderBy("scheduled_at", "desc")
        .orderBy("delivery_attempt_id", "desc")
        .limit(normalizeQueryLimit(options?.limit, 500)),
    );
    return rows.map(rowToDeliveryAttemptEvidence);
  };

  return {
    claimNextWakeObligationRecord,
    completeWakeObligationClaimRecord,
    renewWakeObligationClaimRecord,
    getDeliveryAttemptEvidenceRecord,
    listDeliveryAttemptEvidenceRecords,
  };
}

export type WakeClaimOperations = ReturnType<typeof createWakeClaimOperations>;
