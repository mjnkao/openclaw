import { randomUUID } from "node:crypto";
import { runSqliteImmediateTransactionSync } from "../infra/sqlite-transaction.js";
import type { DurableSqliteStoreContext } from "./sqlite-store-context.js";
import {
  optionalText,
  DURABLE_STEP_LEASE_SCOPE,
  requirePositiveSafeInteger,
  durableStepLeaseKey,
  serializeJson,
  assertCompatibleStepReplay,
  mergeMetadataJson,
} from "./sqlite-store-support-core.js";
import type { DurableRuntimeStepRow } from "./sqlite-store-support-core.js";
import {
  rowToStep,
  queryRows,
  queryFirst,
  executeQuery,
  isTerminalStepStatus,
  isTerminalStepRow,
  assertCoherentStepLifecycle,
  isSameSqlValue,
} from "./sqlite-store-support-query.js";
import type {
  DurableRuntimeStep,
  CreateDurableRuntimeStepInput,
  UpdateDurableRuntimeStepInput,
  ClaimDurableRuntimeStepInput,
  DurableRuntimeStepClaim,
} from "./types.js";

export function createStepMethods(context: DurableSqliteStoreContext) {
  const { db, durableDb } = context;
  return {
    createStep(input: CreateDurableRuntimeStepInput): DurableRuntimeStep {
      const now = input.now ?? Date.now();
      const stepId = input.stepId ?? `step_${randomUUID()}`;
      const status = input.status ?? "pending";
      const recoveryState =
        input.recoveryState ?? (isTerminalStepStatus(status) ? "terminal" : "runnable");
      const completedAt = isTerminalStepStatus(status) ? now : null;
      assertCoherentStepLifecycle({ status, recoveryState, completedAt });
      const attempt = input.attempt ?? 1;
      const idempotencyKey = optionalText(input.idempotencyKey);
      return runSqliteImmediateTransactionSync(db, () => {
        const existingById = input.stepId
          ? queryFirst<DurableRuntimeStepRow>(
              db,
              durableDb
                .selectFrom("durable_execution_steps")
                .selectAll()
                .where("runtime_run_id", "=", input.runtimeRunId)
                .where("step_id", "=", input.stepId),
            )
          : undefined;
        const existingByIdempotency = idempotencyKey
          ? queryFirst<DurableRuntimeStepRow>(
              db,
              durableDb
                .selectFrom("durable_execution_steps")
                .selectAll()
                .where("runtime_run_id", "=", input.runtimeRunId)
                .where("idempotency_key", "=", idempotencyKey),
            )
          : undefined;
        if (
          existingById &&
          existingByIdempotency &&
          existingById.step_id !== existingByIdempotency.step_id
        ) {
          throw new Error(`Durable runtime step replay conflict for step id ${input.stepId}`);
        }
        const existing = existingById ?? existingByIdempotency;
        if (existing) {
          assertCompatibleStepReplay(
            existing,
            input,
            existingById
              ? `step id ${existing.step_id}`
              : `idempotency key ${input.runtimeRunId}:${idempotencyKey}`,
          );
          return rowToStep(existing);
        }
        executeQuery(
          db,
          durableDb.insertInto("durable_execution_steps").values({
            runtime_run_id: input.runtimeRunId,
            step_id: stepId,
            parent_step_id: optionalText(input.parentStepId),
            step_type: input.stepType,
            status,
            recovery_state: recoveryState,
            attempt,
            max_attempts: input.maxAttempts ?? null,
            idempotency_key: idempotencyKey,
            input_ref: optionalText(input.inputRef),
            output_ref: optionalText(input.outputRef),
            error_ref: optionalText(input.errorRef),
            checkpoint_ref: optionalText(input.checkpointRef),
            claimed_by: null,
            claim_expires_at: null,
            heartbeat_at: null,
            created_at: now,
            started_at: status === "running" ? now : null,
            updated_at: now,
            completed_at: completedAt,
            metadata_json: serializeJson(input.metadata),
          }),
        );
        const row = queryFirst<DurableRuntimeStepRow>(
          db,
          durableDb
            .selectFrom("durable_execution_steps")
            .selectAll()
            .where("runtime_run_id", "=", input.runtimeRunId)
            .where("step_id", "=", stepId),
        );
        return rowToStep(row!);
      });
    },

    updateStep(input: UpdateDurableRuntimeStepInput): DurableRuntimeStep | undefined {
      const now = input.now ?? Date.now();
      return runSqliteImmediateTransactionSync(db, () => {
        const current = queryFirst<DurableRuntimeStepRow>(
          db,
          durableDb
            .selectFrom("durable_execution_steps")
            .selectAll()
            .where("runtime_run_id", "=", input.runtimeRunId)
            .where("step_id", "=", input.stepId),
        );
        if (!current) {
          return undefined;
        }
        const expectedClaimToken = optionalText(input.expectedClaimToken);
        const nextStatus = input.status ?? current.status;
        const nextRecoveryState =
          input.recoveryState ??
          (input.status !== undefined && isTerminalStepStatus(nextStatus)
            ? "terminal"
            : current.recovery_state);
        const nextAttempt = input.attempt ?? current.attempt;
        const nextMaxAttempts =
          input.maxAttempts === undefined ? current.max_attempts : input.maxAttempts;
        const nextInputRef =
          input.inputRef === undefined
            ? current.input_ref
            : optionalText(input.inputRef ?? undefined);
        const nextOutputRef =
          input.outputRef === undefined
            ? current.output_ref
            : optionalText(input.outputRef ?? undefined);
        const nextErrorRef =
          input.errorRef === undefined
            ? current.error_ref
            : optionalText(input.errorRef ?? undefined);
        const nextCheckpointRef =
          input.checkpointRef === undefined
            ? current.checkpoint_ref
            : optionalText(input.checkpointRef ?? undefined);
        let nextClaimedBy =
          input.claimedBy === undefined
            ? current.claimed_by
            : optionalText(input.claimedBy ?? undefined);
        let nextClaimExpiresAt =
          input.claimExpiresAt === undefined ? current.claim_expires_at : input.claimExpiresAt;
        let nextHeartbeatAt =
          input.heartbeatAt === undefined ? current.heartbeat_at : input.heartbeatAt;
        const nextStartedAt =
          input.startedAt === undefined
            ? current.started_at
            : input.startedAt === null
              ? null
              : input.startedAt;
        const nextCompletedAt =
          input.completedAt === undefined
            ? input.status !== undefined && isTerminalStepStatus(nextStatus)
              ? now
              : current.completed_at
            : input.completedAt === null
              ? null
              : input.completedAt;
        assertCoherentStepLifecycle({
          status: nextStatus,
          recoveryState: nextRecoveryState,
          completedAt: nextCompletedAt,
        });
        const nextMetadataJson = mergeMetadataJson(current.metadata_json, input.metadata);
        if (nextClaimedBy !== null && nextClaimedBy !== current.claimed_by) {
          return undefined;
        }
        if (
          !expectedClaimToken &&
          ((input.claimExpiresAt != null && input.claimExpiresAt !== current.claim_expires_at) ||
            (input.heartbeatAt != null && input.heartbeatAt !== current.heartbeat_at))
        ) {
          return undefined;
        }
        if (isTerminalStepRow(current)) {
          if (expectedClaimToken && current.claimed_by !== expectedClaimToken) {
            return undefined;
          }
          const isNoOp =
            nextStatus === current.status &&
            nextRecoveryState === current.recovery_state &&
            isSameSqlValue(nextAttempt, current.attempt) &&
            isSameSqlValue(nextMaxAttempts, current.max_attempts) &&
            isSameSqlValue(nextInputRef, current.input_ref) &&
            isSameSqlValue(nextOutputRef, current.output_ref) &&
            isSameSqlValue(nextErrorRef, current.error_ref) &&
            isSameSqlValue(nextCheckpointRef, current.checkpoint_ref) &&
            isSameSqlValue(nextClaimedBy, current.claimed_by) &&
            isSameSqlValue(nextClaimExpiresAt, current.claim_expires_at) &&
            isSameSqlValue(nextHeartbeatAt, current.heartbeat_at) &&
            isSameSqlValue(nextStartedAt, current.started_at) &&
            isSameSqlValue(nextCompletedAt, current.completed_at) &&
            isSameSqlValue(nextMetadataJson, current.metadata_json);
          return isNoOp ? rowToStep(current) : undefined;
        }
        if (current.claimed_by !== null) {
          if (!expectedClaimToken || current.claimed_by !== expectedClaimToken) {
            return undefined;
          }
        } else if (expectedClaimToken) {
          return undefined;
        }
        if (expectedClaimToken) {
          const lease = queryFirst<{ expires_at: number | bigint | null }>(
            db,
            durableDb
              .selectFrom("state_leases")
              .select("expires_at")
              .where("scope", "=", DURABLE_STEP_LEASE_SCOPE)
              .where("lease_key", "=", durableStepLeaseKey(input.runtimeRunId, input.stepId))
              .where("owner", "=", expectedClaimToken),
          );
          if (!lease || lease.expires_at === null || Number(lease.expires_at) <= now) {
            return undefined;
          }
        }
        const settlesTerminal =
          isTerminalStepStatus(nextStatus) ||
          nextRecoveryState === "terminal" ||
          nextCompletedAt !== null;
        if (settlesTerminal) {
          nextClaimedBy = null;
          nextClaimExpiresAt = null;
          nextHeartbeatAt = null;
        }
        if (expectedClaimToken && nextClaimedBy === null) {
          executeQuery(
            db,
            durableDb
              .deleteFrom("state_leases")
              .where("scope", "=", DURABLE_STEP_LEASE_SCOPE)
              .where("lease_key", "=", durableStepLeaseKey(input.runtimeRunId, input.stepId))
              .where("owner", "=", expectedClaimToken),
          );
        }
        executeQuery(
          db,
          durableDb
            .updateTable("durable_execution_steps")
            .set({
              status: nextStatus,
              recovery_state: nextRecoveryState,
              attempt: nextAttempt,
              max_attempts: nextMaxAttempts,
              input_ref: nextInputRef,
              output_ref: nextOutputRef,
              error_ref: nextErrorRef,
              checkpoint_ref: nextCheckpointRef,
              claimed_by: nextClaimedBy,
              claim_expires_at: nextClaimExpiresAt,
              heartbeat_at: nextHeartbeatAt,
              started_at: nextStartedAt,
              completed_at: nextCompletedAt,
              updated_at: now,
              metadata_json: nextMetadataJson,
            })
            .where("runtime_run_id", "=", input.runtimeRunId)
            .where("step_id", "=", input.stepId),
        );
        const row = queryFirst<DurableRuntimeStepRow>(
          db,
          durableDb
            .selectFrom("durable_execution_steps")
            .selectAll()
            .where("runtime_run_id", "=", input.runtimeRunId)
            .where("step_id", "=", input.stepId),
        );
        return rowToStep(row!);
      });
    },

    claimNextRunnableStep(
      input: ClaimDurableRuntimeStepInput,
    ): DurableRuntimeStepClaim | undefined {
      requirePositiveSafeInteger(input.claimTtlMs, "Durable step claimTtlMs");
      const now = input.now ?? Date.now();
      const claimExpiresAt = now + input.claimTtlMs;
      return runSqliteImmediateTransactionSync(db, () => {
        const operationKind = optionalText(input.operationKind);
        const workerId = optionalText(input.workerId);
        if (!workerId) {
          throw new Error("Durable step claim requires workerId");
        }
        const row = queryFirst<DurableRuntimeStepRow>(
          db,
          durableDb
            .selectFrom("durable_execution_steps as s")
            .innerJoin("durable_execution_records as r", "r.runtime_run_id", "s.runtime_run_id")
            .selectAll("s")
            .where("s.status", "in", ["pending", "queued"])
            .where("s.recovery_state", "in", ["runnable", "claimed"])
            .where("s.completed_at", "is", null)
            .where((eb) =>
              eb.or([
                eb("s.claimed_by", "is", null),
                eb("s.claim_expires_at", "is", null),
                eb("s.claim_expires_at", "<=", now),
              ]),
            )
            .where("r.status", "not in", ["succeeded", "failed", "cancelled", "lost"])
            .where("r.recovery_state", "!=", "terminal")
            .where("r.completed_at", "is", null)
            .$if(Boolean(operationKind), (qb) => qb.where("r.operation_kind", "=", operationKind!))
            .$if(Boolean(input.stepType), (qb) => qb.where("s.step_type", "=", input.stepType!))
            .orderBy("s.updated_at", "asc")
            .orderBy("s.runtime_run_id", "asc")
            .orderBy("s.step_id", "asc")
            .limit(1),
        );
        if (!row) {
          return undefined;
        }
        const leaseKey = durableStepLeaseKey(row.runtime_run_id, row.step_id);
        executeQuery(
          db,
          durableDb
            .deleteFrom("state_leases")
            .where("scope", "=", DURABLE_STEP_LEASE_SCOPE)
            .where("lease_key", "=", leaseKey)
            .where("expires_at", "<=", now),
        );
        const existingLease = queryFirst<{ owner: string }>(
          db,
          durableDb
            .selectFrom("state_leases")
            .select("owner")
            .where("scope", "=", DURABLE_STEP_LEASE_SCOPE)
            .where("lease_key", "=", leaseKey),
        );
        if (existingLease) {
          return undefined;
        }
        const claimToken = `claim_${randomUUID()}`;
        executeQuery(
          db,
          durableDb.insertInto("state_leases").values({
            scope: DURABLE_STEP_LEASE_SCOPE,
            lease_key: leaseKey,
            owner: claimToken,
            expires_at: claimExpiresAt,
            heartbeat_at: now,
            payload_json: serializeJson({
              runtimeRunId: row.runtime_run_id,
              stepId: row.step_id,
              workerId,
            }),
            created_at: now,
            updated_at: now,
          }),
        );
        executeQuery(
          db,
          durableDb
            .updateTable("durable_execution_steps")
            .set({
              status: "queued",
              recovery_state: "claimed",
              claimed_by: claimToken,
              claim_expires_at: claimExpiresAt,
              heartbeat_at: now,
              updated_at: now,
            })
            .where("runtime_run_id", "=", row.runtime_run_id)
            .where("step_id", "=", row.step_id),
        );
        const claimed = queryFirst<DurableRuntimeStepRow>(
          db,
          durableDb
            .selectFrom("durable_execution_steps")
            .selectAll()
            .where("runtime_run_id", "=", row.runtime_run_id)
            .where("step_id", "=", row.step_id),
        );
        return {
          step: rowToStep(claimed!),
          claimToken,
          claimExpiresAt,
        };
      });
    },

    renewStepClaim(input: {
      runtimeRunId: string;
      stepId: string;
      claimToken: string;
      claimTtlMs: number;
      now?: number;
    }): DurableRuntimeStep | undefined {
      requirePositiveSafeInteger(input.claimTtlMs, "Durable step claimTtlMs");
      const now = input.now ?? Date.now();
      const claimExpiresAt = now + input.claimTtlMs;
      return runSqliteImmediateTransactionSync(db, () => {
        const lease = queryFirst<{ expires_at: number | bigint | null }>(
          db,
          durableDb
            .selectFrom("state_leases")
            .select("expires_at")
            .where("scope", "=", DURABLE_STEP_LEASE_SCOPE)
            .where("lease_key", "=", durableStepLeaseKey(input.runtimeRunId, input.stepId))
            .where("owner", "=", input.claimToken)
            .where("expires_at", ">", now),
        );
        const current = queryFirst<DurableRuntimeStepRow>(
          db,
          durableDb
            .selectFrom("durable_execution_steps")
            .selectAll()
            .where("runtime_run_id", "=", input.runtimeRunId)
            .where("step_id", "=", input.stepId)
            .where("claimed_by", "=", input.claimToken)
            .where("status", "not in", ["succeeded", "failed", "cancelled", "lost", "skipped"]),
        );
        if (!lease || !current) {
          return undefined;
        }
        executeQuery(
          db,
          durableDb
            .updateTable("state_leases")
            .set({
              expires_at: claimExpiresAt,
              heartbeat_at: now,
              updated_at: now,
            })
            .where("scope", "=", DURABLE_STEP_LEASE_SCOPE)
            .where("lease_key", "=", durableStepLeaseKey(input.runtimeRunId, input.stepId))
            .where("owner", "=", input.claimToken),
        );
        executeQuery(
          db,
          durableDb
            .updateTable("durable_execution_steps")
            .set({
              claim_expires_at: claimExpiresAt,
              heartbeat_at: now,
              updated_at: now,
            })
            .where("runtime_run_id", "=", input.runtimeRunId)
            .where("step_id", "=", input.stepId)
            .where("claimed_by", "=", input.claimToken)
            .where("status", "not in", ["succeeded", "failed", "cancelled", "lost", "skipped"]),
        );
        const row = queryFirst<DurableRuntimeStepRow>(
          db,
          durableDb
            .selectFrom("durable_execution_steps")
            .selectAll()
            .where("runtime_run_id", "=", input.runtimeRunId)
            .where("step_id", "=", input.stepId),
        );
        return row ? rowToStep(row) : undefined;
      });
    },

    releaseStepClaim(input: {
      runtimeRunId: string;
      stepId: string;
      claimToken: string;
      now?: number;
    }): DurableRuntimeStep | undefined {
      const now = input.now ?? Date.now();
      return runSqliteImmediateTransactionSync(db, () => {
        const current = queryFirst<DurableRuntimeStepRow>(
          db,
          durableDb
            .selectFrom("durable_execution_steps")
            .selectAll()
            .where("runtime_run_id", "=", input.runtimeRunId)
            .where("step_id", "=", input.stepId)
            .where("claimed_by", "=", input.claimToken)
            .where("status", "not in", ["succeeded", "failed", "cancelled", "lost", "skipped"])
            .where("recovery_state", "!=", "terminal")
            .where("completed_at", "is", null),
        );
        if (!current) {
          return undefined;
        }
        const lease = queryFirst<{ owner: string }>(
          db,
          durableDb
            .selectFrom("state_leases")
            .select("owner")
            .where("scope", "=", DURABLE_STEP_LEASE_SCOPE)
            .where("lease_key", "=", durableStepLeaseKey(input.runtimeRunId, input.stepId))
            .where("owner", "=", input.claimToken),
        );
        if (!lease) {
          return undefined;
        }
        executeQuery(
          db,
          durableDb
            .deleteFrom("state_leases")
            .where("scope", "=", DURABLE_STEP_LEASE_SCOPE)
            .where("lease_key", "=", durableStepLeaseKey(input.runtimeRunId, input.stepId))
            .where("owner", "=", input.claimToken),
        );
        executeQuery(
          db,
          durableDb
            .updateTable("durable_execution_steps")
            .set({
              status: current.status === "running" ? "queued" : current.status,
              recovery_state: "runnable",
              claimed_by: null,
              claim_expires_at: null,
              heartbeat_at: null,
              updated_at: now,
            })
            .where("runtime_run_id", "=", input.runtimeRunId)
            .where("step_id", "=", input.stepId),
        );
        const row = queryFirst<DurableRuntimeStepRow>(
          db,
          durableDb
            .selectFrom("durable_execution_steps")
            .selectAll()
            .where("runtime_run_id", "=", input.runtimeRunId)
            .where("step_id", "=", input.stepId),
        );
        return row ? rowToStep(row) : undefined;
      });
    },

    listSteps(runtimeRunId: string): DurableRuntimeStep[] {
      const rows = queryRows<DurableRuntimeStepRow>(
        db,
        durableDb
          .selectFrom("durable_execution_steps")
          .selectAll()
          .where("runtime_run_id", "=", runtimeRunId)
          .orderBy("created_at", "asc")
          .orderBy("step_id", "asc"),
      );
      return rows.map(rowToStep);
    },
  };
}
