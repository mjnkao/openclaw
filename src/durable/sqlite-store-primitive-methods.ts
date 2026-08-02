import { randomUUID } from "node:crypto";
import { runSqliteImmediateTransactionSync } from "../infra/sqlite-transaction.js";
import type { DurableSqliteStoreContext } from "./sqlite-store-context.js";
import {
  optionalText,
  serializeJson,
  assertCompatibleRefReplay,
  assertCompatibleLinkReplay,
  assertCompatibleTimerReplay,
  assertCompatibleSignalReplay,
  mergeMetadataJson,
} from "./sqlite-store-support-core.js";
import type {
  DurableRuntimeRefRow,
  DurableRuntimeLinkRow,
  DurableRuntimeTimerRow,
  DurableRuntimeSignalRow,
} from "./sqlite-store-support-core.js";
import {
  rowToRef,
  rowToLink,
  rowToTimer,
  rowToSignal,
  queryRows,
  queryFirst,
  executeQuery,
} from "./sqlite-store-support-query.js";
import type {
  DurableRuntimeRef,
  DurableRuntimeLink,
  DurableRuntimeTimer,
  DurableRuntimeSignal,
  CreateDurableRuntimeRefInput,
  CreateDurableRuntimeLinkInput,
  UpdateDurableRuntimeLinkInput,
  CreateDurableRuntimeTimerInput,
  UpdateDurableRuntimeTimerInput,
  CreateDurableRuntimeSignalInput,
} from "./types.js";

export function createPrimitiveMethods(context: DurableSqliteStoreContext) {
  const { db, durableDb } = context;
  return {
    createRef(input: CreateDurableRuntimeRefInput): DurableRuntimeRef {
      const now = input.now ?? Date.now();
      const refId = input.refId ?? `ref_${randomUUID()}`;
      return runSqliteImmediateTransactionSync(db, () => {
        const existing = input.refId
          ? queryFirst<DurableRuntimeRefRow>(
              db,
              durableDb
                .selectFrom("durable_payload_refs")
                .selectAll()
                .where("ref_id", "=", input.refId),
            )
          : undefined;
        if (existing) {
          assertCompatibleRefReplay(existing, input, `ref id ${input.refId}`);
          return rowToRef(existing);
        }
        executeQuery(
          db,
          durableDb.insertInto("durable_payload_refs").values({
            ref_id: refId,
            runtime_run_id: input.runtimeRunId,
            step_id: optionalText(input.stepId),
            ref_kind: input.refKind,
            media_type: optionalText(input.mediaType),
            hash: optionalText(input.hash),
            storage_kind: input.storageKind ?? "external",
            storage_uri: optionalText(input.storageUri),
            created_at: now,
            metadata_json: serializeJson(input.metadata),
          }),
        );
        const row = queryFirst<DurableRuntimeRefRow>(
          db,
          durableDb.selectFrom("durable_payload_refs").selectAll().where("ref_id", "=", refId),
        );
        return rowToRef(row!);
      });
    },

    getRef(refId: string): DurableRuntimeRef | undefined {
      const row = queryFirst<DurableRuntimeRefRow>(
        db,
        durableDb.selectFrom("durable_payload_refs").selectAll().where("ref_id", "=", refId),
      );
      return row ? rowToRef(row) : undefined;
    },

    listRefs(runtimeRunId: string): DurableRuntimeRef[] {
      const rows = queryRows<DurableRuntimeRefRow>(
        db,
        durableDb
          .selectFrom("durable_payload_refs")
          .selectAll()
          .where("runtime_run_id", "=", runtimeRunId)
          .orderBy("created_at", "asc")
          .orderBy("ref_id", "asc"),
      );
      return rows.map(rowToRef);
    },

    createLink(input: CreateDurableRuntimeLinkInput): DurableRuntimeLink {
      const now = input.now ?? Date.now();
      return runSqliteImmediateTransactionSync(db, () => {
        const identity = `${input.parentRuntimeRunId}:${input.parentStepId}:${input.childRuntimeRunId}`;
        const existing = queryFirst<DurableRuntimeLinkRow>(
          db,
          durableDb
            .selectFrom("durable_run_correlations")
            .selectAll()
            .where("parent_runtime_run_id", "=", input.parentRuntimeRunId)
            .where("parent_step_id", "=", input.parentStepId)
            .where("child_runtime_run_id", "=", input.childRuntimeRunId),
        );
        if (existing) {
          assertCompatibleLinkReplay(existing, input, `link identity ${identity}`);
          return rowToLink(existing);
        }
        executeQuery(
          db,
          durableDb.insertInto("durable_run_correlations").values({
            parent_runtime_run_id: input.parentRuntimeRunId,
            parent_step_id: input.parentStepId,
            child_runtime_run_id: input.childRuntimeRunId,
            link_type: input.linkType,
            status: input.status ?? "pending",
            created_at: now,
            updated_at: now,
            metadata_json: serializeJson(input.metadata),
          }),
        );
        const row = queryFirst<DurableRuntimeLinkRow>(
          db,
          durableDb
            .selectFrom("durable_run_correlations")
            .selectAll()
            .where("parent_runtime_run_id", "=", input.parentRuntimeRunId)
            .where("parent_step_id", "=", input.parentStepId)
            .where("child_runtime_run_id", "=", input.childRuntimeRunId),
        );
        return rowToLink(row!);
      });
    },

    updateLink(input: UpdateDurableRuntimeLinkInput): DurableRuntimeLink | undefined {
      const now = input.now ?? Date.now();
      return runSqliteImmediateTransactionSync(db, () => {
        const current = queryFirst<DurableRuntimeLinkRow>(
          db,
          durableDb
            .selectFrom("durable_run_correlations")
            .selectAll()
            .where("parent_runtime_run_id", "=", input.parentRuntimeRunId)
            .where("parent_step_id", "=", input.parentStepId)
            .where("child_runtime_run_id", "=", input.childRuntimeRunId),
        );
        if (!current) {
          return undefined;
        }
        executeQuery(
          db,
          durableDb
            .updateTable("durable_run_correlations")
            .set({
              status: input.status ?? current.status,
              updated_at: now,
              metadata_json: mergeMetadataJson(current.metadata_json, input.metadata),
            })
            .where("parent_runtime_run_id", "=", input.parentRuntimeRunId)
            .where("parent_step_id", "=", input.parentStepId)
            .where("child_runtime_run_id", "=", input.childRuntimeRunId),
        );
        const row = queryFirst<DurableRuntimeLinkRow>(
          db,
          durableDb
            .selectFrom("durable_run_correlations")
            .selectAll()
            .where("parent_runtime_run_id", "=", input.parentRuntimeRunId)
            .where("parent_step_id", "=", input.parentStepId)
            .where("child_runtime_run_id", "=", input.childRuntimeRunId),
        );
        return rowToLink(row!);
      });
    },

    listChildLinks(parentRuntimeRunId: string): DurableRuntimeLink[] {
      const rows = queryRows<DurableRuntimeLinkRow>(
        db,
        durableDb
          .selectFrom("durable_run_correlations")
          .selectAll()
          .where("parent_runtime_run_id", "=", parentRuntimeRunId)
          .orderBy("created_at", "asc")
          .orderBy("child_runtime_run_id", "asc"),
      );
      return rows.map(rowToLink);
    },

    listParentLinks(childRuntimeRunId: string): DurableRuntimeLink[] {
      const rows = queryRows<DurableRuntimeLinkRow>(
        db,
        durableDb
          .selectFrom("durable_run_correlations")
          .selectAll()
          .where("child_runtime_run_id", "=", childRuntimeRunId)
          .orderBy("created_at", "asc")
          .orderBy("parent_runtime_run_id", "asc")
          .orderBy("parent_step_id", "asc"),
      );
      return rows.map(rowToLink);
    },

    createTimer(input: CreateDurableRuntimeTimerInput): DurableRuntimeTimer {
      const now = input.now ?? Date.now();
      const timerId = input.timerId ?? `timer_${randomUUID()}`;
      return runSqliteImmediateTransactionSync(db, () => {
        const existing = input.timerId
          ? queryFirst<DurableRuntimeTimerRow>(
              db,
              durableDb
                .selectFrom("durable_timer_obligations")
                .selectAll()
                .where("timer_id", "=", input.timerId),
            )
          : undefined;
        if (existing) {
          assertCompatibleTimerReplay(existing, input, `timer id ${input.timerId}`);
          return rowToTimer(existing);
        }
        executeQuery(
          db,
          durableDb.insertInto("durable_timer_obligations").values({
            timer_id: timerId,
            runtime_run_id: input.runtimeRunId,
            step_id: optionalText(input.stepId),
            timer_type: input.timerType,
            due_at: input.dueAt,
            status: "pending",
            created_at: now,
            fired_at: null,
            cancelled_at: null,
            metadata_json: serializeJson(input.metadata),
          }),
        );
        const row = queryFirst<DurableRuntimeTimerRow>(
          db,
          durableDb
            .selectFrom("durable_timer_obligations")
            .selectAll()
            .where("timer_id", "=", timerId),
        );
        return rowToTimer(row!);
      });
    },

    updateTimer(input: UpdateDurableRuntimeTimerInput): DurableRuntimeTimer | undefined {
      const now = input.now ?? Date.now();
      return runSqliteImmediateTransactionSync(db, () => {
        const current = queryFirst<DurableRuntimeTimerRow>(
          db,
          durableDb
            .selectFrom("durable_timer_obligations")
            .selectAll()
            .where("timer_id", "=", input.timerId),
        );
        if (!current) {
          return undefined;
        }
        executeQuery(
          db,
          durableDb
            .updateTable("durable_timer_obligations")
            .set({
              status: input.status,
              fired_at:
                input.firedAt === undefined
                  ? input.status === "fired"
                    ? now
                    : current.fired_at
                  : input.firedAt,
              cancelled_at:
                input.cancelledAt === undefined
                  ? input.status === "cancelled"
                    ? now
                    : current.cancelled_at
                  : input.cancelledAt,
            })
            .where("timer_id", "=", input.timerId),
        );
        const row = queryFirst<DurableRuntimeTimerRow>(
          db,
          durableDb
            .selectFrom("durable_timer_obligations")
            .selectAll()
            .where("timer_id", "=", input.timerId),
        );
        return rowToTimer(row!);
      });
    },

    fireDueTimer(input: { timerId: string; now?: number }): DurableRuntimeTimer | undefined {
      const now = input.now ?? Date.now();
      return runSqliteImmediateTransactionSync(db, () => {
        const updated = executeQuery(
          db,
          durableDb
            .updateTable("durable_timer_obligations")
            .set({ status: "fired", fired_at: now })
            .where("timer_id", "=", input.timerId)
            .where("status", "=", "pending")
            .where("due_at", "<=", now),
        );
        if (updated !== 1) {
          return undefined;
        }
        const row = queryFirst<DurableRuntimeTimerRow>(
          db,
          durableDb
            .selectFrom("durable_timer_obligations")
            .selectAll()
            .where("timer_id", "=", input.timerId),
        );
        return row ? rowToTimer(row) : undefined;
      });
    },

    listTimers(runtimeRunId?: string): DurableRuntimeTimer[] {
      const rows = queryRows<DurableRuntimeTimerRow>(
        db,
        durableDb
          .selectFrom("durable_timer_obligations")
          .selectAll()
          .$if(Boolean(runtimeRunId), (qb) => qb.where("runtime_run_id", "=", runtimeRunId!))
          .orderBy("due_at", "asc")
          .orderBy("timer_id", "asc"),
      );
      return rows.map(rowToTimer);
    },

    listDueTimers(now: number, options?: { limit?: number }): DurableRuntimeTimer[] {
      const limit = Math.max(1, Math.min(5000, Math.trunc(options?.limit ?? 500)));
      const rows = queryRows<DurableRuntimeTimerRow>(
        db,
        durableDb
          .selectFrom("durable_timer_obligations")
          .selectAll()
          .where("status", "=", "pending")
          .where("due_at", "<=", now)
          .orderBy("due_at", "asc")
          .orderBy("timer_id", "asc")
          .limit(limit),
      );
      return rows.map(rowToTimer);
    },

    createSignal(input: CreateDurableRuntimeSignalInput): DurableRuntimeSignal {
      const now = input.now ?? Date.now();
      const idempotencyKey = optionalText(input.idempotencyKey);
      return runSqliteImmediateTransactionSync(db, () => {
        const existingById = input.signalId
          ? queryFirst<DurableRuntimeSignalRow>(
              db,
              durableDb
                .selectFrom("durable_signal_evidence")
                .selectAll()
                .where("signal_id", "=", input.signalId),
            )
          : undefined;
        const existingByIdempotency = idempotencyKey
          ? queryFirst<DurableRuntimeSignalRow>(
              db,
              durableDb
                .selectFrom("durable_signal_evidence")
                .selectAll()
                .where("runtime_run_id", "=", input.runtimeRunId)
                .where("idempotency_key", "=", idempotencyKey),
            )
          : undefined;
        if (
          existingById &&
          existingByIdempotency &&
          existingById.signal_id !== existingByIdempotency.signal_id
        ) {
          throw new Error(`Durable runtime signal replay conflict for signal id ${input.signalId}`);
        }
        const existing = existingById ?? existingByIdempotency;
        if (existing) {
          assertCompatibleSignalReplay(
            existing,
            input,
            existingById
              ? `signal id ${existing.signal_id}`
              : `idempotency key ${input.runtimeRunId}:${idempotencyKey}`,
          );
          return rowToSignal(existing);
        }
        const signalId = input.signalId ?? `sig_${randomUUID()}`;
        executeQuery(
          db,
          durableDb.insertInto("durable_signal_evidence").values({
            signal_id: signalId,
            runtime_run_id: input.runtimeRunId,
            step_id: optionalText(input.stepId),
            signal_type: input.signalType,
            idempotency_key: idempotencyKey,
            payload_ref: optionalText(input.payloadRef),
            correlation_id: optionalText(input.correlationId),
            received_at: now,
            consumed_at: null,
            metadata_json: serializeJson(input.metadata),
          }),
        );
        const row = queryFirst<DurableRuntimeSignalRow>(
          db,
          durableDb
            .selectFrom("durable_signal_evidence")
            .selectAll()
            .where("signal_id", "=", signalId),
        );
        return rowToSignal(row!);
      });
    },

    consumeSignal(input: { signalId: string; now?: number }): DurableRuntimeSignal | undefined {
      const now = input.now ?? Date.now();
      return runSqliteImmediateTransactionSync(db, () => {
        const current = queryFirst<DurableRuntimeSignalRow>(
          db,
          durableDb
            .selectFrom("durable_signal_evidence")
            .selectAll()
            .where("signal_id", "=", input.signalId),
        );
        if (!current) {
          return undefined;
        }
        executeQuery(
          db,
          durableDb
            .updateTable("durable_signal_evidence")
            .set({ consumed_at: current.consumed_at ?? now })
            .where("signal_id", "=", input.signalId),
        );
        const row = queryFirst<DurableRuntimeSignalRow>(
          db,
          durableDb
            .selectFrom("durable_signal_evidence")
            .selectAll()
            .where("signal_id", "=", input.signalId),
        );
        return row ? rowToSignal(row) : undefined;
      });
    },

    consumePendingSignal(input: {
      signalId: string;
      now?: number;
    }): DurableRuntimeSignal | undefined {
      const now = input.now ?? Date.now();
      return runSqliteImmediateTransactionSync(db, () => {
        const updated = executeQuery(
          db,
          durableDb
            .updateTable("durable_signal_evidence")
            .set({ consumed_at: now })
            .where("signal_id", "=", input.signalId)
            .where("consumed_at", "is", null),
        );
        if (updated !== 1) {
          return undefined;
        }
        const row = queryFirst<DurableRuntimeSignalRow>(
          db,
          durableDb
            .selectFrom("durable_signal_evidence")
            .selectAll()
            .where("signal_id", "=", input.signalId),
        );
        return row ? rowToSignal(row) : undefined;
      });
    },

    listPendingSignals(options?: { limit?: number }): DurableRuntimeSignal[] {
      const limit = Math.max(1, Math.min(5000, Math.trunc(options?.limit ?? 500)));
      const rows = queryRows<DurableRuntimeSignalRow>(
        db,
        durableDb
          .selectFrom("durable_signal_evidence")
          .selectAll()
          .where("consumed_at", "is", null)
          .orderBy("received_at", "asc")
          .orderBy("signal_id", "asc")
          .limit(limit),
      );
      return rows.map(rowToSignal);
    },

    listSignals(runtimeRunId: string): DurableRuntimeSignal[] {
      const rows = queryRows<DurableRuntimeSignalRow>(
        db,
        durableDb
          .selectFrom("durable_signal_evidence")
          .selectAll()
          .where("runtime_run_id", "=", runtimeRunId)
          .orderBy("received_at", "asc")
          .orderBy("signal_id", "asc"),
      );
      return rows.map(rowToSignal);
    },
  };
}
