import { randomUUID } from "node:crypto";
import { runSqliteImmediateTransactionSync } from "../infra/sqlite-transaction.js";
import type { DurableSqliteStoreContext } from "./sqlite-store-context.js";
import {
  optionalText,
  MAX_OPEN_RUN_PAGE_SIZE,
  OPEN_RUN_CURSOR_VERSION,
  requirePositiveSafeInteger,
  serializeJson,
  assertCompatibleEventReplay,
  assertCompatibleRunReplay,
  mergeMetadataJson,
} from "./sqlite-store-support-core.js";
import type { DurableRuntimeRunRow, DurableRuntimeEventRow } from "./sqlite-store-support-core.js";
import {
  rowToRun,
  rowToEvent,
  queryRows,
  queryFirst,
  executeQuery,
  encodeOpenRunPageCursor,
  decodeOpenRunPageCursor,
  isTerminalRunStatus,
  isTerminalRunRow,
  assertCoherentRunLifecycle,
  isSameSqlValue,
} from "./sqlite-store-support-query.js";
import type {
  DurableRuntimeRun,
  DurableRuntimeRunPage,
  DurableRuntimeEvent,
  CreateDurableRuntimeRunInput,
  AppendDurableRuntimeEventInput,
  UpdateDurableRuntimeRunInput,
} from "./types.js";

export function createRunMethods(context: DurableSqliteStoreContext) {
  const { db, durableDb } = context;
  return {
    withTransaction<T>(operation: () => T): T {
      return runSqliteImmediateTransactionSync(db, operation);
    },

    createRun(input: CreateDurableRuntimeRunInput): DurableRuntimeRun {
      const sourceOwner = optionalText(input.sourceOwner);
      const sourceRef = optionalText(input.sourceRef);
      const rootOperationReason = optionalText(input.rootOperationReason);
      if ((sourceOwner && !sourceRef) || (!sourceOwner && sourceRef)) {
        throw new Error(
          "Durable execution record sourceOwner and sourceRef must be provided together",
        );
      }
      if (sourceOwner && sourceRef && rootOperationReason) {
        throw new Error(
          "Durable execution record must use sourceOwner/sourceRef or rootOperationReason, not both",
        );
      }
      if ((!sourceOwner || !sourceRef) && !rootOperationReason) {
        throw new Error(
          "Durable execution record requires sourceOwner/sourceRef or rootOperationReason",
        );
      }
      const now = input.now ?? Date.now();
      const runtimeRunId = input.runtimeRunId ?? `run_${randomUUID()}`;
      const operationVersion = input.operationVersion ?? "1";
      const status = input.status ?? "received";
      const recoveryState =
        input.recoveryState ?? (isTerminalRunStatus(status) ? "terminal" : "runnable");
      const completedAt = input.completedAt ?? (isTerminalRunStatus(status) ? now : null);
      assertCoherentRunLifecycle({ status, recoveryState, completedAt });
      const idempotencyKey = optionalText(input.idempotencyKey);
      const metadata = {
        ...input.metadata,
        ...(rootOperationReason ? { rootOperationReason } : {}),
      };
      return runSqliteImmediateTransactionSync(db, () => {
        const existingById = input.runtimeRunId
          ? queryFirst<DurableRuntimeRunRow>(
              db,
              durableDb
                .selectFrom("durable_execution_records")
                .selectAll()
                .where("runtime_run_id", "=", input.runtimeRunId),
            )
          : undefined;
        const existingByIdempotency = idempotencyKey
          ? queryFirst<DurableRuntimeRunRow>(
              db,
              durableDb
                .selectFrom("durable_execution_records")
                .selectAll()
                .where("operation_kind", "=", input.operationKind)
                .where("idempotency_key", "=", idempotencyKey),
            )
          : undefined;
        if (
          existingById &&
          existingByIdempotency &&
          existingById.runtime_run_id !== existingByIdempotency.runtime_run_id
        ) {
          throw new Error(
            `Durable runtime run replay conflict for runtime run id ${input.runtimeRunId}`,
          );
        }
        const existing = existingById ?? existingByIdempotency;
        if (existing) {
          assertCompatibleRunReplay(
            existing,
            input,
            { operationVersion, sourceOwner, sourceRef, rootOperationReason },
            existingById
              ? `runtime run id ${existing.runtime_run_id}`
              : `idempotency key ${input.operationKind}:${idempotencyKey}`,
          );
          return rowToRun(existing);
        }
        executeQuery(
          db,
          durableDb.insertInto("durable_execution_records").values({
            runtime_run_id: runtimeRunId,
            operation_kind: input.operationKind,
            operation_version: operationVersion,
            idempotency_key: idempotencyKey,
            request_hash: optionalText(input.requestHash),
            status,
            source_owner: sourceOwner,
            source_ref: sourceRef,
            input_ref: optionalText(input.inputRef),
            created_at: now,
            updated_at: now,
            completed_at: completedAt,
            recovery_state: recoveryState,
            checkpoint_ref: optionalText(input.checkpointRef),
            parent_runtime_run_id: optionalText(input.parentRuntimeRunId),
            parent_step_id: optionalText(input.parentStepId),
            message_id: optionalText(input.messageId),
            turn_id: optionalText(input.turnId),
            work_unit_id: optionalText(input.workUnitId),
            report_route_ref: optionalText(input.reportRouteRef),
            heartbeat_at: null,
            metadata_json: serializeJson(metadata),
          }),
        );
        const row = queryFirst<DurableRuntimeRunRow>(
          db,
          durableDb
            .selectFrom("durable_execution_records")
            .selectAll()
            .where("runtime_run_id", "=", runtimeRunId),
        );
        return rowToRun(row!);
      });
    },

    getRun(runtimeRunId: string): DurableRuntimeRun | undefined {
      const row = queryFirst<DurableRuntimeRunRow>(
        db,
        durableDb
          .selectFrom("durable_execution_records")
          .selectAll()
          .where("runtime_run_id", "=", runtimeRunId),
      );
      return row ? rowToRun(row) : undefined;
    },

    getRunByIdempotencyKey(
      operationKind: string,
      idempotencyKey: string,
    ): DurableRuntimeRun | undefined {
      const row = queryFirst<DurableRuntimeRunRow>(
        db,
        durableDb
          .selectFrom("durable_execution_records")
          .selectAll()
          .where("operation_kind", "=", operationKind)
          .where("idempotency_key", "=", idempotencyKey),
      );
      return row ? rowToRun(row) : undefined;
    },

    updateRun(input: UpdateDurableRuntimeRunInput): DurableRuntimeRun | undefined {
      const now = input.now ?? Date.now();
      return runSqliteImmediateTransactionSync(db, () => {
        const current = queryFirst<DurableRuntimeRunRow>(
          db,
          durableDb
            .selectFrom("durable_execution_records")
            .selectAll()
            .where("runtime_run_id", "=", input.runtimeRunId),
        );
        if (!current) {
          return undefined;
        }
        const nextStatus = input.status ?? current.status;
        const nextRecoveryState =
          input.recoveryState ??
          (input.status !== undefined && isTerminalRunStatus(nextStatus)
            ? "terminal"
            : current.recovery_state);
        const completedAt =
          input.completedAt === undefined
            ? input.status !== undefined && isTerminalRunStatus(nextStatus)
              ? now
              : current.completed_at
            : input.completedAt === null
              ? null
              : input.completedAt;
        assertCoherentRunLifecycle({
          status: nextStatus,
          recoveryState: nextRecoveryState,
          completedAt,
        });
        const nextCheckpointRef =
          input.checkpointRef === undefined
            ? current.checkpoint_ref
            : optionalText(input.checkpointRef ?? undefined);
        const nextWorkUnitId =
          input.workUnitId === undefined
            ? current.work_unit_id
            : optionalText(input.workUnitId ?? undefined);
        const nextReportRouteRef =
          input.reportRouteRef === undefined
            ? current.report_route_ref
            : optionalText(input.reportRouteRef ?? undefined);
        const nextHeartbeatAt =
          input.heartbeatAt === undefined ? current.heartbeat_at : input.heartbeatAt;
        const nextMetadataJson = mergeMetadataJson(current.metadata_json, input.metadata);
        if (isTerminalRunRow(current)) {
          const isNoOp =
            nextStatus === current.status &&
            nextRecoveryState === current.recovery_state &&
            isSameSqlValue(completedAt, current.completed_at) &&
            isSameSqlValue(nextCheckpointRef, current.checkpoint_ref) &&
            isSameSqlValue(nextWorkUnitId, current.work_unit_id) &&
            isSameSqlValue(nextReportRouteRef, current.report_route_ref) &&
            isSameSqlValue(nextHeartbeatAt, current.heartbeat_at) &&
            isSameSqlValue(nextMetadataJson, current.metadata_json);
          return isNoOp ? rowToRun(current) : undefined;
        }
        executeQuery(
          db,
          durableDb
            .updateTable("durable_execution_records")
            .set({
              status: nextStatus,
              recovery_state: nextRecoveryState,
              updated_at: now,
              completed_at: completedAt,
              checkpoint_ref: nextCheckpointRef,
              work_unit_id: nextWorkUnitId,
              report_route_ref: nextReportRouteRef,
              heartbeat_at: nextHeartbeatAt,
              metadata_json: nextMetadataJson,
            })
            .where("runtime_run_id", "=", input.runtimeRunId),
        );
        const row = queryFirst<DurableRuntimeRunRow>(
          db,
          durableDb
            .selectFrom("durable_execution_records")
            .selectAll()
            .where("runtime_run_id", "=", input.runtimeRunId),
        );
        return rowToRun(row!);
      });
    },

    appendEvent(input: AppendDurableRuntimeEventInput): DurableRuntimeEvent {
      const now = input.eventTime ?? Date.now();
      const recordedAt = Date.now();
      const eventId = input.eventId ?? `evt_${randomUUID()}`;
      return runSqliteImmediateTransactionSync(db, () => {
        const existingById = input.eventId
          ? queryFirst<DurableRuntimeEventRow>(
              db,
              durableDb
                .selectFrom("durable_event_evidence")
                .selectAll()
                .where("event_id", "=", input.eventId),
            )
          : undefined;
        if (existingById) {
          assertCompatibleEventReplay(existingById, input, `event id ${input.eventId}`);
          return rowToEvent(existingById);
        }
        const existingByIdempotency = input.idempotencyKey
          ? queryFirst<DurableRuntimeEventRow>(
              db,
              durableDb
                .selectFrom("durable_event_evidence")
                .selectAll()
                .where("runtime_run_id", "=", input.runtimeRunId)
                .where("event_type", "=", input.eventType)
                .where("idempotency_key", "=", input.idempotencyKey),
            )
          : undefined;
        if (existingByIdempotency) {
          assertCompatibleEventReplay(
            existingByIdempotency,
            input,
            `idempotency key ${input.runtimeRunId}:${input.eventType}:${input.idempotencyKey}`,
          );
          return rowToEvent(existingByIdempotency);
        }
        const latestEvent = queryFirst<Pick<DurableRuntimeEventRow, "event_seq">>(
          db,
          durableDb
            .selectFrom("durable_event_evidence")
            .select("event_seq")
            .where("runtime_run_id", "=", input.runtimeRunId)
            .orderBy("event_seq", "desc")
            .limit(1),
        );
        const nextSeq = (latestEvent?.event_seq ?? 0) + 1;
        executeQuery(
          db,
          durableDb.insertInto("durable_event_evidence").values({
            event_id: eventId,
            runtime_run_id: input.runtimeRunId,
            event_seq: nextSeq,
            event_type: input.eventType,
            event_time: now,
            step_id: optionalText(input.stepId),
            agent_invocation_id: optionalText(input.agentInvocationId),
            tool_invocation_id: optionalText(input.toolInvocationId),
            idempotency_key: optionalText(input.idempotencyKey),
            payload_json: serializeJson(input.payload),
            payload_hash: optionalText(input.payloadHash),
            checkpoint_ref: optionalText(input.checkpointRef),
            causation_event_id: optionalText(input.causationEventId),
            correlation_id: optionalText(input.correlationId),
            recorded_at: recordedAt,
          }),
        );
        executeQuery(
          db,
          durableDb
            .updateTable("durable_execution_records")
            .set({ updated_at: recordedAt })
            .where("runtime_run_id", "=", input.runtimeRunId),
        );
        const row = queryFirst<DurableRuntimeEventRow>(
          db,
          durableDb
            .selectFrom("durable_event_evidence")
            .selectAll()
            .where("runtime_run_id", "=", input.runtimeRunId)
            .where("event_seq", "=", nextSeq),
        );
        return rowToEvent(row!);
      });
    },

    listRuns(options?: { limit?: number }): DurableRuntimeRun[] {
      const limit = Math.max(1, Math.min(500, Math.trunc(options?.limit ?? 50)));
      const rows = queryRows<DurableRuntimeRunRow>(
        db,
        durableDb
          .selectFrom("durable_execution_records")
          .selectAll()
          .orderBy("updated_at", "desc")
          .orderBy("runtime_run_id", "desc")
          .limit(limit),
      );
      return rows.map(rowToRun);
    },

    listOpenRuns(options?: {
      operationKind?: string;
      updatedAtOrBefore?: number;
      cursor?: string;
      limit?: number;
    }): DurableRuntimeRunPage {
      const limit = options?.limit ?? MAX_OPEN_RUN_PAGE_SIZE;
      requirePositiveSafeInteger(limit, "Open durable run page limit");
      if (limit > MAX_OPEN_RUN_PAGE_SIZE) {
        throw new Error(`Open durable run page limit must not exceed ${MAX_OPEN_RUN_PAGE_SIZE}`);
      }
      if (
        options?.updatedAtOrBefore !== undefined &&
        !Number.isSafeInteger(options.updatedAtOrBefore)
      ) {
        throw new Error("Open durable run updatedAtOrBefore must be a safe integer");
      }
      const requestedOperationKind = optionalText(options?.operationKind);
      const requestedUpdatedAtOrBefore = options?.updatedAtOrBefore ?? null;
      const cursor = options?.cursor ? decodeOpenRunPageCursor(options.cursor) : undefined;
      if (
        cursor &&
        requestedOperationKind !== null &&
        requestedOperationKind !== cursor.operationKind
      ) {
        throw new Error("Open durable run page cursor does not match operationKind");
      }
      if (
        cursor &&
        options?.updatedAtOrBefore !== undefined &&
        requestedUpdatedAtOrBefore !== cursor.updatedAtOrBefore
      ) {
        throw new Error("Open durable run page cursor does not match updatedAtOrBefore");
      }
      const operationKind = cursor?.operationKind ?? requestedOperationKind;
      const updatedAtOrBefore = cursor?.updatedAtOrBefore ?? requestedUpdatedAtOrBefore;
      const query = durableDb
        .selectFrom("durable_execution_records")
        .selectAll()
        .where("status", "not in", ["succeeded", "failed", "cancelled", "lost"])
        .where("recovery_state", "!=", "terminal")
        .where("completed_at", "is", null)
        .$if(Boolean(operationKind), (qb) => qb.where("operation_kind", "=", operationKind!))
        .$if(updatedAtOrBefore !== null, (qb) => qb.where("updated_at", "<=", updatedAtOrBefore!))
        .$if(cursor !== undefined, (qb) =>
          qb.where((eb) =>
            eb.or([
              eb("updated_at", ">", cursor!.updatedAt),
              eb.and([
                eb("updated_at", "=", cursor!.updatedAt),
                eb("runtime_run_id", ">", cursor!.runtimeRunId),
              ]),
            ]),
          ),
        )
        .orderBy("updated_at", "asc")
        .orderBy("runtime_run_id", "asc")
        .limit(limit + 1);
      const rows = queryRows<DurableRuntimeRunRow>(db, query);
      const pageRows = rows.slice(0, limit);
      const complete = rows.length <= limit;
      const last = pageRows.at(-1);
      return {
        runs: pageRows.map(rowToRun),
        complete,
        ...(!complete && last
          ? {
              nextCursor: encodeOpenRunPageCursor({
                version: OPEN_RUN_CURSOR_VERSION,
                operationKind,
                updatedAtOrBefore,
                updatedAt: last.updated_at,
                runtimeRunId: last.runtime_run_id,
              }),
            }
          : {}),
      };
    },
  };
}
