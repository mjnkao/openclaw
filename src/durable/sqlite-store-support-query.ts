import type { DatabaseSync } from "node:sqlite";
import { sql } from "kysely";
import { executeSqliteQuerySync, executeSqliteQueryTakeFirstSync } from "../infra/kysely-sync.js";
import {
  optionalText,
  metadataText,
  UNRESOLVED_WAKE_CURSOR_VERSION,
  OPEN_RUN_CURSOR_VERSION,
  parseStoredJsonRecord,
  isRecordValue,
  parseMetadata,
  deliveryAttemptPublicMetadata,
} from "./sqlite-store-support-core.js";
import type {
  DurableRuntimeRunRow,
  DurableRuntimeEventRow,
  DurableRuntimeStepRow,
  DurableRuntimeRefRow,
  DurableRuntimeLinkRow,
  DurableRuntimeTimerRow,
  DurableRuntimeSignalRow,
  WakeObligationRow,
  UncertaintyFactRow,
  DeliveryAttemptEvidenceRow,
  DurableUnresolvedObligationRow,
  CountRow,
  SyncQuery,
} from "./sqlite-store-support-core.js";
import type {
  DurableRuntimeRunStatus,
  DurableRecoveryState,
  DurableRuntimeStepStatus,
  DurableRuntimeRun,
  DurableRuntimeStep,
  DurableRuntimeRef,
  DurableRuntimeLink,
  DurableRuntimeTimer,
  DurableRuntimeSignal,
  DurableRuntimeEvent,
  WakeObligationStatus,
  WakeObligation,
  UncertaintyFact,
  DeliveryAttemptEvidence,
  DurableUnresolvedObligation,
} from "./types.js";

export function rowToRun(row: DurableRuntimeRunRow): DurableRuntimeRun {
  const metadata = parseMetadata(row.metadata_json);
  const rootOperationReason = metadataText(metadata.rootOperationReason);
  return {
    runtimeRunId: row.runtime_run_id,
    operationKind: row.operation_kind,
    operationVersion: row.operation_version,
    status: row.status,
    recoveryState: row.recovery_state,
    ...(row.idempotency_key ? { idempotencyKey: row.idempotency_key } : {}),
    ...(row.request_hash ? { requestHash: row.request_hash } : {}),
    ...(row.source_owner ? { sourceOwner: row.source_owner } : {}),
    ...(row.source_ref ? { sourceRef: row.source_ref } : {}),
    ...(rootOperationReason ? { rootOperationReason } : {}),
    ...(row.input_ref ? { inputRef: row.input_ref } : {}),
    ...(row.checkpoint_ref ? { checkpointRef: row.checkpoint_ref } : {}),
    ...(row.parent_runtime_run_id ? { parentRuntimeRunId: row.parent_runtime_run_id } : {}),
    ...(row.parent_step_id ? { parentStepId: row.parent_step_id } : {}),
    ...(row.message_id ? { messageId: row.message_id } : {}),
    ...(row.turn_id ? { turnId: row.turn_id } : {}),
    ...(row.work_unit_id ? { workUnitId: row.work_unit_id } : {}),
    ...(row.report_route_ref ? { reportRouteRef: row.report_route_ref } : {}),
    ...(row.heartbeat_at == null ? {} : { heartbeatAt: row.heartbeat_at }),
    ...(row.metadata_json ? { metadata } : {}),
    createdAt: row.created_at,
    updatedAt: row.updated_at,
    ...(row.completed_at == null ? {} : { completedAt: row.completed_at }),
  };
}

export function rowToEvent(row: DurableRuntimeEventRow): DurableRuntimeEvent {
  return {
    eventId: row.event_id,
    runtimeRunId: row.runtime_run_id,
    eventSeq: row.event_seq,
    eventType: row.event_type,
    eventTime: row.event_time,
    ...(row.step_id ? { stepId: row.step_id } : {}),
    ...(row.agent_invocation_id ? { agentInvocationId: row.agent_invocation_id } : {}),
    ...(row.tool_invocation_id ? { toolInvocationId: row.tool_invocation_id } : {}),
    ...(row.idempotency_key ? { idempotencyKey: row.idempotency_key } : {}),
    ...(row.payload_json
      ? { payload: parseStoredJsonRecord(row.payload_json, "Durable event payload") }
      : {}),
    ...(row.payload_hash ? { payloadHash: row.payload_hash } : {}),
    ...(row.checkpoint_ref ? { checkpointRef: row.checkpoint_ref } : {}),
    ...(row.causation_event_id ? { causationEventId: row.causation_event_id } : {}),
    ...(row.correlation_id ? { correlationId: row.correlation_id } : {}),
    recordedAt: row.recorded_at,
  };
}

export function rowToStep(row: DurableRuntimeStepRow): DurableRuntimeStep {
  return {
    runtimeRunId: row.runtime_run_id,
    stepId: row.step_id,
    ...(row.parent_step_id ? { parentStepId: row.parent_step_id } : {}),
    stepType: row.step_type,
    status: row.status,
    recoveryState: row.recovery_state,
    attempt: row.attempt,
    ...(row.max_attempts == null ? {} : { maxAttempts: row.max_attempts }),
    ...(row.idempotency_key ? { idempotencyKey: row.idempotency_key } : {}),
    ...(row.input_ref ? { inputRef: row.input_ref } : {}),
    ...(row.output_ref ? { outputRef: row.output_ref } : {}),
    ...(row.error_ref ? { errorRef: row.error_ref } : {}),
    ...(row.checkpoint_ref ? { checkpointRef: row.checkpoint_ref } : {}),
    ...(row.claimed_by ? { claimedBy: row.claimed_by } : {}),
    ...(row.claim_expires_at == null ? {} : { claimExpiresAt: row.claim_expires_at }),
    ...(row.heartbeat_at == null ? {} : { heartbeatAt: row.heartbeat_at }),
    ...(row.metadata_json ? { metadata: parseMetadata(row.metadata_json) } : {}),
    createdAt: row.created_at,
    ...(row.started_at == null ? {} : { startedAt: row.started_at }),
    updatedAt: row.updated_at,
    ...(row.completed_at == null ? {} : { completedAt: row.completed_at }),
  };
}

export function rowToRef(row: DurableRuntimeRefRow): DurableRuntimeRef {
  return {
    refId: row.ref_id,
    runtimeRunId: row.runtime_run_id,
    ...(row.step_id ? { stepId: row.step_id } : {}),
    refKind: row.ref_kind,
    ...(row.media_type ? { mediaType: row.media_type } : {}),
    ...(row.hash ? { hash: row.hash } : {}),
    storageKind: row.storage_kind,
    ...(row.storage_uri ? { storageUri: row.storage_uri } : {}),
    ...(row.metadata_json ? { metadata: parseMetadata(row.metadata_json) } : {}),
    createdAt: row.created_at,
  };
}

export function rowToLink(row: DurableRuntimeLinkRow): DurableRuntimeLink {
  return {
    parentRuntimeRunId: row.parent_runtime_run_id,
    parentStepId: row.parent_step_id,
    childRuntimeRunId: row.child_runtime_run_id,
    linkType: row.link_type,
    status: row.status,
    ...(row.metadata_json ? { metadata: parseMetadata(row.metadata_json) } : {}),
    createdAt: row.created_at,
    updatedAt: row.updated_at,
  };
}

export function rowToTimer(row: DurableRuntimeTimerRow): DurableRuntimeTimer {
  return {
    timerId: row.timer_id,
    runtimeRunId: row.runtime_run_id,
    ...(row.step_id ? { stepId: row.step_id } : {}),
    timerType: row.timer_type,
    dueAt: row.due_at,
    status: row.status,
    ...(row.metadata_json ? { metadata: parseMetadata(row.metadata_json) } : {}),
    createdAt: row.created_at,
    ...(row.fired_at == null ? {} : { firedAt: row.fired_at }),
    ...(row.cancelled_at == null ? {} : { cancelledAt: row.cancelled_at }),
  };
}

export function rowToSignal(row: DurableRuntimeSignalRow): DurableRuntimeSignal {
  return {
    signalId: row.signal_id,
    runtimeRunId: row.runtime_run_id,
    ...(row.step_id ? { stepId: row.step_id } : {}),
    signalType: row.signal_type,
    ...(row.idempotency_key ? { idempotencyKey: row.idempotency_key } : {}),
    ...(row.payload_ref ? { payloadRef: row.payload_ref } : {}),
    ...(row.correlation_id ? { correlationId: row.correlation_id } : {}),
    ...(row.metadata_json ? { metadata: parseMetadata(row.metadata_json) } : {}),
    receivedAt: row.received_at,
    ...(row.consumed_at == null ? {} : { consumedAt: row.consumed_at }),
  };
}

export function rowToWakeObligation(row: WakeObligationRow): WakeObligation {
  const metadata = parseMetadata(row.metadata_json);
  const sourceRevision = metadataText(metadata.sourceRevision);
  return {
    wakeId: row.wake_id,
    sourceOwner: row.source_owner,
    sourceRef: row.source_ref,
    coalescingPolicy:
      row.coalescing_mode === "none"
        ? { mode: "none" }
        : { mode: "while_unresolved", recurrence: row.recurrence_policy! },
    ...(row.parent_run_id ? { parentRunId: row.parent_run_id } : {}),
    ...(row.parent_session_key ? { parentSessionKey: row.parent_session_key } : {}),
    ...(row.target_kind ? { targetKind: row.target_kind } : {}),
    ...(row.target_ref ? { targetRef: row.target_ref } : {}),
    ...(row.owner_kind ? { ownerKind: row.owner_kind } : {}),
    ...(row.owner_ref ? { ownerRef: row.owner_ref } : {}),
    ...(row.report_route_ref ? { reportRouteRef: row.report_route_ref } : {}),
    ...(row.target_resolution_status
      ? { targetResolutionStatus: row.target_resolution_status }
      : {}),
    ...(row.target_resolution_reason
      ? { targetResolutionReason: row.target_resolution_reason }
      : {}),
    reason: row.reason,
    ...(row.facts_ref ? { factsRef: row.facts_ref } : {}),
    ...(row.source_run_id ? { sourceRunId: row.source_run_id } : {}),
    ...(sourceRevision ? { sourceRevision } : {}),
    deliveryRevision: row.delivery_revision,
    ...(row.suspension_class ? { suspensionClass: row.suspension_class } : {}),
    attemptCount: row.attempt_count,
    ...(row.last_attempt_at == null ? {} : { lastAttemptAt: row.last_attempt_at }),
    ...(row.next_attempt_at == null ? {} : { nextAttemptAt: row.next_attempt_at }),
    ...(row.acked_at == null ? {} : { ackedAt: row.acked_at }),
    ...(row.failed_reason ? { failedReason: row.failed_reason } : {}),
    status: row.status,
    ...(row.metadata_json ? { metadata } : {}),
    createdAt: row.created_at,
    updatedAt: row.updated_at,
  };
}

export function rowToUncertaintyFact(row: UncertaintyFactRow): UncertaintyFact {
  return {
    factId: row.fact_id,
    sourceOwner: row.source_owner,
    sourceRef: row.source_ref,
    kind: row.kind,
    ...(row.source_run_id ? { sourceRunId: row.source_run_id } : {}),
    ...(row.step_id ? { stepId: row.step_id } : {}),
    ...(row.event_id ? { eventId: row.event_id } : {}),
    ...(row.ref_id ? { refId: row.ref_id } : {}),
    ...(row.facts_ref ? { factsRef: row.facts_ref } : {}),
    ...(row.dedupe_key ? { dedupeKey: row.dedupe_key } : {}),
    ...(row.facts_json
      ? { facts: parseStoredJsonRecord(row.facts_json, "Durable uncertainty facts") }
      : {}),
    status: row.status,
    ...(row.resolution_kind ? { resolutionKind: row.resolution_kind } : {}),
    ...(row.resolution_ref ? { resolutionRef: row.resolution_ref } : {}),
    ...(row.resolved_at == null ? {} : { resolvedAt: row.resolved_at }),
    ...(row.metadata_json ? { metadata: parseMetadata(row.metadata_json) } : {}),
    createdAt: row.created_at,
    updatedAt: row.updated_at,
  };
}

export function rowToDeliveryAttemptEvidence(
  row: DeliveryAttemptEvidenceRow,
): DeliveryAttemptEvidence {
  const metadata = deliveryAttemptPublicMetadata(row.metadata_json);
  return {
    deliveryAttemptId: row.delivery_attempt_id,
    sourceOwner: row.source_owner,
    sourceRef: row.source_ref,
    wakeId: row.wake_id,
    dedupeKey: row.dedupe_key,
    ...(row.replay_pass_id ? { replayPassId: row.replay_pass_id } : {}),
    ...(row.target_kind ? { targetKind: row.target_kind } : {}),
    ...(row.target_ref ? { targetRef: row.target_ref } : {}),
    ...(row.route_kind ? { routeKind: row.route_kind } : {}),
    ...(row.route_ref ? { routeRef: row.route_ref } : {}),
    status: row.status,
    claimedWakeDeliveryRevision: row.claimed_wake_delivery_revision,
    ...(row.evidence_json
      ? { evidence: parseStoredJsonRecord(row.evidence_json, "Durable delivery evidence") }
      : {}),
    ...(row.error_message ? { error: row.error_message } : {}),
    scheduledAt: row.scheduled_at,
    ...(row.attempted_at == null ? {} : { attemptedAt: row.attempted_at }),
    ...(row.handoff_accepted_at == null ? {} : { handoffAcceptedAt: row.handoff_accepted_at }),
    ...(row.failed_at == null ? {} : { failedAt: row.failed_at }),
    ...(row.unknown_at == null ? {} : { unknownAt: row.unknown_at }),
    ...(row.delivery_claimed_by ? { deliveryClaimedBy: row.delivery_claimed_by } : {}),
    ...(row.delivery_claim_expires_at == null
      ? {}
      : { deliveryClaimExpiresAt: row.delivery_claim_expires_at }),
    createdAt: row.created_at,
    updatedAt: row.updated_at,
    ...(metadata ? { metadata } : {}),
  };
}

export function rowToUnresolvedObligation(
  row: DurableUnresolvedObligationRow,
): DurableUnresolvedObligation {
  return {
    obligationId: row.obligation_id,
    sourceOwner: row.source_owner,
    sourceRef: row.source_ref,
    kind: row.kind,
    ...(row.runtime_run_id ? { runtimeRunId: row.runtime_run_id } : {}),
    ...(row.step_id ? { stepId: row.step_id } : {}),
    ...(row.wake_id ? { wakeId: row.wake_id } : {}),
    ...(row.uncertainty_fact_id ? { uncertaintyFactId: row.uncertainty_fact_id } : {}),
    ...(row.subject_ref ? { subjectRef: row.subject_ref } : {}),
    ...(row.reason ? { reason: row.reason } : {}),
    status: row.status,
    createdAt: Number(row.created_at),
    updatedAt: Number(row.updated_at),
    ...(row.metadata_json ? { metadata: parseMetadata(row.metadata_json) } : {}),
  };
}

export function queryRows<Row>(db: DatabaseSync, query: SyncQuery<Row>): Row[] {
  return executeSqliteQuerySync(db, query).rows as Row[];
}

export function queryFirst<Row>(db: DatabaseSync, query: SyncQuery<Row>): Row | undefined {
  return executeSqliteQueryTakeFirstSync(db, query) as Row | undefined;
}

export function executeQuery(db: DatabaseSync, query: SyncQuery<unknown>): number {
  const result = executeSqliteQuerySync(db, query);
  return Number(result.numAffectedRows ?? 0);
}

export function count(db: DatabaseSync, query: SyncQuery<CountRow>): number {
  const row = queryFirst<CountRow>(db, query);
  return Number(row?.count ?? 0);
}

export function normalizeQueryLimit(limit: number | undefined, fallback: number): number {
  return Math.max(1, Math.min(5000, Math.trunc(limit ?? fallback)));
}

export type UnresolvedWakePageCursor = {
  version: typeof UNRESOLVED_WAKE_CURSOR_VERSION;
  sourceOwner: string | null;
  createdAtOrBefore: number | null;
  createdAt: number;
  wakeId: string;
};

export function encodeUnresolvedWakePageCursor(cursor: UnresolvedWakePageCursor): string {
  return Buffer.from(JSON.stringify(cursor), "utf8").toString("base64url");
}

export function decodeUnresolvedWakePageCursor(value: string): UnresolvedWakePageCursor {
  try {
    if (Buffer.byteLength(value, "utf8") > 2048) {
      throw new Error("cursor is too large");
    }
    const decoded = JSON.parse(Buffer.from(value, "base64url").toString("utf8")) as unknown;
    if (!isRecordValue(decoded)) {
      throw new Error("cursor is not an object");
    }
    const sourceOwner = decoded.sourceOwner;
    const createdAtOrBefore = decoded.createdAtOrBefore;
    const createdAt = decoded.createdAt;
    const wakeId = decoded.wakeId;
    if (
      decoded.version !== UNRESOLVED_WAKE_CURSOR_VERSION ||
      (sourceOwner !== null &&
        (typeof sourceOwner !== "string" || optionalText(sourceOwner) !== sourceOwner)) ||
      (createdAtOrBefore !== null &&
        (typeof createdAtOrBefore !== "number" ||
          !Number.isSafeInteger(createdAtOrBefore) ||
          createdAtOrBefore < 0)) ||
      typeof createdAt !== "number" ||
      !Number.isSafeInteger(createdAt) ||
      typeof wakeId !== "string" ||
      optionalText(wakeId) !== wakeId
    ) {
      throw new Error("cursor fields are invalid");
    }
    return {
      version: UNRESOLVED_WAKE_CURSOR_VERSION,
      sourceOwner,
      createdAtOrBefore,
      createdAt,
      wakeId,
    };
  } catch {
    throw new Error("Invalid unresolved wake page cursor");
  }
}

export type OpenRunPageCursor = {
  version: typeof OPEN_RUN_CURSOR_VERSION;
  operationKind: string | null;
  updatedAtOrBefore: number | null;
  updatedAt: number;
  runtimeRunId: string;
};

export function encodeOpenRunPageCursor(cursor: OpenRunPageCursor): string {
  return Buffer.from(JSON.stringify(cursor), "utf8").toString("base64url");
}

export function decodeOpenRunPageCursor(value: string): OpenRunPageCursor {
  try {
    if (Buffer.byteLength(value, "utf8") > 2048) {
      throw new Error("cursor is too large");
    }
    const decoded = JSON.parse(Buffer.from(value, "base64url").toString("utf8")) as unknown;
    if (!isRecordValue(decoded)) {
      throw new Error("cursor is not an object");
    }
    const operationKind = decoded.operationKind;
    const updatedAtOrBefore = decoded.updatedAtOrBefore;
    const updatedAt = decoded.updatedAt;
    const runtimeRunId = decoded.runtimeRunId;
    if (
      decoded.version !== OPEN_RUN_CURSOR_VERSION ||
      (operationKind !== null &&
        (typeof operationKind !== "string" || optionalText(operationKind) !== operationKind)) ||
      (updatedAtOrBefore !== null &&
        (typeof updatedAtOrBefore !== "number" || !Number.isSafeInteger(updatedAtOrBefore))) ||
      typeof updatedAt !== "number" ||
      !Number.isSafeInteger(updatedAt) ||
      typeof runtimeRunId !== "string" ||
      optionalText(runtimeRunId) !== runtimeRunId
    ) {
      throw new Error("cursor fields are invalid");
    }
    return {
      version: OPEN_RUN_CURSOR_VERSION,
      operationKind,
      updatedAtOrBefore,
      updatedAt,
      runtimeRunId,
    };
  } catch {
    throw new Error("Invalid open durable run page cursor");
  }
}

export function isTerminalRunStatus(status: DurableRuntimeRunStatus): boolean {
  return (
    status === "succeeded" || status === "failed" || status === "cancelled" || status === "lost"
  );
}

export function isTerminalStepStatus(status: DurableRuntimeStepStatus): boolean {
  return (
    status === "succeeded" ||
    status === "failed" ||
    status === "cancelled" ||
    status === "lost" ||
    status === "skipped"
  );
}

export function isTerminalRunRow(row: DurableRuntimeRunRow): boolean {
  return (
    isTerminalRunStatus(row.status) ||
    row.recovery_state === "terminal" ||
    row.completed_at !== null
  );
}

export function isTerminalStepRow(row: DurableRuntimeStepRow): boolean {
  return (
    isTerminalStepStatus(row.status) ||
    row.recovery_state === "terminal" ||
    row.completed_at !== null
  );
}

export function assertCoherentRunLifecycle(input: {
  status: DurableRuntimeRunStatus;
  recoveryState: DurableRecoveryState;
  completedAt: number | bigint | null;
}): void {
  const terminal = isTerminalRunStatus(input.status);
  if (
    terminal !== (input.recoveryState === "terminal") ||
    terminal !== (input.completedAt !== null)
  ) {
    throw new Error(
      "Durable run lifecycle requires terminal status, terminal recovery, and completedAt to advance together",
    );
  }
}

export function assertCoherentStepLifecycle(input: {
  status: DurableRuntimeStepStatus;
  recoveryState: DurableRecoveryState;
  completedAt: number | bigint | null;
}): void {
  const terminal = isTerminalStepStatus(input.status);
  if (
    terminal !== (input.recoveryState === "terminal") ||
    terminal !== (input.completedAt !== null)
  ) {
    throw new Error(
      "Durable step lifecycle requires terminal status, terminal recovery, and completedAt to advance together",
    );
  }
}

export function isTerminalWakeStatus(status: WakeObligationStatus): boolean {
  return status === "acked" || status === "superseded";
}

export function unresolvedWakeStatusSql() {
  return sql<boolean>`status NOT IN ('acked', 'superseded')`; // kysely-allow-raw: closed status set
}

export function isAllowedWakeStatusTransition(
  current: WakeObligationStatus,
  next: WakeObligationStatus,
): boolean {
  if (current === next) {
    return true;
  }
  if (current === "pending") {
    return (
      next === "handoff_accepted" ||
      next === "acked" ||
      next === "failed" ||
      next === "suspended" ||
      next === "superseded"
    );
  }
  if (current === "handoff_accepted") {
    return next === "acked" || next === "failed" || next === "suspended" || next === "superseded";
  }
  if (current === "failed") {
    return (
      next === "handoff_accepted" ||
      next === "acked" ||
      next === "suspended" ||
      next === "superseded"
    );
  }
  if (current === "suspended") {
    return next === "pending" || next === "acked" || next === "superseded";
  }
  return false;
}

export function isSameSqlValue(
  left: string | number | bigint | null,
  right: string | number | bigint | null,
): boolean {
  return left === right;
}
