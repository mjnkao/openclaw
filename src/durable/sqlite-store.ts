// SQLite-backed durable runtime store for the native control plane.
import { createHash, randomUUID } from "node:crypto";
import path from "node:path";
import type { DatabaseSync } from "node:sqlite";
import { sql, type Selectable } from "kysely";
import {
  executeSqliteQuerySync,
  executeSqliteQueryTakeFirstSync,
  getNodeSqliteKysely,
} from "../infra/kysely-sync.js";
import { runSqliteImmediateTransactionSync } from "../infra/sqlite-transaction.js";
import type { DB as OpenClawStateKyselyDatabase } from "../state/openclaw-state-db.generated.js";
import { acquireOpenClawStateDatabaseLease } from "../state/openclaw-state-db.js";
import { resolveOpenClawStateSqlitePath } from "../state/openclaw-state-db.paths.js";
import type { DB as DurableSchemaKyselyDatabase } from "./schema-db.generated.js";
import { ensureDurableRuntimeSchema, openDurableRuntimeSchemaReadOnly } from "./schema.js";
import type {
  AppendDurableRuntimeEventInput,
  ClaimNextWakeObligationInput,
  ClaimDurableRuntimeStepInput,
  CompactDurableRuntimeRunInput,
  CompactDurableRuntimeRunResult,
  CompleteWakeObligationClaimInput,
  CreateDurableRuntimeLinkInput,
  CreateWakeObligationInput,
  CreateDurableRuntimeRefInput,
  CreateDurableRuntimeRunInput,
  CreateDurableRuntimeSignalInput,
  CreateDurableRuntimeStepInput,
  CreateDurableRuntimeTimerInput,
  CreateUncertaintyFactInput,
  ReconcileWakeObligationInput,
  ReconcileWakeObligationResult,
  WakeObligation,
  WakeObligationClaim,
  WakeObligationOwnerKind,
  WakeObligationStatus,
  WakeObligationTargetKind,
  WakeObligationTargetResolutionStatus,
  DurableRuntimeLink,
  DurableRuntimeLinkStatus,
  DurableRuntimeLinkType,
  DurableRecoveryState,
  DurableRuntimeEvent,
  DurableRuntimeRef,
  DurableRuntimeRefKind,
  DurableRuntimeRun,
  DurableRuntimeRunStatus,
  DurableRuntimeSignal,
  DurableRuntimeStep,
  DurableRuntimeStepClaim,
  DurableRuntimeStepStatus,
  DurableRuntimeStepType,
  DurableRuntimeStore,
  DurableRuntimeStoreStats,
  DurableRuntimeTimelineOptions,
  DurableRuntimeTimer,
  DurableRuntimeTimerStatus,
  UncertaintyFact,
  UncertaintyFactStatus,
  DurableUnresolvedObligation,
  WakeObligationControlDecision,
  WakeObligationControlDecisionKind,
  DeliveryAttemptEvidence,
  DeliveryAttemptEvidenceStatus,
  WakeObligationInspection,
  UpdateDurableRuntimeRunInput,
  UpdateDurableRuntimeLinkInput,
  ResumeWakeObligationInput,
  ResolveUncertaintyFactInput,
  RenewWakeObligationClaimInput,
  MarkWakeObligationDecisionRequiredInput,
  SupersedeWakeObligationInput,
  SuspendWakeObligationInput,
  WakeObligationControlInput,
  UpdateWakeObligationInput,
  UpdateWakeObligationProjectionInput,
  UpdateDurableRuntimeStepInput,
  UpdateDurableRuntimeTimerInput,
} from "./types.js";

type DurableRow<Table extends keyof DurableSchemaKyselyDatabase> = Selectable<
  DurableSchemaKyselyDatabase[Table]
>;

type DurableRuntimeRunRow = Omit<
  DurableRow<"durable_execution_records">,
  "status" | "recovery_state"
> & {
  status: DurableRuntimeRunStatus;
  recovery_state: DurableRecoveryState;
};

type DurableRuntimeEventRow = DurableRow<"durable_event_evidence">;

type DurableRuntimeStepRow = Omit<
  DurableRow<"durable_execution_steps">,
  "step_type" | "status" | "recovery_state"
> & {
  step_type: DurableRuntimeStepType;
  status: DurableRuntimeStepStatus;
  recovery_state: DurableRecoveryState;
};

type DurableRuntimeRefRow = Omit<
  DurableRow<"durable_payload_refs">,
  "ref_kind" | "storage_kind"
> & {
  ref_kind: DurableRuntimeRefKind;
  storage_kind: "inline" | "file" | "external";
};

type DurableRuntimeLinkRow = Omit<
  DurableRow<"durable_run_correlations">,
  "link_type" | "status"
> & {
  link_type: DurableRuntimeLinkType;
  status: DurableRuntimeLinkStatus;
};

type DurableRuntimeTimerRow = Omit<
  DurableRow<"durable_timer_obligations">,
  "timer_type" | "status"
> & {
  timer_type: DurableRuntimeTimer["timerType"];
  status: DurableRuntimeTimerStatus;
};

type DurableRuntimeSignalRow = Omit<DurableRow<"durable_signal_evidence">, "signal_type"> & {
  signal_type: DurableRuntimeSignal["signalType"];
};

type WakeObligationRow = Omit<
  DurableRow<"wake_obligations">,
  | "coalescing_mode"
  | "recurrence_policy"
  | "target_kind"
  | "owner_kind"
  | "target_resolution_status"
  | "reason"
  | "status"
> & {
  coalescing_mode: "none" | "while_unresolved";
  recurrence_policy: "never" | "after_terminal" | null;
  target_kind: WakeObligationTargetKind | null;
  owner_kind: WakeObligationOwnerKind | null;
  target_resolution_status: WakeObligationTargetResolutionStatus | null;
  reason: WakeObligation["reason"];
  status: WakeObligationStatus;
};

type WakeObligationOccurrenceRow = DurableRow<"wake_obligation_occurrences">;

type UncertaintyFactRow = Omit<DurableRow<"uncertainty_facts">, "kind" | "status"> & {
  kind: UncertaintyFact["kind"];
  status: UncertaintyFactStatus;
};

type DeliveryAttemptEvidenceRow = Omit<
  DurableRow<"delivery_attempt_evidence">,
  "target_kind" | "route_kind" | "status"
> & {
  target_kind: WakeObligationTargetKind | null;
  route_kind: WakeObligationTargetKind | null;
  status: DeliveryAttemptEvidenceStatus;
};

type DurableUnresolvedObligationRow = {
  obligation_id: string;
  source_owner: string;
  source_ref: string;
  kind: DurableUnresolvedObligation["kind"];
  runtime_run_id: string | null;
  step_id: string | null;
  wake_id: string | null;
  uncertainty_fact_id: string | null;
  subject_ref: string | null;
  reason: string | null;
  status: string;
  created_at: number | bigint;
  updated_at: number | bigint;
  metadata_json: string | null;
};

type ExpiredStateLeaseRow = Selectable<OpenClawStateKyselyDatabase["state_leases"]>;

type CountRow = { count: number | bigint };
type DurableRuntimeDatabase = DurableSchemaKyselyDatabase &
  Pick<OpenClawStateKyselyDatabase, "state_leases">;
type SyncQuery<Row> = Parameters<typeof executeSqliteQuerySync<Row>>[1];

function optionalText(value: string | undefined): string | null {
  const normalized = value?.trim();
  return normalized ? normalized : null;
}

function metadataText(value: unknown): string | undefined {
  return typeof value === "string" ? (optionalText(value) ?? undefined) : undefined;
}

const DURABLE_STEP_LEASE_SCOPE = "durable_execution_step";
const WAKE_OBLIGATION_LEASE_SCOPE = "wake_obligation";
const WAKE_RECONCILIATION_PAGE_SIZE = 64;
const WAKE_RECONCILIATION_MAX_PAGES = 8;
const MAX_WAKE_INSPECTION_RELATED_ITEMS = 100;
const MAX_WAKE_INSPECTION_OCCURRENCE_KEYS = 100;
const MAX_DURABLE_JSON_BYTES = 64 * 1024;
const MAX_WAKE_CONTROL_HISTORY = 32;

function requirePositiveSafeInteger(value: number, subject: string): void {
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new Error(`${subject} must be a positive safe integer`);
  }
}

function wakeRetryDelayMs(params: {
  wakeId: string;
  attemptCount: number;
  retryBaseMs: number;
  retryMaxMs: number;
}): number {
  const exponent = Math.max(0, Math.min(20, params.attemptCount - 1));
  const base = Math.min(params.retryMaxMs, params.retryBaseMs * 2 ** exponent);
  let hash = 0;
  for (const char of params.wakeId) {
    hash += char.codePointAt(0) ?? 0;
  }
  const jitter = 0.75 + (hash % 51) / 100;
  return Math.min(params.retryMaxMs, Math.max(params.retryBaseMs, Math.round(base * jitter)));
}

function durableStepLeaseKey(runtimeRunId: string, stepId: string): string {
  return JSON.stringify([runtimeRunId, stepId]);
}

function requireSourceRef(
  input: { sourceOwner: string; sourceRef: string },
  subject: string,
): { sourceOwner: string; sourceRef: string } {
  const sourceOwner = optionalText(input.sourceOwner);
  const sourceRef = optionalText(input.sourceRef);
  if (!sourceOwner || !sourceRef) {
    throw new Error(`${subject} requires sourceOwner and sourceRef`);
  }
  return { sourceOwner, sourceRef };
}

function serializeJson(value: Record<string, unknown> | undefined): string | null {
  if (value === undefined) {
    return null;
  }
  const serialized = JSON.stringify(value);
  if (Buffer.byteLength(serialized, "utf8") > MAX_DURABLE_JSON_BYTES) {
    throw new Error(`Durable JSON payload exceeds ${MAX_DURABLE_JSON_BYTES} bytes`);
  }
  return serialized;
}

function stableJsonStringify(value: unknown): string {
  const serialized = JSON.stringify(value);
  if (serialized === undefined) {
    throw new Error("Durable projection cannot be represented as JSON");
  }
  const sortValue = (entry: unknown): unknown => {
    if (Array.isArray(entry)) {
      return entry.map(sortValue);
    }
    if (isRecordValue(entry)) {
      return Object.fromEntries(
        Object.entries(entry)
          .toSorted(([left], [right]) => left.localeCompare(right))
          .map(([key, nested]) => [key, sortValue(nested)]),
      );
    }
    return entry;
  };
  return JSON.stringify(sortValue(JSON.parse(serialized) as unknown));
}

function sanitizeWakeProjectionMetadata(
  metadata: Record<string, unknown> | undefined,
): Record<string, unknown> {
  const {
    durableWakeControl: _durableWakeControl,
    durableWakeControls: _durableWakeControls,
    wakeReconciliation: _wakeReconciliation,
    ...projectionMetadata
  } = metadata ?? {};
  return projectionMetadata;
}

function wakeProjectionMetadata(input: CreateWakeObligationInput): Record<string, unknown> {
  const sourceRevision =
    optionalText(input.sourceRevision) ?? metadataText(input.metadata?.sourceRevision);
  return {
    ...sanitizeWakeProjectionMetadata(input.metadata),
    ...(sourceRevision ? { sourceRevision } : {}),
  };
}

function wakeProjectionHash(
  input: CreateWakeObligationInput,
  policy: ReconcileWakeObligationInput["policy"] = { mode: "none" },
): string {
  const projection = {
    sourceOwner: optionalText(input.sourceOwner),
    sourceRef: optionalText(input.sourceRef),
    parentRunId: optionalText(input.parentRunId),
    parentSessionKey: optionalText(input.parentSessionKey),
    targetKind: optionalText(input.targetKind),
    targetRef: optionalText(input.targetRef),
    ownerKind: optionalText(input.ownerKind),
    ownerRef: optionalText(input.ownerRef),
    reportRouteRef: optionalText(input.reportRouteRef),
    targetResolutionStatus: optionalText(input.targetResolutionStatus),
    targetResolutionReason: optionalText(input.targetResolutionReason),
    reason: input.reason,
    factsRef: optionalText(input.factsRef),
    sourceRunId: optionalText(input.sourceRunId),
    sourceRevision:
      optionalText(input.sourceRevision) ?? metadataText(input.metadata?.sourceRevision) ?? null,
    metadata: wakeProjectionMetadata(input),
    policy,
  };
  return createHash("sha256").update(stableJsonStringify(projection)).digest("hex");
}

function assertCompatibleEventReplay(
  existing: DurableRuntimeEventRow,
  input: AppendDurableRuntimeEventInput,
  identity: string,
): void {
  const replayPayloadJson = input.payload === undefined ? undefined : serializeJson(input.payload);
  const payloadMismatched =
    replayPayloadJson !== undefined &&
    (existing.payload_json !== null
      ? existing.payload_json !== replayPayloadJson
      : existing.payload_hash !==
        createHash("sha256")
          .update(replayPayloadJson ?? "null")
          .digest("hex"));
  const mismatched =
    existing.runtime_run_id !== input.runtimeRunId ||
    existing.event_type !== input.eventType ||
    (input.eventId !== undefined && existing.event_id !== input.eventId) ||
    (input.stepId !== undefined && existing.step_id !== optionalText(input.stepId)) ||
    (input.agentInvocationId !== undefined &&
      existing.agent_invocation_id !== optionalText(input.agentInvocationId)) ||
    (input.toolInvocationId !== undefined &&
      existing.tool_invocation_id !== optionalText(input.toolInvocationId)) ||
    (input.idempotencyKey !== undefined &&
      existing.idempotency_key !== optionalText(input.idempotencyKey)) ||
    payloadMismatched ||
    (input.payloadHash !== undefined &&
      existing.payload_hash !== optionalText(input.payloadHash)) ||
    (input.checkpointRef !== undefined &&
      existing.checkpoint_ref !== optionalText(input.checkpointRef)) ||
    (input.causationEventId !== undefined &&
      existing.causation_event_id !== optionalText(input.causationEventId)) ||
    (input.correlationId !== undefined &&
      existing.correlation_id !== optionalText(input.correlationId));
  if (mismatched) {
    throw new Error(`Durable event replay conflict for ${identity}`);
  }
}

function sameJsonRecord(
  existingJson: string | null,
  candidate: Record<string, unknown> | undefined,
): boolean {
  if (candidate === undefined) {
    return existingJson === null;
  }
  return stableJsonStringify(parseMetadata(existingJson)) === stableJsonStringify(candidate);
}

function assertCompatibleRunReplay(
  existing: DurableRuntimeRunRow,
  input: CreateDurableRuntimeRunInput,
  params: {
    operationVersion: string;
    sourceOwner: string | null;
    sourceRef: string | null;
    rootOperationReason: string | null;
  },
  identity: string,
): void {
  const existingRootOperationReason = metadataText(
    parseMetadata(existing.metadata_json).rootOperationReason,
  );
  const mismatched =
    (input.runtimeRunId !== undefined && existing.runtime_run_id !== input.runtimeRunId) ||
    existing.operation_kind !== input.operationKind ||
    existing.operation_version !== params.operationVersion ||
    existing.idempotency_key !== optionalText(input.idempotencyKey) ||
    existing.request_hash !== optionalText(input.requestHash) ||
    existing.source_owner !== params.sourceOwner ||
    existing.source_ref !== params.sourceRef ||
    existing.input_ref !== optionalText(input.inputRef) ||
    existing.parent_runtime_run_id !== optionalText(input.parentRuntimeRunId) ||
    existing.parent_step_id !== optionalText(input.parentStepId) ||
    existing.message_id !== optionalText(input.messageId) ||
    existing.turn_id !== optionalText(input.turnId) ||
    existingRootOperationReason !== (params.rootOperationReason ?? undefined);
  if (mismatched) {
    throw new Error(`Durable runtime run replay conflict for ${identity}`);
  }
}

function assertCompatibleStepReplay(
  existing: DurableRuntimeStepRow,
  input: CreateDurableRuntimeStepInput,
  identity: string,
): void {
  const mismatched =
    existing.runtime_run_id !== input.runtimeRunId ||
    (input.stepId !== undefined && existing.step_id !== input.stepId) ||
    existing.parent_step_id !== optionalText(input.parentStepId) ||
    existing.step_type !== input.stepType ||
    existing.idempotency_key !== optionalText(input.idempotencyKey);
  if (mismatched) {
    throw new Error(`Durable runtime step replay conflict for ${identity}`);
  }
}

function assertCompatibleRefReplay(
  existing: DurableRuntimeRefRow,
  input: CreateDurableRuntimeRefInput,
  identity: string,
): void {
  const mismatched =
    (input.refId !== undefined && existing.ref_id !== input.refId) ||
    existing.runtime_run_id !== input.runtimeRunId ||
    existing.step_id !== optionalText(input.stepId) ||
    existing.ref_kind !== input.refKind ||
    existing.media_type !== optionalText(input.mediaType) ||
    existing.hash !== optionalText(input.hash) ||
    existing.storage_kind !== (input.storageKind ?? "external") ||
    existing.storage_uri !== optionalText(input.storageUri) ||
    !sameJsonRecord(existing.metadata_json, input.metadata);
  if (mismatched) {
    throw new Error(`Durable runtime ref replay conflict for ${identity}`);
  }
}

function assertCompatibleLinkReplay(
  existing: DurableRuntimeLinkRow,
  input: CreateDurableRuntimeLinkInput,
  identity: string,
): void {
  if (existing.link_type !== input.linkType) {
    throw new Error(`Durable runtime link replay conflict for ${identity}`);
  }
}

function assertCompatibleTimerReplay(
  existing: DurableRuntimeTimerRow,
  input: CreateDurableRuntimeTimerInput,
  identity: string,
): void {
  const mismatched =
    (input.timerId !== undefined && existing.timer_id !== input.timerId) ||
    existing.runtime_run_id !== input.runtimeRunId ||
    existing.step_id !== optionalText(input.stepId) ||
    existing.timer_type !== input.timerType ||
    existing.due_at !== input.dueAt ||
    !sameJsonRecord(existing.metadata_json, input.metadata);
  if (mismatched) {
    throw new Error(`Durable runtime timer replay conflict for ${identity}`);
  }
}

function assertCompatibleSignalReplay(
  existing: DurableRuntimeSignalRow,
  input: CreateDurableRuntimeSignalInput,
  identity: string,
): void {
  const mismatched =
    (input.signalId !== undefined && existing.signal_id !== input.signalId) ||
    existing.runtime_run_id !== input.runtimeRunId ||
    existing.step_id !== optionalText(input.stepId) ||
    existing.signal_type !== input.signalType ||
    existing.idempotency_key !== optionalText(input.idempotencyKey) ||
    existing.payload_ref !== optionalText(input.payloadRef) ||
    existing.correlation_id !== optionalText(input.correlationId) ||
    !sameJsonRecord(existing.metadata_json, input.metadata);
  if (mismatched) {
    throw new Error(`Durable runtime signal replay conflict for ${identity}`);
  }
}

function assertCompatibleUncertaintyReplay(
  existing: UncertaintyFactRow,
  input: CreateUncertaintyFactInput,
  identity: string,
): void {
  const mismatched =
    (input.factId !== undefined && existing.fact_id !== input.factId) ||
    existing.source_owner !== optionalText(input.sourceOwner) ||
    existing.source_ref !== optionalText(input.sourceRef) ||
    existing.kind !== input.kind ||
    (input.sourceRunId !== undefined &&
      existing.source_run_id !== optionalText(input.sourceRunId)) ||
    (input.stepId !== undefined && existing.step_id !== optionalText(input.stepId)) ||
    (input.eventId !== undefined && existing.event_id !== optionalText(input.eventId)) ||
    (input.refId !== undefined && existing.ref_id !== optionalText(input.refId)) ||
    (input.factsRef !== undefined && existing.facts_ref !== optionalText(input.factsRef)) ||
    (input.facts !== undefined && existing.facts_json !== serializeJson(input.facts)) ||
    (input.metadata !== undefined && existing.metadata_json !== serializeJson(input.metadata));
  if (mismatched) {
    throw new Error(`Durable uncertainty replay conflict for ${identity}`);
  }
}

function parseJsonRecord(value: string | null): Record<string, unknown> | undefined {
  if (!value) {
    return undefined;
  }
  try {
    const parsed = JSON.parse(value) as unknown;
    return parsed && typeof parsed === "object" && !Array.isArray(parsed)
      ? (parsed as Record<string, unknown>)
      : undefined;
  } catch {
    return undefined;
  }
}

function parseStoredJsonRecord(
  value: string | null,
  subject: string,
): Record<string, unknown> | undefined {
  if (value === null) {
    return undefined;
  }
  if (Buffer.byteLength(value, "utf8") > MAX_DURABLE_JSON_BYTES) {
    throw new Error(`${subject} exceeds ${MAX_DURABLE_JSON_BYTES} bytes`);
  }
  const parsed = parseJsonRecord(value);
  if (!parsed) {
    throw new Error(`${subject} is malformed or is not a JSON object`);
  }
  return parsed;
}

function isRecordValue(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function parseMetadata(value: string | null): Record<string, unknown> {
  return parseStoredJsonRecord(value, "Durable metadata") ?? {};
}

function mergeMetadataJson(
  currentMetadataJson: string | null,
  patch: Record<string, unknown> | undefined,
): string | null {
  return patch === undefined
    ? currentMetadataJson
    : serializeJson({ ...parseMetadata(currentMetadataJson), ...patch });
}

function buildWakeControlDecision(
  input: WakeObligationControlInput,
  kind: WakeObligationControlDecisionKind,
  now: number,
): WakeObligationControlDecision {
  const actorRef = optionalText(input.actorRef);
  if (!actorRef) {
    throw new Error("Durable wake control requires actorRef");
  }
  return {
    kind,
    actorKind: input.actorKind,
    actorRef,
    ...(input.reason ? { reason: input.reason } : {}),
    ...(input.decisionRef ? { decisionRef: input.decisionRef } : {}),
    ...(input.idempotencyKey ? { idempotencyKey: input.idempotencyKey } : {}),
    ...(input.expectedSourceRevision
      ? { expectedSourceRevision: input.expectedSourceRevision }
      : {}),
    ...(input.evidence ? { evidence: input.evidence } : {}),
    ...(input.metadata ? { metadata: input.metadata } : {}),
    decidedAt: now,
  };
}

function mergeWakeControlMetadata(
  currentMetadataJson: string | null,
  decision: WakeObligationControlDecision,
  extras?: Record<string, unknown>,
): Record<string, unknown> {
  const metadata = parseMetadata(currentMetadataJson);
  const existingControls = Array.isArray(metadata.durableWakeControls)
    ? metadata.durableWakeControls
    : [];
  const boundedControls = [...existingControls, decision].slice(-MAX_WAKE_CONTROL_HISTORY);
  return {
    ...metadata,
    durableWakeControl: decision,
    durableWakeControls: boundedControls,
    ...extras,
  };
}

function latestWakeControl(metadataJson: string | null): Record<string, unknown> | undefined {
  const metadata = parseMetadata(metadataJson);
  const control = metadata?.durableWakeControl;
  return isRecordValue(control) ? control : undefined;
}

function hasWakeControlEvidence(metadataJson: string | null): boolean {
  const metadata = parseMetadata(metadataJson);
  return (
    isRecordValue(metadata.durableWakeControl) ||
    (Array.isArray(metadata.durableWakeControls) && metadata.durableWakeControls.length > 0)
  );
}

function matchesExpectedWakeSourceRevision(
  current: WakeObligationRow,
  expectedSourceRevision: string | undefined,
): boolean {
  const expected = optionalText(expectedSourceRevision);
  if (!expected) {
    return true;
  }
  const metadata = parseMetadata(current.metadata_json);
  return metadataText(metadata.sourceRevision) === expected;
}

function isMatchingControlNoop(
  current: WakeObligationRow,
  kind: WakeObligationControlDecisionKind,
  idempotencyKey: string | undefined,
): boolean {
  const control = latestWakeControl(current.metadata_json);
  if (!control || control.kind !== kind) {
    return false;
  }
  return idempotencyKey ? control.idempotencyKey === idempotencyKey : true;
}

function rowToRun(row: DurableRuntimeRunRow): DurableRuntimeRun {
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

function rowToEvent(row: DurableRuntimeEventRow): DurableRuntimeEvent {
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

function rowToStep(row: DurableRuntimeStepRow): DurableRuntimeStep {
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

function rowToRef(row: DurableRuntimeRefRow): DurableRuntimeRef {
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

function rowToLink(row: DurableRuntimeLinkRow): DurableRuntimeLink {
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

function rowToTimer(row: DurableRuntimeTimerRow): DurableRuntimeTimer {
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

function rowToSignal(row: DurableRuntimeSignalRow): DurableRuntimeSignal {
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

function rowToWakeObligation(row: WakeObligationRow): WakeObligation {
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

function rowToUncertaintyFact(row: UncertaintyFactRow): UncertaintyFact {
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

function rowToDeliveryAttemptEvidence(row: DeliveryAttemptEvidenceRow): DeliveryAttemptEvidence {
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
    ...(row.metadata_json ? { metadata: parseMetadata(row.metadata_json) } : {}),
  };
}

function rowToUnresolvedObligation(
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

function queryRows<Row>(db: DatabaseSync, query: SyncQuery<Row>): Row[] {
  return executeSqliteQuerySync(db, query).rows as Row[];
}

function queryFirst<Row>(db: DatabaseSync, query: SyncQuery<Row>): Row | undefined {
  return executeSqliteQueryTakeFirstSync(db, query) as Row | undefined;
}

function executeQuery(db: DatabaseSync, query: SyncQuery<unknown>): number {
  const result = executeSqliteQuerySync(db, query);
  return Number(result.numAffectedRows ?? 0);
}

function count(db: DatabaseSync, query: SyncQuery<CountRow>): number {
  const row = queryFirst<CountRow>(db, query);
  return Number(row?.count ?? 0);
}

function normalizeQueryLimit(limit: number | undefined, fallback: number): number {
  return Math.max(1, Math.min(5000, Math.trunc(limit ?? fallback)));
}

const NO_SILENCE_DIAGNOSTIC_PATHS = {
  overdue: "$.diagnostics.noSilenceSla.overdue",
  slaMs: "$.diagnostics.noSilenceSla.slaMs",
} as const;

function noSilenceDiagnosticNumber(
  jsonPath: (typeof NO_SILENCE_DIAGNOSTIC_PATHS)[keyof typeof NO_SILENCE_DIAGNOSTIC_PATHS],
) {
  return sql<number>`json_extract(metadata_json, ${jsonPath})`; // kysely-allow-raw: closed path union
}

function isTerminalRunStatus(status: DurableRuntimeRunStatus): boolean {
  return (
    status === "succeeded" || status === "failed" || status === "cancelled" || status === "lost"
  );
}

function isTerminalStepStatus(status: DurableRuntimeStepStatus): boolean {
  return (
    status === "succeeded" ||
    status === "failed" ||
    status === "cancelled" ||
    status === "lost" ||
    status === "skipped"
  );
}

function isTerminalRunRow(row: DurableRuntimeRunRow): boolean {
  return (
    isTerminalRunStatus(row.status) ||
    row.recovery_state === "terminal" ||
    row.completed_at !== null
  );
}

function isTerminalStepRow(row: DurableRuntimeStepRow): boolean {
  return (
    isTerminalStepStatus(row.status) ||
    row.recovery_state === "terminal" ||
    row.completed_at !== null
  );
}

function assertCoherentRunLifecycle(input: {
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

function assertCoherentStepLifecycle(input: {
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

function isTerminalWakeStatus(status: WakeObligationStatus): boolean {
  return status === "acked" || status === "superseded";
}

function unresolvedWakeStatusSql() {
  return sql<boolean>`status NOT IN ('acked', 'superseded')`; // kysely-allow-raw: closed status set
}

function isAllowedWakeStatusTransition(
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

function isSameSqlValue(
  left: string | number | bigint | null,
  right: string | number | bigint | null,
): boolean {
  return left === right;
}

export function openDurableRuntimeSqliteStore(storeOptions?: {
  path?: string;
  env?: NodeJS.ProcessEnv;
  readOnly?: boolean;
}): DurableRuntimeStore {
  const env = storeOptions?.env ?? process.env;
  const pathname = path.resolve(storeOptions?.path ?? resolveOpenClawStateSqlitePath(env));
  const readOnly = storeOptions?.readOnly === true;
  let db: DatabaseSync;
  let releaseDatabase: () => void;
  if (readOnly) {
    db = openDurableRuntimeSchemaReadOnly(pathname);
    releaseDatabase = () => db.close();
  } else {
    const stateDatabaseLease = acquireOpenClawStateDatabaseLease({ env, path: pathname });
    db = stateDatabaseLease.database.db;
    releaseDatabase = stateDatabaseLease.release;
  }
  const durableDb = (() => {
    try {
      if (!readOnly) {
        ensureDurableRuntimeSchema(db);
      }
      return getNodeSqliteKysely<DurableRuntimeDatabase>(db);
    } catch (err) {
      releaseDatabase();
      throw err;
    }
  })();
  let closed = false;

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

  const updateWakeObligationRecord = (
    input: Omit<UpdateWakeObligationInput, "status"> & {
      status?: WakeObligationStatus;
      finalizeActiveClaim?: {
        attemptStatus: Extract<DeliveryAttemptEvidenceStatus, "handoff_accepted" | "superseded">;
        error?: string;
      };
    },
  ): WakeObligation | undefined => {
    const now = input.now ?? Date.now();
    return runSqliteImmediateTransactionSync(db, () => {
      const finalizeActiveClaim = () => {
        if (!input.finalizeActiveClaim) {
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
      const nextMetadataJson =
        input.metadata === undefined
          ? current.metadata_json
          : serializeJson({ ...parseMetadata(current.metadata_json), ...input.metadata });
      const nextFactsRef =
        input.factsRef === undefined ? current.facts_ref : optionalText(input.factsRef);
      if (isTerminalWakeStatus(current.status)) {
        const isNoOp =
          nextStatus === current.status &&
          isSameSqlValue(nextAttemptCount, current.attempt_count) &&
          isSameSqlValue(nextLastAttemptAt, current.last_attempt_at) &&
          isSameSqlValue(nextNextAttemptAt, current.next_attempt_at) &&
          isSameSqlValue(nextAckedAt, current.acked_at) &&
          isSameSqlValue(nextFailedReason, current.failed_reason) &&
          isSameSqlValue(nextFactsRef, current.facts_ref) &&
          isSameSqlValue(nextMetadataJson, current.metadata_json);
        if (!isNoOp) {
          return undefined;
        }
        finalizeActiveClaim();
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
            facts_ref: nextFactsRef,
            updated_at: now,
            metadata_json: nextMetadataJson,
          })
          .where("wake_id", "=", input.wakeId),
      );
      finalizeActiveClaim();
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
    const current = getWakeObligationRecord(input.wakeId);
    if (!current || isTerminalWakeStatus(current.status)) {
      return undefined;
    }
    return updateWakeObligationRecord({
      wakeId: input.wakeId,
      status: "suspended",
      failedReason: input.failedReason,
      metadata: sanitizeWakeProjectionMetadata(input.metadata),
      now: input.now,
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

  const listOwnerWakeObligationsForReconciliationRecords = (input: {
    sourceOwner: string;
    afterWakeId?: string;
    limit: number;
  }): WakeObligation[] => {
    const sourceOwner = optionalText(input.sourceOwner);
    if (!sourceOwner) {
      throw new Error("Owner wake reconciliation requires a sourceOwner");
    }
    const afterWakeId = optionalText(input.afterWakeId);
    const rows = queryRows<WakeObligationRow>(
      db,
      durableDb
        .selectFrom("wake_obligations")
        .selectAll()
        .where("source_owner", "=", sourceOwner)
        .where("status", "in", ["pending", "handoff_accepted", "failed", "suspended"])
        .$if(Boolean(afterWakeId), (qb) => qb.where("wake_id", ">", afterWakeId!))
        .orderBy("wake_id", "asc")
        .limit(normalizeQueryLimit(input.limit, 500)),
    );
    return rows.map(rowToWakeObligation);
  };

  const listWakeObligationsNeedingNoSilenceDiagnosticRecords = (input: {
    overdueBefore: number;
    slaMs: number;
    limit?: number;
  }): WakeObligation[] => {
    const rows = queryRows<WakeObligationRow>(
      db,
      durableDb
        .selectFrom("wake_obligations")
        .selectAll()
        .where("status", "not in", ["acked", "superseded"])
        .where("created_at", "<=", input.overdueBefore)
        .where((eb) =>
          eb.or([
            eb(noSilenceDiagnosticNumber(NO_SILENCE_DIAGNOSTIC_PATHS.overdue), "is not", 1),
            eb(noSilenceDiagnosticNumber(NO_SILENCE_DIAGNOSTIC_PATHS.slaMs), "is not", input.slaMs),
          ]),
        )
        .orderBy("created_at", "asc")
        .orderBy("wake_id", "asc")
        .limit(normalizeQueryLimit(input.limit, 500)),
    );
    return rows.map(rowToWakeObligation);
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
      if (!matchesExpectedWakeSourceRevision(current, input.expectedSourceRevision)) {
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
      if (!matchesExpectedWakeSourceRevision(current, input.expectedSourceRevision)) {
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
      if (!matchesExpectedWakeSourceRevision(current, input.expectedSourceRevision)) {
        return undefined;
      }
      if (current.status !== "suspended") {
        return isMatchingControlNoop(current, "resumed", input.idempotencyKey)
          ? rowToWakeObligation(current)
          : undefined;
      }
      const decision = buildWakeControlDecision(input, "resumed", now);
      return updateWakeObligationRecord({
        wakeId: input.wakeId,
        status: "pending",
        failedReason: null,
        nextAttemptAt: null,
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
      if (!matchesExpectedWakeSourceRevision(current, input.expectedSourceRevision)) {
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
        metadata: mergeWakeControlMetadata(current.metadata_json, decision),
        now,
      });
    });
  };

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
        executeQuery(
          db,
          durableDb
            .updateTable("delivery_attempt_evidence")
            .set({
              status: "unknown",
              error_message: "wake dispatch claim expired before durable completion evidence",
              unknown_at: now,
              delivery_claimed_by: null,
              delivery_claim_expires_at: null,
              updated_at: now,
            })
            .where("delivery_attempt_id", "=", ambiguousAttempt.delivery_attempt_id),
        );
        executeQuery(
          db,
          durableDb
            .updateTable("wake_obligations")
            .set({
              status: "suspended",
              failed_reason: "dispatch_outcome_unknown",
              updated_at: now,
            })
            .where("wake_id", "=", candidate.wake_id),
        );
        executeQuery(
          db,
          durableDb
            .deleteFrom("state_leases")
            .where("scope", "=", WAKE_OBLIGATION_LEASE_SCOPE)
            .where("lease_key", "=", candidate.wake_id),
        );
        executeQuery(
          db,
          durableDb
            .insertInto("uncertainty_facts")
            .values({
              fact_id: `uncertainty_${randomUUID()}`,
              source_owner: candidate.source_owner,
              source_ref: candidate.source_ref,
              kind: "delivery_unknown",
              source_run_id: candidate.source_run_id,
              step_id: null,
              event_id: null,
              ref_id: ambiguousAttempt.delivery_attempt_id,
              facts_ref: candidate.facts_ref,
              dedupe_key: `wake-dispatch-unknown:${ambiguousAttempt.delivery_attempt_id}`,
              facts_json: serializeJson({
                wakeId: candidate.wake_id,
                deliveryAttemptId: ambiguousAttempt.delivery_attempt_id,
              }),
              status: "open",
              resolution_kind: null,
              resolution_ref: null,
              resolved_at: null,
              created_at: now,
              updated_at: now,
              metadata_json: null,
            })
            .onConflict((conflict) => conflict.doNothing()),
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
      const candidates = queryRows<WakeObligationRow>(
        db,
        durableDb
          .selectFrom("wake_obligations as w")
          .leftJoin("state_leases as l", (join) =>
            join
              .onRef("l.lease_key", "=", "w.wake_id")
              .on("l.scope", "=", WAKE_OBLIGATION_LEASE_SCOPE),
          )
          .selectAll("w")
          .where("w.status", "in", ["pending", "failed"])
          .where((eb) =>
            eb.or([eb("w.next_attempt_at", "is", null), eb("w.next_attempt_at", "<=", now)]),
          )
          .where("l.lease_key", "is", null)
          .orderBy("w.next_attempt_at", "asc")
          .orderBy("w.updated_at", "asc")
          .orderBy("w.wake_id", "asc")
          .limit(100),
      );
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
            metadata_json: null,
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
          .where("delivery_claimed_by", "=", input.claimToken),
      );
      const wake = queryFirst<WakeObligationRow>(
        db,
        durableDb.selectFrom("wake_obligations").selectAll().where("wake_id", "=", input.wakeId),
      );
      if (!current || !wake || isTerminalWakeStatus(wake.status)) {
        return undefined;
      }
      if (!isAllowedWakeStatusTransition(wake.status, input.wakeStatus)) {
        return undefined;
      }
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
            unknown_at: input.attemptStatus === "unknown" ? now : null,
            delivery_claimed_by: null,
            delivery_claim_expires_at: null,
            updated_at: now,
          })
          .where("delivery_attempt_id", "=", input.deliveryAttemptId),
      );
      executeQuery(
        db,
        durableDb
          .updateTable("wake_obligations")
          .set({
            status: input.wakeStatus,
            acked_at: input.wakeStatus === "acked" ? now : null,
            failed_reason:
              input.wakeStatus === "failed" || input.wakeStatus === "suspended"
                ? optionalText(input.error)
                : null,
            updated_at: now,
          })
          .where("wake_id", "=", input.wakeId),
      );
      executeQuery(
        db,
        durableDb
          .deleteFrom("state_leases")
          .where("scope", "=", WAKE_OBLIGATION_LEASE_SCOPE)
          .where("lease_key", "=", input.wakeId)
          .where("owner", "=", input.claimToken),
      );
      if (input.attemptStatus === "unknown") {
        executeQuery(
          db,
          durableDb
            .insertInto("uncertainty_facts")
            .values({
              fact_id: `uncertainty_${randomUUID()}`,
              source_owner: wake.source_owner,
              source_ref: wake.source_ref,
              kind: "delivery_unknown",
              source_run_id: wake.source_run_id,
              step_id: null,
              event_id: null,
              ref_id: input.deliveryAttemptId,
              facts_ref: wake.facts_ref,
              dedupe_key: `wake-dispatch-unknown:${input.deliveryAttemptId}`,
              facts_json: serializeJson(input.evidence),
              status: "open",
              resolution_kind: null,
              resolution_ref: null,
              resolved_at: null,
              created_at: now,
              updated_at: now,
              metadata_json: null,
            })
            .onConflict((conflict) => conflict.doNothing()),
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
      const attempt = queryFirst<{
        delivery_claim_expires_at: number | bigint | null;
      }>(
        db,
        durableDb
          .selectFrom("delivery_attempt_evidence")
          .select("delivery_claim_expires_at")
          .where("delivery_attempt_id", "=", input.deliveryAttemptId)
          .where("wake_id", "=", input.wakeId)
          .where("status", "=", "attempted")
          .where("delivery_claimed_by", "=", input.claimToken),
      );
      if (
        !lease ||
        lease.expires_at === null ||
        Number(lease.expires_at) <= now ||
        !attempt ||
        attempt.delivery_claim_expires_at === null ||
        Number(attempt.delivery_claim_expires_at) <= now
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

    listOpenRuns(options?: { operationKind?: string; limit?: number }): DurableRuntimeRun[] {
      const limit = Math.max(1, Math.min(5000, Math.trunc(options?.limit ?? 500)));
      const operationKind = optionalText(options?.operationKind);
      const query = durableDb
        .selectFrom("durable_execution_records")
        .selectAll()
        .where("status", "not in", ["succeeded", "failed", "cancelled", "lost"])
        .where("recovery_state", "!=", "terminal")
        .where("completed_at", "is", null)
        .$if(Boolean(operationKind), (qb) => qb.where("operation_kind", "=", operationKind!))
        .orderBy("updated_at", "asc")
        .orderBy("runtime_run_id", "asc")
        .limit(limit);
      return queryRows<DurableRuntimeRunRow>(db, query).map(rowToRun);
    },

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

    listOwnerWakeObligationsForReconciliation(input: {
      sourceOwner: string;
      afterWakeId?: string;
      limit: number;
    }): WakeObligation[] {
      return listOwnerWakeObligationsForReconciliationRecords(input);
    },

    listWakeObligationsNeedingNoSilenceDiagnostic(input: {
      overdueBefore: number;
      slaMs: number;
      limit?: number;
    }): WakeObligation[] {
      return listWakeObligationsNeedingNoSilenceDiagnosticRecords(input);
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

    listUnresolvedObligations(options?: {
      now?: number;
      limit?: number;
    }): DurableUnresolvedObligation[] {
      const now = options?.now ?? Date.now();
      const limit = normalizeQueryLimit(options?.limit, 500);
      const wakeRows = queryRows<WakeObligationRow>(
        db,
        durableDb
          .selectFrom("wake_obligations")
          .selectAll()
          .where("status", "in", ["pending", "handoff_accepted", "failed", "suspended"])
          .orderBy("updated_at", "desc")
          .orderBy("wake_id", "desc")
          .limit(limit),
      ).map(
        (row): DurableUnresolvedObligationRow => ({
          obligation_id: `wake:${row.wake_id}`,
          source_owner: row.source_owner,
          source_ref: row.source_ref,
          kind: "pending_wake",
          runtime_run_id: row.source_run_id,
          step_id: null,
          wake_id: row.wake_id,
          uncertainty_fact_id: null,
          subject_ref: row.facts_ref ?? row.wake_id,
          reason: row.reason,
          status: row.status,
          created_at: row.created_at,
          updated_at: row.updated_at,
          metadata_json: row.metadata_json,
        }),
      );
      const uncertaintyRows = queryRows<UncertaintyFactRow>(
        db,
        durableDb
          .selectFrom("uncertainty_facts")
          .selectAll()
          .where("status", "=", "open")
          .orderBy("updated_at", "desc")
          .orderBy("fact_id", "desc")
          .limit(limit),
      ).map(
        (row): DurableUnresolvedObligationRow => ({
          obligation_id: `uncertainty:${row.fact_id}`,
          source_owner: row.source_owner,
          source_ref: row.source_ref,
          kind: "unresolved_uncertainty",
          runtime_run_id: row.source_run_id,
          step_id: row.step_id,
          wake_id: null,
          uncertainty_fact_id: row.fact_id,
          subject_ref: row.facts_ref ?? row.ref_id ?? row.event_id ?? row.dedupe_key,
          reason: row.kind,
          status: row.status,
          created_at: row.created_at,
          updated_at: row.updated_at,
          metadata_json: row.metadata_json,
        }),
      );
      const childRows = queryRows<DurableRuntimeLinkRow>(
        db,
        durableDb
          .selectFrom("durable_run_correlations")
          .selectAll()
          .where("status", "in", ["pending", "running"])
          .orderBy("updated_at", "desc")
          .orderBy("child_runtime_run_id", "desc")
          .limit(limit),
      ).map(
        (row): DurableUnresolvedObligationRow => ({
          obligation_id: `child:${row.parent_runtime_run_id}:${row.parent_step_id}:${row.child_runtime_run_id}`,
          source_owner: "durable_run_correlations",
          source_ref: `${row.parent_runtime_run_id}:${row.parent_step_id}:${row.child_runtime_run_id}`,
          kind: "open_child",
          runtime_run_id: row.parent_runtime_run_id,
          step_id: row.parent_step_id,
          wake_id: null,
          uncertainty_fact_id: null,
          subject_ref: row.child_runtime_run_id,
          reason: row.link_type,
          status: row.status,
          created_at: row.created_at,
          updated_at: row.updated_at,
          metadata_json: row.metadata_json,
        }),
      );
      const expiredStateLeaseRows = queryRows<ExpiredStateLeaseRow>(
        db,
        durableDb
          .selectFrom("state_leases")
          .selectAll()
          .where("scope", "=", DURABLE_STEP_LEASE_SCOPE)
          .where("expires_at", "is not", null)
          .where("expires_at", "<=", now)
          .orderBy("updated_at", "desc")
          .orderBy("scope", "asc")
          .orderBy("lease_key", "asc")
          .limit(limit),
      ).map((row): DurableUnresolvedObligationRow => {
        const payload = parseJsonRecord(row.payload_json);
        const runtimeRunId =
          typeof payload?.runtimeRunId === "string" ? payload.runtimeRunId : null;
        const stepId = typeof payload?.stepId === "string" ? payload.stepId : null;
        return {
          obligation_id: `state-lease:${row.scope}:${row.lease_key}`,
          source_owner: "state_leases",
          source_ref: `${row.scope}:${row.lease_key}`,
          kind: "expired_state_lease",
          runtime_run_id: runtimeRunId,
          step_id: stepId,
          wake_id: null,
          uncertainty_fact_id: null,
          subject_ref: row.owner,
          reason: "lease_expired",
          status: "expired",
          created_at: row.created_at,
          updated_at: row.updated_at,
          metadata_json: serializeJson({
            expiresAt: row.expires_at,
            heartbeatAt: row.heartbeat_at,
          }),
        };
      });
      return [...wakeRows, ...uncertaintyRows, ...childRows, ...expiredStateLeaseRows]
        .toSorted((left, right) => {
          const updated = Number(right.updated_at) - Number(left.updated_at);
          return updated === 0 ? right.obligation_id.localeCompare(left.obligation_id) : updated;
        })
        .slice(0, limit)
        .map(rowToUnresolvedObligation);
    },

    getTimeline(
      runtimeRunId: string,
      timelineOptions?: DurableRuntimeTimelineOptions,
    ): DurableRuntimeEvent[] {
      const afterEventSeq = Math.max(0, Math.trunc(timelineOptions?.afterEventSeq ?? 0));
      const shouldLimit = timelineOptions?.limit !== undefined || afterEventSeq !== 0;
      const rows = queryRows<DurableRuntimeEventRow>(
        db,
        durableDb
          .selectFrom("durable_event_evidence")
          .selectAll()
          .where("runtime_run_id", "=", runtimeRunId)
          .$if(afterEventSeq !== 0, (qb) => qb.where("event_seq", ">", afterEventSeq))
          .orderBy("event_seq", "asc")
          .$if(shouldLimit, (qb) => qb.limit(normalizeQueryLimit(timelineOptions?.limit, 500))),
      );
      return rows.map(rowToEvent);
    },

    compactTerminalRun(input: CompactDurableRuntimeRunInput): CompactDurableRuntimeRunResult {
      const keepLastEvents = normalizeQueryLimit(input.keepLastEvents, 200);
      return runSqliteImmediateTransactionSync(db, () => {
        const run = queryFirst<DurableRuntimeRunRow>(
          db,
          durableDb
            .selectFrom("durable_execution_records")
            .selectAll()
            .where("runtime_run_id", "=", input.runtimeRunId),
        );
        if (!run || !isTerminalRunStatus(run.status)) {
          return {
            runtimeRunId: input.runtimeRunId,
            compacted: false,
            redactedEventPayloads: 0,
            hasMore: false,
          };
        }
        const totalEvents = count(
          db,
          durableDb
            .selectFrom("durable_event_evidence")
            .select((eb) => eb.fn.countAll<number>().as("count"))
            .where("runtime_run_id", "=", input.runtimeRunId),
        );
        if (totalEvents <= keepLastEvents) {
          return {
            runtimeRunId: input.runtimeRunId,
            compacted: false,
            redactedEventPayloads: 0,
            hasMore: false,
          };
        }
        const cutoff = queryFirst<{ event_seq: number | bigint }>(
          db,
          durableDb
            .selectFrom("durable_event_evidence")
            .select("event_seq")
            .where("runtime_run_id", "=", input.runtimeRunId)
            .orderBy("event_seq", "desc")
            .limit(1)
            .offset(keepLastEvents - 1),
        );
        const cutoffSeq = Number(cutoff?.event_seq ?? 0);
        if (cutoffSeq <= 1) {
          return {
            runtimeRunId: input.runtimeRunId,
            compacted: false,
            redactedEventPayloads: 0,
            hasMore: false,
          };
        }
        const redactionCandidates = queryRows<
          Pick<DurableRuntimeEventRow, "event_id" | "payload_json" | "payload_hash">
        >(
          db,
          durableDb
            .selectFrom("durable_event_evidence")
            .select(["event_id", "payload_json", "payload_hash"])
            .where("runtime_run_id", "=", input.runtimeRunId)
            .where("event_seq", "<", cutoffSeq)
            .where("payload_json", "is not", null)
            .orderBy("event_seq", "asc")
            .limit(5001),
        );
        const boundedCandidates = redactionCandidates.slice(0, 5000);
        for (const candidate of boundedCandidates) {
          const payloadJson = candidate.payload_json!;
          executeQuery(
            db,
            durableDb
              .updateTable("durable_event_evidence")
              .set({
                payload_json: null,
                payload_hash:
                  candidate.payload_hash ?? createHash("sha256").update(payloadJson).digest("hex"),
              })
              .where("event_id", "=", candidate.event_id)
              .where("payload_json", "=", payloadJson),
          );
        }
        if (boundedCandidates.length === 0) {
          return {
            runtimeRunId: input.runtimeRunId,
            compacted: false,
            redactedEventPayloads: 0,
            hasMore: false,
          };
        }
        return {
          runtimeRunId: input.runtimeRunId,
          compacted: true,
          redactedEventPayloads: boundedCandidates.length,
          hasMore: redactionCandidates.length > boundedCandidates.length,
        };
      });
    },

    getStats(): DurableRuntimeStoreStats {
      return {
        path: pathname,
        runs: count(
          db,
          durableDb
            .selectFrom("durable_execution_records")
            .select((eb) => eb.fn.countAll<number>().as("count")),
        ),
        events: count(
          db,
          durableDb
            .selectFrom("durable_event_evidence")
            .select((eb) => eb.fn.countAll<number>().as("count")),
        ),
        steps: count(
          db,
          durableDb
            .selectFrom("durable_execution_steps")
            .select((eb) => eb.fn.countAll<number>().as("count")),
        ),
        openRuns: count(
          db,
          durableDb
            .selectFrom("durable_execution_records")
            .select((eb) => eb.fn.countAll<number>().as("count"))
            .where("status", "not in", ["succeeded", "failed", "cancelled", "lost"]),
        ),
        pendingWakes: count(
          db,
          durableDb
            .selectFrom("wake_obligations")
            .select((eb) => eb.fn.countAll<number>().as("count"))
            .where("status", "in", ["pending", "handoff_accepted", "failed", "suspended"]),
        ),
        unresolvedUncertaintyFacts: count(
          db,
          durableDb
            .selectFrom("uncertainty_facts")
            .select((eb) => eb.fn.countAll<number>().as("count"))
            .where("status", "=", "open"),
        ),
      };
    },

    close(): void {
      if (closed) {
        return;
      }
      closed = true;
      releaseDatabase();
    },
  };
}
