import { createHash } from "node:crypto";
import type { Selectable } from "kysely";
import { executeSqliteQuerySync } from "../infra/kysely-sync.js";
import type { DB as OpenClawStateKyselyDatabase } from "../state/openclaw-state-db.generated.js";
import type { DB as DurableSchemaKyselyDatabase } from "./schema-db.generated.js";
import type {
  DurableRuntimeRunStatus,
  DurableRecoveryState,
  DurableRuntimeStepType,
  DurableRuntimeStepStatus,
  DurableRuntimeRefKind,
  DurableRuntimeLinkType,
  DurableRuntimeLinkStatus,
  DurableRuntimeTimerStatus,
  DurableRuntimeTimer,
  DurableRuntimeSignal,
  CreateDurableRuntimeRunInput,
  AppendDurableRuntimeEventInput,
  CreateDurableRuntimeStepInput,
  CreateDurableRuntimeRefInput,
  CreateDurableRuntimeLinkInput,
  CreateDurableRuntimeTimerInput,
  CreateDurableRuntimeSignalInput,
  WakeObligationStatus,
  WakeObligationTargetKind,
  WakeObligationOwnerKind,
  WakeObligationTargetResolutionStatus,
  UncertaintyFactStatus,
  DeliveryAttemptEvidenceStatus,
  WakeObligationControlDecisionKind,
  WakeObligationControlDecision,
  WakeObligationSuspensionClass,
  WakeObligation,
  UncertaintyFact,
  DurableUnresolvedObligation,
  CreateWakeObligationInput,
  ReconcileWakeObligationInput,
  WakeObligationControlInput,
  CreateUncertaintyFactInput,
} from "./types.js";

export type DurableRow<Table extends keyof DurableSchemaKyselyDatabase> = Selectable<
  DurableSchemaKyselyDatabase[Table]
>;

export type DurableRuntimeRunRow = Omit<
  DurableRow<"durable_execution_records">,
  "status" | "recovery_state"
> & {
  status: DurableRuntimeRunStatus;
  recovery_state: DurableRecoveryState;
};

export type DurableRuntimeEventRow = DurableRow<"durable_event_evidence">;

export type DurableRuntimeStepRow = Omit<
  DurableRow<"durable_execution_steps">,
  "step_type" | "status" | "recovery_state"
> & {
  step_type: DurableRuntimeStepType;
  status: DurableRuntimeStepStatus;
  recovery_state: DurableRecoveryState;
};

export type DurableRuntimeRefRow = Omit<
  DurableRow<"durable_payload_refs">,
  "ref_kind" | "storage_kind"
> & {
  ref_kind: DurableRuntimeRefKind;
  storage_kind: "inline" | "file" | "external";
};

export type DurableRuntimeLinkRow = Omit<
  DurableRow<"durable_run_correlations">,
  "link_type" | "status"
> & {
  link_type: DurableRuntimeLinkType;
  status: DurableRuntimeLinkStatus;
};

export type DurableRuntimeTimerRow = Omit<
  DurableRow<"durable_timer_obligations">,
  "timer_type" | "status"
> & {
  timer_type: DurableRuntimeTimer["timerType"];
  status: DurableRuntimeTimerStatus;
};

export type DurableRuntimeSignalRow = Omit<DurableRow<"durable_signal_evidence">, "signal_type"> & {
  signal_type: DurableRuntimeSignal["signalType"];
};

export type WakeObligationRow = Omit<
  DurableRow<"wake_obligations">,
  | "coalescing_mode"
  | "recurrence_policy"
  | "target_kind"
  | "owner_kind"
  | "target_resolution_status"
  | "reason"
  | "status"
  | "suspension_class"
> & {
  coalescing_mode: "none" | "while_unresolved";
  recurrence_policy: "never" | "after_terminal" | null;
  target_kind: WakeObligationTargetKind | null;
  owner_kind: WakeObligationOwnerKind | null;
  target_resolution_status: WakeObligationTargetResolutionStatus | null;
  reason: WakeObligation["reason"];
  status: WakeObligationStatus;
  suspension_class: WakeObligationSuspensionClass | null;
};

export type WakeObligationOccurrenceRow = DurableRow<"wake_obligation_occurrences">;

export type UncertaintyFactRow = Omit<DurableRow<"uncertainty_facts">, "kind" | "status"> & {
  kind: UncertaintyFact["kind"];
  status: UncertaintyFactStatus;
};

export type DeliveryAttemptEvidenceRow = Omit<
  DurableRow<"delivery_attempt_evidence">,
  "target_kind" | "route_kind" | "status"
> & {
  target_kind: WakeObligationTargetKind | null;
  route_kind: WakeObligationTargetKind | null;
  status: DeliveryAttemptEvidenceStatus;
};

export type DurableUnresolvedObligationRow = {
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

export type ExpiredStateLeaseRow = Selectable<OpenClawStateKyselyDatabase["state_leases"]>;

export type CountRow = { count: number | bigint };
export type DurableRuntimeDatabase = DurableSchemaKyselyDatabase &
  Pick<OpenClawStateKyselyDatabase, "state_leases">;
export type SyncQuery<Row> = Parameters<typeof executeSqliteQuerySync<Row>>[1];

export function optionalText(value: string | undefined): string | null {
  const normalized = value?.trim();
  return normalized ? normalized : null;
}

export function metadataText(value: unknown): string | undefined {
  return typeof value === "string" ? (optionalText(value) ?? undefined) : undefined;
}

export const DURABLE_STEP_LEASE_SCOPE = "durable_execution_step";
export const WAKE_OBLIGATION_LEASE_SCOPE = "wake_obligation";
export const WAKE_RECONCILIATION_PAGE_SIZE = 64;
export const WAKE_RECONCILIATION_MAX_PAGES = 8;
export const MAX_UNRESOLVED_WAKE_PAGE_SIZE = 500;
export const UNRESOLVED_WAKE_CURSOR_VERSION = 1;
export const MAX_OPEN_RUN_PAGE_SIZE = 500;
export const OPEN_RUN_CURSOR_VERSION = 1;
export const MAX_WAKE_INSPECTION_RELATED_ITEMS = 100;
export const MAX_WAKE_INSPECTION_OCCURRENCE_KEYS = 100;
export const MAX_DURABLE_JSON_BYTES = 64 * 1024;
export const MAX_WAKE_CONTROL_HISTORY = 32;

export function requirePositiveSafeInteger(value: number, subject: string): void {
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new Error(`${subject} must be a positive safe integer`);
  }
}

export function wakeRetryDelayMs(params: {
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

export function durableStepLeaseKey(runtimeRunId: string, stepId: string): string {
  return JSON.stringify([runtimeRunId, stepId]);
}

export function requireSourceRef(
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

export function serializeJson(value: Record<string, unknown> | undefined): string | null {
  if (value === undefined) {
    return null;
  }
  const serialized = JSON.stringify(value);
  if (Buffer.byteLength(serialized, "utf8") > MAX_DURABLE_JSON_BYTES) {
    throw new Error(`Durable JSON payload exceeds ${MAX_DURABLE_JSON_BYTES} bytes`);
  }
  return serialized;
}

export function stableJsonStringify(value: unknown): string {
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
          .toSorted(([left], [right]) => (left < right ? -1 : left > right ? 1 : 0))
          .map(([key, nested]) => [key, sortValue(nested)]),
      );
    }
    return entry;
  };
  return JSON.stringify(sortValue(JSON.parse(serialized) as unknown));
}

export function sanitizeWakeProjectionMetadata(
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

export function wakeProjectionMetadata(input: CreateWakeObligationInput): Record<string, unknown> {
  const sourceRevision =
    optionalText(input.sourceRevision) ?? metadataText(input.metadata?.sourceRevision);
  return {
    ...sanitizeWakeProjectionMetadata(input.metadata),
    ...(sourceRevision ? { sourceRevision } : {}),
  };
}

export function wakeDeliveryMetadata(metadata: Record<string, unknown>): Record<string, unknown> {
  const {
    diagnostics: _diagnostics,
    evidence: _evidence,
    ...deliveryMetadata
  } = sanitizeWakeProjectionMetadata(metadata);
  return deliveryMetadata;
}

export function wakeProjectionHash(
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

export function assertCompatibleEventReplay(
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

export function sameJsonRecord(
  existingJson: string | null,
  candidate: Record<string, unknown> | undefined,
): boolean {
  if (candidate === undefined) {
    return existingJson === null;
  }
  return stableJsonStringify(parseMetadata(existingJson)) === stableJsonStringify(candidate);
}

export function assertCompatibleRunReplay(
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

export function assertCompatibleStepReplay(
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

export function assertCompatibleRefReplay(
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

export function assertCompatibleLinkReplay(
  existing: DurableRuntimeLinkRow,
  input: CreateDurableRuntimeLinkInput,
  identity: string,
): void {
  if (existing.link_type !== input.linkType) {
    throw new Error(`Durable runtime link replay conflict for ${identity}`);
  }
}

export function assertCompatibleTimerReplay(
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

export function assertCompatibleSignalReplay(
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

export function assertCompatibleUncertaintyReplay(
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

export function parseJsonRecord(value: string | null): Record<string, unknown> | undefined {
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

export function parseStoredJsonRecord(
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

export function isRecordValue(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

export function parseMetadata(value: string | null): Record<string, unknown> {
  return parseStoredJsonRecord(value, "Durable metadata") ?? {};
}

export const DELIVERY_ATTEMPT_INTERNAL_METADATA_KEY = "__durableInternal";
export const DELIVERY_ATTEMPT_INTERNAL_METADATA_VERSION = 1;
export const WAKE_SUSPENSION_METADATA_KEY = "durableSuspension";
export const LEGACY_CLAIMED_WAKE_SOURCE_REVISION_KEY = "claimedWakeSourceRevision";
export const LEGACY_CLAIMED_WAKE_OCCURRENCE_KEY = "claimedWakeOccurrenceKey";

export function wakeProjectionFingerprint(
  row: WakeObligationRow,
  metadataJson = row.metadata_json,
  factsRef = row.facts_ref,
): string {
  const metadata = parseMetadata(metadataJson);
  const reconciliation = isRecordValue(metadata.wakeReconciliation)
    ? metadata.wakeReconciliation
    : {};
  return createHash("sha256")
    .update(
      stableJsonStringify({
        sourceOwner: row.source_owner,
        sourceRef: row.source_ref,
        parentRunId: row.parent_run_id,
        parentSessionKey: row.parent_session_key,
        targetKind: row.target_kind,
        targetRef: row.target_ref,
        ownerKind: row.owner_kind,
        ownerRef: row.owner_ref,
        reportRouteRef: row.report_route_ref,
        targetResolutionStatus: row.target_resolution_status,
        targetResolutionReason: row.target_resolution_reason,
        reason: row.reason,
        factsRef,
        sourceRunId: row.source_run_id,
        sourceRevision: metadataText(metadata.sourceRevision) ?? null,
        metadata: wakeDeliveryMetadata(metadata),
        policy:
          row.coalescing_mode === "none"
            ? { mode: "none" }
            : { mode: "while_unresolved", recurrence: row.recurrence_policy },
        latestOccurrenceKey: metadataText(reconciliation.latestOccurrenceKey) ?? null,
      }),
    )
    .digest("hex");
}

export function attemptMatchesClaimedWakeRevision(
  attempt: DeliveryAttemptEvidenceRow,
  wake: WakeObligationRow,
): boolean {
  return attempt.claimed_wake_delivery_revision === wake.delivery_revision;
}

export function deliveryAttemptClaimMetadata(
  attempt: DeliveryAttemptEvidenceRow,
): Record<string, unknown> | undefined {
  const internal = parseMetadata(attempt.metadata_json)[DELIVERY_ATTEMPT_INTERNAL_METADATA_KEY];
  return isRecordValue(internal) && internal.version === DELIVERY_ATTEMPT_INTERNAL_METADATA_VERSION
    ? internal
    : undefined;
}

export function deliveryAttemptClaimedOptionalText(
  attempt: DeliveryAttemptEvidenceRow,
  key: "claimedFactsRef" | "claimedSourceRunId",
): string | null | undefined {
  const internal = deliveryAttemptClaimMetadata(attempt);
  if (!internal || !Object.hasOwn(internal, key)) {
    return undefined;
  }
  const value = internal[key];
  return value === null ? null : metadataText(value);
}

export function deliveryAttemptPublicMetadata(
  metadataJson: string | null,
): Record<string, unknown> | undefined {
  const metadata = parseMetadata(metadataJson);
  const {
    [DELIVERY_ATTEMPT_INTERNAL_METADATA_KEY]: _internal,
    [LEGACY_CLAIMED_WAKE_SOURCE_REVISION_KEY]: _legacySourceRevision,
    [LEGACY_CLAIMED_WAKE_OCCURRENCE_KEY]: _legacyOccurrenceKey,
    ...publicMetadata
  } = metadata;
  return Object.keys(publicMetadata).length > 0 ? publicMetadata : undefined;
}

export function mergeMetadataJson(
  currentMetadataJson: string | null,
  patch: Record<string, unknown> | undefined,
): string | null {
  return patch === undefined
    ? currentMetadataJson
    : serializeJson({ ...parseMetadata(currentMetadataJson), ...patch });
}

export function buildWakeControlDecision(
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
    ...(input.expectedDeliveryRevision
      ? { expectedDeliveryRevision: input.expectedDeliveryRevision }
      : {}),
    ...(input.evidence ? { evidence: input.evidence } : {}),
    ...(input.metadata ? { metadata: input.metadata } : {}),
    decidedAt: now,
  };
}

export function mergeWakeControlMetadata(
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

export function latestWakeControl(
  metadataJson: string | null,
): Record<string, unknown> | undefined {
  const metadata = parseMetadata(metadataJson);
  const control = metadata?.durableWakeControl;
  return isRecordValue(control) ? control : undefined;
}

export function hasWakeControlEvidence(metadataJson: string | null): boolean {
  const metadata = parseMetadata(metadataJson);
  return (
    isRecordValue(metadata.durableWakeControl) ||
    (Array.isArray(metadata.durableWakeControls) && metadata.durableWakeControls.length > 0)
  );
}

export function matchesExpectedWakeRevision(
  current: WakeObligationRow,
  input: WakeObligationControlInput,
): boolean {
  const expectedSourceRevision = optionalText(input.expectedSourceRevision);
  if (
    expectedSourceRevision &&
    metadataText(parseMetadata(current.metadata_json).sourceRevision) !== expectedSourceRevision
  ) {
    return false;
  }
  const expectedDeliveryRevision = input.expectedDeliveryRevision;
  if (expectedDeliveryRevision !== undefined) {
    requirePositiveSafeInteger(expectedDeliveryRevision, "Expected wake delivery revision");
    return current.delivery_revision === expectedDeliveryRevision;
  }
  return input.actorKind === "operator" || input.actorKind === "admin";
}

export function isMatchingControlNoop(
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
