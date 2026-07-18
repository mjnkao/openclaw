// Gateway Protocol schema module defines durable runtime control-plane shapes.
import { Type } from "typebox";
import { NonEmptyString } from "./primitives.js";

const TimestampMsSchema = Type.Integer({ minimum: 0 });
const JsonRecordSchema = Type.Record(Type.String(), Type.Unknown());

type TimestampMs = number;
type JsonRecord = Record<string, unknown>;
type DurableRuntimeRunStatus =
  | "accepted"
  | "received"
  | "queued"
  | "running"
  | "waiting"
  | "waiting_signal"
  | "waiting_timer"
  | "waiting_child"
  | "blocked"
  | "retrying"
  | "retry_scheduled"
  | "canceling"
  | "unknown_after_side_effect"
  | "succeeded"
  | "failed"
  | "cancelled"
  | "lost";
type DurableRecoveryState =
  | "runnable"
  | "claimed"
  | "running"
  | "waiting_signal"
  | "waiting_timer"
  | "waiting_child"
  | "retry_scheduled"
  | "reconciling"
  | "unknown_after_side_effect"
  | "lost"
  | "terminal";
type DurableCoordinationWaitingReason =
  | "signal"
  | "timer"
  | "child"
  | "retry"
  | "worker"
  | "unknown";
type WakeObligationStatus =
  | "pending"
  | "delivered"
  | "acked"
  | "failed"
  | "suspended"
  | "superseded";
type WakeObligationReason =
  | "child_terminal"
  | "child_overdue"
  | "fan_in_incomplete"
  | "restart_interrupted"
  | "delivery_unknown"
  | "side_effect_uncertain"
  | "no_handler"
  | "operator_requested";
type WakeObligationTargetKind =
  | "agent_session"
  | "run"
  | "channel_route"
  | "external_route"
  | "taskflow"
  | "scheduler"
  | "workboard"
  | "plugin"
  | "operator"
  | "inspect_only";
type WakeObligationOwnerKind =
  | "agent_session"
  | "run"
  | "taskflow"
  | "scheduler"
  | "workboard"
  | "plugin"
  | "operator"
  | "external_route";
type WakeObligationTargetResolutionStatus =
  | "unresolved"
  | "resolved"
  | "ambiguous"
  | "missing"
  | "unauthorized"
  | "inspect_only";
type DeliveryAttemptEvidenceStatus =
  | "pending"
  | "attempted"
  | "delivered"
  | "failed"
  | "unknown"
  | "superseded";
type DurableCoordinationExternalRefs = {
  workUnitId?: string;
  reportRouteId?: string;
  taskId?: string;
  taskFlowId?: string;
  sessionKey?: string;
  childSessionKey?: string;
  runId?: string;
  agentId?: string;
  requesterAgentId?: string;
};
type DurableCoordinationChildCounts = {
  total: number;
  pending: number;
  running: number;
  succeeded: number;
  failed: number;
  cancelled: number;
  lost: number;
  terminal: number;
  open: number;
};
type DurableCoordinationRefs = {
  inputRef?: string;
  checkpointRef?: string;
  outputRefs: string[];
  errorRefs: string[];
  artifactRefs: string[];
};
type DurableCoordinationControls = {
  canCancel: boolean;
  canRetry: boolean;
  canResume: boolean;
  canSignal: boolean;
  canOpenTimeline: boolean;
};
type DurableCoordinationRecoveryDiagnostic = {
  state: "lost" | "unknown_after_side_effect";
  severity: "warning" | "error";
  reportable: boolean;
  retryable: boolean;
  reason?: string;
  message: string;
  nextAction: string;
  safeRecoveryActions?: string[];
  input?: {
    inputRef?: string;
    inputAvailability?: string;
    canReplay?: boolean;
    reason?: string;
    messageLength?: number;
    messageHash?: string;
  };
  detectedAt?: TimestampMs;
  processInstanceId?: string;
};
type DurableCoordinationProjection = {
  runtimeRunId: string;
  operationKind: string;
  operationVersion: string;
  status: DurableRuntimeRunStatus;
  recoveryState: DurableRecoveryState;
  sourceOwner?: string;
  sourceRef?: string;
  parentRuntimeRunId?: string;
  parentStepId?: string;
  workUnitId?: string;
  reportRouteId?: string;
  currentStepId?: string;
  waitingReason?: DurableCoordinationWaitingReason;
  heartbeatAt?: TimestampMs;
  updatedAt: TimestampMs;
  completedAt?: TimestampMs;
  refs: DurableCoordinationRefs;
  external: DurableCoordinationExternalRefs;
  children: DurableCoordinationChildCounts;
  controls: DurableCoordinationControls;
  recovery?: DurableCoordinationRecoveryDiagnostic;
};
type WakeObligation = {
  wakeId: string;
  sourceOwner: string;
  sourceRef: string;
  parentRunId?: string;
  parentSessionKey?: string;
  targetAgent?: string;
  targetSession?: string;
  targetChannel?: string;
  targetKind?: WakeObligationTargetKind;
  targetRef?: string;
  ownerKind?: WakeObligationOwnerKind;
  ownerRef?: string;
  reportRouteRef?: string;
  targetResolutionStatus?: WakeObligationTargetResolutionStatus;
  targetResolutionReason?: string;
  reason: WakeObligationReason;
  factsRef?: string;
  sourceRunId?: string;
  dedupeKey: string;
  attemptCount: number;
  lastAttemptAt?: TimestampMs;
  ackedAt?: TimestampMs;
  failedReason?: string;
  status: WakeObligationStatus;
  metadata?: JsonRecord;
  createdAt: TimestampMs;
  updatedAt: TimestampMs;
};
type DeliveryAttemptEvidence = {
  deliveryAttemptId: string;
  sourceOwner: string;
  sourceRef: string;
  wakeId: string;
  dedupeKey: string;
  replayPassId?: string;
  targetKind?: WakeObligationTargetKind;
  targetRef?: string;
  routeKind?: WakeObligationTargetKind;
  routeRef?: string;
  status: DeliveryAttemptEvidenceStatus;
  evidence?: JsonRecord;
  error?: string;
  scheduledAt: TimestampMs;
  attemptedAt?: TimestampMs;
  deliveredAt?: TimestampMs;
  failedAt?: TimestampMs;
  unknownAt?: TimestampMs;
  deliveryClaimedBy?: string;
  deliveryClaimExpiresAt?: TimestampMs;
  createdAt: TimestampMs;
  updatedAt: TimestampMs;
  metadata?: JsonRecord;
};
type UncertaintyFact = {
  factId: string;
  sourceOwner: string;
  sourceRef: string;
  kind:
    | "unknown_after_side_effect"
    | "interrupted_during_tool"
    | "lost_after_dispatch"
    | "delivery_unknown"
    | "requires_owner_decision";
  sourceRunId?: string;
  stepId?: string;
  eventId?: string;
  refId?: string;
  factsRef?: string;
  dedupeKey?: string;
  facts?: JsonRecord;
  status: "open" | "resolved" | "superseded";
  resolutionKind?: string;
  resolutionRef?: string;
  resolvedAt?: TimestampMs;
  metadata?: JsonRecord;
  createdAt: TimestampMs;
  updatedAt: TimestampMs;
};
type DurableUnresolvedObligation = {
  obligationId: string;
  sourceOwner: string;
  sourceRef: string;
  kind:
    | "pending_wake"
    | "unresolved_uncertainty"
    | "open_child"
    | "pending_subagent_delivery"
    | "pending_delivery_queue"
    | "expired_state_lease";
  runtimeRunId?: string;
  stepId?: string;
  wakeId?: string;
  uncertaintyFactId?: string;
  subjectRef?: string;
  reason?: string;
  status: string;
  createdAt: TimestampMs;
  updatedAt: TimestampMs;
  metadata?: JsonRecord;
};
type WakeObligationInspection = {
  wake: WakeObligation;
  targetResolution: {
    status?: WakeObligationTargetResolutionStatus;
    reason?: string;
    targetKind?: WakeObligationTargetKind;
    targetRef?: string;
    ownerKind?: WakeObligationOwnerKind;
    ownerRef?: string;
    reportRouteRef?: string;
    factsRef?: string;
    sourceRunId?: string;
    diagnostics?: JsonRecord;
    evidence?: JsonRecord;
  };
  deliveryAttemptEvidence: DeliveryAttemptEvidence[];
  unresolvedUncertaintyFacts: UncertaintyFact[];
  sourceRefs: {
    sourceOwner: string;
    sourceRef: string;
    factsRef?: string;
    sourceRunId?: string;
    dedupeKey: string;
    parentRunId?: string;
    parentSessionKey?: string;
  };
};
type DurableHealthResult = {
  enabled: boolean;
  authority: boolean;
  process: {
    status: "healthy" | "degraded";
    lastSuccessAt?: TimestampMs;
    lastFailure?: {
      component: string;
      operation: string;
      message: string;
      failedAt: TimestampMs;
      failureCount: number;
    };
  };
  store?: {
    path: string;
    schemaVersion: number;
    runs: number;
    events: number;
    steps: number;
    openRuns: number;
    pendingWakes: number;
    unresolvedUncertaintyFacts: number;
  };
};

export const DurableRuntimeRunStatusSchema = Type.Union([
  Type.Literal("accepted"),
  Type.Literal("received"),
  Type.Literal("queued"),
  Type.Literal("running"),
  Type.Literal("waiting"),
  Type.Literal("waiting_signal"),
  Type.Literal("waiting_timer"),
  Type.Literal("waiting_child"),
  Type.Literal("blocked"),
  Type.Literal("retrying"),
  Type.Literal("retry_scheduled"),
  Type.Literal("canceling"),
  Type.Literal("unknown_after_side_effect"),
  Type.Literal("succeeded"),
  Type.Literal("failed"),
  Type.Literal("cancelled"),
  Type.Literal("lost"),
]);

export const DurableRecoveryStateSchema = Type.Union([
  Type.Literal("runnable"),
  Type.Literal("claimed"),
  Type.Literal("running"),
  Type.Literal("waiting_signal"),
  Type.Literal("waiting_timer"),
  Type.Literal("waiting_child"),
  Type.Literal("retry_scheduled"),
  Type.Literal("reconciling"),
  Type.Literal("unknown_after_side_effect"),
  Type.Literal("lost"),
  Type.Literal("terminal"),
]);

export const DurableCoordinationWaitingReasonSchema = Type.Union([
  Type.Literal("signal"),
  Type.Literal("timer"),
  Type.Literal("child"),
  Type.Literal("retry"),
  Type.Literal("worker"),
  Type.Literal("unknown"),
]);

export const DurableCoordinationGetParamsSchema = Type.Object(
  {
    runtimeRunId: NonEmptyString,
  },
  { additionalProperties: false },
);

export const DurableCoordinationExternalRefsSchema = Type.Unsafe<DurableCoordinationExternalRefs>(
  Type.Object(
    {
      workUnitId: Type.Optional(Type.String()),
      reportRouteId: Type.Optional(Type.String()),
      taskId: Type.Optional(Type.String()),
      taskFlowId: Type.Optional(Type.String()),
      sessionKey: Type.Optional(Type.String()),
      childSessionKey: Type.Optional(Type.String()),
      runId: Type.Optional(Type.String()),
      agentId: Type.Optional(Type.String()),
      requesterAgentId: Type.Optional(Type.String()),
    },
    { additionalProperties: false },
  ),
);

export const DurableCoordinationChildCountsSchema = Type.Unsafe<DurableCoordinationChildCounts>(
  Type.Object(
    {
      total: Type.Integer({ minimum: 0 }),
      pending: Type.Integer({ minimum: 0 }),
      running: Type.Integer({ minimum: 0 }),
      succeeded: Type.Integer({ minimum: 0 }),
      failed: Type.Integer({ minimum: 0 }),
      cancelled: Type.Integer({ minimum: 0 }),
      lost: Type.Integer({ minimum: 0 }),
      terminal: Type.Integer({ minimum: 0 }),
      open: Type.Integer({ minimum: 0 }),
    },
    { additionalProperties: false },
  ),
);

export const DurableCoordinationRefsSchema = Type.Unsafe<DurableCoordinationRefs>(
  Type.Object(
    {
      inputRef: Type.Optional(Type.String()),
      checkpointRef: Type.Optional(Type.String()),
      outputRefs: Type.Array(Type.String()),
      errorRefs: Type.Array(Type.String()),
      artifactRefs: Type.Array(Type.String()),
    },
    { additionalProperties: false },
  ),
);

export const DurableCoordinationControlsSchema = Type.Unsafe<DurableCoordinationControls>(
  Type.Object(
    {
      canCancel: Type.Boolean(),
      canRetry: Type.Boolean(),
      canResume: Type.Boolean(),
      canSignal: Type.Boolean(),
      canOpenTimeline: Type.Boolean(),
    },
    { additionalProperties: false },
  ),
);

export const DurableCoordinationRecoveryDiagnosticSchema =
  Type.Unsafe<DurableCoordinationRecoveryDiagnostic>(
    Type.Object(
      {
        state: Type.Union([Type.Literal("lost"), Type.Literal("unknown_after_side_effect")]),
        severity: Type.Union([Type.Literal("warning"), Type.Literal("error")]),
        reportable: Type.Boolean(),
        retryable: Type.Boolean(),
        reason: Type.Optional(Type.String()),
        message: Type.String(),
        nextAction: Type.String(),
        safeRecoveryActions: Type.Optional(Type.Array(Type.String())),
        input: Type.Optional(
          Type.Object(
            {
              inputRef: Type.Optional(Type.String()),
              inputAvailability: Type.Optional(Type.String()),
              canReplay: Type.Optional(Type.Boolean()),
              reason: Type.Optional(Type.String()),
              messageLength: Type.Optional(Type.Integer({ minimum: 0 })),
              messageHash: Type.Optional(Type.String()),
            },
            { additionalProperties: false },
          ),
        ),
        detectedAt: Type.Optional(TimestampMsSchema),
        processInstanceId: Type.Optional(Type.String()),
      },
      { additionalProperties: false },
    ),
  );

export const DurableCoordinationProjectionSchema = Type.Unsafe<DurableCoordinationProjection>(
  Type.Object(
    {
      runtimeRunId: NonEmptyString,
      operationKind: NonEmptyString,
      operationVersion: NonEmptyString,
      status: DurableRuntimeRunStatusSchema,
      recoveryState: DurableRecoveryStateSchema,
      sourceOwner: Type.Optional(Type.String()),
      sourceRef: Type.Optional(Type.String()),
      parentRuntimeRunId: Type.Optional(Type.String()),
      parentStepId: Type.Optional(Type.String()),
      workUnitId: Type.Optional(Type.String()),
      reportRouteId: Type.Optional(Type.String()),
      currentStepId: Type.Optional(Type.String()),
      waitingReason: Type.Optional(DurableCoordinationWaitingReasonSchema),
      heartbeatAt: Type.Optional(TimestampMsSchema),
      updatedAt: TimestampMsSchema,
      completedAt: Type.Optional(TimestampMsSchema),
      refs: DurableCoordinationRefsSchema,
      external: DurableCoordinationExternalRefsSchema,
      children: DurableCoordinationChildCountsSchema,
      controls: DurableCoordinationControlsSchema,
      recovery: Type.Optional(DurableCoordinationRecoveryDiagnosticSchema),
    },
    { additionalProperties: false },
  ),
);

export const DurableCoordinationGetResultSchema = Type.Object(
  {
    projection: DurableCoordinationProjectionSchema,
  },
  { additionalProperties: false },
);

export const WakeObligationStatusSchema = Type.Union([
  Type.Literal("pending"),
  Type.Literal("delivered"),
  Type.Literal("acked"),
  Type.Literal("failed"),
  Type.Literal("suspended"),
  Type.Literal("superseded"),
]);

export const WakeObligationReasonSchema = Type.Union([
  Type.Literal("child_terminal"),
  Type.Literal("child_overdue"),
  Type.Literal("fan_in_incomplete"),
  Type.Literal("restart_interrupted"),
  Type.Literal("delivery_unknown"),
  Type.Literal("side_effect_uncertain"),
  Type.Literal("no_handler"),
  Type.Literal("operator_requested"),
]);

export const WakeObligationTargetKindSchema = Type.Union([
  Type.Literal("agent_session"),
  Type.Literal("run"),
  Type.Literal("channel_route"),
  Type.Literal("external_route"),
  Type.Literal("taskflow"),
  Type.Literal("scheduler"),
  Type.Literal("workboard"),
  Type.Literal("plugin"),
  Type.Literal("operator"),
  Type.Literal("inspect_only"),
]);

export const WakeObligationOwnerKindSchema = Type.Union([
  Type.Literal("agent_session"),
  Type.Literal("run"),
  Type.Literal("taskflow"),
  Type.Literal("scheduler"),
  Type.Literal("workboard"),
  Type.Literal("plugin"),
  Type.Literal("operator"),
  Type.Literal("external_route"),
]);

export const WakeObligationTargetResolutionStatusSchema = Type.Union([
  Type.Literal("unresolved"),
  Type.Literal("resolved"),
  Type.Literal("ambiguous"),
  Type.Literal("missing"),
  Type.Literal("unauthorized"),
  Type.Literal("inspect_only"),
]);

export const DeliveryAttemptEvidenceStatusSchema = Type.Union([
  Type.Literal("pending"),
  Type.Literal("attempted"),
  Type.Literal("delivered"),
  Type.Literal("failed"),
  Type.Literal("unknown"),
  Type.Literal("superseded"),
]);

export const WakeObligationSchema = Type.Unsafe<WakeObligation>(
  Type.Object(
    {
      wakeId: NonEmptyString,
      sourceOwner: NonEmptyString,
      sourceRef: NonEmptyString,
      parentRunId: Type.Optional(Type.String()),
      parentSessionKey: Type.Optional(Type.String()),
      targetAgent: Type.Optional(Type.String()),
      targetSession: Type.Optional(Type.String()),
      targetChannel: Type.Optional(Type.String()),
      targetKind: Type.Optional(WakeObligationTargetKindSchema),
      targetRef: Type.Optional(Type.String()),
      ownerKind: Type.Optional(WakeObligationOwnerKindSchema),
      ownerRef: Type.Optional(Type.String()),
      reportRouteRef: Type.Optional(Type.String()),
      targetResolutionStatus: Type.Optional(WakeObligationTargetResolutionStatusSchema),
      targetResolutionReason: Type.Optional(Type.String()),
      reason: WakeObligationReasonSchema,
      factsRef: Type.Optional(Type.String()),
      sourceRunId: Type.Optional(Type.String()),
      dedupeKey: NonEmptyString,
      attemptCount: Type.Integer({ minimum: 0 }),
      lastAttemptAt: Type.Optional(TimestampMsSchema),
      ackedAt: Type.Optional(TimestampMsSchema),
      failedReason: Type.Optional(Type.String()),
      status: WakeObligationStatusSchema,
      metadata: Type.Optional(JsonRecordSchema),
      createdAt: TimestampMsSchema,
      updatedAt: TimestampMsSchema,
    },
    { additionalProperties: false },
  ),
);

export const DeliveryAttemptEvidenceSchema = Type.Unsafe<DeliveryAttemptEvidence>(
  Type.Object(
    {
      deliveryAttemptId: NonEmptyString,
      sourceOwner: NonEmptyString,
      sourceRef: NonEmptyString,
      wakeId: NonEmptyString,
      dedupeKey: NonEmptyString,
      replayPassId: Type.Optional(Type.String()),
      targetKind: Type.Optional(WakeObligationTargetKindSchema),
      targetRef: Type.Optional(Type.String()),
      routeKind: Type.Optional(WakeObligationTargetKindSchema),
      routeRef: Type.Optional(Type.String()),
      status: DeliveryAttemptEvidenceStatusSchema,
      evidence: Type.Optional(JsonRecordSchema),
      error: Type.Optional(Type.String()),
      scheduledAt: TimestampMsSchema,
      attemptedAt: Type.Optional(TimestampMsSchema),
      deliveredAt: Type.Optional(TimestampMsSchema),
      failedAt: Type.Optional(TimestampMsSchema),
      unknownAt: Type.Optional(TimestampMsSchema),
      deliveryClaimedBy: Type.Optional(Type.String()),
      deliveryClaimExpiresAt: Type.Optional(TimestampMsSchema),
      createdAt: TimestampMsSchema,
      updatedAt: TimestampMsSchema,
      metadata: Type.Optional(JsonRecordSchema),
    },
    { additionalProperties: false },
  ),
);

export const UncertaintyFactSchema = Type.Unsafe<UncertaintyFact>(
  Type.Object(
    {
      factId: NonEmptyString,
      sourceOwner: NonEmptyString,
      sourceRef: NonEmptyString,
      kind: Type.Union([
        Type.Literal("unknown_after_side_effect"),
        Type.Literal("interrupted_during_tool"),
        Type.Literal("lost_after_dispatch"),
        Type.Literal("delivery_unknown"),
        Type.Literal("requires_owner_decision"),
      ]),
      sourceRunId: Type.Optional(Type.String()),
      stepId: Type.Optional(Type.String()),
      eventId: Type.Optional(Type.String()),
      refId: Type.Optional(Type.String()),
      factsRef: Type.Optional(Type.String()),
      dedupeKey: Type.Optional(Type.String()),
      facts: Type.Optional(JsonRecordSchema),
      status: Type.Union([
        Type.Literal("open"),
        Type.Literal("resolved"),
        Type.Literal("superseded"),
      ]),
      resolutionKind: Type.Optional(Type.String()),
      resolutionRef: Type.Optional(Type.String()),
      resolvedAt: Type.Optional(TimestampMsSchema),
      metadata: Type.Optional(JsonRecordSchema),
      createdAt: TimestampMsSchema,
      updatedAt: TimestampMsSchema,
    },
    { additionalProperties: false },
  ),
);

export const DurableUnresolvedObligationSchema = Type.Unsafe<DurableUnresolvedObligation>(
  Type.Object(
    {
      obligationId: NonEmptyString,
      sourceOwner: NonEmptyString,
      sourceRef: NonEmptyString,
      kind: Type.Union([
        Type.Literal("pending_wake"),
        Type.Literal("unresolved_uncertainty"),
        Type.Literal("open_child"),
        Type.Literal("pending_subagent_delivery"),
        Type.Literal("pending_delivery_queue"),
        Type.Literal("expired_state_lease"),
      ]),
      runtimeRunId: Type.Optional(Type.String()),
      stepId: Type.Optional(Type.String()),
      wakeId: Type.Optional(Type.String()),
      uncertaintyFactId: Type.Optional(Type.String()),
      subjectRef: Type.Optional(Type.String()),
      reason: Type.Optional(Type.String()),
      status: Type.String(),
      createdAt: TimestampMsSchema,
      updatedAt: TimestampMsSchema,
      metadata: Type.Optional(JsonRecordSchema),
    },
    { additionalProperties: false },
  ),
);

export const WakeObligationInspectionSchema = Type.Unsafe<WakeObligationInspection>(
  Type.Object(
    {
      wake: WakeObligationSchema,
      targetResolution: Type.Object(
        {
          status: Type.Optional(WakeObligationTargetResolutionStatusSchema),
          reason: Type.Optional(Type.String()),
          targetKind: Type.Optional(WakeObligationTargetKindSchema),
          targetRef: Type.Optional(Type.String()),
          ownerKind: Type.Optional(WakeObligationOwnerKindSchema),
          ownerRef: Type.Optional(Type.String()),
          reportRouteRef: Type.Optional(Type.String()),
          factsRef: Type.Optional(Type.String()),
          sourceRunId: Type.Optional(Type.String()),
          diagnostics: Type.Optional(JsonRecordSchema),
          evidence: Type.Optional(JsonRecordSchema),
        },
        { additionalProperties: false },
      ),
      deliveryAttemptEvidence: Type.Array(DeliveryAttemptEvidenceSchema),
      unresolvedUncertaintyFacts: Type.Array(UncertaintyFactSchema),
      sourceRefs: Type.Object(
        {
          sourceOwner: NonEmptyString,
          sourceRef: NonEmptyString,
          factsRef: Type.Optional(Type.String()),
          sourceRunId: Type.Optional(Type.String()),
          dedupeKey: NonEmptyString,
          parentRunId: Type.Optional(Type.String()),
          parentSessionKey: Type.Optional(Type.String()),
        },
        { additionalProperties: false },
      ),
    },
    { additionalProperties: false },
  ),
);

export const DurableLimitParamsSchema = Type.Object(
  {
    limit: Type.Optional(Type.Integer({ minimum: 1, maximum: 500 })),
  },
  { additionalProperties: false },
);

export const WakeObligationIdParamsSchema = Type.Object(
  {
    wakeId: NonEmptyString,
  },
  { additionalProperties: false },
);

const WakeControlFields = {
  wakeId: NonEmptyString,
  reason: Type.Optional(Type.String()),
  decisionRef: Type.Optional(Type.String()),
  idempotencyKey: Type.Optional(Type.String()),
  expectedSourceRevision: Type.Optional(NonEmptyString),
  evidence: Type.Optional(JsonRecordSchema),
};

export const WakeObligationControlParamsSchema = Type.Object(WakeControlFields, {
  additionalProperties: false,
});

export const WakeObligationSupersedeParamsSchema = Type.Object(
  {
    ...WakeControlFields,
    supersededByRef: Type.Optional(Type.String()),
  },
  { additionalProperties: false },
);

export const UncertaintyFactResolveParamsSchema = Type.Object(
  {
    factId: NonEmptyString,
    status: Type.Union([Type.Literal("resolved"), Type.Literal("superseded")]),
    resolutionKind: NonEmptyString,
    resolutionRef: Type.Optional(Type.String()),
    expectedUpdatedAt: Type.Optional(Type.Integer({ minimum: 0 })),
    metadata: Type.Optional(JsonRecordSchema),
  },
  { additionalProperties: false },
);

export const DurableHealthGetParamsSchema = Type.Object({}, { additionalProperties: false });

export const DeliveryAttemptEvidenceListParamsSchema = Type.Object(
  {
    wakeId: NonEmptyString,
    limit: Type.Optional(Type.Integer({ minimum: 1, maximum: 500 })),
  },
  { additionalProperties: false },
);

export const WakeObligationListResultSchema = Type.Object(
  {
    wakes: Type.Array(WakeObligationSchema),
  },
  { additionalProperties: false },
);

export const DurableObligationsListResultSchema = Type.Object(
  {
    obligations: Type.Array(DurableUnresolvedObligationSchema),
  },
  { additionalProperties: false },
);

export const WakeObligationInspectResultSchema = Type.Object(
  {
    inspection: WakeObligationInspectionSchema,
  },
  { additionalProperties: false },
);

export const WakeObligationControlResultSchema = Type.Object(
  {
    wake: WakeObligationSchema,
  },
  { additionalProperties: false },
);

export const UncertaintyFactResolveResultSchema = Type.Object(
  {
    uncertaintyFact: UncertaintyFactSchema,
  },
  { additionalProperties: false },
);

export const DurableHealthResultSchema = Type.Unsafe<DurableHealthResult>(
  Type.Object(
    {
      enabled: Type.Boolean(),
      authority: Type.Boolean(),
      process: Type.Object(
        {
          status: Type.Union([Type.Literal("healthy"), Type.Literal("degraded")]),
          lastSuccessAt: Type.Optional(TimestampMsSchema),
          lastFailure: Type.Optional(
            Type.Object(
              {
                component: NonEmptyString,
                operation: NonEmptyString,
                message: Type.String(),
                failedAt: TimestampMsSchema,
                failureCount: Type.Integer({ minimum: 1 }),
              },
              { additionalProperties: false },
            ),
          ),
        },
        { additionalProperties: false },
      ),
      store: Type.Optional(
        Type.Object(
          {
            path: Type.String(),
            schemaVersion: Type.Integer({ minimum: 0 }),
            runs: Type.Integer({ minimum: 0 }),
            events: Type.Integer({ minimum: 0 }),
            steps: Type.Integer({ minimum: 0 }),
            openRuns: Type.Integer({ minimum: 0 }),
            pendingWakes: Type.Integer({ minimum: 0 }),
            unresolvedUncertaintyFacts: Type.Integer({ minimum: 0 }),
          },
          { additionalProperties: false },
        ),
      ),
    },
    { additionalProperties: false },
  ),
);

export const DeliveryAttemptEvidenceListResultSchema = Type.Object(
  {
    deliveryAttemptEvidence: Type.Array(DeliveryAttemptEvidenceSchema),
  },
  { additionalProperties: false },
);

export const UncertaintyFactListResultSchema = Type.Object(
  {
    uncertaintyFacts: Type.Array(UncertaintyFactSchema),
  },
  { additionalProperties: false },
);
