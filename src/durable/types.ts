// Shared durable runtime control-plane types.
export type DurableRuntimeRunStatus =
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

export type DurableRecoveryState =
  | "runnable"
  | "claimed"
  | "running"
  | "waiting_signal"
  | "waiting_timer"
  | "waiting_child"
  | "retry_scheduled"
  | "reconciling"
  | "requires_owner_decision"
  | "unknown_after_side_effect"
  | "lost"
  | "terminal";

export type DurableRuntimeStepType =
  | "agent"
  | "tool"
  | "timer"
  | "signal"
  | "child_runtime"
  | "checkpoint"
  | "fan_in";

export type DurableRuntimeStepStatus =
  | "pending"
  | "queued"
  | "running"
  | "waiting"
  | "retry_scheduled"
  | "succeeded"
  | "failed"
  | "cancelled"
  | "lost"
  | "skipped";

export type DurableRuntimeRefKind = "input" | "output" | "error" | "artifact";

export type DurableRuntimeLinkType =
  | "child_runtime"
  | "handoff"
  | "subagent"
  | "evidence"
  | "artifact";

export type DurableRuntimeLinkStatus =
  | "pending"
  | "running"
  | "succeeded"
  | "failed"
  | "cancelled"
  | "lost";

export type DurableRuntimeTimerStatus = "pending" | "fired" | "cancelled";

export type WakeObligationStatus =
  | "pending"
  | "handoff_accepted"
  | "acked"
  | "failed"
  | "suspended"
  | "superseded";

export type WakeObligationReason =
  | "child_terminal"
  | "child_overdue"
  | "fan_in_incomplete"
  | "restart_interrupted"
  | "delivery_unknown"
  | "side_effect_uncertain"
  | "no_handler"
  | "operator_requested";

export type WakeObligationTargetKind =
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

export type WakeObligationOwnerKind =
  | "agent_session"
  | "run"
  | "taskflow"
  | "scheduler"
  | "workboard"
  | "plugin"
  | "operator"
  | "external_route";

export type WakeObligationTargetResolutionStatus =
  | "unresolved"
  | "resolved"
  | "ambiguous"
  | "missing"
  | "unauthorized"
  | "inspect_only";

export type UncertaintyFactKind =
  | "unknown_after_side_effect"
  | "interrupted_during_tool"
  | "lost_after_dispatch"
  | "delivery_unknown"
  | "requires_owner_decision";

export type UncertaintyFactStatus = "open" | "resolved" | "superseded";

export type DeliveryAttemptEvidenceStatus =
  | "pending"
  | "attempted"
  | "handoff_accepted"
  | "failed"
  | "unknown"
  | "superseded";

export type WakeObligationControlActorKind =
  | "owner"
  | "requester"
  | "controller"
  | "operator"
  | "system_worker"
  | "admin";

export type WakeObligationControlDecisionKind =
  | "acknowledged"
  | "superseded"
  | "resumed"
  | "inspected"
  | "requires_human_decision"
  | "requires_operator_decision";

export type WakeObligationControlDecision = {
  kind: WakeObligationControlDecisionKind;
  actorKind: WakeObligationControlActorKind;
  actorRef: string;
  reason?: string;
  decisionRef?: string;
  idempotencyKey?: string;
  expectedSourceRevision?: string;
  expectedDeliveryRevision?: string;
  evidence?: Record<string, unknown>;
  metadata?: Record<string, unknown>;
  decidedAt: number;
};

export type DurableRuntimeRun = {
  runtimeRunId: string;
  operationKind: string;
  operationVersion: string;
  status: DurableRuntimeRunStatus;
  recoveryState: DurableRecoveryState;
  idempotencyKey?: string;
  requestHash?: string;
  sourceOwner?: string;
  sourceRef?: string;
  rootOperationReason?: string;
  inputRef?: string;
  checkpointRef?: string;
  parentRuntimeRunId?: string;
  parentStepId?: string;
  messageId?: string;
  turnId?: string;
  workUnitId?: string;
  reportRouteRef?: string;
  heartbeatAt?: number;
  metadata?: Record<string, unknown>;
  createdAt: number;
  updatedAt: number;
  completedAt?: number;
};

export type DurableRuntimeStep = {
  runtimeRunId: string;
  stepId: string;
  parentStepId?: string;
  stepType: DurableRuntimeStepType;
  status: DurableRuntimeStepStatus;
  recoveryState: DurableRecoveryState;
  attempt: number;
  maxAttempts?: number;
  idempotencyKey?: string;
  inputRef?: string;
  outputRef?: string;
  errorRef?: string;
  checkpointRef?: string;
  claimedBy?: string;
  claimExpiresAt?: number;
  heartbeatAt?: number;
  metadata?: Record<string, unknown>;
  createdAt: number;
  startedAt?: number;
  updatedAt: number;
  completedAt?: number;
};

export type DurableRuntimeRef = {
  refId: string;
  runtimeRunId: string;
  stepId?: string;
  refKind: DurableRuntimeRefKind;
  mediaType?: string;
  hash?: string;
  storageKind: "inline" | "file" | "external";
  storageUri?: string;
  metadata?: Record<string, unknown>;
  createdAt: number;
};

export type DurableRuntimeLink = {
  parentRuntimeRunId: string;
  parentStepId: string;
  childRuntimeRunId: string;
  linkType: DurableRuntimeLinkType;
  status: DurableRuntimeLinkStatus;
  metadata?: Record<string, unknown>;
  createdAt: number;
  updatedAt: number;
};

export type DurableRuntimeTimer = {
  timerId: string;
  runtimeRunId: string;
  stepId?: string;
  timerType: "retry" | "deadline" | "sleep" | "human_timeout" | "child_timeout" | "scheduled_start";
  dueAt: number;
  status: DurableRuntimeTimerStatus;
  metadata?: Record<string, unknown>;
  createdAt: number;
  firedAt?: number;
  cancelledAt?: number;
};

export type DurableRuntimeSignal = {
  signalId: string;
  runtimeRunId: string;
  stepId?: string;
  signalType:
    | "human_input"
    | "approval"
    | "rejection"
    | "external_callback"
    | "child_completed"
    | "child_failed"
    | "cancel"
    | "resume";
  idempotencyKey?: string;
  payloadRef?: string;
  correlationId?: string;
  metadata?: Record<string, unknown>;
  receivedAt: number;
  consumedAt?: number;
};

export type DurableRuntimeEvent = {
  eventId: string;
  runtimeRunId: string;
  eventSeq: number;
  eventType: string;
  eventTime: number;
  stepId?: string;
  agentInvocationId?: string;
  toolInvocationId?: string;
  idempotencyKey?: string;
  payload?: Record<string, unknown>;
  payloadHash?: string;
  checkpointRef?: string;
  causationEventId?: string;
  correlationId?: string;
  recordedAt: number;
};

export type WakeObligation = {
  wakeId: string;
  sourceOwner: string;
  sourceRef: string;
  coalescingPolicy: WakeObligationCoalescingPolicy;
  parentRunId?: string;
  parentSessionKey?: string;
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
  sourceRevision?: string;
  /** Store-owned projection and occurrence revision used to fence delivery consumers. */
  deliveryRevision: string;
  attemptCount: number;
  lastAttemptAt?: number;
  nextAttemptAt?: number;
  ackedAt?: number;
  failedReason?: string;
  status: WakeObligationStatus;
  metadata?: Record<string, unknown>;
  createdAt: number;
  updatedAt: number;
};

export type UncertaintyFact = {
  factId: string;
  sourceOwner: string;
  sourceRef: string;
  kind: UncertaintyFactKind;
  sourceRunId?: string;
  stepId?: string;
  eventId?: string;
  refId?: string;
  factsRef?: string;
  dedupeKey?: string;
  facts?: Record<string, unknown>;
  status: UncertaintyFactStatus;
  resolutionKind?: string;
  resolutionRef?: string;
  resolvedAt?: number;
  metadata?: Record<string, unknown>;
  createdAt: number;
  updatedAt: number;
};

export type DeliveryAttemptEvidence = {
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
  evidence?: Record<string, unknown>;
  error?: string;
  scheduledAt: number;
  attemptedAt?: number;
  handoffAcceptedAt?: number;
  failedAt?: number;
  unknownAt?: number;
  deliveryClaimedBy?: string;
  deliveryClaimExpiresAt?: number;
  createdAt: number;
  updatedAt: number;
  metadata?: Record<string, unknown>;
};

export type WakeObligationTargetResolutionInspection = {
  status?: WakeObligationTargetResolutionStatus;
  reason?: string;
  targetKind?: WakeObligationTargetKind;
  targetRef?: string;
  ownerKind?: WakeObligationOwnerKind;
  ownerRef?: string;
  reportRouteRef?: string;
  factsRef?: string;
  sourceRunId?: string;
  diagnostics?: Record<string, unknown>;
  evidence?: Record<string, unknown>;
};

export type WakeObligationInspection = {
  wake: WakeObligation;
  targetResolution: WakeObligationTargetResolutionInspection;
  deliveryAttemptEvidence: DeliveryAttemptEvidence[];
  deliveryAttemptEvidenceCount: number;
  deliveryAttemptEvidenceTruncated: boolean;
  unresolvedUncertaintyFacts: UncertaintyFact[];
  unresolvedUncertaintyFactCount: number;
  unresolvedUncertaintyFactsTruncated: boolean;
  sourceRefs: {
    sourceOwner: string;
    sourceRef: string;
    factsRef?: string;
    sourceRunId?: string;
    sourceRevision?: string;
    occurrenceKeys: string[];
    occurrenceCount: number;
    occurrenceKeysTruncated: boolean;
    parentRunId?: string;
    parentSessionKey?: string;
  };
};

export type DurableUnresolvedObligationKind =
  | "pending_wake"
  | "unresolved_uncertainty"
  | "open_child"
  | "expired_state_lease";

export type DurableUnresolvedObligation = {
  obligationId: string;
  sourceOwner: string;
  sourceRef: string;
  kind: DurableUnresolvedObligationKind;
  runtimeRunId?: string;
  stepId?: string;
  wakeId?: string;
  uncertaintyFactId?: string;
  subjectRef?: string;
  reason?: string;
  status: string;
  createdAt: number;
  updatedAt: number;
  metadata?: Record<string, unknown>;
};

export type CreateDurableRuntimeRunInput = {
  runtimeRunId?: string;
  operationKind: string;
  operationVersion?: string;
  status?: DurableRuntimeRunStatus;
  recoveryState?: DurableRecoveryState;
  idempotencyKey?: string;
  requestHash?: string;
  sourceOwner?: string;
  sourceRef?: string;
  /** Required by source-ref contract only for root durable operations with no source owner/ref. */
  rootOperationReason?: string;
  inputRef?: string;
  checkpointRef?: string;
  parentRuntimeRunId?: string;
  parentStepId?: string;
  messageId?: string;
  turnId?: string;
  workUnitId?: string;
  reportRouteRef?: string;
  completedAt?: number;
  metadata?: Record<string, unknown>;
  now?: number;
};

export type AppendDurableRuntimeEventInput = {
  runtimeRunId: string;
  eventId?: string;
  eventType: string;
  eventTime?: number;
  stepId?: string;
  agentInvocationId?: string;
  toolInvocationId?: string;
  idempotencyKey?: string;
  payload?: Record<string, unknown>;
  payloadHash?: string;
  checkpointRef?: string;
  causationEventId?: string;
  correlationId?: string;
};

export type UpdateDurableRuntimeRunInput = {
  runtimeRunId: string;
  status?: DurableRuntimeRunStatus;
  recoveryState?: DurableRecoveryState;
  completedAt?: number | null;
  checkpointRef?: string | null;
  workUnitId?: string | null;
  reportRouteRef?: string | null;
  heartbeatAt?: number | null;
  metadata?: Record<string, unknown>;
  now?: number;
};

export type CreateDurableRuntimeStepInput = {
  runtimeRunId: string;
  stepId?: string;
  parentStepId?: string;
  stepType: DurableRuntimeStepType;
  status?: DurableRuntimeStepStatus;
  recoveryState?: DurableRecoveryState;
  attempt?: number;
  maxAttempts?: number;
  idempotencyKey?: string;
  inputRef?: string;
  outputRef?: string;
  errorRef?: string;
  checkpointRef?: string;
  metadata?: Record<string, unknown>;
  now?: number;
};

export type UpdateDurableRuntimeStepInput = {
  runtimeRunId: string;
  stepId: string;
  expectedClaimToken?: string;
  status?: DurableRuntimeStepStatus;
  recoveryState?: DurableRecoveryState;
  attempt?: number;
  maxAttempts?: number | null;
  inputRef?: string | null;
  outputRef?: string | null;
  errorRef?: string | null;
  checkpointRef?: string | null;
  claimedBy?: string | null;
  claimExpiresAt?: number | null;
  heartbeatAt?: number | null;
  startedAt?: number | null;
  completedAt?: number | null;
  metadata?: Record<string, unknown>;
  now?: number;
};

export type CreateDurableRuntimeRefInput = {
  refId?: string;
  runtimeRunId: string;
  stepId?: string;
  refKind: DurableRuntimeRefKind;
  mediaType?: string;
  hash?: string;
  storageKind?: "inline" | "file" | "external";
  storageUri?: string;
  metadata?: Record<string, unknown>;
  now?: number;
};

export type CreateDurableRuntimeLinkInput = {
  parentRuntimeRunId: string;
  parentStepId: string;
  childRuntimeRunId: string;
  linkType: DurableRuntimeLinkType;
  status?: DurableRuntimeLinkStatus;
  metadata?: Record<string, unknown>;
  now?: number;
};

export type UpdateDurableRuntimeLinkInput = {
  parentRuntimeRunId: string;
  parentStepId: string;
  childRuntimeRunId: string;
  status?: DurableRuntimeLinkStatus;
  metadata?: Record<string, unknown>;
  now?: number;
};

export type CreateDurableRuntimeTimerInput = {
  timerId?: string;
  runtimeRunId: string;
  stepId?: string;
  timerType: DurableRuntimeTimer["timerType"];
  dueAt: number;
  metadata?: Record<string, unknown>;
  now?: number;
};

export type UpdateDurableRuntimeTimerInput = {
  timerId: string;
  status: DurableRuntimeTimerStatus;
  firedAt?: number | null;
  cancelledAt?: number | null;
  now?: number;
};

export type CreateDurableRuntimeSignalInput = {
  signalId?: string;
  runtimeRunId: string;
  stepId?: string;
  signalType: DurableRuntimeSignal["signalType"];
  idempotencyKey?: string;
  payloadRef?: string;
  correlationId?: string;
  metadata?: Record<string, unknown>;
  now?: number;
};

export type CreateWakeObligationInput = {
  wakeId?: string;
  sourceOwner: string;
  sourceRef: string;
  parentRunId?: string;
  parentSessionKey?: string;
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
  sourceRevision?: string;
  occurrenceKey: string;
  metadata?: Record<string, unknown>;
  now?: number;
};

export type WakeObligationCoalescingPolicy =
  | { mode: "none" }
  | {
      mode: "while_unresolved";
      recurrence: "never" | "after_terminal";
    };

export type ReconcileWakeObligationInput = {
  candidate: CreateWakeObligationInput;
  policy: WakeObligationCoalescingPolicy;
};

export type WakeObligationReconciliationDisposition =
  | "created"
  | "exact_match"
  | "coalesced"
  | "terminal_match";

export type WakeObligationDuplicateScan = "complete" | "truncated" | "not_scanned";

export type ReconcileWakeObligationAppliedResult = {
  wake: WakeObligation;
  disposition: WakeObligationReconciliationDisposition;
  candidatePersisted: true;
  duplicateScan: WakeObligationDuplicateScan;
  duplicateWakeIds: string[];
};

export type WakeObligationReconciliationConflictReason =
  | "idempotency_conflict"
  | "policy_conflict"
  | "multiple_evidence_bearing_wakes"
  | "scan_truncated";

export type ReconcileWakeObligationConflictResult = {
  disposition: "conflict";
  reason: WakeObligationReconciliationConflictReason;
  candidatePersisted: false;
  duplicateScan: WakeObligationDuplicateScan;
  conflictingWakeIds: string[];
  representativeWake?: WakeObligation;
};

export type ReconcileWakeObligationResult =
  | ReconcileWakeObligationAppliedResult
  | ReconcileWakeObligationConflictResult;

export type UpdateWakeObligationProjectionInput = {
  wakeId: string;
  metadata: Record<string, unknown>;
  factsRef?: string;
  sourceRevision?: string;
  now?: number;
};

export type SuspendWakeObligationInput = {
  wakeId: string;
  failedReason: string;
  metadata?: Record<string, unknown>;
  now?: number;
};

export type WakeObligationControlInput = {
  wakeId: string;
  actorKind: WakeObligationControlActorKind;
  actorRef: string;
  reason?: string;
  decisionRef?: string;
  evidence?: Record<string, unknown>;
  metadata?: Record<string, unknown>;
  idempotencyKey?: string;
  expectedSourceRevision?: string;
  expectedDeliveryRevision?: string;
  now?: number;
};

export type SupersedeWakeObligationInput = WakeObligationControlInput & {
  supersededByRef?: string;
};

export type MarkWakeObligationDecisionRequiredInput = WakeObligationControlInput & {
  decisionKind: Extract<
    WakeObligationControlDecisionKind,
    "inspected" | "requires_human_decision" | "requires_operator_decision"
  >;
};

export type ResumeWakeObligationInput = WakeObligationControlInput;

export type CreateUncertaintyFactInput = {
  factId?: string;
  sourceOwner: string;
  sourceRef: string;
  kind: UncertaintyFactKind;
  sourceRunId?: string;
  stepId?: string;
  eventId?: string;
  refId?: string;
  factsRef?: string;
  dedupeKey?: string;
  facts?: Record<string, unknown>;
  metadata?: Record<string, unknown>;
  now?: number;
};

export type ResolveUncertaintyFactInput = {
  factId: string;
  status: Extract<UncertaintyFactStatus, "resolved" | "superseded">;
  resolutionKind?: string;
  resolutionRef?: string;
  expectedUpdatedAt?: number;
  metadata?: Record<string, unknown>;
  now?: number;
};

export type WakeObligationClaim = {
  wake: WakeObligation;
  deliveryAttempt: DeliveryAttemptEvidence;
  claimToken: string;
  claimExpiresAt: number;
};

export type ClaimNextWakeObligationInput = {
  workerId: string;
  claimTtlMs: number;
  retryBaseMs: number;
  retryMaxMs: number;
  now?: number;
};

export type RenewWakeObligationClaimInput = {
  wakeId: string;
  deliveryAttemptId: string;
  claimToken: string;
  claimTtlMs: number;
  now?: number;
};

export type CompleteWakeObligationClaimInput = {
  wakeId: string;
  deliveryAttemptId: string;
  claimToken: string;
  attemptStatus: Extract<
    DeliveryAttemptEvidenceStatus,
    "handoff_accepted" | "failed" | "unknown" | "superseded"
  >;
  wakeStatus: Extract<
    WakeObligationStatus,
    "handoff_accepted" | "acked" | "failed" | "suspended" | "superseded"
  >;
  evidence?: Record<string, unknown>;
  error?: string;
  now?: number;
};

export type ClaimDurableRuntimeStepInput = {
  operationKind?: string;
  stepType?: DurableRuntimeStepType;
  workerId: string;
  claimTtlMs: number;
  now?: number;
};

export type DurableRuntimeStepClaim = {
  step: DurableRuntimeStep;
  claimToken: string;
  claimExpiresAt: number;
};

export type DurableRuntimeStoreStats = {
  path: string;
  runs: number;
  events: number;
  steps: number;
  openRuns: number;
  pendingWakes: number;
  unresolvedUncertaintyFacts: number;
};

export type DurableRuntimeTimelineOptions = {
  limit?: number;
  afterEventSeq?: number;
};

export type CompactDurableRuntimeRunInput = {
  runtimeRunId: string;
  keepLastEvents?: number;
  now?: number;
};

export type CompactDurableRuntimeRunResult = {
  runtimeRunId: string;
  compacted: boolean;
  redactedEventPayloads: number;
  hasMore: boolean;
};

export type DurableRuntimeStore = {
  withTransaction<T>(operation: () => T): T;
  createRun(input: CreateDurableRuntimeRunInput): DurableRuntimeRun;
  getRun(runtimeRunId: string): DurableRuntimeRun | undefined;
  getRunByIdempotencyKey(
    operationKind: string,
    idempotencyKey: string,
  ): DurableRuntimeRun | undefined;
  updateRun(input: UpdateDurableRuntimeRunInput): DurableRuntimeRun | undefined;
  appendEvent(input: AppendDurableRuntimeEventInput): DurableRuntimeEvent;
  listRuns(options?: { limit?: number }): DurableRuntimeRun[];
  listOpenRuns(options?: { operationKind?: string; limit?: number }): DurableRuntimeRun[];
  createStep(input: CreateDurableRuntimeStepInput): DurableRuntimeStep;
  updateStep(input: UpdateDurableRuntimeStepInput): DurableRuntimeStep | undefined;
  claimNextRunnableStep(input: ClaimDurableRuntimeStepInput): DurableRuntimeStepClaim | undefined;
  renewStepClaim(input: {
    runtimeRunId: string;
    stepId: string;
    claimToken: string;
    claimTtlMs: number;
    now?: number;
  }): DurableRuntimeStep | undefined;
  releaseStepClaim(input: {
    runtimeRunId: string;
    stepId: string;
    claimToken: string;
    now?: number;
  }): DurableRuntimeStep | undefined;
  listSteps(runtimeRunId: string): DurableRuntimeStep[];
  createRef(input: CreateDurableRuntimeRefInput): DurableRuntimeRef;
  getRef(refId: string): DurableRuntimeRef | undefined;
  listRefs(runtimeRunId: string): DurableRuntimeRef[];
  createLink(input: CreateDurableRuntimeLinkInput): DurableRuntimeLink;
  updateLink(input: UpdateDurableRuntimeLinkInput): DurableRuntimeLink | undefined;
  listChildLinks(parentRuntimeRunId: string): DurableRuntimeLink[];
  listParentLinks(childRuntimeRunId: string): DurableRuntimeLink[];
  createTimer(input: CreateDurableRuntimeTimerInput): DurableRuntimeTimer;
  updateTimer(input: UpdateDurableRuntimeTimerInput): DurableRuntimeTimer | undefined;
  fireDueTimer(input: { timerId: string; now?: number }): DurableRuntimeTimer | undefined;
  listTimers(runtimeRunId?: string): DurableRuntimeTimer[];
  listDueTimers(now: number, options?: { limit?: number }): DurableRuntimeTimer[];
  createSignal(input: CreateDurableRuntimeSignalInput): DurableRuntimeSignal;
  consumeSignal(input: { signalId: string; now?: number }): DurableRuntimeSignal | undefined;
  consumePendingSignal(input: { signalId: string; now?: number }): DurableRuntimeSignal | undefined;
  listPendingSignals(options?: { limit?: number }): DurableRuntimeSignal[];
  listSignals(runtimeRunId: string): DurableRuntimeSignal[];
  reconcileWakeObligation(input: ReconcileWakeObligationInput): ReconcileWakeObligationResult;
  createWakeObligation(input: CreateWakeObligationInput): WakeObligation;
  updateWakeObligationProjection(
    input: UpdateWakeObligationProjectionInput,
  ): WakeObligation | undefined;
  suspendWakeObligation(input: SuspendWakeObligationInput): WakeObligation | undefined;
  acknowledgeWakeObligation(input: WakeObligationControlInput): WakeObligation | undefined;
  supersedeWakeObligation(input: SupersedeWakeObligationInput): WakeObligation | undefined;
  resumeWakeObligation(input: ResumeWakeObligationInput): WakeObligation | undefined;
  markWakeObligationDecisionRequired(
    input: MarkWakeObligationDecisionRequiredInput,
  ): WakeObligation | undefined;
  getWakeObligation(wakeId: string): WakeObligation | undefined;
  getWakeObligationByOccurrenceKey(input: {
    sourceOwner: string;
    sourceRef: string;
    occurrenceKey: string;
  }): WakeObligation | undefined;
  getWakeObligationInspection(wakeId: string): WakeObligationInspection | undefined;
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
  }): WakeObligation[];
  listUnresolvedWakeObligationsPage(input: {
    sourceOwner?: string;
    createdAtOrBefore?: number;
    afterWakeId?: string;
    limit: number;
  }): WakeObligation[];
  recordUncertaintyFact(input: CreateUncertaintyFactInput): UncertaintyFact;
  resolveUncertaintyFact(input: ResolveUncertaintyFactInput): UncertaintyFact | undefined;
  listUncertaintyFacts(options?: {
    sourceOwner?: string;
    sourceRef?: string;
    sourceRunId?: string;
    status?: UncertaintyFactStatus;
    limit?: number;
  }): UncertaintyFact[];
  claimNextWakeObligation(input: ClaimNextWakeObligationInput): WakeObligationClaim | undefined;
  renewWakeObligationClaim(input: RenewWakeObligationClaimInput): boolean;
  completeWakeObligationClaim(
    input: CompleteWakeObligationClaimInput,
  ): DeliveryAttemptEvidence | undefined;
  getDeliveryAttemptEvidence(deliveryAttemptId: string): DeliveryAttemptEvidence | undefined;
  listDeliveryAttemptEvidence(options?: {
    wakeId?: string;
    dedupeKey?: string;
    status?: DeliveryAttemptEvidenceStatus;
    limit?: number;
  }): DeliveryAttemptEvidence[];
  listPendingWakeObligations(options?: { limit?: number }): WakeObligation[];
  listUnresolvedUncertaintyFacts(options?: {
    sourceRunId?: string;
    limit?: number;
  }): UncertaintyFact[];
  listUnresolvedObligations(options?: {
    now?: number;
    limit?: number;
  }): DurableUnresolvedObligation[];
  getTimeline(runtimeRunId: string, options?: DurableRuntimeTimelineOptions): DurableRuntimeEvent[];
  compactTerminalRun(input: CompactDurableRuntimeRunInput): CompactDurableRuntimeRunResult;
  getStats(): DurableRuntimeStoreStats;
  close(): void;
};

export type DurableRuntimeReadStore = Pick<
  DurableRuntimeStore,
  | "getRun"
  | "getRunByIdempotencyKey"
  | "listRuns"
  | "listOpenRuns"
  | "listSteps"
  | "getRef"
  | "listRefs"
  | "listChildLinks"
  | "listParentLinks"
  | "listTimers"
  | "listDueTimers"
  | "listPendingSignals"
  | "listSignals"
  | "getWakeObligation"
  | "getWakeObligationInspection"
  | "listWakeObligations"
  | "listUncertaintyFacts"
  | "getDeliveryAttemptEvidence"
  | "listDeliveryAttemptEvidence"
  | "listPendingWakeObligations"
  | "listUnresolvedUncertaintyFacts"
  | "listUnresolvedObligations"
  | "getTimeline"
  | "getStats"
  | "close"
>;
