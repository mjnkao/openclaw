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
  expectedDeliveryRevision?: number;
  evidence?: Record<string, unknown>;
  metadata?: Record<string, unknown>;
  decidedAt: number;
};

export type WakeObligationSuspensionClass =
  | "capability_unavailable"
  | "target_unavailable_before_attempt"
  | "delivery_outcome_unknown"
  | "reconciliation_conflict"
  | "owner_decision_required"
  | "retry_exhausted";

export type AutoResumableWakeObligationSuspensionClass = Extract<
  WakeObligationSuspensionClass,
  "capability_unavailable" | "target_unavailable_before_attempt"
>;

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
  deliveryRevision: number;
  suspensionClass?: WakeObligationSuspensionClass;
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
  claimedWakeDeliveryRevision: number;
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

export type WakeObligationPage = {
  wakes: WakeObligation[];
  complete: boolean;
  /** Opaque continuation bound to the filters used to create this page. */
  nextCursor?: string;
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
  suspensionClass: WakeObligationSuspensionClass;
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
  expectedDeliveryRevision?: number;
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

export type ResumeWakeObligationInput = WakeObligationControlInput & {
  expectedDeliveryRevision: number;
  expectedSuspensionClass: AutoResumableWakeObligationSuspensionClass;
};

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
