import type {
  AppendDurableRuntimeEventInput,
  ClaimDurableRuntimeStepInput,
  CompactDurableRuntimeRunInput,
  CompactDurableRuntimeRunResult,
  CreateDurableRuntimeLinkInput,
  CreateDurableRuntimeRefInput,
  CreateDurableRuntimeRunInput,
  CreateDurableRuntimeSignalInput,
  CreateDurableRuntimeStepInput,
  CreateDurableRuntimeTimerInput,
  DurableRuntimeEvent,
  DurableRuntimeLink,
  DurableRuntimeRef,
  DurableRuntimeRun,
  DurableRuntimeRunPage,
  DurableRuntimeSignal,
  DurableRuntimeStep,
  DurableRuntimeStepClaim,
  DurableRuntimeStoreStats,
  DurableRuntimeTimelineOptions,
  DurableRuntimeTimer,
  UpdateDurableRuntimeLinkInput,
  UpdateDurableRuntimeRunInput,
  UpdateDurableRuntimeStepInput,
  UpdateDurableRuntimeTimerInput,
} from "./types-core.js";
import type {
  ClaimNextWakeObligationInput,
  CompleteWakeObligationClaimInput,
  CreateUncertaintyFactInput,
  CreateWakeObligationInput,
  DeliveryAttemptEvidence,
  DeliveryAttemptEvidenceStatus,
  DurableUnresolvedObligation,
  MarkWakeObligationDecisionRequiredInput,
  ReconcileWakeObligationInput,
  ReconcileWakeObligationResult,
  RenewWakeObligationClaimInput,
  ResolveUncertaintyFactInput,
  ResumeWakeObligationInput,
  SupersedeWakeObligationInput,
  SuspendWakeObligationInput,
  UncertaintyFact,
  UncertaintyFactStatus,
  UpdateWakeObligationProjectionInput,
  WakeObligation,
  WakeObligationClaim,
  WakeObligationControlInput,
  WakeObligationInspection,
  WakeObligationOwnerKind,
  WakeObligationPage,
  WakeObligationStatus,
  WakeObligationTargetKind,
  WakeObligationTargetResolutionStatus,
} from "./types-wake.js";

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
  listOpenRuns(options?: {
    operationKind?: string;
    updatedAtOrBefore?: number;
    cursor?: string;
    limit?: number;
  }): DurableRuntimeRunPage;
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
    cursor?: string;
    limit: number;
  }): WakeObligationPage;
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
