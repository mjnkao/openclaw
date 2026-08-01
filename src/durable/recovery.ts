import { createSubsystemLogger } from "../logging/subsystem.js";
// Recovery reconciliation for durable runtime runs.
import {
  isDurableWorkerEnabled,
  resolveDurableRuntimeSqlitePath,
  resolveDurableWorkerClaimTtlMs,
  resolveDurableWorkerPollIntervalMs,
} from "./config.js";
import { recordDurableRuntimeHealthFailure, recordDurableRuntimeHealthSuccess } from "./health.js";
import {
  DURABLE_AGENT_TURN_OPERATION_KIND,
  DURABLE_CHAT_SEND_OPERATION_KIND,
} from "./runtime-ids.js";
import { recoverDurableSessionAttentionDeliveries } from "./session-owner-adapter.js";
import { openDurableRuntimeStore } from "./store-factory.js";
import type {
  DurableRuntimeRun,
  DurableRuntimeSignal,
  DurableRuntimeStep,
  DurableRuntimeStore,
  DurableRuntimeTimer,
} from "./types.js";
import { runDurableWakeDispatcherOnce } from "./wake-dispatcher.js";

const log = createSubsystemLogger("durable/recovery");

const MIN_STALE_RUNTIME_RUN_AFTER_MS = 2 * 60_000;
const DEFAULT_STALE_SCAN_INTERVAL_MS = 60_000;
const RECOVERY_RUN_SCAN_PAGE_SIZE = 500;
const RECOVERY_DIAGNOSTIC_METADATA_KEY = "recoveryDiagnostic";

export function resolveDurableStaleRuntimeRunAfterMs(): number {
  return Math.max(MIN_STALE_RUNTIME_RUN_AFTER_MS, resolveDurableWorkerClaimTtlMs() * 2);
}

export type DurableRecoveryResult = {
  scanned: number;
  markedLost: number;
  firedTimers?: number;
  consumedSignals?: number;
  queuedRuns?: number;
};

export type DurableStaleRecoveryResult = DurableRecoveryResult & {
  nextCursor?: string;
  complete: boolean;
};

function shouldMarkAgentTurnLost(run: DurableRuntimeRun): boolean {
  if (run.operationKind !== DURABLE_AGENT_TURN_OPERATION_KIND) {
    return false;
  }
  return run.status === "received" || run.status === "running";
}

function shouldMarkChatSendLost(run: DurableRuntimeRun): boolean {
  if (run.operationKind !== DURABLE_CHAT_SEND_OPERATION_KIND) {
    return false;
  }
  return run.status === "received" || run.status === "queued" || run.status === "running";
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function firstString(...values: unknown[]): string | undefined {
  for (const value of values) {
    if (typeof value === "string" && value.trim()) {
      return value.trim();
    }
  }
  return undefined;
}

function firstBoolean(...values: unknown[]): boolean | undefined {
  for (const value of values) {
    if (typeof value === "boolean") {
      return value;
    }
  }
  return undefined;
}

function correlationIdForRun(run: DurableRuntimeRun): string | undefined {
  return firstString(run.metadata?.sessionKey, run.sourceRef);
}

function runtimeSubject(run: DurableRuntimeRun): string {
  if (run.operationKind === DURABLE_AGENT_TURN_OPERATION_KIND) {
    return "Agent turn";
  }
  if (run.operationKind === DURABLE_CHAT_SEND_OPERATION_KIND) {
    return "Chat send";
  }
  return "Runtime run";
}

function lostNextAction(run: DurableRuntimeRun): string {
  const input = lostInputRecoveryHint(run);
  const canReplay = input?.canReplay === true;
  if (run.operationKind === DURABLE_AGENT_TURN_OPERATION_KIND) {
    return canReplay
      ? "inspect_timeline_then_requeue_agent_step"
      : "inspect_timeline_then_retry_or_resume";
  }
  if (run.operationKind === DURABLE_CHAT_SEND_OPERATION_KIND) {
    return canReplay
      ? "inspect_timeline_then_requeue_chat_send"
      : "inspect_timeline_then_retry_request";
  }
  return "inspect_timeline_then_apply_policy";
}

function safeRecoveryActions(run: DurableRuntimeRun): string[] {
  const input = lostInputRecoveryHint(run);
  const actions = ["inspect_timeline"];
  if (input?.canReplay) {
    actions.push("requeue_from_durable_input");
  }
  actions.push("retry_request");
  return actions;
}

function lostInputRecoveryHint(run: DurableRuntimeRun):
  | {
      inputRef?: string;
      inputAvailability?: string;
      canReplay?: boolean;
      reason?: string;
      messageLength?: number;
      messageHash?: string;
    }
  | undefined {
  const metadata = isRecord(run.metadata) ? run.metadata : {};
  const envelope = isRecord(metadata.intakeEnvelope) ? metadata.intakeEnvelope : undefined;
  const replay = isRecord(envelope?.replay) ? envelope.replay : undefined;
  const message = isRecord(envelope?.message) ? envelope.message : undefined;
  if (!run.inputRef && !envelope) {
    return undefined;
  }
  return {
    ...(run.inputRef ? { inputRef: run.inputRef } : {}),
    ...(firstString(replay?.inputAvailability)
      ? { inputAvailability: firstString(replay?.inputAvailability) }
      : {}),
    ...(firstBoolean(replay?.canReplay) !== undefined
      ? { canReplay: firstBoolean(replay?.canReplay) }
      : {}),
    ...(firstString(replay?.reason) ? { reason: firstString(replay?.reason) } : {}),
    ...(typeof message?.length === "number" ? { messageLength: message.length } : {}),
    ...(firstString(message?.hash, metadata.messageHash)
      ? { messageHash: firstString(message?.hash, metadata.messageHash) }
      : {}),
  };
}

function buildLostRecoveryDiagnostic(params: {
  run: DurableRuntimeRun;
  now: number;
  reason: string;
  processInstanceId: string;
}): Record<string, unknown> {
  const subject = runtimeSubject(params.run);
  const input = lostInputRecoveryHint(params.run);
  return {
    state: "lost",
    severity: "error",
    reportable: true,
    retryable: true,
    reason: params.reason,
    message: `${subject} was marked lost during durable recovery; it did not reach a terminal result before the runner disappeared.`,
    nextAction: lostNextAction(params.run),
    processInstanceId: params.processInstanceId,
    detectedAt: params.now,
    previousStatus: params.run.status,
    previousRecoveryState: params.run.recoveryState,
    operationKind: params.run.operationKind,
    runtimeRunId: params.run.runtimeRunId,
    safeRecoveryActions: safeRecoveryActions(params.run),
    ...(input ? { input } : {}),
    ...(params.run.sourceRef ? { sourceRef: params.run.sourceRef } : {}),
  };
}

function mergeRecoveryDiagnosticMetadata(
  metadata: Record<string, unknown> | undefined,
  diagnostic: Record<string, unknown>,
): Record<string, unknown> {
  return {
    ...metadata,
    [RECOVERY_DIAGNOSTIC_METADATA_KEY]: diagnostic,
  };
}

function matchesLostRecoverySnapshot(
  scanned: DurableRuntimeRun,
  current: DurableRuntimeRun,
): boolean {
  // updatedAt fences metadata and other persisted run changes; the remaining
  // fields make the eligibility/liveness comparison explicit at this boundary.
  return (
    scanned.runtimeRunId === current.runtimeRunId &&
    scanned.operationKind === current.operationKind &&
    scanned.status === current.status &&
    scanned.recoveryState === current.recoveryState &&
    scanned.updatedAt === current.updatedAt &&
    scanned.completedAt === current.completedAt &&
    scanned.heartbeatAt === current.heartbeatAt
  );
}

function hasUnexpiredStepClaim(step: DurableRuntimeStep, now: number): boolean {
  return (
    !isTerminalStep(step) &&
    Boolean(step.claimedBy) &&
    (step.claimExpiresAt === undefined || step.claimExpiresAt > now)
  );
}

function markRunLost(params: {
  store: DurableRuntimeStore;
  run: DurableRuntimeRun;
  isEligible: (run: DurableRuntimeRun) => boolean;
  now: number;
  reason: string;
  processInstanceId: string;
  eventType: string;
  stepId: string;
  agentInvocationId?: string;
}): boolean {
  return params.store.withTransaction(() => {
    const currentRun = params.store.getRun(params.run.runtimeRunId);
    if (
      !currentRun ||
      !matchesLostRecoverySnapshot(params.run, currentRun) ||
      !params.isEligible(currentRun)
    ) {
      return false;
    }
    const scannedSteps = params.store.listSteps(currentRun.runtimeRunId);
    if (scannedSteps.some((step) => hasUnexpiredStepClaim(step, params.now))) {
      return false;
    }
    for (const step of scannedSteps) {
      if (isTerminalStep(step) || !step.claimedBy) {
        continue;
      }
      const released = params.store.releaseStepClaim({
        runtimeRunId: currentRun.runtimeRunId,
        stepId: step.stepId,
        claimToken: step.claimedBy,
        now: params.now,
      });
      if (!released) {
        throw new Error(
          `durable expired step claim could not be released: ${currentRun.runtimeRunId}:${step.stepId}`,
        );
      }
    }
    const currentSteps = params.store.listSteps(currentRun.runtimeRunId);
    const diagnostic = buildLostRecoveryDiagnostic({
      run: currentRun,
      now: params.now,
      reason: params.reason,
      processInstanceId: params.processInstanceId,
    });
    const updatedRun = params.store.updateRun({
      runtimeRunId: currentRun.runtimeRunId,
      status: "lost",
      recoveryState: "terminal",
      completedAt: params.now,
      metadata: mergeRecoveryDiagnosticMetadata(currentRun.metadata, diagnostic),
      now: params.now,
    });
    if (!updatedRun) {
      throw new Error(`durable run could not be marked lost: ${currentRun.runtimeRunId}`);
    }
    for (const step of currentSteps) {
      if (isTerminalStep(step)) {
        continue;
      }
      const updatedStep = params.store.updateStep({
        runtimeRunId: currentRun.runtimeRunId,
        stepId: step.stepId,
        status: "lost",
        recoveryState: "terminal",
        completedAt: params.now,
        metadata: mergeRecoveryDiagnosticMetadata(step.metadata, diagnostic),
        now: params.now,
      });
      if (!updatedStep) {
        throw new Error(
          `durable step could not be marked lost: ${currentRun.runtimeRunId}:${step.stepId}`,
        );
      }
    }
    params.store.appendEvent({
      runtimeRunId: currentRun.runtimeRunId,
      eventType: params.eventType,
      eventTime: params.now,
      stepId: params.stepId,
      agentInvocationId: params.agentInvocationId,
      idempotencyKey: currentRun.idempotencyKey,
      correlationId: correlationIdForRun(currentRun),
      payload: {
        reason: params.reason,
        processInstanceId: params.processInstanceId,
        previousStatus: currentRun.status,
        previousRecoveryState: currentRun.recoveryState,
        recoveryDiagnostic: diagnostic,
      },
    });
    const sourceOwner = currentRun.sourceOwner ?? "durable_execution_records";
    const sourceRef = currentRun.sourceRef ?? currentRun.runtimeRunId;
    const sessionKey = firstString(
      currentRun.metadata?.sessionKey,
      currentRun.sourceOwner === "session_store" ? currentRun.sourceRef : undefined,
    );
    const fact = params.store.recordUncertaintyFact({
      sourceOwner,
      sourceRef,
      kind: "lost_after_dispatch",
      sourceRunId: currentRun.runtimeRunId,
      stepId: params.stepId,
      refId: params.processInstanceId,
      dedupeKey: `restart-lost:${currentRun.runtimeRunId}:${params.processInstanceId}`,
      facts: diagnostic,
      now: params.now,
    });
    params.store.createWakeObligation({
      sourceOwner,
      sourceRef,
      targetKind: sessionKey ? "agent_session" : "run",
      targetRef: sessionKey ?? currentRun.runtimeRunId,
      ownerKind: sessionKey ? "agent_session" : "run",
      ownerRef: sessionKey ?? currentRun.runtimeRunId,
      reportRouteRef: sessionKey,
      targetResolutionStatus: "resolved",
      targetResolutionReason: sessionKey
        ? "session resolved from the source execution record"
        : "durable execution record is the inspectable owner",
      reason: "restart_interrupted",
      factsRef: `uncertainty_facts:${fact.factId}`,
      sourceRunId: currentRun.runtimeRunId,
      occurrenceKey: `restart-wake:${currentRun.runtimeRunId}:${sessionKey ?? "run"}`,
      metadata: diagnostic,
      now: params.now,
    });
    return true;
  });
}

function markAgentTurnLost(params: {
  store: DurableRuntimeStore;
  run: DurableRuntimeRun;
  now: number;
  reason: string;
  processInstanceId: string;
}): boolean {
  if (!shouldMarkAgentTurnLost(params.run)) {
    return false;
  }
  return markRunLost({
    ...params,
    isEligible: shouldMarkAgentTurnLost,
    eventType: "agent.turn.lost",
    stepId: "recovery",
    agentInvocationId: params.run.idempotencyKey,
  });
}

function markChatSendLost(params: {
  store: DurableRuntimeStore;
  run: DurableRuntimeRun;
  now: number;
  reason: string;
  processInstanceId: string;
}): boolean {
  if (!shouldMarkChatSendLost(params.run)) {
    return false;
  }
  return markRunLost({
    ...params,
    isEligible: shouldMarkChatSendLost,
    eventType: "chat.send.lost",
    stepId: "intake",
  });
}

export function reconcileDurableAgentTurnsOnGatewayStartup(params: {
  store: DurableRuntimeStore;
  processInstanceId: string;
  now: number;
  limit?: number;
}): DurableRecoveryResult {
  const page = params.store.listOpenRuns({
    operationKind: DURABLE_AGENT_TURN_OPERATION_KIND,
    limit: Math.max(
      1,
      Math.min(
        RECOVERY_RUN_SCAN_PAGE_SIZE,
        Math.trunc(params.limit ?? RECOVERY_RUN_SCAN_PAGE_SIZE),
      ),
    ),
  });
  let markedLost = 0;
  for (const run of page.runs) {
    if (
      markAgentTurnLost({
        store: params.store,
        run,
        now: params.now,
        reason: "gateway_startup_reconciliation",
        processInstanceId: params.processInstanceId,
      })
    ) {
      markedLost += 1;
    }
  }
  return { scanned: page.runs.length, markedLost };
}

export function reconcileDurableChatSendsOnGatewayStartup(params: {
  store: DurableRuntimeStore;
  processInstanceId: string;
  now: number;
  limit?: number;
}): DurableRecoveryResult {
  const page = params.store.listOpenRuns({
    operationKind: DURABLE_CHAT_SEND_OPERATION_KIND,
    limit: Math.max(
      1,
      Math.min(
        RECOVERY_RUN_SCAN_PAGE_SIZE,
        Math.trunc(params.limit ?? RECOVERY_RUN_SCAN_PAGE_SIZE),
      ),
    ),
  });
  let markedLost = 0;
  for (const run of page.runs) {
    if (
      markChatSendLost({
        store: params.store,
        run,
        now: params.now,
        reason: "gateway_startup_reconciliation",
        processInstanceId: params.processInstanceId,
      })
    ) {
      markedLost += 1;
    }
  }
  return { scanned: page.runs.length, markedLost };
}

export function reconcileStaleDurableAgentTurns(params: {
  store: DurableRuntimeStore;
  processInstanceId: string;
  now: number;
  staleAfterMs: number;
  cursor?: string;
  limit?: number;
}): DurableStaleRecoveryResult {
  const cutoff = params.now - params.staleAfterMs;
  const limit = Math.max(
    1,
    Math.min(RECOVERY_RUN_SCAN_PAGE_SIZE, Math.trunc(params.limit ?? RECOVERY_RUN_SCAN_PAGE_SIZE)),
  );
  const page = params.store.listOpenRuns({
    operationKind: DURABLE_AGENT_TURN_OPERATION_KIND,
    ...(params.cursor ? { cursor: params.cursor } : { updatedAtOrBefore: cutoff }),
    limit,
  });
  let markedLost = 0;
  for (const run of page.runs) {
    if (
      markAgentTurnLost({
        store: params.store,
        run,
        now: params.now,
        reason: "stale_agent_turn_reconciliation",
        processInstanceId: params.processInstanceId,
      })
    ) {
      markedLost += 1;
    }
  }
  return {
    scanned: page.runs.length,
    markedLost,
    complete: page.complete,
    ...(page.nextCursor ? { nextCursor: page.nextCursor } : {}),
  };
}

export function reconcileStaleDurableChatSends(params: {
  store: DurableRuntimeStore;
  processInstanceId: string;
  now: number;
  staleAfterMs: number;
  cursor?: string;
  limit?: number;
}): DurableStaleRecoveryResult {
  const cutoff = params.now - params.staleAfterMs;
  const limit = Math.max(
    1,
    Math.min(RECOVERY_RUN_SCAN_PAGE_SIZE, Math.trunc(params.limit ?? RECOVERY_RUN_SCAN_PAGE_SIZE)),
  );
  const page = params.store.listOpenRuns({
    operationKind: DURABLE_CHAT_SEND_OPERATION_KIND,
    ...(params.cursor ? { cursor: params.cursor } : { updatedAtOrBefore: cutoff }),
    limit,
  });
  let markedLost = 0;
  for (const run of page.runs) {
    if (
      markChatSendLost({
        store: params.store,
        run,
        now: params.now,
        reason: "stale_chat_send_reconciliation",
        processInstanceId: params.processInstanceId,
      })
    ) {
      markedLost += 1;
    }
  }
  return {
    scanned: page.runs.length,
    markedLost,
    complete: page.complete,
    ...(page.nextCursor ? { nextCursor: page.nextCursor } : {}),
  };
}

function isTerminalRun(run: DurableRuntimeRun): boolean {
  return (
    run.status === "succeeded" ||
    run.status === "failed" ||
    run.status === "cancelled" ||
    run.status === "lost"
  );
}

function isTerminalStep(step: DurableRuntimeStep): boolean {
  return (
    step.status === "succeeded" ||
    step.status === "failed" ||
    step.status === "cancelled" ||
    step.status === "lost" ||
    step.status === "skipped"
  );
}

function markOpenStepsTerminal(params: {
  store: DurableRuntimeStore;
  runtimeRunId: string;
  status: "cancelled" | "lost";
  now: number;
}): void {
  for (const step of params.store.listSteps(params.runtimeRunId)) {
    if (isTerminalStep(step)) {
      continue;
    }
    params.store.updateStep({
      runtimeRunId: params.runtimeRunId,
      stepId: step.stepId,
      status: params.status,
      recoveryState: "terminal",
      claimedBy: null,
      claimExpiresAt: null,
      heartbeatAt: null,
      completedAt: params.now,
      now: params.now,
    });
  }
}

function queueStepForRecovery(params: {
  store: DurableRuntimeStore;
  runtimeRunId: string;
  stepId?: string;
  expectedStatus: "waiting" | "retry_scheduled";
  expectedRecoveryState: "waiting_signal" | "waiting_timer" | "retry_scheduled";
  now: number;
}): number {
  let queued = 0;
  for (const step of params.store.listSteps(params.runtimeRunId)) {
    if (isTerminalStep(step)) {
      continue;
    }
    if (params.stepId && step.stepId !== params.stepId) {
      continue;
    }
    if (
      step.status !== params.expectedStatus ||
      step.recoveryState !== params.expectedRecoveryState
    ) {
      continue;
    }
    const updated = params.store.updateStep({
      runtimeRunId: params.runtimeRunId,
      stepId: step.stepId,
      status: "queued",
      recoveryState: "runnable",
      claimedBy: null,
      claimExpiresAt: null,
      heartbeatAt: null,
      now: params.now,
    });
    if (updated?.status === "queued" && updated.recoveryState === "runnable") {
      queued += 1;
    }
  }
  return queued;
}

function reconcileDueDurableTimer(params: {
  store: DurableRuntimeStore;
  timer: DurableRuntimeTimer;
  processInstanceId: string;
  now: number;
}): { fired: boolean; queued: boolean } {
  return params.store.withTransaction(() => {
    const fired = params.store.fireDueTimer({ timerId: params.timer.timerId, now: params.now });
    if (!fired) {
      return { fired: false, queued: false };
    }
    const run = params.store.getRun(fired.runtimeRunId);
    params.store.appendEvent({
      runtimeRunId: fired.runtimeRunId,
      eventType: "runtime.timer.fired",
      eventTime: params.now,
      stepId: fired.stepId,
      payload: {
        timerId: fired.timerId,
        timerType: fired.timerType,
        processInstanceId: params.processInstanceId,
      },
    });
    if (!run || isTerminalRun(run)) {
      return { fired: true, queued: false };
    }
    if (fired.timerType === "retry") {
      if (run.status !== "retry_scheduled" || run.recoveryState !== "retry_scheduled") {
        return { fired: true, queued: false };
      }
      const queuedSteps = queueStepForRecovery({
        store: params.store,
        runtimeRunId: fired.runtimeRunId,
        stepId: fired.stepId,
        expectedStatus: "retry_scheduled",
        expectedRecoveryState: "retry_scheduled",
        now: params.now,
      });
      if (queuedSteps === 0) {
        return { fired: true, queued: false };
      }
      const updatedRun = params.store.updateRun({
        runtimeRunId: fired.runtimeRunId,
        status: "queued",
        recoveryState: "runnable",
        now: params.now,
      });
      if (!updatedRun) {
        throw new Error(`durable retry run could not be queued: ${fired.runtimeRunId}`);
      }
      params.store.appendEvent({
        runtimeRunId: fired.runtimeRunId,
        eventType: "runtime.retry_due",
        eventTime: params.now,
        stepId: fired.stepId,
        payload: {
          timerId: fired.timerId,
          processInstanceId: params.processInstanceId,
        },
      });
      return { fired: true, queued: true };
    }
    if (run.status === "waiting_timer" && run.recoveryState === "waiting_timer") {
      const queuedSteps = queueStepForRecovery({
        store: params.store,
        runtimeRunId: fired.runtimeRunId,
        stepId: fired.stepId,
        expectedStatus: "waiting",
        expectedRecoveryState: "waiting_timer",
        now: params.now,
      });
      if (queuedSteps === 0) {
        return { fired: true, queued: false };
      }
      const updatedRun = params.store.updateRun({
        runtimeRunId: fired.runtimeRunId,
        status: "queued",
        recoveryState: "runnable",
        now: params.now,
      });
      if (!updatedRun) {
        throw new Error(`durable timer run could not be queued: ${fired.runtimeRunId}`);
      }
      params.store.appendEvent({
        runtimeRunId: fired.runtimeRunId,
        eventType: "runtime.timer.resume_queued",
        eventTime: params.now,
        stepId: fired.stepId,
        payload: {
          timerId: fired.timerId,
          timerType: fired.timerType,
          processInstanceId: params.processInstanceId,
        },
      });
      return { fired: true, queued: true };
    }
    return { fired: true, queued: false };
  });
}

export function reconcileDueDurableTimers(params: {
  store: DurableRuntimeStore;
  processInstanceId: string;
  now: number;
  limit?: number;
}): DurableRecoveryResult {
  const timers = params.store.listDueTimers(params.now, { limit: params.limit ?? 500 });
  let firedTimers = 0;
  let queuedRuns = 0;
  for (const timer of timers) {
    const reconciled = reconcileDueDurableTimer({ ...params, timer });
    if (reconciled.fired) {
      firedTimers += 1;
    }
    if (reconciled.queued) {
      queuedRuns += 1;
    }
  }
  return { scanned: timers.length, markedLost: 0, firedTimers, queuedRuns };
}

function reconcilePendingDurableSignal(params: {
  store: DurableRuntimeStore;
  signal: DurableRuntimeSignal;
  processInstanceId: string;
  now: number;
}): { consumed: boolean; queued: boolean } {
  return params.store.withTransaction(() => {
    const { signal } = params;
    const run = params.store.getRun(signal.runtimeRunId);
    if (!run || isTerminalRun(run)) {
      if (!params.store.consumePendingSignal({ signalId: signal.signalId, now: params.now })) {
        return { consumed: false, queued: false };
      }
      return { consumed: true, queued: false };
    }
    if (signal.signalType === "cancel") {
      if (!params.store.consumePendingSignal({ signalId: signal.signalId, now: params.now })) {
        return { consumed: false, queued: false };
      }
      markOpenStepsTerminal({
        store: params.store,
        runtimeRunId: run.runtimeRunId,
        status: "cancelled",
        now: params.now,
      });
      params.store.updateRun({
        runtimeRunId: run.runtimeRunId,
        status: "cancelled",
        recoveryState: "terminal",
        completedAt: params.now,
        now: params.now,
      });
      params.store.appendEvent({
        runtimeRunId: run.runtimeRunId,
        eventType: "runtime.signal.cancelled",
        eventTime: params.now,
        correlationId: signal.correlationId,
        payload: {
          signalId: signal.signalId,
          processInstanceId: params.processInstanceId,
        },
      });
      return { consumed: true, queued: false };
    }
    const runIsWaitingForSignal =
      run.status === "waiting_signal" && run.recoveryState === "waiting_signal";
    if (signal.signalType === "resume" || runIsWaitingForSignal) {
      if (!params.store.consumePendingSignal({ signalId: signal.signalId, now: params.now })) {
        return { consumed: false, queued: false };
      }
      if (!runIsWaitingForSignal) {
        return { consumed: true, queued: false };
      }
      const queuedSteps = queueStepForRecovery({
        store: params.store,
        runtimeRunId: run.runtimeRunId,
        stepId: signal.stepId,
        expectedStatus: "waiting",
        expectedRecoveryState: "waiting_signal",
        now: params.now,
      });
      if (queuedSteps === 0) {
        return { consumed: true, queued: false };
      }
      const updatedRun = params.store.updateRun({
        runtimeRunId: run.runtimeRunId,
        status: "queued",
        recoveryState: "runnable",
        now: params.now,
      });
      if (!updatedRun) {
        throw new Error(`durable signal run could not be queued: ${run.runtimeRunId}`);
      }
      params.store.appendEvent({
        runtimeRunId: run.runtimeRunId,
        eventType: "runtime.signal.resume_queued",
        eventTime: params.now,
        correlationId: signal.correlationId,
        payload: {
          signalId: signal.signalId,
          signalType: signal.signalType,
          processInstanceId: params.processInstanceId,
        },
      });
      return { consumed: true, queued: true };
    }
    return { consumed: false, queued: false };
  });
}

export function reconcilePendingDurableSignals(params: {
  store: DurableRuntimeStore;
  processInstanceId: string;
  now: number;
  limit?: number;
}): DurableRecoveryResult {
  const signals = params.store.listPendingSignals({ limit: params.limit ?? 5000 });
  let consumedSignals = 0;
  let queuedRuns = 0;
  for (const signal of signals) {
    const reconciled = reconcilePendingDurableSignal({ ...params, signal });
    if (reconciled.consumed) {
      consumedSignals += 1;
    }
    if (reconciled.queued) {
      queuedRuns += 1;
    }
  }
  return { scanned: signals.length, markedLost: 0, consumedSignals, queuedRuns };
}

export function startDurableRecoveryWorker(params: {
  processInstanceId: string;
  env?: NodeJS.ProcessEnv;
}): () => Promise<void> {
  const env = params.env ?? process.env;
  if (!isDurableWorkerEnabled()) {
    return async () => {};
  }
  const pollIntervalMs = resolveDurableWorkerPollIntervalMs();
  const claimTtlMs = resolveDurableWorkerClaimTtlMs();
  const staleAfterMs = resolveDurableStaleRuntimeRunAfterMs();
  const staleScanIntervalMs = Math.max(DEFAULT_STALE_SCAN_INTERVAL_MS, pollIntervalMs);
  let running = false;
  let stopped = false;
  let nextStaleScanAt = 0;
  let staleAgentTurnCursor: string | undefined;
  let staleChatSendCursor: string | undefined;
  let wakeOverdueScanCursor: string | undefined;
  const idleWaiters = new Set<() => void>();

  const reconcileOnce = async () => {
    if (running || stopped) {
      return;
    }
    running = true;
    let store: DurableRuntimeStore | null = null;
    try {
      store = openDurableRuntimeStore({ env });
      const now = Date.now();
      const shouldScanStaleRuns = now >= nextStaleScanAt;
      const result = shouldScanStaleRuns
        ? reconcileStaleDurableAgentTurns({
            store,
            processInstanceId: params.processInstanceId,
            now,
            staleAfterMs,
            ...(staleAgentTurnCursor ? { cursor: staleAgentTurnCursor } : {}),
          })
        : { scanned: 0, markedLost: 0, complete: false };
      const chatSendResult = shouldScanStaleRuns
        ? reconcileStaleDurableChatSends({
            store,
            processInstanceId: params.processInstanceId,
            now,
            staleAfterMs,
            ...(staleChatSendCursor ? { cursor: staleChatSendCursor } : {}),
          })
        : { scanned: 0, markedLost: 0, complete: false };
      if (shouldScanStaleRuns) {
        staleAgentTurnCursor = result.complete ? undefined : result.nextCursor;
        staleChatSendCursor = chatSendResult.complete ? undefined : chatSendResult.nextCursor;
        nextStaleScanAt =
          now + (result.complete && chatSendResult.complete ? staleScanIntervalMs : pollIntervalMs);
      }
      const timerResult = reconcileDueDurableTimers({
        store,
        processInstanceId: params.processInstanceId,
        now,
      });
      const signalResult = reconcilePendingDurableSignals({
        store,
        processInstanceId: params.processInstanceId,
        now,
      });
      const wakeResult = await runDurableWakeDispatcherOnce({
        store,
        workerId: params.processInstanceId,
        claimTtlMs,
        reconcileOwnerFacts: shouldScanStaleRuns,
        ownerFactReconciliationKey: resolveDurableRuntimeSqlitePath(env),
        ...(wakeOverdueScanCursor ? { wakeOverdueScanCursor } : {}),
      });
      wakeOverdueScanCursor = wakeResult.wakeOverdueNextCursor;
      await recoverDurableSessionAttentionDeliveries({ log });
      recordDurableRuntimeHealthSuccess();
      if (
        result.markedLost > 0 ||
        chatSendResult.markedLost > 0 ||
        (timerResult.firedTimers ?? 0) > 0 ||
        (signalResult.consumedSignals ?? 0) > 0 ||
        wakeResult.obligationsCreated > 0 ||
        wakeResult.acknowledged > 0 ||
        wakeResult.handoffAccepted > 0 ||
        wakeResult.reconciliationConflicts > 0 ||
        wakeResult.suspended > 0 ||
        wakeResult.overdue > 0
      ) {
        log.warn("reconciled durable runtime state", {
          staleScanned: result.scanned,
          markedLost: result.markedLost,
          staleChatSendsScanned: chatSendResult.scanned,
          markedLostChatSends: chatSendResult.markedLost,
          firedTimers: timerResult.firedTimers ?? 0,
          consumedSignals: signalResult.consumedSignals ?? 0,
          queuedRuns: (timerResult.queuedRuns ?? 0) + (signalResult.queuedRuns ?? 0),
          ownerFactsScanned: wakeResult.ownerFactsScanned,
          wakeObligationsCreated: wakeResult.obligationsCreated,
          wakeClaims: wakeResult.claimed,
          wakesAcknowledged: wakeResult.acknowledged,
          wakeHandoffsAccepted: wakeResult.handoffAccepted,
          wakeReconciliationConflicts: wakeResult.reconciliationConflicts,
          wakesFailed: wakeResult.failed,
          wakesSuspended: wakeResult.suspended,
          wakesOverdue: wakeResult.overdue,
          staleAfterMs,
        });
      }
    } catch (err) {
      recordDurableRuntimeHealthFailure({
        component: "recovery",
        operation: "reconcile_once",
        error: err,
      });
      log.warn(`durable recovery worker failed: ${String(err)}`);
    } finally {
      store?.close();
      running = false;
      for (const resolve of idleWaiters) {
        resolve();
      }
      idleWaiters.clear();
    }
  };

  const timer = setInterval(() => {
    void reconcileOnce();
  }, pollIntervalMs);
  timer.unref?.();
  void reconcileOnce();
  log.info("started durable recovery worker", {
    pollIntervalMs,
    claimTtlMs,
    staleAfterMs,
    staleScanIntervalMs,
    processInstanceId: params.processInstanceId,
  });

  return async () => {
    stopped = true;
    clearInterval(timer);
    if (running) {
      await new Promise<void>((resolve) => {
        idleWaiters.add(resolve);
      });
    }
  };
}
