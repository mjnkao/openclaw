// Durable restart interruption reconciliation for approved gateway restarts.
import { isDurableRuntimesEnabled } from "./config.js";
import { reconcileDurableFanIn, type DurableFanInPolicy } from "./fan-in.js";
import { DURABLE_SUBAGENT_RUN_OPERATION_KIND } from "./runtime-ids.js";
import { openDurableRuntimeStore } from "./store-factory.js";
import type {
  DurableRuntimeRun,
  DurableRuntimeStep,
  DurableRuntimeStore,
  DurableRuntimeRunStatus,
} from "./types.js";

export const DURABLE_GATEWAY_RESTART_OPERATION_KIND = "openclaw.gateway.restart";

export type DurableGatewayRestartInterruptionResult = {
  enabled: boolean;
  restartRuntimeRunId?: string;
  inspectedRuns: number;
  interruptedRuns: number;
  interruptedChildren: number;
  reconciledParents: number;
};

const RESTART_INTERRUPTION_METADATA_KEY = "restartInterruption";

function isRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === "object" && !Array.isArray(value);
}

function isTerminalRunStatus(status: DurableRuntimeRunStatus): boolean {
  return (
    status === "succeeded" || status === "failed" || status === "cancelled" || status === "lost"
  );
}

function shouldInterruptForPlannedRestart(run: DurableRuntimeRun): boolean {
  if (isTerminalRunStatus(run.status)) {
    return false;
  }
  if (run.operationKind === DURABLE_GATEWAY_RESTART_OPERATION_KIND) {
    return false;
  }
  return run.status === "received" || run.status === "queued" || run.status === "running";
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

function compactRestartMetadata(params: {
  restartRuntimeRunId: string;
  reason?: string;
  sessionKey?: string;
  now: number;
}): Record<string, unknown> {
  return {
    state: "unknown_after_side_effect",
    severity: "warning",
    reportable: true,
    retryable: false,
    reason: params.reason ?? "planned_gateway_restart",
    message:
      "Runtime run was interrupted by an approved gateway restart; side effects may have completed and should be reconciled before retry.",
    nextAction: "inspect_timeline_then_resume_or_reconcile",
    safeRecoveryActions: ["inspect_timeline", "resume_parent", "reconcile_side_effects"],
    restartRuntimeRunId: params.restartRuntimeRunId,
    ...(params.sessionKey ? { sessionKey: params.sessionKey } : {}),
    detectedAt: params.now,
  };
}

function mergeRestartMetadata(
  metadata: Record<string, unknown> | undefined,
  interruption: Record<string, unknown>,
): Record<string, unknown> {
  return {
    ...(metadata ?? {}),
    recoveryDiagnostic: interruption,
    [RESTART_INTERRUPTION_METADATA_KEY]: interruption,
  };
}

function isDurableFanInPolicy(value: unknown): value is DurableFanInPolicy {
  return (
    value === "all_succeeded" ||
    value === "all_terminal" ||
    value === "first_success" ||
    value === "continue_on_child_failure" ||
    value === "fail_parent_on_child_failure"
  );
}

function fanInPolicyForParentStep(params: {
  store: DurableRuntimeStore;
  parentRuntimeRunId: string;
  parentStepId: string;
}): DurableFanInPolicy {
  const parentStep = params.store
    .listSteps(params.parentRuntimeRunId)
    .find((step) => step.stepId === params.parentStepId);
  const policy = isRecord(parentStep?.metadata) ? parentStep.metadata.policy : undefined;
  return isDurableFanInPolicy(policy) ? policy : "continue_on_child_failure";
}

function markOpenStepsInterrupted(params: {
  store: DurableRuntimeStore;
  run: DurableRuntimeRun;
  now: number;
  interruption: Record<string, unknown>;
}): void {
  for (const step of params.store.listSteps(params.run.runtimeRunId)) {
    if (isTerminalStep(step)) {
      continue;
    }
    params.store.updateStep({
      runtimeRunId: params.run.runtimeRunId,
      stepId: step.stepId,
      status: "lost",
      recoveryState: "unknown_after_side_effect",
      claimedBy: null,
      claimExpiresAt: null,
      heartbeatAt: null,
      completedAt: params.now,
      metadata: mergeRestartMetadata(step.metadata, params.interruption),
      now: params.now,
    });
  }
}

function markParentLinksInterrupted(params: {
  store: DurableRuntimeStore;
  run: DurableRuntimeRun;
  now: number;
  interruption: Record<string, unknown>;
}): number {
  let reconciledParents = 0;
  for (const link of params.store.listParentLinks(params.run.runtimeRunId)) {
    if (link.status === "succeeded" || link.status === "failed" || link.status === "cancelled") {
      continue;
    }
    params.store.updateLink({
      parentRuntimeRunId: link.parentRuntimeRunId,
      parentStepId: link.parentStepId,
      childRuntimeRunId: link.childRuntimeRunId,
      status: "lost",
      metadata: {
        ...(isRecord(link.metadata) ? link.metadata : {}),
        terminalOutcome: "unknown_after_side_effect",
        recoveryDiagnostic: params.interruption,
      },
      now: params.now,
    });
    params.store.appendEvent({
      runtimeRunId: link.parentRuntimeRunId,
      eventType: "subagent.child.restart_interrupted",
      eventTime: params.now,
      stepId: link.parentStepId,
      agentInvocationId: params.run.idempotencyKey,
      correlationId: params.run.sourceRef,
      payload: {
        childRuntimeRunId: params.run.runtimeRunId,
        recoveryDiagnostic: params.interruption,
      },
    });
    reconcileDurableFanIn({
      store: params.store,
      parentRuntimeRunId: link.parentRuntimeRunId,
      parentStepId: link.parentStepId,
      policy: fanInPolicyForParentStep({
        store: params.store,
        parentRuntimeRunId: link.parentRuntimeRunId,
        parentStepId: link.parentStepId,
      }),
      now: params.now,
    });
    reconciledParents += 1;
  }
  return reconciledParents;
}

function recordRestartRun(params: {
  store: DurableRuntimeStore;
  reason?: string;
  sessionKey?: string;
  now: number;
}): DurableRuntimeRun {
  const run = params.store.createRun({
    operationKind: DURABLE_GATEWAY_RESTART_OPERATION_KIND,
    operationVersion: "1",
    status: "succeeded",
    recoveryState: "terminal",
    sourceType: "gateway",
    sourceRef: params.sessionKey ?? "gateway",
    idempotencyKey: `gateway-restart:${params.now}:${params.sessionKey ?? "gateway"}`,
    metadata: {
      reason: params.reason,
      sessionKey: params.sessionKey,
      approved: true,
      planned: true,
    },
    completedAt: params.now,
    now: params.now,
  });
  params.store.appendEvent({
    runtimeRunId: run.runtimeRunId,
    eventType: "gateway.restart.approved",
    eventTime: params.now,
    correlationId: params.sessionKey,
    payload: {
      reason: params.reason,
      sessionKey: params.sessionKey,
      approved: true,
      planned: true,
    },
  });
  return run;
}

export function recordDurableGatewayRestartInterruption(params: {
  reason?: string;
  sessionKey?: string;
  env?: NodeJS.ProcessEnv;
  now?: number;
}): DurableGatewayRestartInterruptionResult {
  const env = params.env ?? process.env;
  if (!isDurableRuntimesEnabled(env)) {
    return {
      enabled: false,
      inspectedRuns: 0,
      interruptedRuns: 0,
      interruptedChildren: 0,
      reconciledParents: 0,
    };
  }

  const now = params.now ?? Date.now();
  const store = openDurableRuntimeStore({ env });
  try {
    const restartRun = recordRestartRun({
      store,
      reason: params.reason,
      sessionKey: params.sessionKey,
      now,
    });
    const openRuns = store.listOpenRuns({ limit: 5000 });
    let interruptedRuns = 0;
    let interruptedChildren = 0;
    let reconciledParents = 0;

    for (const run of openRuns) {
      if (!shouldInterruptForPlannedRestart(run)) {
        continue;
      }
      const interruption = compactRestartMetadata({
        restartRuntimeRunId: restartRun.runtimeRunId,
        reason: params.reason,
        sessionKey: params.sessionKey,
        now,
      });
      store.updateRun({
        runtimeRunId: run.runtimeRunId,
        status: "unknown_after_side_effect",
        recoveryState: "unknown_after_side_effect",
        claimedBy: null,
        claimExpiresAt: null,
        heartbeatAt: null,
        metadata: mergeRestartMetadata(run.metadata, interruption),
        now,
      });
      markOpenStepsInterrupted({ store, run, now, interruption });
      store.appendEvent({
        runtimeRunId: run.runtimeRunId,
        eventType: "gateway.restart.interrupted",
        eventTime: now,
        agentInvocationId: run.idempotencyKey,
        correlationId: run.sourceRef ?? params.sessionKey,
        payload: {
          restartRuntimeRunId: restartRun.runtimeRunId,
          reason: params.reason,
          sessionKey: params.sessionKey,
          previousStatus: run.status,
          previousRecoveryState: run.recoveryState,
          recoveryDiagnostic: interruption,
        },
      });
      interruptedRuns += 1;
      if (run.operationKind === DURABLE_SUBAGENT_RUN_OPERATION_KIND) {
        interruptedChildren += 1;
        reconciledParents += markParentLinksInterrupted({
          store,
          run,
          now,
          interruption,
        });
      }
    }

    return {
      enabled: true,
      restartRuntimeRunId: restartRun.runtimeRunId,
      inspectedRuns: openRuns.length,
      interruptedRuns,
      interruptedChildren,
      reconciledParents,
    };
  } finally {
    store.close();
  }
}

export function buildDefaultDurableRestartContinuationMessage(): string {
  return [
    "Gateway restarted after an approved restart.",
    "Inspect this session and durable coordination state, then continue the interrupted work or report what still needs human/operator action.",
    "Do not repeat side-effecting steps unless the durable timeline or external evidence shows they did not complete.",
  ].join(" ");
}

export function isDurableGatewayRestartContinuationUseful(params: {
  sessionKey?: string;
  continuationMessage?: string | null;
  env?: NodeJS.ProcessEnv;
}): boolean {
  return Boolean(
    params.sessionKey?.trim() &&
    !params.continuationMessage?.trim() &&
    isDurableRuntimesEnabled(params.env ?? process.env),
  );
}

export const testing = {
  shouldInterruptForPlannedRestart,
};
