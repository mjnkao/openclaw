import type {
  DurableRuntimeRegistry,
  DurableRuntimeStepHandlerResult,
  DurableRuntimeStepSideEffectPolicy,
} from "./registry.js";
import type {
  DurableRuntimeRef,
  DurableRuntimeRun,
  DurableRuntimeStep,
  DurableRuntimeStepType,
  DurableRuntimeStore,
  UpdateDurableRuntimeStepInput,
} from "./types.js";

export type DurableExecutorRunOnceOptions = {
  store: DurableRuntimeStore;
  registry: DurableRuntimeRegistry;
  workerId: string;
  claimTtlMs?: number;
  operationKind?: string;
  stepType?: DurableRuntimeStepType;
  now?: () => number;
};

export type DurableExecutorRunOnceResult =
  | {
      claimed: false;
      reason: "no_runnable_step";
    }
  | {
      claimed: true;
      runtimeRunId: string;
      stepId: string;
      outcome:
        | DurableRuntimeStepHandlerResult["kind"]
        | "no_handler"
        | "handler_exception"
        | "claim_lost";
    };

const DEFAULT_CLAIM_TTL_MS = 5 * 60 * 1000;

function terminalRunStatus(run: DurableRuntimeRun): boolean {
  return (
    run.status === "succeeded" ||
    run.status === "failed" ||
    run.status === "cancelled" ||
    run.status === "lost"
  );
}

function createJsonRef(params: {
  store: DurableRuntimeStore;
  run: DurableRuntimeRun;
  step: DurableRuntimeStep;
  refKind: DurableRuntimeRef["refKind"];
  metadata: Record<string, unknown>;
  label: string;
  now: number;
}): DurableRuntimeRef {
  return params.store.createRef({
    runtimeRunId: params.run.runtimeRunId,
    stepId: params.step.stepId,
    refKind: params.refKind,
    mediaType: "application/json",
    storageKind: "inline",
    storageUri: `inline:durable-step:${params.step.stepId}:${params.label}`,
    metadata: params.metadata,
    now: params.now,
  });
}

function clearStepClaimFields(workerId: string): {
  claimedBy: null;
  claimExpiresAt: null;
  heartbeatAt: null;
} {
  void workerId;
  return {
    claimedBy: null,
    claimExpiresAt: null,
    heartbeatAt: null,
  };
}

function isStepClaimOwned(params: {
  store: DurableRuntimeStore;
  step: DurableRuntimeStep;
  workerId: string;
}): boolean {
  return params.store
    .listSteps(params.step.runtimeRunId)
    .some((step) => step.stepId === params.step.stepId && step.claimedBy === params.workerId);
}

function markClaimLost(params: {
  store: DurableRuntimeStore;
  run: DurableRuntimeRun;
  step: DurableRuntimeStep;
  workerId: string;
  now: number;
}): DurableExecutorRunOnceResult {
  params.store.appendEvent({
    runtimeRunId: params.run.runtimeRunId,
    eventType: "runtime.step.claim_lost",
    eventTime: params.now,
    stepId: params.step.stepId,
    payload: {
      stepType: params.step.stepType,
      workerId: params.workerId,
    },
  });
  return {
    claimed: true,
    runtimeRunId: params.run.runtimeRunId,
    stepId: params.step.stepId,
    outcome: "claim_lost",
  };
}

function updateOwnedStep(params: {
  store: DurableRuntimeStore;
  step: DurableRuntimeStep;
  workerId: string;
  input: Omit<UpdateDurableRuntimeStepInput, "runtimeRunId" | "stepId" | "expectedClaimedBy">;
}): DurableRuntimeStep | undefined {
  return params.store.updateStep({
    runtimeRunId: params.step.runtimeRunId,
    stepId: params.step.stepId,
    expectedClaimedBy: params.workerId,
    ...params.input,
  });
}

function recordExecutionUncertainty(params: {
  store: DurableRuntimeStore;
  run: DurableRuntimeRun;
  step: DurableRuntimeStep;
  kind: "unknown_after_side_effect" | "requires_owner_decision";
  reason: "side_effect_uncertain" | "no_handler";
  detail?: string;
  now: number;
}): void {
  const sourceOwner = params.run.sourceOwner ?? "durable_execution_records";
  const sourceRef = params.run.sourceRef ?? params.run.runtimeRunId;
  const dedupeKey = `${params.reason}:${params.run.runtimeRunId}:${params.step.stepId}:${params.step.attempt}`;
  const fact = params.store.recordUncertaintyFact({
    sourceOwner,
    sourceRef,
    kind: params.kind,
    sourceRunId: params.run.runtimeRunId,
    stepId: params.step.stepId,
    dedupeKey,
    facts: {
      reason: params.reason,
      detail: params.detail,
      attempt: params.step.attempt,
    },
    now: params.now,
  });
  params.store.createWakeObligation({
    sourceOwner,
    sourceRef,
    parentRunId: params.run.parentRuntimeRunId,
    targetKind: "run",
    targetRef: params.run.runtimeRunId,
    ownerKind: "run",
    ownerRef: params.run.runtimeRunId,
    targetResolutionStatus: "resolved",
    targetResolutionReason: "durable execution record owns the blocked step",
    reason: params.reason,
    factsRef: `uncertainty_facts:${fact.factId}`,
    sourceRunId: params.run.runtimeRunId,
    dedupeKey: `wake:${dedupeKey}`,
    metadata: {
      stepId: params.step.stepId,
      detail: params.detail,
    },
    now: params.now,
  });
}

function markNoHandler(params: {
  store: DurableRuntimeStore;
  run: DurableRuntimeRun;
  step: DurableRuntimeStep;
  workerId: string;
  now: number;
}): DurableExecutorRunOnceResult {
  const updatedStep = updateOwnedStep({
    store: params.store,
    step: params.step,
    workerId: params.workerId,
    input: {
      status: "waiting",
      recoveryState: "requires_owner_decision",
      ...clearStepClaimFields(params.workerId),
      now: params.now,
    },
  });
  if (!updatedStep) {
    return markClaimLost(params);
  }
  params.store.updateRun({
    runtimeRunId: params.run.runtimeRunId,
    status: "blocked",
    recoveryState: "requires_owner_decision",
    heartbeatAt: null,
    now: params.now,
  });
  params.store.appendEvent({
    runtimeRunId: params.run.runtimeRunId,
    eventType: "runtime.step.no_handler",
    eventTime: params.now,
    stepId: params.step.stepId,
    payload: {
      stepType: params.step.stepType,
      workerId: params.workerId,
    },
  });
  recordExecutionUncertainty({
    store: params.store,
    run: params.run,
    step: params.step,
    kind: "requires_owner_decision",
    reason: "no_handler",
    detail: `No handler is registered for step type ${params.step.stepType}`,
    now: params.now,
  });
  return {
    claimed: true,
    runtimeRunId: params.run.runtimeRunId,
    stepId: params.step.stepId,
    outcome: "no_handler",
  };
}

function markHandlerException(params: {
  store: DurableRuntimeStore;
  run: DurableRuntimeRun;
  step: DurableRuntimeStep;
  workerId: string;
  now: number;
  err: unknown;
  sideEffectPolicy: DurableRuntimeStepSideEffectPolicy;
}): DurableExecutorRunOnceResult {
  if (!isStepClaimOwned(params)) {
    return markClaimLost(params);
  }
  const errorRef = createJsonRef({
    store: params.store,
    run: params.run,
    step: params.step,
    refKind: "error",
    label: "handler-exception",
    metadata: { message: String(params.err) },
    now: params.now,
  });
  const sideEffectUncertain =
    (params.sideEffectPolicy === "non_idempotent" || params.sideEffectPolicy === "unknown") &&
    !params.step.idempotencyKey;
  if (sideEffectUncertain) {
    const updatedStep = updateOwnedStep({
      store: params.store,
      step: params.step,
      workerId: params.workerId,
      input: {
        status: "waiting",
        recoveryState: "unknown_after_side_effect",
        errorRef: errorRef.refId,
        ...clearStepClaimFields(params.workerId),
        now: params.now,
      },
    });
    if (!updatedStep) {
      return markClaimLost(params);
    }
    params.store.updateRun({
      runtimeRunId: params.run.runtimeRunId,
      status: "waiting",
      recoveryState: "unknown_after_side_effect",
      heartbeatAt: null,
      now: params.now,
    });
    params.store.appendEvent({
      runtimeRunId: params.run.runtimeRunId,
      eventType: "runtime.step.handler_exception_unknown_side_effect",
      eventTime: params.now,
      stepId: params.step.stepId,
      payload: {
        errorRef: errorRef.refId,
        sideEffectPolicy: params.sideEffectPolicy,
        workerId: params.workerId,
      },
    });
    recordExecutionUncertainty({
      store: params.store,
      run: params.run,
      step: params.step,
      kind: "unknown_after_side_effect",
      reason: "side_effect_uncertain",
      detail: String(params.err),
      now: params.now,
    });
    return {
      claimed: true,
      runtimeRunId: params.run.runtimeRunId,
      stepId: params.step.stepId,
      outcome: "handler_exception",
    };
  }
  const updatedStep = updateOwnedStep({
    store: params.store,
    step: params.step,
    workerId: params.workerId,
    input: {
      status: "failed",
      recoveryState: "terminal",
      errorRef: errorRef.refId,
      completedAt: params.now,
      ...clearStepClaimFields(params.workerId),
      now: params.now,
    },
  });
  if (!updatedStep) {
    return markClaimLost(params);
  }
  params.store.updateRun({
    runtimeRunId: params.run.runtimeRunId,
    status: "failed",
    recoveryState: "terminal",
    completedAt: params.now,
    heartbeatAt: null,
    now: params.now,
  });
  params.store.appendEvent({
    runtimeRunId: params.run.runtimeRunId,
    eventType: "runtime.step.handler_exception",
    eventTime: params.now,
    stepId: params.step.stepId,
    payload: {
      errorRef: errorRef.refId,
      workerId: params.workerId,
    },
  });
  return {
    claimed: true,
    runtimeRunId: params.run.runtimeRunId,
    stepId: params.step.stepId,
    outcome: "handler_exception",
  };
}

function applyStepResult(params: {
  store: DurableRuntimeStore;
  run: DurableRuntimeRun;
  step: DurableRuntimeStep;
  workerId: string;
  now: number;
  result: DurableRuntimeStepHandlerResult;
  sideEffectPolicy: DurableRuntimeStepSideEffectPolicy;
}): DurableExecutorRunOnceResult {
  const { store, run, step, workerId, now, result, sideEffectPolicy } = params;
  if (!isStepClaimOwned({ store, step, workerId })) {
    return markClaimLost({ store, run, step, workerId, now });
  }

  if (result.kind === "succeeded") {
    const outputRef =
      result.outputRef ??
      (result.output
        ? createJsonRef({
            store,
            run,
            step,
            refKind: "output",
            label: "output",
            metadata: result.output,
            now,
          }).refId
        : undefined);
    const updatedStep = updateOwnedStep({
      store,
      step,
      workerId,
      input: {
        status: "succeeded",
        recoveryState: "terminal",
        outputRef,
        checkpointRef: result.checkpointRef ?? null,
        completedAt: now,
        ...clearStepClaimFields(workerId),
        now,
      },
    });
    if (!updatedStep) {
      return markClaimLost({ store, run, step, workerId, now });
    }
    store.appendEvent({
      runtimeRunId: run.runtimeRunId,
      eventType: "runtime.step.succeeded",
      eventTime: now,
      stepId: step.stepId,
      payload: { outputRef, workerId },
    });
    store.updateRun(
      result.completeRun
        ? {
            runtimeRunId: run.runtimeRunId,
            status: "succeeded",
            recoveryState: "terminal",
            checkpointRef: result.checkpointRef ?? null,
            completedAt: now,
            heartbeatAt: null,
            now,
          }
        : {
            runtimeRunId: run.runtimeRunId,
            status: "queued",
            recoveryState: "runnable",
            checkpointRef: result.checkpointRef ?? undefined,
            heartbeatAt: null,
            now,
          },
    );
    return {
      claimed: true,
      runtimeRunId: run.runtimeRunId,
      stepId: step.stepId,
      outcome: result.kind,
    };
  }

  if (result.kind === "failed") {
    const errorRef = createJsonRef({
      store,
      run,
      step,
      refKind: "error",
      label: "error",
      metadata: result.error ?? { message: "durable step failed" },
      now,
    });
    const retryRequested = Boolean(
      result.retryAfterMs && (!step.maxAttempts || step.attempt < step.maxAttempts),
    );
    const retrySafe =
      sideEffectPolicy === "none" ||
      sideEffectPolicy === "idempotent" ||
      Boolean(step.idempotencyKey);
    const retryAllowed = retryRequested && retrySafe;
    if (retryRequested && !retrySafe) {
      const updatedStep = updateOwnedStep({
        store,
        step,
        workerId,
        input: {
          status: "waiting",
          recoveryState: "unknown_after_side_effect",
          errorRef: errorRef.refId,
          checkpointRef: result.checkpointRef ?? null,
          ...clearStepClaimFields(workerId),
          now,
        },
      });
      if (!updatedStep) {
        return markClaimLost({ store, run, step, workerId, now });
      }
      store.updateRun({
        runtimeRunId: run.runtimeRunId,
        status: "waiting",
        recoveryState: "unknown_after_side_effect",
        checkpointRef: result.checkpointRef ?? undefined,
        heartbeatAt: null,
        now,
      });
      store.appendEvent({
        runtimeRunId: run.runtimeRunId,
        eventType: "runtime.step.retry_blocked_unknown_side_effect",
        eventTime: now,
        stepId: step.stepId,
        payload: {
          errorRef: errorRef.refId,
          retryAfterMs: result.retryAfterMs,
          sideEffectPolicy,
          workerId,
        },
      });
      recordExecutionUncertainty({
        store,
        run,
        step,
        kind: "unknown_after_side_effect",
        reason: "side_effect_uncertain",
        detail: `Retry blocked for ${sideEffectPolicy} step without an idempotency key`,
        now,
      });
      return {
        claimed: true,
        runtimeRunId: run.runtimeRunId,
        stepId: step.stepId,
        outcome: "unknown_after_side_effect",
      };
    }
    if (retryAllowed) {
      const timer = store.createTimer({
        runtimeRunId: run.runtimeRunId,
        stepId: step.stepId,
        timerType: "retry",
        dueAt: now + Math.max(0, Math.trunc(result.retryAfterMs ?? 0)),
        metadata: { errorRef: errorRef.refId },
        now,
      });
      const updatedStep = updateOwnedStep({
        store,
        step,
        workerId,
        input: {
          status: "retry_scheduled",
          recoveryState: "retry_scheduled",
          attempt: step.attempt + 1,
          errorRef: errorRef.refId,
          checkpointRef: result.checkpointRef ?? null,
          ...clearStepClaimFields(workerId),
          now,
        },
      });
      if (!updatedStep) {
        return markClaimLost({ store, run, step, workerId, now });
      }
      store.updateRun({
        runtimeRunId: run.runtimeRunId,
        status: "retry_scheduled",
        recoveryState: "retry_scheduled",
        checkpointRef: result.checkpointRef ?? undefined,
        heartbeatAt: null,
        now,
      });
      store.appendEvent({
        runtimeRunId: run.runtimeRunId,
        eventType: "runtime.step.retry_scheduled",
        eventTime: now,
        stepId: step.stepId,
        payload: { errorRef: errorRef.refId, timerId: timer.timerId, workerId },
      });
      return {
        claimed: true,
        runtimeRunId: run.runtimeRunId,
        stepId: step.stepId,
        outcome: result.kind,
      };
    }
    const updatedStep = updateOwnedStep({
      store,
      step,
      workerId,
      input: {
        status: "failed",
        recoveryState: "terminal",
        errorRef: errorRef.refId,
        checkpointRef: result.checkpointRef ?? null,
        completedAt: now,
        ...clearStepClaimFields(workerId),
        now,
      },
    });
    if (!updatedStep) {
      return markClaimLost({ store, run, step, workerId, now });
    }
    if (result.completeRun !== false) {
      store.updateRun({
        runtimeRunId: run.runtimeRunId,
        status: "failed",
        recoveryState: "terminal",
        checkpointRef: result.checkpointRef ?? null,
        completedAt: now,
        heartbeatAt: null,
        now,
      });
    }
    store.appendEvent({
      runtimeRunId: run.runtimeRunId,
      eventType: "runtime.step.failed",
      eventTime: now,
      stepId: step.stepId,
      payload: { errorRef: errorRef.refId, workerId },
    });
    return {
      claimed: true,
      runtimeRunId: run.runtimeRunId,
      stepId: step.stepId,
      outcome: result.kind,
    };
  }

  if (result.kind === "waiting_signal") {
    const updatedStep = updateOwnedStep({
      store,
      step,
      workerId,
      input: {
        status: "waiting",
        recoveryState: "waiting_signal",
        checkpointRef: result.checkpointRef ?? null,
        ...clearStepClaimFields(workerId),
        now,
      },
    });
    if (!updatedStep) {
      return markClaimLost({ store, run, step, workerId, now });
    }
    store.updateRun({
      runtimeRunId: run.runtimeRunId,
      status: "waiting_signal",
      recoveryState: "waiting_signal",
      checkpointRef: result.checkpointRef ?? undefined,
      heartbeatAt: null,
      now,
    });
    store.appendEvent({
      runtimeRunId: run.runtimeRunId,
      eventType: "runtime.step.waiting_signal",
      eventTime: now,
      stepId: step.stepId,
      payload: { reason: result.reason, workerId },
    });
    return {
      claimed: true,
      runtimeRunId: run.runtimeRunId,
      stepId: step.stepId,
      outcome: result.kind,
    };
  }

  if (result.kind === "waiting_timer") {
    const timer = store.createTimer({
      runtimeRunId: run.runtimeRunId,
      stepId: step.stepId,
      timerType: result.timerType ?? "sleep",
      dueAt: result.dueAt,
      metadata: { reason: result.reason },
      now,
    });
    const updatedStep = updateOwnedStep({
      store,
      step,
      workerId,
      input: {
        status: "waiting",
        recoveryState: "waiting_timer",
        checkpointRef: result.checkpointRef ?? null,
        ...clearStepClaimFields(workerId),
        now,
      },
    });
    if (!updatedStep) {
      return markClaimLost({ store, run, step, workerId, now });
    }
    store.updateRun({
      runtimeRunId: run.runtimeRunId,
      status: "waiting_timer",
      recoveryState: "waiting_timer",
      checkpointRef: result.checkpointRef ?? undefined,
      heartbeatAt: null,
      now,
    });
    store.appendEvent({
      runtimeRunId: run.runtimeRunId,
      eventType: "runtime.step.waiting_timer",
      eventTime: now,
      stepId: step.stepId,
      payload: { timerId: timer.timerId, reason: result.reason, workerId },
    });
    return {
      claimed: true,
      runtimeRunId: run.runtimeRunId,
      stepId: step.stepId,
      outcome: result.kind,
    };
  }

  const updatedStep = updateOwnedStep({
    store,
    step,
    workerId,
    input: {
      status: "waiting",
      recoveryState: "unknown_after_side_effect",
      checkpointRef: result.checkpointRef ?? null,
      ...clearStepClaimFields(workerId),
      now,
    },
  });
  if (!updatedStep) {
    return markClaimLost({ store, run, step, workerId, now });
  }
  store.updateRun({
    runtimeRunId: run.runtimeRunId,
    status: "waiting",
    recoveryState: "unknown_after_side_effect",
    checkpointRef: result.checkpointRef ?? undefined,
    heartbeatAt: null,
    now,
  });
  store.appendEvent({
    runtimeRunId: run.runtimeRunId,
    eventType: "runtime.step.side_effect_uncertain",
    eventTime: now,
    stepId: step.stepId,
    payload: { reason: result.reason, workerId },
  });
  recordExecutionUncertainty({
    store,
    run,
    step,
    kind: "unknown_after_side_effect",
    reason: "side_effect_uncertain",
    detail: result.reason,
    now,
  });
  return {
    claimed: true,
    runtimeRunId: run.runtimeRunId,
    stepId: step.stepId,
    outcome: result.kind,
  };
}

export async function runDurableExecutorOnce(
  options: DurableExecutorRunOnceOptions,
): Promise<DurableExecutorRunOnceResult> {
  const now = options.now ?? (() => Date.now());
  const claimTtlMs = options.claimTtlMs ?? DEFAULT_CLAIM_TTL_MS;
  const claimTime = now();
  const step = options.store.claimNextRunnableStep({
    operationKind: options.operationKind,
    stepType: options.stepType,
    workerId: options.workerId,
    claimTtlMs,
    now: claimTime,
  });
  if (!step) {
    return { claimed: false, reason: "no_runnable_step" };
  }
  const claimToken = step.claimedBy!;
  const run = options.store.getRun(step.runtimeRunId);
  if (!run || terminalRunStatus(run)) {
    options.store.releaseStepClaim({
      runtimeRunId: step.runtimeRunId,
      stepId: step.stepId,
      workerId: claimToken,
      now: now(),
    });
    return { claimed: false, reason: "no_runnable_step" };
  }
  const registration = options.registry.getStepHandlerRegistration(step.stepType);
  const handler = registration?.handler;
  const startTime = now();
  const runningStep = updateOwnedStep({
    store: options.store,
    step,
    workerId: claimToken,
    input: {
      status: "running",
      recoveryState: "running",
      startedAt: step.startedAt ?? startTime,
      heartbeatAt: startTime,
      now: startTime,
    },
  });
  if (!runningStep) {
    return markClaimLost({
      store: options.store,
      run,
      step,
      workerId: claimToken,
      now: startTime,
    });
  }
  options.store.updateRun({
    runtimeRunId: run.runtimeRunId,
    status: "running",
    recoveryState: "running",
    heartbeatAt: startTime,
    now: startTime,
  });
  options.store.appendEvent({
    runtimeRunId: run.runtimeRunId,
    eventType: "runtime.step.running",
    eventTime: startTime,
    stepId: step.stepId,
    payload: {
      stepType: step.stepType,
      workerId: options.workerId,
      claimToken,
    },
  });
  if (!handler) {
    return markNoHandler({
      store: options.store,
      run,
      step,
      workerId: claimToken,
      now: now(),
    });
  }

  let claimLost = false;
  const heartbeat = (payload?: Record<string, unknown>): boolean => {
    const heartbeatAt = now();
    const heartbeatStep = options.store.renewStepClaim({
      runtimeRunId: step.runtimeRunId,
      stepId: step.stepId,
      workerId: claimToken,
      claimTtlMs,
      now: heartbeatAt,
    });
    if (!heartbeatStep) {
      if (!claimLost) {
        claimLost = true;
        options.store.appendEvent({
          runtimeRunId: run.runtimeRunId,
          eventType: "runtime.step.claim_lost",
          eventTime: heartbeatAt,
          stepId: step.stepId,
          payload: {
            phase: "heartbeat",
            stepType: step.stepType,
            workerId: options.workerId,
            claimToken,
          },
        });
      }
      return false;
    }
    options.store.updateRun({
      runtimeRunId: run.runtimeRunId,
      heartbeatAt,
      now: heartbeatAt,
    });
    options.store.appendEvent({
      runtimeRunId: run.runtimeRunId,
      eventType: "runtime.step.heartbeat",
      eventTime: heartbeatAt,
      stepId: step.stepId,
      payload: { ...payload, workerId: options.workerId, claimToken },
    });
    return true;
  };
  const heartbeatTimer = setInterval(
    () => heartbeat({ automatic: true }),
    Math.max(1, Math.floor(claimTtlMs / 3)),
  );
  heartbeatTimer.unref?.();
  try {
    const result = await handler({
      store: options.store,
      run,
      step,
      workerId: options.workerId,
      now,
      heartbeat: (payload?: Record<string, unknown>) => {
        heartbeat(payload);
      },
    });
    return applyStepResult({
      store: options.store,
      run,
      step,
      workerId: claimToken,
      now: now(),
      result,
      sideEffectPolicy: registration?.sideEffectPolicy ?? "unknown",
    });
  } catch (err) {
    return markHandlerException({
      store: options.store,
      run,
      step,
      workerId: claimToken,
      now: now(),
      err,
      sideEffectPolicy: registration?.sideEffectPolicy ?? "unknown",
    });
  } finally {
    clearInterval(heartbeatTimer);
  }
}
