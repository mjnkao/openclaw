// Reconciliation helpers for internal agent-turn continuations.
import {
  DURABLE_AGENT_TURN_OPERATION_KIND,
  DURABLE_SUBAGENT_RUN_OPERATION_KIND,
} from "./runtime-ids.js";
import type { DurableRuntimeRun, DurableRuntimeRunStatus, DurableRuntimeStore } from "./types.js";

export type CloseSupersededAgentTurnContinuationsResult = {
  scanned: number;
  closed: number;
};

function isRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === "object" && !Array.isArray(value);
}

function runSessionKey(run: DurableRuntimeRun): string | undefined {
  const metadata = isRecord(run.metadata) ? run.metadata : {};
  const metadataSessionKey = metadata.sessionKey;
  if (typeof metadataSessionKey === "string" && metadataSessionKey.trim()) {
    return metadataSessionKey.trim();
  }
  if (typeof run.sourceRef === "string" && run.sourceRef.trim()) {
    return run.sourceRef.trim();
  }
  return undefined;
}

function isTerminalRunStatus(status: DurableRuntimeRunStatus): boolean {
  return (
    status === "succeeded" || status === "failed" || status === "cancelled" || status === "lost"
  );
}

function isSubagentAnnounceIdempotencyKey(value: string | undefined): value is string {
  return typeof value === "string" && value.startsWith("announce:v1:");
}

function parseSubagentAnnounceRunId(value: string | undefined): string | undefined {
  if (!isSubagentAnnounceIdempotencyKey(value)) {
    return undefined;
  }
  const lastSeparator = value.lastIndexOf(":");
  const prefixLength = "announce:v1:".length;
  if (lastSeparator <= prefixLength || lastSeparator >= value.length - 1) {
    return undefined;
  }
  return value.slice(lastSeparator + 1);
}

function closeContinuation(params: {
  store: DurableRuntimeStore;
  run: DurableRuntimeRun;
  parentRuntimeRunId: string;
  reason: string;
  processInstanceId?: string;
  now: number;
}): void {
  const metadata = {
    ...(isRecord(params.run.metadata) ? params.run.metadata : {}),
    supersededContinuation: {
      parentRuntimeRunId: params.parentRuntimeRunId,
      reason: params.reason,
      processInstanceId: params.processInstanceId,
      closedAt: params.now,
    },
  };
  params.store.updateRun({
    runtimeRunId: params.run.runtimeRunId,
    status: "succeeded",
    recoveryState: "terminal",
    completedAt: params.now,
    metadata,
    now: params.now,
  });
  params.store.updateStep({
    runtimeRunId: params.run.runtimeRunId,
    stepId: "agent_invocation",
    status: "succeeded",
    recoveryState: "terminal",
    completedAt: params.now,
    metadata,
    now: params.now,
  });
  params.store.appendEvent({
    runtimeRunId: params.run.runtimeRunId,
    eventType: "agent.turn.continuation_superseded",
    eventTime: params.now,
    stepId: "agent_invocation",
    agentInvocationId: params.run.idempotencyKey,
    idempotencyKey: `${params.run.runtimeRunId}:continuation-superseded:${params.parentRuntimeRunId}`,
    correlationId: runSessionKey(params.run),
    payload: {
      parentRuntimeRunId: params.parentRuntimeRunId,
      reason: params.reason,
      processInstanceId: params.processInstanceId,
    },
  });
}

export function closeSupersededAgentTurnContinuations(params: {
  store: DurableRuntimeStore;
  sessionKey: string;
  parentRuntimeRunId: string;
  excludeRuntimeRunId?: string;
  reason: string;
  processInstanceId?: string;
  now: number;
}): CloseSupersededAgentTurnContinuationsResult {
  let scanned = 0;
  let closed = 0;
  for (const run of params.store.listOpenRuns({
    operationKind: DURABLE_AGENT_TURN_OPERATION_KIND,
    limit: 5000,
  })) {
    if (run.runtimeRunId === params.excludeRuntimeRunId) {
      continue;
    }
    if (!isSubagentAnnounceIdempotencyKey(run.idempotencyKey)) {
      continue;
    }
    if (runSessionKey(run) !== params.sessionKey) {
      continue;
    }
    scanned += 1;
    closeContinuation({
      store: params.store,
      run,
      parentRuntimeRunId: params.parentRuntimeRunId,
      reason: params.reason,
      processInstanceId: params.processInstanceId,
      now: params.now,
    });
    closed += 1;
  }
  return { scanned, closed };
}

export function reconcileSupersededAgentTurnContinuations(params: {
  store: DurableRuntimeStore;
  processInstanceId?: string;
  now: number;
}): CloseSupersededAgentTurnContinuationsResult {
  const recentRuns = params.store.listRuns({ limit: 5000 });
  let scanned = 0;
  let closed = 0;
  for (const run of params.store.listOpenRuns({
    operationKind: DURABLE_AGENT_TURN_OPERATION_KIND,
    limit: 5000,
  })) {
    if (!isSubagentAnnounceIdempotencyKey(run.idempotencyKey)) {
      continue;
    }
    scanned += 1;
    const childRunId = parseSubagentAnnounceRunId(run.idempotencyKey);
    if (!childRunId) {
      continue;
    }
    const child = recentRuns.find(
      (candidate) =>
        candidate.operationKind === DURABLE_SUBAGENT_RUN_OPERATION_KIND &&
        candidate.idempotencyKey === childRunId,
    );
    const parentRuntimeRunId = child?.parentRuntimeRunId;
    if (!parentRuntimeRunId) {
      continue;
    }
    const parent = params.store.getRun(parentRuntimeRunId);
    if (!parent || !isTerminalRunStatus(parent.status)) {
      continue;
    }
    closeContinuation({
      store: params.store,
      run,
      parentRuntimeRunId,
      reason: "parent_terminal_reconciliation",
      processInstanceId: params.processInstanceId,
      now: params.now,
    });
    closed += 1;
  }
  return { scanned, closed };
}
