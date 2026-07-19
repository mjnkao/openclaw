import { describe, expect, it } from "vitest";
import {
  buildDurableCoordinationProjection,
  buildDurableTaskFlowStateProjection,
  buildDurableWorkboardMetadataProjection,
  mergeDurableProjectionIntoJsonObject,
} from "./coordination-projection.js";
import type { DurableRuntimeLink, DurableRuntimeRun, DurableRuntimeStep } from "./types.js";

describe("durable coordination projection", () => {
  it("summarizes waiting child runs for taskflow and workboard consumers", () => {
    const run: DurableRuntimeRun = {
      runtimeRunId: "wfr_parent",
      operationKind: "openclaw.agent.turn",
      operationVersion: "1",
      status: "waiting_child",
      recoveryState: "waiting_child",
      sourceOwner: "session_store",
      sourceRef: "agent:bo:discord:channel:bo-main",
      workUnitId: "workboard:default:card-parent",
      reportRouteId: "discord:bo-main",
      heartbeatAt: 120,
      metadata: {
        taskId: "task_parent",
        taskFlowId: "flow_parent",
        agentId: "bo",
      },
      createdAt: 100,
      updatedAt: 150,
    };
    const steps: DurableRuntimeStep[] = [
      {
        runtimeRunId: run.runtimeRunId,
        stepId: "subagents",
        stepType: "fan_in",
        status: "waiting",
        recoveryState: "waiting_child",
        attempt: 1,
        createdAt: 110,
        updatedAt: 140,
      },
    ];
    const childLinks: DurableRuntimeLink[] = [
      {
        parentRuntimeRunId: run.runtimeRunId,
        parentStepId: "subagents",
        childRuntimeRunId: "wfr_child_1",
        linkType: "subagent",
        status: "succeeded",
        createdAt: 120,
        updatedAt: 130,
      },
      {
        parentRuntimeRunId: run.runtimeRunId,
        parentStepId: "subagents",
        childRuntimeRunId: "wfr_child_2",
        linkType: "subagent",
        status: "failed",
        createdAt: 121,
        updatedAt: 131,
      },
      {
        parentRuntimeRunId: run.runtimeRunId,
        parentStepId: "subagents",
        childRuntimeRunId: "wfr_child_3",
        linkType: "subagent",
        status: "running",
        createdAt: 122,
        updatedAt: 132,
      },
    ];

    const projection = buildDurableCoordinationProjection({ run, steps, childLinks });

    expect(projection).toMatchObject({
      runtimeRunId: "wfr_parent",
      workUnitId: "workboard:default:card-parent",
      reportRouteId: "discord:bo-main",
      status: "waiting_child",
      recoveryState: "waiting_child",
      currentStepId: "subagents",
      waitingReason: "child",
      external: {
        workUnitId: "workboard:default:card-parent",
        reportRouteId: "discord:bo-main",
        taskId: "task_parent",
        taskFlowId: "flow_parent",
        sessionKey: "agent:bo:discord:channel:bo-main",
        agentId: "bo",
      },
      children: {
        total: 3,
        succeeded: 1,
        failed: 1,
        running: 1,
        terminal: 2,
        open: 1,
      },
      controls: {
        canCancel: true,
        canResume: true,
        canOpenTimeline: true,
      },
    });

    expect(buildDurableTaskFlowStateProjection(projection)).toMatchObject({
      runtimeRunId: "wfr_parent",
      workUnitId: "workboard:default:card-parent",
      reportRouteId: "discord:bo-main",
      waitingReason: "child",
      children: { open: 1, failed: 1 },
    });
    expect(buildDurableWorkboardMetadataProjection(projection)).toMatchObject({
      runtimeRunId: "wfr_parent",
      workUnitId: "workboard:default:card-parent",
      reportRouteId: "discord:bo-main",
      taskId: "task_parent",
      taskFlowId: "flow_parent",
      timelineCommand: "openclaw durable timeline wfr_parent",
    });
    expect(
      mergeDurableProjectionIntoJsonObject(
        { existing: true },
        buildDurableTaskFlowStateProjection(projection),
      ),
    ).toMatchObject({
      existing: true,
      durable: {
        runtimeRunId: "wfr_parent",
      },
    });
  });

  it("exposes recovery diagnostics for lost runs without requiring a Workboard card", () => {
    const run: DurableRuntimeRun = {
      runtimeRunId: "wfr_lost",
      operationKind: "openclaw.agent.turn",
      operationVersion: "1",
      status: "lost",
      recoveryState: "lost",
      sourceOwner: "agent",
      sourceRef: "agent:bo:direct",
      metadata: {
        sessionKey: "agent:bo:direct",
        recoveryDiagnostic: {
          state: "lost",
          severity: "error",
          reportable: true,
          retryable: true,
          reason: "gateway_startup_reconciliation",
          message: "Agent turn was marked lost during durable recovery.",
          nextAction: "inspect_timeline_then_retry_or_resume",
          safeRecoveryActions: ["inspect_timeline", "retry_request"],
          input: {
            inputRef: "agent-turn:lost:input",
            inputAvailability: "preview_only",
            canReplay: false,
            reason: "bounded preview only",
            messageLength: 12,
            messageHash: "hash-lost",
          },
          detectedAt: 200,
          processInstanceId: "process-1",
        },
      },
      createdAt: 100,
      updatedAt: 200,
      completedAt: 200,
    };

    const projection = buildDurableCoordinationProjection({ run });

    expect(projection).toMatchObject({
      runtimeRunId: "wfr_lost",
      status: "lost",
      recoveryState: "lost",
      external: {
        sessionKey: "agent:bo:direct",
      },
      controls: {
        canRetry: true,
        canOpenTimeline: true,
      },
      recovery: {
        state: "lost",
        severity: "error",
        reportable: true,
        retryable: true,
        reason: "gateway_startup_reconciliation",
        nextAction: "inspect_timeline_then_retry_or_resume",
        safeRecoveryActions: ["inspect_timeline", "retry_request"],
        input: {
          inputRef: "agent-turn:lost:input",
          inputAvailability: "preview_only",
          canReplay: false,
          reason: "bounded preview only",
          messageLength: 12,
          messageHash: "hash-lost",
        },
      },
    });
    expect(buildDurableTaskFlowStateProjection(projection)).toMatchObject({
      recovery: {
        state: "lost",
        nextAction: "inspect_timeline_then_retry_or_resume",
      },
    });
    expect(buildDurableWorkboardMetadataProjection(projection)).toMatchObject({
      recovery: {
        state: "lost",
        nextAction: "inspect_timeline_then_retry_or_resume",
      },
    });
  });

  it("treats unknown-after-side-effect runs as resumable reconciliation work", () => {
    const run: DurableRuntimeRun = {
      runtimeRunId: "wfr_unknown",
      operationKind: "openclaw.agent.turn",
      operationVersion: "1",
      status: "unknown_after_side_effect",
      recoveryState: "unknown_after_side_effect",
      sourceOwner: "agent",
      sourceRef: "agent:bo:main",
      metadata: {
        sessionKey: "agent:bo:main",
        recoveryDiagnostic: {
          state: "unknown_after_side_effect",
          severity: "warning",
          reportable: true,
          retryable: false,
          reason: "planned_gateway_restart",
          message: "Runtime run was interrupted by an approved gateway restart.",
          nextAction: "inspect_timeline_then_resume_or_reconcile",
          safeRecoveryActions: ["inspect_timeline", "resume_parent", "reconcile_side_effects"],
        },
      },
      createdAt: 100,
      updatedAt: 200,
    };

    const projection = buildDurableCoordinationProjection({ run });

    expect(projection).toMatchObject({
      status: "unknown_after_side_effect",
      recoveryState: "unknown_after_side_effect",
      waitingReason: "unknown",
      controls: {
        canRetry: true,
        canResume: true,
        canOpenTimeline: true,
      },
      recovery: {
        state: "unknown_after_side_effect",
        severity: "warning",
        retryable: false,
        nextAction: "inspect_timeline_then_resume_or_reconcile",
      },
    });
  });

  it("keeps missing-handler owner decisions distinct from side-effect uncertainty", () => {
    const run: DurableRuntimeRun = {
      runtimeRunId: "wfr_owner_decision",
      operationKind: "test.runtime",
      operationVersion: "1",
      status: "blocked",
      recoveryState: "requires_owner_decision",
      sourceOwner: "durable_execution_records",
      sourceRef: "wfr_owner_decision",
      createdAt: 100,
      updatedAt: 200,
    };

    expect(buildDurableCoordinationProjection({ run })).toMatchObject({
      status: "blocked",
      recoveryState: "requires_owner_decision",
      waitingReason: "unknown",
      controls: {
        canRetry: false,
        canResume: false,
        canOpenTimeline: true,
      },
      recovery: {
        state: "requires_owner_decision",
        retryable: false,
        nextAction: "inspect_timeline_then_resolve_owner_decision",
      },
    });
  });
});
