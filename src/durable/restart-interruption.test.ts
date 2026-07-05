import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { describe, expect, it } from "vitest";
import { recordDurableGatewayRestartInterruption } from "./restart-interruption.js";
import {
  DURABLE_AGENT_TURN_OPERATION_KIND,
  DURABLE_SUBAGENT_RUN_OPERATION_KIND,
} from "./runtime-ids.js";
import { openDurableRuntimeStore } from "./store-factory.js";

describe("durable gateway restart interruption", () => {
  it("unblocks parent fan-in when an approved restart interrupts a running child", () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "openclaw-durable-restart-"));
    const env = {
      ...process.env,
      OPENCLAW_STATE_DIR: dir,
      OPENCLAW_DURABLE_RUNTIME: "1",
    };
    const store = openDurableRuntimeStore({ env });
    try {
      const parent = store.createRun({
        operationKind: DURABLE_AGENT_TURN_OPERATION_KIND,
        idempotencyKey: "parent-run",
        status: "waiting_child",
        recoveryState: "waiting_child",
        sourceRef: "agent:bo:main",
        metadata: { sessionKey: "agent:bo:main" },
        now: 100,
      });
      store.createStep({
        runtimeRunId: parent.runtimeRunId,
        stepId: "subagents",
        stepType: "fan_in",
        status: "waiting",
        recoveryState: "waiting_child",
        metadata: { policy: "continue_on_child_failure" },
        now: 100,
      });
      const child = store.createRun({
        operationKind: DURABLE_SUBAGENT_RUN_OPERATION_KIND,
        idempotencyKey: "child-run",
        status: "running",
        recoveryState: "running",
        sourceType: "subagent",
        sourceRef: "agent:bo-worker:subagent:child",
        parentRuntimeRunId: parent.runtimeRunId,
        parentStepId: "subagents",
        metadata: {
          childSessionKey: "agent:bo-worker:subagent:child",
          requesterSessionKey: "agent:bo:main",
        },
        now: 100,
      });
      store.createStep({
        runtimeRunId: child.runtimeRunId,
        stepId: "subagent_run",
        stepType: "agent",
        status: "running",
        recoveryState: "running",
        now: 100,
      });
      store.createLink({
        parentRuntimeRunId: parent.runtimeRunId,
        parentStepId: "subagents",
        childRuntimeRunId: child.runtimeRunId,
        linkType: "subagent",
        status: "running",
        now: 100,
      });
    } finally {
      store.close();
    }

    const result = recordDurableGatewayRestartInterruption({
      env,
      reason: "operator approved restart",
      sessionKey: "agent:bo:main",
      now: 200,
    });

    expect(result).toMatchObject({
      enabled: true,
      inspectedRuns: 2,
      interruptedRuns: 1,
      interruptedChildren: 1,
      reconciledParents: 1,
    });

    const verifyStore = openDurableRuntimeStore({ env });
    try {
      const runs = verifyStore.listRuns({ limit: 10 });
      const child = runs.find((run) => run.idempotencyKey === "child-run");
      const parent = runs.find((run) => run.idempotencyKey === "parent-run");
      expect(child).toMatchObject({
        status: "unknown_after_side_effect",
        recoveryState: "unknown_after_side_effect",
        metadata: {
          recoveryDiagnostic: expect.objectContaining({
            state: "unknown_after_side_effect",
            recoveryReason: "unknown_after_side_effect",
            retrySafety: "unsafe_without_parent_decision",
            requiredAction: "parent_reconcile_side_effect_boundary",
            sideEffectBoundarySeen: true,
            nextAction: "inspect_timeline_then_record_parent_decision",
          }),
        },
      });
      expect(parent).toMatchObject({
        status: "queued",
        recoveryState: "runnable",
      });
      expect(verifyStore.listChildLinks(parent!.runtimeRunId)).toEqual([
        expect.objectContaining({
          status: "lost",
          metadata: expect.objectContaining({
            terminalOutcome: "unknown_after_side_effect",
            recoveryDiagnostic: expect.objectContaining({
              retrySafety: "unsafe_without_parent_decision",
            }),
          }),
        }),
      ]);
      expect(verifyStore.listSteps(parent!.runtimeRunId)).toContainEqual(
        expect.objectContaining({
          stepType: "result_mailbox",
          status: "queued",
          recoveryState: "runnable",
          metadata: expect.objectContaining({
            kind: "child_result_mailbox",
            childRuntimeRunId: child!.runtimeRunId,
            outcome: expect.objectContaining({
              terminalOutcome: "unknown_after_side_effect",
              reason: "unknown_after_side_effect",
              recoveryDiagnostic: expect.objectContaining({
                requiredAction: "parent_reconcile_side_effect_boundary",
              }),
            }),
          }),
        }),
      );
      expect(verifyStore.getTimeline(parent!.runtimeRunId).map((event) => event.eventType)).toEqual(
        [
          "subagent.child.restart_interrupted",
          "subagent.child.result_mailbox_queued",
          "fan_in.ready",
        ],
      );
    } finally {
      verifyStore.close();
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });
});
