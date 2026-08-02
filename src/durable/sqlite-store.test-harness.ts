import { spawn, type ChildProcess } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { describe, expect, it, vi } from "vitest";
import { requireNodeSqlite } from "../infra/node-sqlite.js";
import { resolveSqliteDatabaseFilePaths } from "../infra/sqlite-files.js";
import {
  OPENCLAW_STATE_SCHEMA_VERSION,
  closeOpenClawStateDatabaseForPathForTest,
  openOpenClawStateDatabase,
} from "../state/openclaw-state-db.js";
import { resolveOpenClawStateSqlitePath } from "../state/openclaw-state-db.paths.js";
import { openDurableRuntimeSqliteStore } from "./sqlite-store.js";
import type {
  ReconcileWakeObligationAppliedResult,
  ReconcileWakeObligationResult,
} from "./types.js";

const DURABLE_TABLES = [
  "delivery_attempt_evidence",
  "durable_event_evidence",
  "durable_execution_records",
  "durable_execution_steps",
  "durable_payload_refs",
  "durable_run_correlations",
  "durable_signal_evidence",
  "durable_timer_obligations",
  "uncertainty_facts",
  "wake_obligation_occurrences",
  "wake_obligations",
] as const;

function requireAppliedWakeResult(
  result: ReconcileWakeObligationResult,
): ReconcileWakeObligationAppliedResult {
  if (result.disposition === "conflict") {
    throw new Error(`Expected applied wake reconciliation, received ${result.reason}`);
  }
  return result;
}

const CONCURRENT_WAKE_WRITER_SCRIPT = String.raw`
const [moduleUrl, dbPath, occurrenceKey] = process.argv.slice(1);
const { openDurableRuntimeSqliteStore } = await import(moduleUrl);
const store = openDurableRuntimeSqliteStore({ path: dbPath });
process.send?.({ type: "ready" });
process.once("message", (message) => {
  if (message !== "go") {
    process.exitCode = 2;
    store.close();
    process.disconnect?.();
    return;
  }
  try {
    const result = store.reconcileWakeObligation({
      candidate: {
        sourceOwner: "test-owner",
        sourceRef: "test-source:concurrent",
        targetKind: "agent_session",
        targetRef: "agent:test:main",
        ownerKind: "agent_session",
        ownerRef: "agent:test:main",
        reportRouteRef: "agent:test:main",
        targetResolutionStatus: "resolved",
        reason: "operator_requested",
        factsRef: "test-facts:concurrent",
        sourceRevision: occurrenceKey,
        occurrenceKey,
        now: 100,
      },
      policy: { mode: "while_unresolved", recurrence: "after_terminal" },
    });
    process.send?.({ type: "result", result });
  } catch (error) {
    process.send?.({
      type: "error",
      error: error instanceof Error ? error.stack ?? error.message : String(error),
    });
    process.exitCode = 1;
  } finally {
    store.close();
    process.disconnect?.();
  }
});
`;

type ConcurrentWakeWriter = {
  child: ChildProcess;
  ready: Promise<void>;
  run: () => Promise<ReconcileWakeObligationResult>;
};

function startConcurrentWakeWriter(params: {
  dbPath: string;
  occurrenceKey: string;
}): ConcurrentWakeWriter {
  const moduleUrl = pathToFileURL(path.resolve("src/durable/sqlite-store.ts")).href;
  const child = spawn(
    process.execPath,
    [
      "--import",
      "tsx",
      "--input-type=module",
      "-e",
      CONCURRENT_WAKE_WRITER_SCRIPT,
      moduleUrl,
      params.dbPath,
      params.occurrenceKey,
    ],
    { cwd: process.cwd(), stdio: ["ignore", "pipe", "pipe", "ipc"] },
  );
  let stderr = "";
  child.stderr?.setEncoding("utf8");
  child.stderr?.on("data", (chunk: string) => {
    stderr += chunk;
  });
  let resolveReady!: () => void;
  let rejectReady!: (error: Error) => void;
  const ready = new Promise<void>((resolve, reject) => {
    resolveReady = resolve;
    rejectReady = reject;
  });
  let resolveResult!: (result: ReconcileWakeObligationResult) => void;
  let rejectResult!: (error: Error) => void;
  const result = new Promise<ReconcileWakeObligationResult>((resolve, reject) => {
    resolveResult = resolve;
    rejectResult = reject;
  });
  let receivedReady = false;
  let receivedResult = false;
  child.on("message", (message: unknown) => {
    if (!message || typeof message !== "object") {
      return;
    }
    const envelope = message as {
      type?: string;
      result?: ReconcileWakeObligationResult;
      error?: string;
    };
    if (envelope.type === "ready") {
      receivedReady = true;
      resolveReady();
    } else if (envelope.type === "result" && envelope.result) {
      receivedResult = true;
      resolveResult(envelope.result);
    } else if (envelope.type === "error") {
      const error = new Error(envelope.error ?? "Concurrent wake writer failed");
      rejectReady(error);
      rejectResult(error);
    }
  });
  child.once("error", (error) => {
    rejectReady(error);
    rejectResult(error);
  });
  child.once("exit", (code) => {
    if (!receivedReady) {
      rejectReady(new Error(`Concurrent wake writer exited ${code}: ${stderr}`));
    }
    if (!receivedResult) {
      rejectResult(new Error(`Concurrent wake writer exited ${code}: ${stderr}`));
    }
  });
  return {
    child,
    ready,
    run: () => {
      child.send("go");
      return result;
    },
  };
}

export {
  spawn,
  fs,
  os,
  path,
  pathToFileURL,
  describe,
  expect,
  it,
  vi,
  requireNodeSqlite,
  resolveSqliteDatabaseFilePaths,
  OPENCLAW_STATE_SCHEMA_VERSION,
  closeOpenClawStateDatabaseForPathForTest,
  openOpenClawStateDatabase,
  resolveOpenClawStateSqlitePath,
  openDurableRuntimeSqliteStore,
  DURABLE_TABLES,
  requireAppliedWakeResult,
  CONCURRENT_WAKE_WRITER_SCRIPT,
  startConcurrentWakeWriter,
};
export type {
  ChildProcess,
  ReconcileWakeObligationAppliedResult,
  ReconcileWakeObligationResult,
  ConcurrentWakeWriter,
};
