import { createHash } from "node:crypto";
import { runSqliteImmediateTransactionSync } from "../infra/sqlite-transaction.js";
import type { DurableSqliteStoreContext } from "./sqlite-store-context.js";
import type { DurableRuntimeRunRow, DurableRuntimeEventRow } from "./sqlite-store-support-core.js";
import {
  rowToEvent,
  queryRows,
  queryFirst,
  executeQuery,
  count,
  normalizeQueryLimit,
  isTerminalRunStatus,
} from "./sqlite-store-support-query.js";
import type {
  DurableRuntimeEvent,
  DurableRuntimeStoreStats,
  DurableRuntimeTimelineOptions,
  CompactDurableRuntimeRunInput,
  CompactDurableRuntimeRunResult,
} from "./types.js";

export function createTimelineMethods(context: DurableSqliteStoreContext) {
  const { db, durableDb, pathname, releaseDatabase } = context;
  return {
    getTimeline(
      runtimeRunId: string,
      timelineOptions?: DurableRuntimeTimelineOptions,
    ): DurableRuntimeEvent[] {
      const afterEventSeq = Math.max(0, Math.trunc(timelineOptions?.afterEventSeq ?? 0));
      const shouldLimit = timelineOptions?.limit !== undefined || afterEventSeq !== 0;
      const rows = queryRows<DurableRuntimeEventRow>(
        db,
        durableDb
          .selectFrom("durable_event_evidence")
          .selectAll()
          .where("runtime_run_id", "=", runtimeRunId)
          .$if(afterEventSeq !== 0, (qb) => qb.where("event_seq", ">", afterEventSeq))
          .orderBy("event_seq", "asc")
          .$if(shouldLimit, (qb) => qb.limit(normalizeQueryLimit(timelineOptions?.limit, 500))),
      );
      return rows.map(rowToEvent);
    },

    compactTerminalRun(input: CompactDurableRuntimeRunInput): CompactDurableRuntimeRunResult {
      const keepLastEvents = normalizeQueryLimit(input.keepLastEvents, 200);
      return runSqliteImmediateTransactionSync(db, () => {
        const run = queryFirst<DurableRuntimeRunRow>(
          db,
          durableDb
            .selectFrom("durable_execution_records")
            .selectAll()
            .where("runtime_run_id", "=", input.runtimeRunId),
        );
        if (!run || !isTerminalRunStatus(run.status)) {
          return {
            runtimeRunId: input.runtimeRunId,
            compacted: false,
            redactedEventPayloads: 0,
            hasMore: false,
          };
        }
        const totalEvents = count(
          db,
          durableDb
            .selectFrom("durable_event_evidence")
            .select((eb) => eb.fn.countAll<number>().as("count"))
            .where("runtime_run_id", "=", input.runtimeRunId),
        );
        if (totalEvents <= keepLastEvents) {
          return {
            runtimeRunId: input.runtimeRunId,
            compacted: false,
            redactedEventPayloads: 0,
            hasMore: false,
          };
        }
        const cutoff = queryFirst<{ event_seq: number | bigint }>(
          db,
          durableDb
            .selectFrom("durable_event_evidence")
            .select("event_seq")
            .where("runtime_run_id", "=", input.runtimeRunId)
            .orderBy("event_seq", "desc")
            .limit(1)
            .offset(keepLastEvents - 1),
        );
        const cutoffSeq = Number(cutoff?.event_seq ?? 0);
        if (cutoffSeq <= 1) {
          return {
            runtimeRunId: input.runtimeRunId,
            compacted: false,
            redactedEventPayloads: 0,
            hasMore: false,
          };
        }
        const redactionCandidates = queryRows<
          Pick<DurableRuntimeEventRow, "event_id" | "payload_json" | "payload_hash">
        >(
          db,
          durableDb
            .selectFrom("durable_event_evidence")
            .select(["event_id", "payload_json", "payload_hash"])
            .where("runtime_run_id", "=", input.runtimeRunId)
            .where("event_seq", "<", cutoffSeq)
            .where("payload_json", "is not", null)
            .orderBy("event_seq", "asc")
            .limit(5001),
        );
        const boundedCandidates = redactionCandidates.slice(0, 5000);
        for (const candidate of boundedCandidates) {
          const payloadJson = candidate.payload_json!;
          executeQuery(
            db,
            durableDb
              .updateTable("durable_event_evidence")
              .set({
                payload_json: null,
                payload_hash:
                  candidate.payload_hash ?? createHash("sha256").update(payloadJson).digest("hex"),
              })
              .where("event_id", "=", candidate.event_id)
              .where("payload_json", "=", payloadJson),
          );
        }
        if (boundedCandidates.length === 0) {
          return {
            runtimeRunId: input.runtimeRunId,
            compacted: false,
            redactedEventPayloads: 0,
            hasMore: false,
          };
        }
        return {
          runtimeRunId: input.runtimeRunId,
          compacted: true,
          redactedEventPayloads: boundedCandidates.length,
          hasMore: redactionCandidates.length > boundedCandidates.length,
        };
      });
    },

    getStats(): DurableRuntimeStoreStats {
      return {
        path: pathname,
        runs: count(
          db,
          durableDb
            .selectFrom("durable_execution_records")
            .select((eb) => eb.fn.countAll<number>().as("count")),
        ),
        events: count(
          db,
          durableDb
            .selectFrom("durable_event_evidence")
            .select((eb) => eb.fn.countAll<number>().as("count")),
        ),
        steps: count(
          db,
          durableDb
            .selectFrom("durable_execution_steps")
            .select((eb) => eb.fn.countAll<number>().as("count")),
        ),
        openRuns: count(
          db,
          durableDb
            .selectFrom("durable_execution_records")
            .select((eb) => eb.fn.countAll<number>().as("count"))
            .where("status", "not in", ["succeeded", "failed", "cancelled", "lost"]),
        ),
        pendingWakes: count(
          db,
          durableDb
            .selectFrom("wake_obligations")
            .select((eb) => eb.fn.countAll<number>().as("count"))
            .where("status", "in", ["pending", "handoff_accepted", "failed", "suspended"]),
        ),
        unresolvedUncertaintyFacts: count(
          db,
          durableDb
            .selectFrom("uncertainty_facts")
            .select((eb) => eb.fn.countAll<number>().as("count"))
            .where("status", "=", "open"),
        ),
      };
    },

    close(): void {
      if (context.closed) {
        return;
      }
      context.closed = true;
      releaseDatabase();
    },
  };
}
