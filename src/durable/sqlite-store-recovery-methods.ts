import type { DurableSqliteStoreContext } from "./sqlite-store-context.js";
import {
  DURABLE_STEP_LEASE_SCOPE,
  serializeJson,
  parseJsonRecord,
} from "./sqlite-store-support-core.js";
import type {
  DurableRuntimeLinkRow,
  WakeObligationRow,
  UncertaintyFactRow,
  DurableUnresolvedObligationRow,
  ExpiredStateLeaseRow,
} from "./sqlite-store-support-core.js";
import {
  rowToUnresolvedObligation,
  queryRows,
  normalizeQueryLimit,
  unresolvedWakeStatusSql,
} from "./sqlite-store-support-query.js";
import type { DurableUnresolvedObligation } from "./types.js";

export function createRecoveryMethods(context: DurableSqliteStoreContext) {
  const { db, durableDb } = context;
  return {
    listUnresolvedObligations(options?: {
      now?: number;
      limit?: number;
    }): DurableUnresolvedObligation[] {
      const now = options?.now ?? Date.now();
      const limit = normalizeQueryLimit(options?.limit, 500);
      const wakeRows = queryRows<WakeObligationRow>(
        db,
        durableDb
          .selectFrom("wake_obligations")
          .selectAll()
          .where(unresolvedWakeStatusSql())
          .orderBy("updated_at", "desc")
          .orderBy("wake_id", "desc")
          .limit(limit),
      ).map(
        (row): DurableUnresolvedObligationRow => ({
          obligation_id: `wake:${row.wake_id}`,
          source_owner: row.source_owner,
          source_ref: row.source_ref,
          kind: "pending_wake",
          runtime_run_id: row.source_run_id,
          step_id: null,
          wake_id: row.wake_id,
          uncertainty_fact_id: null,
          subject_ref: row.facts_ref ?? row.wake_id,
          reason: row.reason,
          status: row.status,
          created_at: row.created_at,
          updated_at: row.updated_at,
          metadata_json: row.metadata_json,
        }),
      );
      const uncertaintyRows = queryRows<UncertaintyFactRow>(
        db,
        durableDb
          .selectFrom("uncertainty_facts")
          .selectAll()
          .where("status", "=", "open")
          .orderBy("updated_at", "desc")
          .orderBy("fact_id", "desc")
          .limit(limit),
      ).map(
        (row): DurableUnresolvedObligationRow => ({
          obligation_id: `uncertainty:${row.fact_id}`,
          source_owner: row.source_owner,
          source_ref: row.source_ref,
          kind: "unresolved_uncertainty",
          runtime_run_id: row.source_run_id,
          step_id: row.step_id,
          wake_id: null,
          uncertainty_fact_id: row.fact_id,
          subject_ref: row.facts_ref ?? row.ref_id ?? row.event_id ?? row.dedupe_key,
          reason: row.kind,
          status: row.status,
          created_at: row.created_at,
          updated_at: row.updated_at,
          metadata_json: row.metadata_json,
        }),
      );
      const childRows = queryRows<DurableRuntimeLinkRow>(
        db,
        durableDb
          .selectFrom("durable_run_correlations")
          .selectAll()
          .where("status", "in", ["pending", "running"])
          .orderBy("updated_at", "desc")
          .orderBy("child_runtime_run_id", "desc")
          .limit(limit),
      ).map(
        (row): DurableUnresolvedObligationRow => ({
          obligation_id: `child:${row.parent_runtime_run_id}:${row.parent_step_id}:${row.child_runtime_run_id}`,
          source_owner: "durable_run_correlations",
          source_ref: `${row.parent_runtime_run_id}:${row.parent_step_id}:${row.child_runtime_run_id}`,
          kind: "open_child",
          runtime_run_id: row.parent_runtime_run_id,
          step_id: row.parent_step_id,
          wake_id: null,
          uncertainty_fact_id: null,
          subject_ref: row.child_runtime_run_id,
          reason: row.link_type,
          status: row.status,
          created_at: row.created_at,
          updated_at: row.updated_at,
          metadata_json: row.metadata_json,
        }),
      );
      const expiredStateLeaseRows = queryRows<ExpiredStateLeaseRow>(
        db,
        durableDb
          .selectFrom("state_leases")
          .selectAll()
          .where("scope", "=", DURABLE_STEP_LEASE_SCOPE)
          .where("expires_at", "is not", null)
          .where("expires_at", "<=", now)
          .orderBy("updated_at", "desc")
          .orderBy("scope", "asc")
          .orderBy("lease_key", "asc")
          .limit(limit),
      ).map((row): DurableUnresolvedObligationRow => {
        const payload = parseJsonRecord(row.payload_json);
        const runtimeRunId =
          typeof payload?.runtimeRunId === "string" ? payload.runtimeRunId : null;
        const stepId = typeof payload?.stepId === "string" ? payload.stepId : null;
        return {
          obligation_id: `state-lease:${row.scope}:${row.lease_key}`,
          source_owner: "state_leases",
          source_ref: `${row.scope}:${row.lease_key}`,
          kind: "expired_state_lease",
          runtime_run_id: runtimeRunId,
          step_id: stepId,
          wake_id: null,
          uncertainty_fact_id: null,
          subject_ref: row.owner,
          reason: "lease_expired",
          status: "expired",
          created_at: row.created_at,
          updated_at: row.updated_at,
          metadata_json: serializeJson({
            expiresAt: row.expires_at,
            heartbeatAt: row.heartbeat_at,
          }),
        };
      });
      return [...wakeRows, ...uncertaintyRows, ...childRows, ...expiredStateLeaseRows]
        .toSorted((left, right) => {
          const updated = Number(right.updated_at) - Number(left.updated_at);
          return updated === 0 ? right.obligation_id.localeCompare(left.obligation_id) : updated;
        })
        .slice(0, limit)
        .map(rowToUnresolvedObligation);
    },
  };
}
