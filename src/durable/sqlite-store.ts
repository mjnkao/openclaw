// SQLite-backed durable runtime store for the native control plane.
import path from "node:path";
import type { DatabaseSync } from "node:sqlite";
import { getNodeSqliteKysely } from "../infra/kysely-sync.js";
import { acquireOpenClawStateDatabaseLease } from "../state/openclaw-state-db.js";
import { resolveOpenClawStateSqlitePath } from "../state/openclaw-state-db.paths.js";
import { ensureDurableRuntimeSchema, openDurableRuntimeSchemaReadOnly } from "./schema.js";
import type { DurableSqliteStoreContext } from "./sqlite-store-context.js";
import { createPrimitiveMethods } from "./sqlite-store-primitive-methods.js";
import { createRecoveryMethods } from "./sqlite-store-recovery-methods.js";
import { createRunMethods } from "./sqlite-store-run-methods.js";
import { createStepMethods } from "./sqlite-store-step-methods.js";
import type { DurableRuntimeDatabase } from "./sqlite-store-support-core.js";
import { createTimelineMethods } from "./sqlite-store-timeline-methods.js";
import { createWakeClaimOperations } from "./sqlite-store-wake-claims.js";
import { createWakeControlOperations } from "./sqlite-store-wake-control.js";
import { createWakeInspectionOperations } from "./sqlite-store-wake-inspection.js";
import { createWakeMethods } from "./sqlite-store-wake-methods.js";
import { createWakeReconciliationOperations } from "./sqlite-store-wake-reconciliation.js";
import type { DurableRuntimeReadStore, DurableRuntimeStore } from "./types.js";

type OpenDurableRuntimeSqliteStoreOptions = {
  path?: string;
  env?: NodeJS.ProcessEnv;
  readOnly?: boolean;
};

export function openDurableRuntimeSqliteStore(
  storeOptions: OpenDurableRuntimeSqliteStoreOptions & { readOnly: true },
): DurableRuntimeReadStore;
export function openDurableRuntimeSqliteStore(
  storeOptions?: OpenDurableRuntimeSqliteStoreOptions & { readOnly?: false },
): DurableRuntimeStore;
export function openDurableRuntimeSqliteStore(
  storeOptions: OpenDurableRuntimeSqliteStoreOptions,
): DurableRuntimeReadStore | DurableRuntimeStore;
export function openDurableRuntimeSqliteStore(
  storeOptions?: OpenDurableRuntimeSqliteStoreOptions,
): DurableRuntimeStore {
  const env = storeOptions?.env ?? process.env;
  const pathname = path.resolve(storeOptions?.path ?? resolveOpenClawStateSqlitePath(env));
  const readOnly = storeOptions?.readOnly === true;
  let db: DatabaseSync;
  let releaseDatabase: () => void;
  if (readOnly) {
    db = openDurableRuntimeSchemaReadOnly(pathname);
    releaseDatabase = () => db.close();
  } else {
    const stateDatabaseLease = acquireOpenClawStateDatabaseLease({ env, path: pathname });
    db = stateDatabaseLease.database.db;
    releaseDatabase = stateDatabaseLease.release;
  }
  const durableDb = (() => {
    try {
      if (!readOnly) {
        ensureDurableRuntimeSchema(db);
      }
      return getNodeSqliteKysely<DurableRuntimeDatabase>(db);
    } catch (error) {
      releaseDatabase();
      throw error;
    }
  })();
  const context: DurableSqliteStoreContext = {
    db,
    durableDb,
    pathname,
    releaseDatabase,
    closed: false,
  };
  const wakeReconciliation = createWakeReconciliationOperations(context);
  const wakeControl = createWakeControlOperations(context, wakeReconciliation);
  const wakeClaims = createWakeClaimOperations(context, wakeReconciliation);
  const wakeInspection = createWakeInspectionOperations(context, wakeControl, wakeClaims);

  return {
    ...createRunMethods(context),
    ...createStepMethods(context),
    ...createPrimitiveMethods(context),
    ...createWakeMethods(context, wakeReconciliation, wakeControl, wakeClaims, wakeInspection),
    ...createRecoveryMethods(context),
    ...createTimelineMethods(context),
  };
}
