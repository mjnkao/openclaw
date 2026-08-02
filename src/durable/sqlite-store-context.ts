import type { DatabaseSync } from "node:sqlite";
import type { Kysely } from "kysely";
import type { DurableRuntimeDatabase } from "./sqlite-store-support-core.js";

export type DurableSqliteStoreContext = {
  db: DatabaseSync;
  durableDb: Kysely<DurableRuntimeDatabase>;
  pathname: string;
  releaseDatabase: () => void;
  closed: boolean;
};
