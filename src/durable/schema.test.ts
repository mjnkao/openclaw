import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { DatabaseSync } from "node:sqlite";
import { describe, expect, it } from "vitest";
import { requireNodeSqlite } from "../infra/node-sqlite.js";
import { OPENCLAW_STATE_SCHEMA_VERSION } from "../state/openclaw-state-db.js";
import { DURABLE_RUNTIME_SCHEMA_SQL } from "./schema.generated.js";
import {
  DURABLE_RUNTIME_TABLE_NAMES,
  ensureDurableRuntimeSchema,
  openDurableRuntimeSchemaReadOnly,
} from "./schema.js";

function listSchemaTables(db: DatabaseSync): string[] {
  return (
    db
      .prepare(
        `SELECT name
           FROM sqlite_schema
          WHERE type = 'table'
            AND name NOT LIKE 'sqlite_%'
          ORDER BY name`,
      )
      .all() as Array<{ name: string }>
  ).map((row) => row.name);
}

function listTableDefinitions(db: DatabaseSync): Array<{ name: string; sql: string | null }> {
  return db
    .prepare(
      `SELECT name, sql
         FROM sqlite_schema
        WHERE type = 'table'
          AND name NOT LIKE 'sqlite_%'
        ORDER BY name`,
    )
    .all() as Array<{ name: string; sql: string | null }>;
}

function withDatabaseFile(run: (db: DatabaseSync, pathname: string) => void): void {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "openclaw-durable-schema-"));
  const pathname = path.join(dir, "openclaw.sqlite");
  const { DatabaseSync } = requireNodeSqlite();
  const db = new DatabaseSync(pathname);
  try {
    run(db, pathname);
  } finally {
    if (db.isOpen) {
      db.close();
    }
    fs.rmSync(dir, { recursive: true, force: true });
  }
}

function closeForReadOnly(db: DatabaseSync): void {
  db.close();
}

describe("durable runtime schema compatibility", () => {
  it("installs exactly the declared eleven-table durable schema", () => {
    const { DatabaseSync } = requireNodeSqlite();
    const expected = new DatabaseSync(":memory:");
    const actual = new DatabaseSync(":memory:");
    try {
      expected.exec(DURABLE_RUNTIME_SCHEMA_SQL);
      ensureDurableRuntimeSchema(actual);
      expect(listSchemaTables(expected)).toEqual([...DURABLE_RUNTIME_TABLE_NAMES].toSorted());
      expect(listSchemaTables(actual)).toEqual([...DURABLE_RUNTIME_TABLE_NAMES].toSorted());
    } finally {
      expected.close();
      actual.close();
    }
  });

  it("stores wake mutation authority in typed schema columns", () => {
    const { DatabaseSync } = requireNodeSqlite();
    const db = new DatabaseSync(":memory:");
    try {
      db.exec(DURABLE_RUNTIME_SCHEMA_SQL);
      const wakeColumns = db.prepare("PRAGMA table_info(wake_obligations)").all() as Array<{
        name: string;
        type: string;
        notnull: number;
      }>;
      const attemptColumns = db
        .prepare("PRAGMA table_info(delivery_attempt_evidence)")
        .all() as Array<{ name: string; type: string; notnull: number }>;

      expect(wakeColumns).toEqual(
        expect.arrayContaining([
          expect.objectContaining({ name: "delivery_revision", type: "INTEGER", notnull: 1 }),
          expect.objectContaining({ name: "suspension_class", type: "TEXT" }),
        ]),
      );
      expect(attemptColumns).toEqual(
        expect.arrayContaining([
          expect.objectContaining({
            name: "claimed_wake_delivery_revision",
            type: "INTEGER",
            notnull: 1,
          }),
        ]),
      );
    } finally {
      db.close();
    }
  });

  it("rejects a future shared-state user_version before a read-only durable open", () => {
    withDatabaseFile((db, pathname) => {
      db.exec(DURABLE_RUNTIME_SCHEMA_SQL);
      db.exec(`PRAGMA user_version = ${OPENCLAW_STATE_SCHEMA_VERSION + 1}`);
      db.close();

      expect(() => openDurableRuntimeSchemaReadOnly(pathname)).toThrow(
        /uses newer schema version .* supports/,
      );
    });
  });

  it("rejects a future shared-state user_version before a writable install", () => {
    const { DatabaseSync } = requireNodeSqlite();
    const db = new DatabaseSync(":memory:");
    try {
      db.exec(`PRAGMA user_version = ${OPENCLAW_STATE_SCHEMA_VERSION + 1}`);
      expect(() => ensureDurableRuntimeSchema(db)).toThrow(/uses newer schema version .* supports/);
      expect(listSchemaTables(db)).toEqual([]);
    } finally {
      db.close();
    }
  });

  it("rejects a partial durable install in read-only and writable modes", () => {
    withDatabaseFile((db, pathname) => {
      db.exec(DURABLE_RUNTIME_SCHEMA_SQL);
      const expectedTables = listSchemaTables(db);
      const missingTable = expectedTables.at(-1);
      expect(missingTable).toBeDefined();
      db.exec(`DROP TABLE "${missingTable}"`);
      db.close();

      expect(() => openDurableRuntimeSchemaReadOnly(pathname)).toThrow(
        /partial durable runtime schema/,
      );

      const { DatabaseSync } = requireNodeSqlite();
      const writable = new DatabaseSync(pathname);
      try {
        expect(() => ensureDurableRuntimeSchema(writable)).toThrow(
          /partial durable runtime schema/,
        );
        expect(listSchemaTables(writable)).toEqual(
          expectedTables.filter((name) => name !== missingTable),
        );
      } finally {
        writable.close();
      }
    });
  });

  it("rejects unknown durable tables without a shared schema-version advance", () => {
    withDatabaseFile((db, pathname) => {
      db.exec(DURABLE_RUNTIME_SCHEMA_SQL);
      db.exec("CREATE TABLE durable_future_records (id TEXT PRIMARY KEY)");
      db.exec(`PRAGMA user_version = ${OPENCLAW_STATE_SCHEMA_VERSION}`);
      db.close();

      expect(() => openDurableRuntimeSchemaReadOnly(pathname)).toThrow(
        /unknown durable tables for shared schema version/,
      );

      const { DatabaseSync } = requireNodeSqlite();
      const writable = new DatabaseSync(pathname);
      try {
        expect(() => ensureDurableRuntimeSchema(writable)).toThrow(
          /unknown durable tables for shared schema version/,
        );
      } finally {
        writable.close();
      }
    });
  });

  it("rejects unknown tables in the plural wake-obligations namespace", () => {
    const { DatabaseSync } = requireNodeSqlite();
    const db = new DatabaseSync(":memory:");
    try {
      db.exec(DURABLE_RUNTIME_SCHEMA_SQL);
      db.exec("CREATE TABLE wake_obligations_archive (id TEXT PRIMARY KEY)");
      expect(() => ensureDurableRuntimeSchema(db)).toThrow(
        /unknown durable tables for shared schema version/,
      );
    } finally {
      db.close();
    }
  });

  it("rolls back every durable object when first-install validation fails", () => {
    const { DatabaseSync } = requireNodeSqlite();
    const db = new DatabaseSync(":memory:");
    try {
      db.exec("CREATE TABLE install_probe (id TEXT PRIMARY KEY)");
      db.exec("CREATE INDEX idx_wake_obligations_status ON install_probe(id)");

      expect(() => ensureDurableRuntimeSchema(db)).toThrow(
        /incompatible durable index .*expected owner/,
      );
      const tables = listSchemaTables(db);
      expect(tables).toEqual(["install_probe"]);
      expect(tables.filter((name) => DURABLE_RUNTIME_TABLE_NAMES.includes(name as never))).toEqual(
        [],
      );
      expect(db.prepare("PRAGMA integrity_check").get()).toEqual({ integrity_check: "ok" });
    } finally {
      db.close();
    }
  });

  it("rejects a wrong durable column shape without repairing indexes", () => {
    withDatabaseFile((db, pathname) => {
      db.exec(DURABLE_RUNTIME_SCHEMA_SQL);
      db.exec("ALTER TABLE durable_execution_records ADD COLUMN unexpected_column TEXT");
      db.exec("DROP INDEX idx_durable_execution_records_status");
      db.close();

      expect(() => openDurableRuntimeSchemaReadOnly(pathname)).toThrow(/column shape differs/);

      const { DatabaseSync } = requireNodeSqlite();
      const writable = new DatabaseSync(pathname);
      try {
        expect(() => ensureDurableRuntimeSchema(writable)).toThrow(/column shape differs/);
        expect(
          writable
            .prepare("SELECT 1 FROM sqlite_schema WHERE type = 'index' AND name = ?")
            .get("idx_durable_execution_records_status"),
        ).toBeUndefined();
      } finally {
        writable.close();
      }
    });
  });

  it("rejects a wrong named index key shape", () => {
    withDatabaseFile((db, pathname) => {
      db.exec(DURABLE_RUNTIME_SCHEMA_SQL);
      db.exec("DROP INDEX idx_wake_obligations_status");
      db.exec("CREATE INDEX idx_wake_obligations_status ON wake_obligations(status, wake_id)");
      db.close();

      expect(() => openDurableRuntimeSchemaReadOnly(pathname)).toThrow(/index .*key shape differs/);

      const { DatabaseSync } = requireNodeSqlite();
      const writable = new DatabaseSync(pathname);
      try {
        expect(() => ensureDurableRuntimeSchema(writable)).toThrow(/index .*key shape differs/);
      } finally {
        writable.close();
      }
    });
  });

  it("rejects a wrong partial-index predicate", () => {
    withDatabaseFile((db, pathname) => {
      db.exec(DURABLE_RUNTIME_SCHEMA_SQL);
      db.exec("DROP INDEX idx_durable_execution_records_idempotency");
      db.exec(`
        CREATE UNIQUE INDEX idx_durable_execution_records_idempotency
          ON durable_execution_records(operation_kind, idempotency_key)
          WHERE idempotency_key IS NULL
      `);
      db.close();

      expect(() => openDurableRuntimeSchemaReadOnly(pathname)).toThrow(
        /partial index .*predicate shape differs/,
      );

      const { DatabaseSync } = requireNodeSqlite();
      const writable = new DatabaseSync(pathname);
      try {
        expect(() => ensureDurableRuntimeSchema(writable)).toThrow(
          /partial index .*predicate shape differs/,
        );
      } finally {
        writable.close();
      }
    });
  });

  it("rejects missing indexes in read-only mode", () => {
    withDatabaseFile((db, pathname) => {
      db.exec(DURABLE_RUNTIME_SCHEMA_SQL);
      db.exec("DROP INDEX idx_durable_execution_records_status");
      db.close();

      expect(() => openDurableRuntimeSchemaReadOnly(pathname)).toThrow(
        /missing required durable indexes/,
      );
    });
  });

  it("repairs only missing canonical indexes and preserves tables and extra indexes", () => {
    const { DatabaseSync } = requireNodeSqlite();
    const db = new DatabaseSync(":memory:");
    try {
      db.exec(DURABLE_RUNTIME_SCHEMA_SQL);
      const tableDefinitions = listTableDefinitions(db);
      db.exec("DROP INDEX idx_durable_execution_records_status");
      db.exec("DROP INDEX idx_durable_execution_records_idempotency");
      db.exec("CREATE INDEX custom_durable_runtime_probe ON durable_execution_records(created_at)");

      ensureDurableRuntimeSchema(db);

      expect(listTableDefinitions(db)).toEqual(tableDefinitions);
      for (const indexName of [
        "idx_durable_execution_records_status",
        "idx_durable_execution_records_idempotency",
        "custom_durable_runtime_probe",
      ]) {
        expect(
          db
            .prepare("SELECT 1 AS present FROM sqlite_schema WHERE type = 'index' AND name = ?")
            .get(indexName),
        ).toEqual({ present: 1 });
      }
      const partialIndexSql = db
        .prepare("SELECT sql FROM sqlite_schema WHERE type = 'index' AND name = ?")
        .get("idx_durable_execution_records_idempotency") as { sql: string };
      expect(partialIndexSql.sql).toMatch(/WHERE idempotency_key IS NOT NULL/);
    } finally {
      db.close();
    }
  });

  it("accepts a canonical durable schema in read-only mode", () => {
    withDatabaseFile((db, pathname) => {
      db.exec(DURABLE_RUNTIME_SCHEMA_SQL);
      db.exec(`PRAGMA user_version = ${OPENCLAW_STATE_SCHEMA_VERSION}`);
      db.close();

      expect(() => closeForReadOnly(openDurableRuntimeSchemaReadOnly(pathname))).not.toThrow();
    });
  });
});
