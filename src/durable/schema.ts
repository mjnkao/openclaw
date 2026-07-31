import { existsSync } from "node:fs";
import type { DatabaseSync } from "node:sqlite";
import { isDeepStrictEqual } from "node:util";
import { requireNodeSqlite } from "../infra/node-sqlite.js";
import { runSqliteImmediateTransactionSync } from "../infra/sqlite-transaction.js";
import { readSqliteUserVersion } from "../infra/sqlite-user-version.js";
import { OPENCLAW_STATE_SCHEMA_VERSION } from "../state/openclaw-state-db.js";
import { DURABLE_RUNTIME_SCHEMA_SQL } from "./schema.generated.js";

type SqliteColumnRow = {
  cid: number;
  name: string;
  type: string;
  notnull: number;
  dflt_value: unknown;
  pk: number;
  hidden: number;
};

type SqliteForeignKeyRow = {
  id: number;
  seq: number;
  table: string;
  from: string;
  to: string | null;
  on_update: string;
  on_delete: string;
  match: string;
};

type SqliteIndexListRow = {
  name: string;
  unique: number;
  origin: string;
  partial: number;
};

type SqliteIndexColumnRow = {
  seqno: number;
  cid: number;
  name: string | null;
  desc: number;
  coll: string;
  key: number;
};

type SqliteSchemaObjectRow = {
  type: string;
  name: string;
  tbl_name: string;
  sql: string | null;
};

type DurableIndexShape = {
  name: string;
  unique: number;
  origin: string;
  partial: number;
  columns: SqliteIndexColumnRow[];
  whereClause: string | null;
  sql: string | null;
};

type DurableTableShape = {
  columns: SqliteColumnRow[];
  foreignKeys: SqliteForeignKeyRow[];
  indexes: DurableIndexShape[];
  definition: string;
};

type DurableSchemaContract = {
  tables: Map<string, DurableTableShape>;
};

let expectedDurableSchemaContract: DurableSchemaContract | undefined;

function quoteSqliteIdentifier(identifier: string): string {
  return `"${identifier.replaceAll('"', '""')}"`;
}

function normalizeSqlFragment(sql: string): string {
  const source = sql.trim().replace(/;\s*$/, "");
  let normalized = "";
  let quote: "'" | '"' | "`" | "]" | undefined;
  let pendingSpace = false;
  for (let index = 0; index < source.length; index += 1) {
    const character = source[index];
    if (quote) {
      normalized += character;
      const quoteEnd = quote === "]" ? "]" : quote;
      if (character === quoteEnd) {
        if (source[index + 1] === quoteEnd && quote !== "]") {
          normalized += source[index + 1];
          index += 1;
        } else {
          quote = undefined;
        }
      }
      continue;
    }
    if (character === "'" || character === '"' || character === "`") {
      if (pendingSpace && normalized && !normalized.endsWith("(")) {
        normalized += " ";
      }
      quote = character;
      normalized += character;
      pendingSpace = false;
      continue;
    }
    if (character === "[") {
      if (pendingSpace && normalized && !normalized.endsWith("(")) {
        normalized += " ";
      }
      quote = "]";
      normalized += character;
      pendingSpace = false;
      continue;
    }
    if (/\s/.test(character)) {
      pendingSpace = true;
      continue;
    }
    if (character === "," || character === "(" || character === ")") {
      normalized = normalized.trimEnd();
      normalized += character;
      pendingSpace = false;
      continue;
    }
    if (pendingSpace && normalized && !normalized.endsWith("(") && !normalized.endsWith(",")) {
      normalized += " ";
    }
    normalized += character.toLowerCase();
    pendingSpace = false;
  }
  return normalized.trim();
}

function normalizeSchemaDefinition(sql: string): string {
  return normalizeSqlFragment(sql.replace(/\bIF\s+NOT\s+EXISTS\b/i, ""));
}

function extractWhereClause(sql: string | null): string | null {
  if (!sql) {
    return null;
  }
  const match = /\bWHERE\b([\s\S]*)$/i.exec(sql);
  return match ? normalizeSqlFragment(match[1]) : null;
}

function collectColumns(db: DatabaseSync, tableName: string): SqliteColumnRow[] {
  return db
    .prepare(`PRAGMA table_xinfo(${quoteSqliteIdentifier(tableName)})`)
    .all()
    .map((row) => {
      const column = row as SqliteColumnRow;
      return {
        cid: column.cid,
        name: column.name,
        type: column.type.trim().toUpperCase(),
        notnull: column.notnull,
        dflt_value:
          typeof column.dflt_value === "string"
            ? normalizeSqlFragment(column.dflt_value)
            : column.dflt_value,
        pk: column.pk,
        hidden: column.hidden,
      };
    });
}

function collectForeignKeys(db: DatabaseSync, tableName: string): SqliteForeignKeyRow[] {
  return db
    .prepare(`PRAGMA foreign_key_list(${quoteSqliteIdentifier(tableName)})`)
    .all()
    .map((row) => {
      const foreignKey = row as SqliteForeignKeyRow;
      return {
        id: foreignKey.id,
        seq: foreignKey.seq,
        table: foreignKey.table,
        from: foreignKey.from,
        to: foreignKey.to,
        on_update: foreignKey.on_update,
        on_delete: foreignKey.on_delete,
        match: foreignKey.match,
      };
    })
    .toSorted((left, right) => left.id - right.id || left.seq - right.seq);
}

function collectIndexShape(
  db: DatabaseSync,
  tableName: string,
  index: SqliteIndexListRow,
): DurableIndexShape {
  const schemaRow = db
    .prepare(
      "SELECT type, name, tbl_name, sql FROM sqlite_schema WHERE type = 'index' AND name = ?",
    )
    .get(index.name) as SqliteSchemaObjectRow | undefined;
  if (!schemaRow || schemaRow.tbl_name !== tableName) {
    throw new Error(`SQLite index ${index.name} is not owned by table ${tableName}.`);
  }
  const columns = db
    .prepare(`PRAGMA index_xinfo(${quoteSqliteIdentifier(index.name)})`)
    .all()
    .map((row) => {
      const column = row as SqliteIndexColumnRow;
      return {
        seqno: column.seqno,
        cid: column.cid,
        name: column.name,
        desc: column.desc,
        coll: column.coll,
        key: column.key,
      };
    });
  return {
    name: index.name,
    unique: index.unique,
    origin: index.origin,
    partial: index.partial,
    columns,
    whereClause: extractWhereClause(schemaRow.sql),
    sql: schemaRow.sql,
  };
}

function collectTableShape(db: DatabaseSync, tableName: string): DurableTableShape {
  const schemaRow = db
    .prepare(
      "SELECT type, name, tbl_name, sql FROM sqlite_schema WHERE type = 'table' AND name = ?",
    )
    .get(tableName) as SqliteSchemaObjectRow | undefined;
  if (!schemaRow?.sql) {
    throw new Error(`SQLite table ${tableName} does not have a canonical schema definition.`);
  }
  const indexes = (
    db
      .prepare(`PRAGMA index_list(${quoteSqliteIdentifier(tableName)})`)
      .all() as SqliteIndexListRow[]
  )
    .map((index) => collectIndexShape(db, tableName, index))
    .toSorted((left, right) => left.name.localeCompare(right.name));
  return {
    columns: collectColumns(db, tableName),
    foreignKeys: collectForeignKeys(db, tableName),
    indexes,
    definition: normalizeSchemaDefinition(schemaRow.sql),
  };
}

function collectExpectedDurableSchemaContract(): DurableSchemaContract {
  const { DatabaseSync } = requireNodeSqlite();
  const db = new DatabaseSync(":memory:");
  try {
    db.exec(DURABLE_RUNTIME_SCHEMA_SQL);
    const tableRows = db
      .prepare(
        `SELECT name
           FROM sqlite_schema
          WHERE type = 'table'
            AND name NOT LIKE 'sqlite_%'
          ORDER BY name`,
      )
      .all() as Array<{ name: string }>;
    if (tableRows.length === 0) {
      throw new Error("Durable runtime schema SQL does not define any tables.");
    }
    return {
      tables: new Map(tableRows.map((row) => [row.name, collectTableShape(db, row.name)] as const)),
    };
  } finally {
    db.close();
  }
}

function getExpectedDurableSchemaContract(): DurableSchemaContract {
  expectedDurableSchemaContract ??= collectExpectedDurableSchemaContract();
  return expectedDurableSchemaContract;
}

function assertSupportedSharedStateSchemaVersion(db: DatabaseSync, pathname?: string): void {
  const userVersion = readSqliteUserVersion(db);
  if (userVersion <= OPENCLAW_STATE_SCHEMA_VERSION) {
    return;
  }
  const databaseLabel = pathname
    ? `OpenClaw state database ${pathname}`
    : "OpenClaw state database";
  throw new Error(
    `${databaseLabel} uses newer schema version ${userVersion}; this OpenClaw build supports ${OPENCLAW_STATE_SCHEMA_VERSION}.`,
  );
}

function collectExpectedSchemaObjects(
  db: DatabaseSync,
  contract: DurableSchemaContract,
): SqliteSchemaObjectRow[] {
  const expectedNames = new Set(contract.tables.keys());
  return (
    db
      .prepare(
        `SELECT type, name, tbl_name, sql
           FROM sqlite_schema
          WHERE name NOT LIKE 'sqlite_%'
          ORDER BY name`,
      )
      .all() as SqliteSchemaObjectRow[]
  ).filter((row) => expectedNames.has(row.name));
}

function assertNoUnknownDurableTables(
  db: DatabaseSync,
  contract: DurableSchemaContract,
  databaseLabel: string,
): void {
  const expectedNames = new Set(contract.tables.keys());
  const unknownTables = (
    db
      .prepare(
        `SELECT name
           FROM sqlite_schema
          WHERE type = 'table'
            AND name NOT LIKE 'sqlite_%'
          ORDER BY name`,
      )
      .all() as Array<{ name: string }>
  )
    .map((row) => row.name)
    .filter(
      (name) =>
        !expectedNames.has(name) &&
        (name.startsWith("durable_") ||
          name.startsWith("wake_obligation_") ||
          name.startsWith("delivery_attempt_") ||
          name.startsWith("uncertainty_")),
    );
  if (unknownTables.length > 0) {
    throw new Error(
      `${databaseLabel} has unknown durable tables for shared schema version ${OPENCLAW_STATE_SCHEMA_VERSION}: ${unknownTables.join(", ")}.`,
    );
  }
}

function assertCompleteDurableTableSet(
  db: DatabaseSync,
  contract: DurableSchemaContract,
  databaseLabel: string,
): "absent" | "complete" {
  assertNoUnknownDurableTables(db, contract, databaseLabel);
  const objects = collectExpectedSchemaObjects(db, contract);
  const wrongObject = objects.find((row) => row.type !== "table");
  if (wrongObject) {
    throw new Error(
      `${databaseLabel} has incompatible durable schema object ${wrongObject.name}: expected table, found ${wrongObject.type}.`,
    );
  }
  const presentNames = new Set(objects.map((row) => row.name));
  if (presentNames.size === 0) {
    return "absent";
  }
  if (presentNames.size !== contract.tables.size) {
    const missingNames = [...contract.tables.keys()].filter((name) => !presentNames.has(name));
    throw new Error(
      `${databaseLabel} has a partial durable runtime schema: found ${presentNames.size} of ${contract.tables.size} tables; missing ${missingNames.join(", ")}.`,
    );
  }
  return "complete";
}

function comparableIndexShape(index: DurableIndexShape): Omit<DurableIndexShape, "sql"> {
  const { sql: _sql, ...shape } = index;
  return shape;
}

function assertCompatibleDurableTables(
  db: DatabaseSync,
  contract: DurableSchemaContract,
  databaseLabel: string,
): string[] {
  const missingIndexSql: string[] = [];
  for (const [tableName, expectedTable] of contract.tables) {
    const actualTable = collectTableShape(db, tableName);
    if (!isDeepStrictEqual(actualTable.columns, expectedTable.columns)) {
      throw new Error(
        `${databaseLabel} has incompatible durable table ${tableName}: column shape differs from this OpenClaw build.`,
      );
    }
    if (!isDeepStrictEqual(actualTable.foreignKeys, expectedTable.foreignKeys)) {
      throw new Error(
        `${databaseLabel} has incompatible durable table ${tableName}: foreign-key shape differs from this OpenClaw build.`,
      );
    }
    if (actualTable.definition !== expectedTable.definition) {
      throw new Error(
        `${databaseLabel} has incompatible durable table ${tableName}: table definition differs from this OpenClaw build.`,
      );
    }

    const expectedImplicitIndexes = expectedTable.indexes
      .filter((index) => index.origin !== "c")
      .map(comparableIndexShape);
    const actualImplicitIndexes = actualTable.indexes
      .filter((index) => index.origin !== "c")
      .map(comparableIndexShape);
    if (!isDeepStrictEqual(actualImplicitIndexes, expectedImplicitIndexes)) {
      throw new Error(
        `${databaseLabel} has incompatible durable table ${tableName}: constraint index shape differs from this OpenClaw build.`,
      );
    }

    const actualIndexesByName = new Map(actualTable.indexes.map((index) => [index.name, index]));
    for (const expectedIndex of expectedTable.indexes.filter((index) => index.origin === "c")) {
      const actualIndex = actualIndexesByName.get(expectedIndex.name);
      if (!actualIndex) {
        const conflictingObject = db
          .prepare("SELECT type, tbl_name FROM sqlite_schema WHERE name = ?")
          .get(expectedIndex.name) as { type: string; tbl_name: string } | undefined;
        if (conflictingObject) {
          throw new Error(
            `${databaseLabel} has incompatible durable index ${expectedIndex.name}: expected owner ${tableName}, found ${conflictingObject.type} on ${conflictingObject.tbl_name}.`,
          );
        }
        if (!expectedIndex.sql) {
          throw new Error(
            `${databaseLabel} is missing durable index ${expectedIndex.name}, but its canonical DDL is unavailable.`,
          );
        }
        missingIndexSql.push(expectedIndex.sql);
        continue;
      }
      if (
        actualIndex.partial !== expectedIndex.partial ||
        actualIndex.whereClause !== expectedIndex.whereClause
      ) {
        throw new Error(
          `${databaseLabel} has incompatible durable partial index ${expectedIndex.name}: predicate shape differs from this OpenClaw build.`,
        );
      }
      if (
        !isDeepStrictEqual(comparableIndexShape(actualIndex), comparableIndexShape(expectedIndex))
      ) {
        throw new Error(
          `${databaseLabel} has incompatible durable index ${expectedIndex.name}: key shape differs from this OpenClaw build.`,
        );
      }
    }
  }
  return missingIndexSql;
}

function assertNoMissingDurableIndexes(missingIndexSql: string[], databaseLabel: string): void {
  if (missingIndexSql.length === 0) {
    return;
  }
  const missingNames = missingIndexSql.map((sql) => {
    const match = /\bINDEX\s+(?:IF\s+NOT\s+EXISTS\s+)?([^\s(]+)/i.exec(sql);
    return match?.[1] ?? "unknown";
  });
  throw new Error(
    `${databaseLabel} is missing required durable indexes: ${missingNames.join(", ")}.`,
  );
}

/** Open an already-installed durable schema without creating or migrating state. */
export function openDurableRuntimeSchemaReadOnly(pathname: string): DatabaseSync {
  if (!existsSync(pathname)) {
    throw new Error(`Durable runtime database ${pathname} is not initialized.`);
  }
  const { DatabaseSync } = requireNodeSqlite();
  const db = new DatabaseSync(pathname, { readOnly: true });
  try {
    assertSupportedSharedStateSchemaVersion(db, pathname);
    const contract = getExpectedDurableSchemaContract();
    const databaseLabel = `Durable runtime database ${pathname}`;
    if (assertCompleteDurableTableSet(db, contract, databaseLabel) === "absent") {
      throw new Error(`${databaseLabel} is not initialized.`);
    }
    assertNoMissingDurableIndexes(
      assertCompatibleDurableTables(db, contract, databaseLabel),
      databaseLabel,
    );
    return db;
  } catch (error) {
    db.close();
    throw error;
  }
}

/** Lazily install or validate the additive durable tables inside the shared state database. */
export function ensureDurableRuntimeSchema(db: DatabaseSync): void {
  runSqliteImmediateTransactionSync(db, () => {
    assertSupportedSharedStateSchemaVersion(db);
    const contract = getExpectedDurableSchemaContract();
    const databaseLabel = "OpenClaw state database";
    const installation = assertCompleteDurableTableSet(db, contract, databaseLabel);
    if (installation === "absent") {
      db.exec(DURABLE_RUNTIME_SCHEMA_SQL);
    } else {
      const missingIndexSql = assertCompatibleDurableTables(db, contract, databaseLabel);
      for (const indexSql of missingIndexSql) {
        db.exec(indexSql);
      }
    }
    if (assertCompleteDurableTableSet(db, contract, databaseLabel) !== "complete") {
      throw new Error(`${databaseLabel} failed to install the durable runtime schema.`);
    }
    assertNoMissingDurableIndexes(
      assertCompatibleDurableTables(db, contract, databaseLabel),
      databaseLabel,
    );
  });
}
