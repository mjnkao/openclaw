import { existsSync } from "node:fs";
import { pathToFileURL } from "node:url";
import { openNodeSqliteDatabase } from "../infra/node-sqlite.js";
import { assertSupportedSchemaVersion } from "./openclaw-state-db-maintenance.js";

/** Inspect the main image without joining recovery or writer locks. */
export function assertOpenClawStateSchemaVersionBeforeOpen(
  pathname: string,
  onUnsupportedVersion: (pathname: string, error: Error) => unknown,
): void {
  if (!existsSync(pathname)) {
    return;
  }
  const database = openNodeSqliteDatabase(`${pathToFileURL(pathname).href}?mode=ro&immutable=1`, {
    readOnly: true,
  });
  try {
    assertSupportedSchemaVersion(database, pathname);
  } catch (error) {
    if (error instanceof Error && error.name === "SqliteSchemaVersionError") {
      onUnsupportedVersion(pathname, error);
    }
    throw error;
  } finally {
    database.close();
  }
}
