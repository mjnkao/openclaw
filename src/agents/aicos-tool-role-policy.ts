import { normalizeLowercaseStringOrEmpty } from "../shared/string-coerce.js";

export type AicosActorRole = "A1" | "A2" | "A3";

export type AicosToolClass =
  | "non_aicos"
  | "low_token_read"
  | "dashboard_read"
  | "broad_read"
  | "bounded_write"
  | "protected_write"
  | "admin_debug";

export type AicosToolPolicyDecision = {
  allowed: boolean;
  toolClass: AicosToolClass;
  reason?: string;
};

type ToolProfilePolicy = {
  allow?: string[];
  deny?: string[];
};

export const AICOS_LOW_TOKEN_READ_TOOLS = [
  "aicos_get_first_contact",
  "aicos_get_startup_bundle",
  "aicos_get_work_items",
  "aicos_get_task_packet",
  "aicos_get_packet_index",
  "aicos_get_status_items",
  "aicos_get_agent_sessions",
  "aicos_get_feedback_digest",
  "aicos_get_project_health",
  "aicos_get_workstream_index",
  "aicos_query_project_context",
] as const;

export const AICOS_DASHBOARD_READ_TOOLS = ["aicos_get_dashboard_bundle"] as const;

export const AICOS_BROAD_READ_TOOLS = [
  "aicos_get_context_registry",
  "aicos_get_document",
  "aicos_get_document_index",
  "aicos_get_handoff_current",
  "aicos_get_project_registry",
  "aicos_get_shared_index",
  "aicos_get_source_ref",
] as const;

export const AICOS_BOUNDED_WRITE_TOOLS = [
  "aicos_record_agent_session",
  "aicos_record_checkpoint",
  "aicos_record_feedback",
  "aicos_register_artifact_ref",
  "aicos_write_handoff_update",
  "aicos_write_task_update",
  "aicos_update_status_item",
  "aicos_upsert_work_item",
] as const;

export const AICOS_PROTECTED_WRITE_TOOLS = [
  "aicos_import_document",
  "aicos_propose_project",
  "aicos_write_project_note",
  "aicos_write_shared_note",
] as const;

export const AICOS_ADMIN_DEBUG_TOOLS = ["aicos_get_help"] as const;

const LOW_TOKEN_READS = new Set<string>(AICOS_LOW_TOKEN_READ_TOOLS);
const DASHBOARD_READS = new Set<string>(AICOS_DASHBOARD_READ_TOOLS);
const BROAD_READS = new Set<string>(AICOS_BROAD_READ_TOOLS);
const BOUNDED_WRITES = new Set<string>(AICOS_BOUNDED_WRITE_TOOLS);
const PROTECTED_WRITES = new Set<string>(AICOS_PROTECTED_WRITE_TOOLS);
const ADMIN_DEBUG_READS = new Set<string>(AICOS_ADMIN_DEBUG_TOOLS);

function canonicalAicosToolName(toolName: string): string | undefined {
  const normalized = normalizeLowercaseStringOrEmpty(toolName);
  const marker = "__aicos_";
  const markerIndex = normalized.indexOf(marker);
  if (markerIndex >= 0) {
    return normalized.slice(markerIndex + 2);
  }
  if (normalized.startsWith("aicos_")) {
    return normalized;
  }
  return undefined;
}

export function classifyAicosTool(toolName: string): AicosToolClass {
  const canonical = canonicalAicosToolName(toolName);
  if (!canonical) {
    return "non_aicos";
  }
  if (LOW_TOKEN_READS.has(canonical)) {
    return "low_token_read";
  }
  if (DASHBOARD_READS.has(canonical)) {
    return "dashboard_read";
  }
  if (BROAD_READS.has(canonical)) {
    return "broad_read";
  }
  if (BOUNDED_WRITES.has(canonical)) {
    return "bounded_write";
  }
  if (PROTECTED_WRITES.has(canonical)) {
    return "protected_write";
  }
  if (ADMIN_DEBUG_READS.has(canonical)) {
    return "admin_debug";
  }
  if (canonical.startsWith("aicos_get_") || canonical.startsWith("aicos_query_")) {
    return "broad_read";
  }
  if (
    canonical.startsWith("aicos_write_") ||
    canonical.startsWith("aicos_record_") ||
    canonical.startsWith("aicos_update_") ||
    canonical.startsWith("aicos_upsert_") ||
    canonical.startsWith("aicos_import_")
  ) {
    return "protected_write";
  }
  return "admin_debug";
}

export function decideAicosToolAccess(params: {
  actorRole: AicosActorRole;
  toolName: string;
}): AicosToolPolicyDecision {
  const toolClass = classifyAicosTool(params.toolName);
  if (toolClass === "non_aicos") {
    return { allowed: true, toolClass };
  }
  if (params.actorRole === "A2") {
    return { allowed: true, toolClass };
  }
  if (params.actorRole === "A3") {
    if (toolClass === "dashboard_read" || toolClass === "low_token_read") {
      return { allowed: true, toolClass };
    }
    return { allowed: false, toolClass, reason: "a3_dashboard_projection_only" };
  }
  if (toolClass === "low_token_read" || toolClass === "bounded_write") {
    return { allowed: true, toolClass };
  }
  return { allowed: false, toolClass, reason: "a1_low_token_read_or_bounded_write_only" };
}

function aicosPrefixedGlob(canonicalToolName: string): string {
  return `*__${canonicalToolName}`;
}

const A1_DENIED_CANONICAL_TOOLS = [
  ...AICOS_DASHBOARD_READ_TOOLS,
  ...AICOS_BROAD_READ_TOOLS,
  ...AICOS_PROTECTED_WRITE_TOOLS,
  ...AICOS_ADMIN_DEBUG_TOOLS,
];

/**
 * Profile-layer AICOS MCP contract:
 * - coding/messaging expose bundled MCP, but keep A1-safe AICOS surfaces only.
 * - full is the explicit A2-capable escape hatch and does not add AICOS denies.
 * - A3 is a dashboard projection role; runtime code should use decideAicosToolAccess.
 */
export function resolveAicosMcpProfilePolicy(profile?: string): ToolProfilePolicy {
  if (profile === "coding" || profile === "messaging") {
    return {
      deny: [...A1_DENIED_CANONICAL_TOOLS, ...A1_DENIED_CANONICAL_TOOLS.map(aicosPrefixedGlob)],
    };
  }
  return { deny: undefined };
}
