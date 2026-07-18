import { loadSessionEntry } from "../config/sessions/session-accessor.js";
import { requestHeartbeat } from "../infra/heartbeat-wake.js";
import { enqueueSystemEventEntry } from "../infra/system-events.js";

export type SessionAttentionDeliveryResult =
  | {
      status: "queued";
      sessionKey: string;
      sessionId?: string;
      duplicate: boolean;
      queuedAt?: number;
    }
  | { status: "missing"; reason: "invalid_session_key" | "session_not_found" };

/** Session-owner front door for bounded internal attention notices. */
export function requestSessionAttentionDelivery(params: {
  sessionKey: string;
  text: string;
  idempotencyKey: string;
}): SessionAttentionDeliveryResult {
  const sessionKey = params.sessionKey.trim();
  if (!sessionKey) {
    return { status: "missing", reason: "invalid_session_key" };
  }
  const entry = loadSessionEntry({ sessionKey, readConsistency: "latest" });
  if (!entry) {
    return { status: "missing", reason: "session_not_found" };
  }

  const queued = enqueueSystemEventEntry(params.text, {
    sessionKey,
    contextKey: params.idempotencyKey,
  });
  requestHeartbeat({
    source: "other",
    intent: "immediate",
    reason: "durable-attention",
    sessionKey,
  });
  return {
    status: "queued",
    sessionKey,
    ...(entry.sessionId ? { sessionId: entry.sessionId } : {}),
    duplicate: queued === null,
    ...(queued ? { queuedAt: queued.ts } : {}),
  };
}
