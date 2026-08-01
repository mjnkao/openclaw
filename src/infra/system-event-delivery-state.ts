import { normalizeOptionalString } from "@openclaw/normalization-core/string-coerce";
import { resolveGlobalMap } from "../shared/global-singleton.js";
import type { SystemEvent } from "./system-event-types.js";

const CONSUMED_SYSTEM_EVENT_DELIVERIES_KEY = Symbol.for("openclaw.systemEvents.consumedDeliveries");
const SYSTEM_EVENT_DELIVERY_INSPECTORS_KEY = Symbol.for("openclaw.systemEvents.deliveryInspectors");
const REJECTED_SYSTEM_EVENT_DELIVERIES_KEY = Symbol.for("openclaw.systemEvents.rejectedDeliveries");

const consumedDeliveryQueueIds = resolveGlobalMap<string, Set<string>>(
  CONSUMED_SYSTEM_EVENT_DELIVERIES_KEY,
);

export type SystemEventDeliveryInspection = "allow" | "reject" | undefined;

type SystemEventDeliveryInspector = (params: {
  sessionKey: string;
  deliveryQueueId: string;
}) => SystemEventDeliveryInspection;

const deliveryInspectors = resolveGlobalMap<string, SystemEventDeliveryInspector>(
  SYSTEM_EVENT_DELIVERY_INSPECTORS_KEY,
);
const rejectedDeliveryQueueIds = resolveGlobalMap<string, number>(
  REJECTED_SYSTEM_EVENT_DELIVERIES_KEY,
);

const MAX_REJECTED_DELIVERY_QUEUE_IDS = 4_096;
const REJECTED_DELIVERY_QUEUE_ID_TTL_MS = 15 * 60_000;

function requireSessionKey(key?: string | null): string {
  const trimmed = normalizeOptionalString(key) ?? "";
  if (!trimmed) {
    throw new Error("system events require a sessionKey");
  }
  return trimmed;
}

function cloneSystemEvent(event: SystemEvent): SystemEvent {
  return {
    ...event,
    ...(event.deliveryContext ? { deliveryContext: { ...event.deliveryContext } } : {}),
    ...(event.deliveryQueueIds ? { deliveryQueueIds: [...event.deliveryQueueIds] } : {}),
  };
}

/** Register an owner-side gate that runs before a persisted event can enter a prompt. */
export function registerSystemEventDeliveryInspector(
  owner: string,
  inspector: SystemEventDeliveryInspector,
): () => void {
  const key = normalizeOptionalString(owner) ?? "";
  if (!key) {
    throw new Error("system event delivery inspectors require an owner");
  }
  deliveryInspectors.set(key, inspector);
  return () => {
    if (deliveryInspectors.get(key) === inspector) {
      deliveryInspectors.delete(key);
    }
  };
}

function rememberRejectedSystemEventDelivery(deliveryQueueId: string): void {
  rejectedDeliveryQueueIds.delete(deliveryQueueId);
  rejectedDeliveryQueueIds.set(deliveryQueueId, Date.now());
  while (rejectedDeliveryQueueIds.size > MAX_REJECTED_DELIVERY_QUEUE_IDS) {
    const oldest = rejectedDeliveryQueueIds.keys().next().value;
    if (typeof oldest !== "string") {
      break;
    }
    rejectedDeliveryQueueIds.delete(oldest);
  }
}

export function isSystemEventDeliveryAllowed(sessionKey: string, deliveryQueueId: string): boolean {
  const rejectedAt = rejectedDeliveryQueueIds.get(deliveryQueueId);
  if (rejectedAt !== undefined) {
    if (Date.now() - rejectedAt <= REJECTED_DELIVERY_QUEUE_ID_TTL_MS) {
      return false;
    }
    rejectedDeliveryQueueIds.delete(deliveryQueueId);
  }
  for (const inspector of deliveryInspectors.values()) {
    try {
      if (inspector({ sessionKey, deliveryQueueId }) === "reject") {
        rememberRejectedSystemEventDelivery(deliveryQueueId);
        return false;
      }
    } catch {
      // An owner inspection failure must never expose its persisted content.
      rememberRejectedSystemEventDelivery(deliveryQueueId);
      return false;
    }
  }
  return true;
}

/** Fence retired owner delivery state, including prompt snapshots already captured in RAM. */
export function rejectSystemEventDeliveryQueueId(deliveryQueueId: string): void {
  const id = normalizeOptionalString(deliveryQueueId);
  if (id) {
    rememberRejectedSystemEventDelivery(id);
  }
}

/** Revalidate a captured queue snapshot immediately before model input assembly. */
export function filterSystemEventEntriesForPrompt(
  sessionKey: string,
  events: readonly SystemEvent[],
): SystemEvent[] {
  const key = requireSessionKey(sessionKey);
  const filtered: SystemEvent[] = [];
  for (const event of events) {
    const deliveryQueueIds = event.deliveryQueueIds;
    if (!deliveryQueueIds || deliveryQueueIds.length === 0) {
      filtered.push(cloneSystemEvent(event));
      continue;
    }
    const allowedIds = deliveryQueueIds.filter((deliveryQueueId) =>
      isSystemEventDeliveryAllowed(key, deliveryQueueId),
    );
    if (allowedIds.length > 0) {
      filtered.push(cloneSystemEvent({ ...event, deliveryQueueIds: allowedIds }));
    }
  }
  return filtered;
}

export function recordConsumedSystemEventDeliveryQueueIds(
  sessionKey: string,
  events: readonly SystemEvent[],
): void {
  const ids = events.flatMap((event) => event.deliveryQueueIds ?? []);
  if (ids.length === 0) {
    return;
  }
  const key = requireSessionKey(sessionKey);
  const pending = consumedDeliveryQueueIds.get(key) ?? new Set<string>();
  for (const id of ids) {
    pending.add(id);
  }
  consumedDeliveryQueueIds.set(key, pending);
}

/** Queue ids whose events crossed into an attached session prompt. */
export function peekConsumedSystemEventDeliveryQueueIds(sessionKey: string): string[] {
  return [...(consumedDeliveryQueueIds.get(requireSessionKey(sessionKey)) ?? [])];
}

/** Forget queue ids only after their attached-session agent run succeeds. */
export function forgetConsumedSystemEventDeliveryQueueIds(
  sessionKey: string,
  acknowledgedIds: readonly string[],
): void {
  const key = requireSessionKey(sessionKey);
  const pending = consumedDeliveryQueueIds.get(key);
  if (!pending) {
    return;
  }
  for (const id of acknowledgedIds) {
    pending.delete(id);
  }
  if (pending.size === 0) {
    consumedDeliveryQueueIds.delete(key);
  }
}

/** Release in-flight queue ids after an attached-session run fails before acknowledgement. */
export function releaseConsumedSystemEventDeliveryQueueIds(sessionKey: string): void {
  consumedDeliveryQueueIds.delete(requireSessionKey(sessionKey));
}

export function resetSystemEventDeliveryStateForTest(): void {
  consumedDeliveryQueueIds.clear();
  rejectedDeliveryQueueIds.clear();
}
