import type { DeliveryContext } from "../utils/delivery-context.types.js";

export type SystemEvent = {
  text: string;
  ts: number;
  contextKey?: string | null;
  deliveryContext?: DeliveryContext;
  deliveryQueueIds?: string[];
};
