// Durable runtime gateway methods expose coordination projections to operator surfaces.
import {
  ErrorCodes,
  errorShape,
  validateDeliveryAttemptEvidenceListParams,
  validateDurableCoordinationGetParams,
  validateDurableHealthGetParams,
  validateDurableLimitParams,
  validateUncertaintyFactResolveParams,
  validateWakeObligationControlParams,
  validateWakeObligationIdParams,
  validateWakeObligationSupersedeParams,
  type DurableCoordinationGetResult,
  type DurableObligationsListResult,
  type DurableHealthResult,
  type UncertaintyFactResolveResult,
  type WakeObligationControlResult,
  type WakeObligationListResult,
  type WakeObligationInspectResult,
  type UncertaintyFactListResult,
  type DeliveryAttemptEvidenceListResult,
} from "../../../packages/gateway-protocol/src/index.js";
import { isDurableAuthorityEnabled, isDurableRuntimesEnabled } from "../../durable/config.js";
import { buildDurableCoordinationProjection } from "../../durable/coordination-projection.js";
import { getDurableRuntimeHealthSnapshot } from "../../durable/health.js";
import { openDurableRuntimeStore } from "../../durable/store-factory.js";
import type { GatewayClient, GatewayRequestHandlers } from "./types.js";

function durableActorRef(client?: GatewayClient | null): string {
  const deviceId = client?.connect?.device?.id?.trim();
  if (deviceId) {
    return `device:${deviceId}`;
  }
  const clientId = client?.connect?.client?.id;
  const instanceId = client?.connect?.client?.instanceId?.trim();
  const connId = client?.connId?.trim();
  return ["gateway-client", clientId, instanceId ?? connId].filter(Boolean).join(":");
}

function respondMutationFailure(
  respond: Parameters<GatewayRequestHandlers[string]>[0]["respond"],
  subject: string,
): void {
  respond(
    false,
    undefined,
    errorShape(
      ErrorCodes.INVALID_REQUEST,
      `${subject} was not found or cannot make that transition.`,
    ),
  );
}

export const durableHandlers: GatewayRequestHandlers = {
  "durable.health.get": ({ params, respond }) => {
    if (!validateDurableHealthGetParams(params)) {
      respond(false, undefined, errorShape(ErrorCodes.INVALID_REQUEST, "Invalid health request."));
      return;
    }
    const enabled = isDurableRuntimesEnabled();
    const result: DurableHealthResult = {
      enabled,
      authority: isDurableAuthorityEnabled(),
      process: getDurableRuntimeHealthSnapshot(),
    };
    if (!enabled) {
      respond(true, result);
      return;
    }
    const store = openDurableRuntimeStore();
    try {
      result.store = store.getStats();
      respond(true, result);
    } finally {
      store.close();
    }
  },
  "durable.coordination.get": ({ params, respond }) => {
    if (!isDurableRuntimesEnabled()) {
      respond(
        false,
        undefined,
        errorShape(ErrorCodes.INVALID_REQUEST, "Durable runtime is disabled."),
      );
      return;
    }
    if (!validateDurableCoordinationGetParams(params)) {
      respond(
        false,
        undefined,
        errorShape(ErrorCodes.INVALID_REQUEST, "runtimeRunId is required."),
      );
      return;
    }
    const { runtimeRunId } = params;
    const store = openDurableRuntimeStore();
    try {
      const run = store.getRun(runtimeRunId);
      if (!run) {
        respond(
          false,
          undefined,
          errorShape(ErrorCodes.INVALID_REQUEST, `durable runtime run not found: ${runtimeRunId}`),
        );
        return;
      }
      const result: DurableCoordinationGetResult = {
        projection: buildDurableCoordinationProjection({
          run,
          steps: store.listSteps(runtimeRunId),
          childLinks: store.listChildLinks(runtimeRunId),
          refs: store.listRefs(runtimeRunId),
        }),
      };
      respond(true, result);
    } finally {
      store.close();
    }
  },
  "durable.obligations.list": ({ params, respond }) => {
    if (!isDurableRuntimesEnabled()) {
      respond(
        false,
        undefined,
        errorShape(ErrorCodes.INVALID_REQUEST, "Durable runtime is disabled."),
      );
      return;
    }
    if (!validateDurableLimitParams(params)) {
      respond(false, undefined, errorShape(ErrorCodes.INVALID_REQUEST, "Invalid durable limit."));
      return;
    }
    const store = openDurableRuntimeStore();
    try {
      const result: DurableObligationsListResult = {
        obligations: store.listUnresolvedObligations({ limit: params.limit }),
      };
      respond(true, result);
    } finally {
      store.close();
    }
  },
  "durable.wakes.list": ({ params, respond }) => {
    if (!isDurableRuntimesEnabled()) {
      respond(
        false,
        undefined,
        errorShape(ErrorCodes.INVALID_REQUEST, "Durable runtime is disabled."),
      );
      return;
    }
    if (!validateDurableLimitParams(params)) {
      respond(false, undefined, errorShape(ErrorCodes.INVALID_REQUEST, "Invalid durable limit."));
      return;
    }
    const store = openDurableRuntimeStore();
    try {
      const result: WakeObligationListResult = {
        wakes: store.listWakeObligations({ limit: params.limit }),
      };
      respond(true, result);
    } finally {
      store.close();
    }
  },
  "durable.wakes.inspect": ({ params, respond }) => {
    if (!isDurableRuntimesEnabled()) {
      respond(
        false,
        undefined,
        errorShape(ErrorCodes.INVALID_REQUEST, "Durable runtime is disabled."),
      );
      return;
    }
    if (!validateWakeObligationIdParams(params)) {
      respond(false, undefined, errorShape(ErrorCodes.INVALID_REQUEST, "wakeId is required."));
      return;
    }
    const store = openDurableRuntimeStore();
    try {
      const inspection = store.getWakeObligationInspection(params.wakeId);
      if (!inspection) {
        respond(
          false,
          undefined,
          errorShape(ErrorCodes.INVALID_REQUEST, `wake obligation not found: ${params.wakeId}`),
        );
        return;
      }
      const result: WakeObligationInspectResult = { inspection };
      respond(true, result);
    } finally {
      store.close();
    }
  },
  "durable.wakes.acknowledge": ({ params, respond, client }) => {
    if (!isDurableRuntimesEnabled()) {
      respond(
        false,
        undefined,
        errorShape(ErrorCodes.INVALID_REQUEST, "Durable runtime is disabled."),
      );
      return;
    }
    if (!validateWakeObligationControlParams(params)) {
      respond(
        false,
        undefined,
        errorShape(ErrorCodes.INVALID_REQUEST, "Invalid wake acknowledgement."),
      );
      return;
    }
    const store = openDurableRuntimeStore();
    try {
      const wake = store.acknowledgeWakeObligation({
        ...params,
        actorKind: "operator",
        actorRef: durableActorRef(client),
      });
      if (!wake) {
        respondMutationFailure(respond, `wake obligation ${params.wakeId}`);
        return;
      }
      const result: WakeObligationControlResult = { wake };
      respond(true, result);
    } finally {
      store.close();
    }
  },
  "durable.wakes.resume": ({ params, respond, client }) => {
    if (!isDurableRuntimesEnabled()) {
      respond(
        false,
        undefined,
        errorShape(ErrorCodes.INVALID_REQUEST, "Durable runtime is disabled."),
      );
      return;
    }
    if (!validateWakeObligationControlParams(params)) {
      respond(false, undefined, errorShape(ErrorCodes.INVALID_REQUEST, "Invalid wake resume."));
      return;
    }
    const store = openDurableRuntimeStore();
    try {
      const wake = store.resumeWakeObligation({
        ...params,
        actorKind: "operator",
        actorRef: durableActorRef(client),
      });
      if (!wake) {
        respondMutationFailure(respond, `wake obligation ${params.wakeId}`);
        return;
      }
      const result: WakeObligationControlResult = { wake };
      respond(true, result);
    } finally {
      store.close();
    }
  },
  "durable.wakes.supersede": ({ params, respond, client }) => {
    if (!isDurableRuntimesEnabled()) {
      respond(
        false,
        undefined,
        errorShape(ErrorCodes.INVALID_REQUEST, "Durable runtime is disabled."),
      );
      return;
    }
    if (!validateWakeObligationSupersedeParams(params)) {
      respond(
        false,
        undefined,
        errorShape(ErrorCodes.INVALID_REQUEST, "Invalid wake supersede request."),
      );
      return;
    }
    const store = openDurableRuntimeStore();
    try {
      const wake = store.supersedeWakeObligation({
        ...params,
        actorKind: "operator",
        actorRef: durableActorRef(client),
      });
      if (!wake) {
        respondMutationFailure(respond, `wake obligation ${params.wakeId}`);
        return;
      }
      const result: WakeObligationControlResult = { wake };
      respond(true, result);
    } finally {
      store.close();
    }
  },
  "durable.uncertainty.list": ({ params, respond }) => {
    if (!isDurableRuntimesEnabled()) {
      respond(
        false,
        undefined,
        errorShape(ErrorCodes.INVALID_REQUEST, "Durable runtime is disabled."),
      );
      return;
    }
    if (!validateDurableLimitParams(params)) {
      respond(false, undefined, errorShape(ErrorCodes.INVALID_REQUEST, "Invalid durable limit."));
      return;
    }
    const store = openDurableRuntimeStore();
    try {
      const result: UncertaintyFactListResult = {
        uncertaintyFacts: store.listUnresolvedUncertaintyFacts({ limit: params.limit }),
      };
      respond(true, result);
    } finally {
      store.close();
    }
  },
  "durable.uncertainty.resolve": ({ params, respond, client }) => {
    if (!isDurableRuntimesEnabled()) {
      respond(
        false,
        undefined,
        errorShape(ErrorCodes.INVALID_REQUEST, "Durable runtime is disabled."),
      );
      return;
    }
    if (!validateUncertaintyFactResolveParams(params)) {
      respond(
        false,
        undefined,
        errorShape(ErrorCodes.INVALID_REQUEST, "Invalid uncertainty resolution."),
      );
      return;
    }
    const store = openDurableRuntimeStore();
    try {
      const uncertaintyFact = store.resolveUncertaintyFact({
        ...params,
        metadata: {
          ...params.metadata,
          decision: {
            actorKind: "operator",
            actorRef: durableActorRef(client),
            decidedAt: Date.now(),
          },
        },
      });
      if (!uncertaintyFact) {
        respondMutationFailure(respond, `uncertainty fact ${params.factId}`);
        return;
      }
      const result: UncertaintyFactResolveResult = { uncertaintyFact };
      respond(true, result);
    } finally {
      store.close();
    }
  },
  "durable.delivery-attempts.list": ({ params, respond }) => {
    if (!isDurableRuntimesEnabled()) {
      respond(
        false,
        undefined,
        errorShape(ErrorCodes.INVALID_REQUEST, "Durable runtime is disabled."),
      );
      return;
    }
    if (!validateDeliveryAttemptEvidenceListParams(params)) {
      respond(false, undefined, errorShape(ErrorCodes.INVALID_REQUEST, "wakeId is required."));
      return;
    }
    const store = openDurableRuntimeStore();
    try {
      const result: DeliveryAttemptEvidenceListResult = {
        deliveryAttemptEvidence: store.listDeliveryAttemptEvidence({
          wakeId: params.wakeId,
          limit: params.limit,
        }),
      };
      respond(true, result);
    } finally {
      store.close();
    }
  },
};
