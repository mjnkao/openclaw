import type { Command } from "commander";
import { durableCommand, type DurableCliAction } from "../../commands/durable.js";
import { defaultRuntime } from "../../runtime.js";

type DurableCliCommanderOptions = {
  json?: boolean;
  limit?: string;
  reason?: string;
  expectedSourceRevision?: string;
  kind?: string;
  ref?: string;
  status?: "resolved" | "superseded";
  expectedUpdatedAt?: string;
};

function parseLimit(value: string | undefined): number | undefined {
  if (value === undefined) {
    return undefined;
  }
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed < 1) {
    defaultRuntime.error("--limit must be a positive integer.");
    defaultRuntime.exit(1);
    return undefined;
  }
  return parsed;
}

function addCommonOptions(command: Command): Command {
  return command.option("--json", "Output JSON instead of text", false);
}

function parseNonNegativeInteger(
  value: string | undefined,
  optionName: string,
): number | undefined {
  if (value === undefined) {
    return undefined;
  }
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed < 0) {
    defaultRuntime.error(`${optionName} must be a non-negative integer.`);
    defaultRuntime.exit(1);
    return undefined;
  }
  return parsed;
}

async function runDurableAction(
  action: DurableCliAction,
  runtimeRunId: string | undefined,
  opts: DurableCliCommanderOptions,
): Promise<void> {
  await durableCommand(
    {
      action,
      runtimeRunId,
      json: Boolean(opts.json),
      limit: parseLimit(opts.limit),
      reason: opts.reason,
      expectedSourceRevision: opts.expectedSourceRevision,
      resolutionKind: opts.kind,
      resolutionRef: opts.ref,
      resolutionStatus: opts.status,
      expectedUpdatedAt: parseNonNegativeInteger(opts.expectedUpdatedAt, "--expected-updated-at"),
    },
    defaultRuntime,
  );
}

export function registerDurableCommand(program: Command) {
  const durable = program
    .command("durable")
    .description("Inspect native durable runtime runs, timelines, and coordination state");

  addCommonOptions(durable.command("stats").description("Show durable runtime store stats")).action(
    async (opts) => {
      await runDurableAction("stats", undefined, opts);
    },
  );

  addCommonOptions(
    durable.command("health").description("Show durable runtime authority health"),
  ).action(async (opts) => {
    await runDurableAction("health", undefined, opts);
  });

  addCommonOptions(
    durable
      .command("runs")
      .description("List recent durable runtime runs")
      .option("--limit <count>", "Maximum runs to show", "50"),
  ).action(async (opts) => {
    await runDurableAction("runs", undefined, opts);
  });

  const obligations = durable.command("obligations").description("Inspect unresolved obligations");
  addCommonOptions(
    obligations
      .command("list")
      .description("List unresolved durable obligations")
      .option("--limit <count>", "Maximum records to show", "50"),
  ).action(async (opts) => {
    await runDurableAction("obligations", undefined, opts);
  });

  const wakes = durable.command("wakes").description("Inspect durable wake obligations");
  addCommonOptions(
    wakes
      .command("list")
      .description("List durable wake obligations")
      .option("--limit <count>", "Maximum records to show", "50"),
  ).action(async (opts) => {
    await runDurableAction("wakes", undefined, opts);
  });
  addCommonOptions(
    wakes.command("inspect <wakeId>").description("Inspect one durable wake obligation"),
  ).action(async (wakeId: string, opts) => {
    await runDurableAction("wake", wakeId, opts);
  });
  for (const [name, action, description] of [
    ["acknowledge", "wake-acknowledge", "Acknowledge that the wake obligation was consumed"],
    ["resume", "wake-resume", "Resume a suspended wake obligation"],
    ["supersede", "wake-supersede", "Supersede a wake obligation with an operator decision"],
  ] as const) {
    addCommonOptions(
      wakes
        .command(`${name} <wakeId>`)
        .description(description)
        .option("--reason <text>")
        .option("--expected-source-revision <revision>"),
    ).action(async (wakeId: string, opts) => {
      await runDurableAction(action, wakeId, opts);
    });
  }

  const uncertainty = durable.command("uncertainty").description("Inspect uncertainty facts");
  addCommonOptions(
    uncertainty
      .command("list")
      .description("List unresolved durable uncertainty facts")
      .option("--limit <count>", "Maximum records to show", "50"),
  ).action(async (opts) => {
    await runDurableAction("uncertainty", undefined, opts);
  });
  addCommonOptions(
    uncertainty
      .command("resolve <factId>")
      .description("Resolve or supersede one uncertainty fact")
      .requiredOption("--kind <kind>", "Resolution kind")
      .option("--ref <reference>", "Resolution reference")
      .option("--expected-updated-at <timestamp>", "Reject a stale uncertainty revision")
      .option("--status <status>", "resolved or superseded", "resolved"),
  ).action(async (factId: string, opts) => {
    if (opts.status !== "resolved" && opts.status !== "superseded") {
      defaultRuntime.error("--status must be resolved or superseded.");
      defaultRuntime.exit(1);
      return;
    }
    await runDurableAction("uncertainty-resolve", factId, opts);
  });

  const deliveryAttempts = durable
    .command("delivery-attempts")
    .description("Inspect delivery attempt evidence");
  addCommonOptions(
    deliveryAttempts
      .command("list <wakeId>")
      .description("List delivery attempt evidence for one wake obligation")
      .option("--limit <count>", "Maximum attempts to show", "50"),
  ).action(async (wakeId: string, opts) => {
    await runDurableAction("delivery-attempts", wakeId, opts);
  });

  for (const [name, action, description] of [
    ["show", "show", "Show a durable runtime run with steps, links, signals, and timeline"],
    ["timeline", "timeline", "Show durable runtime events for one run"],
    ["steps", "steps", "Show durable runtime steps for one run"],
    ["children", "children", "Show child runtime links for one run"],
    ["parents", "parents", "Show parent runtime links for one run"],
    ["why", "why", "Explain why a durable runtime run is quiet or what state it is in"],
    [
      "coordination",
      "coordination",
      "Show durable coordination projection for task/session runtime consumers",
    ],
    ["signals", "signals", "Show durable runtime signals for one run"],
    ["refs", "refs", "Show durable runtime state refs for one run"],
    ["timers", "timers", "Show durable runtime timers for one run"],
  ] as const) {
    addCommonOptions(durable.command(`${name} <runtimeRunId>`).description(description)).action(
      async (runtimeRunId: string, opts) => {
        await runDurableAction(action, runtimeRunId, opts);
      },
    );
  }
}
