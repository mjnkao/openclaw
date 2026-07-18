import { Command } from "commander";
import { describe, expect, it } from "vitest";
import { registerDurableCommand } from "./register.durable.js";

function command(parent: Command, name: string): Command {
  const match = parent.commands.find((candidate) => candidate.name() === name);
  expect(match, `missing command: ${name}`).toBeDefined();
  return match!;
}

describe("registerDurableCommand", () => {
  it("registers source-oriented inspection resources with explicit actions", () => {
    const program = new Command();
    registerDurableCommand(program);

    const durable = command(program, "durable");
    expect(command(durable, "obligations").commands.map((child) => child.name())).toEqual(["list"]);
    expect(command(durable, "wakes").commands.map((child) => child.name())).toEqual([
      "list",
      "inspect",
      "acknowledge",
      "resume",
      "supersede",
    ]);
    expect(command(durable, "uncertainty").commands.map((child) => child.name())).toEqual([
      "list",
      "resolve",
    ]);
    expect(command(durable, "delivery-attempts").commands.map((child) => child.name())).toEqual([
      "list",
    ]);
    expect(durable.commands.map((child) => child.name())).not.toContain("wake");
    expect(durable.commands.map((child) => child.name())).toContain("health");
  });
});
