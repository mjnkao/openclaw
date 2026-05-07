import { describe, expect, it } from "vitest";
import {
  classifyAicosTool,
  decideAicosToolAccess,
  resolveAicosMcpProfilePolicy,
} from "./aicos-tool-role-policy.js";

describe("aicos tool role policy", () => {
  it("classifies prefixed AICOS MCP tools by canonical operation", () => {
    expect(classifyAicosTool("aicos-x__aicos_get_first_contact")).toBe("low_token_read");
    expect(classifyAicosTool("aicos-x__aicos_get_dashboard_bundle")).toBe("dashboard_read");
    expect(classifyAicosTool("aicos-x__aicos_get_source_ref")).toBe("broad_read");
    expect(classifyAicosTool("aicos-x__aicos_write_project_note")).toBe("protected_write");
    expect(classifyAicosTool("read")).toBe("non_aicos");
  });

  it("keeps A1 on low-token reads and bounded writes only", () => {
    expect(
      decideAicosToolAccess({ actorRole: "A1", toolName: "aicos-x__aicos_get_work_items" }),
    ).toMatchObject({ allowed: true, toolClass: "low_token_read" });
    expect(
      decideAicosToolAccess({ actorRole: "A1", toolName: "aicos-x__aicos_record_checkpoint" }),
    ).toMatchObject({ allowed: true, toolClass: "bounded_write" });
    expect(
      decideAicosToolAccess({ actorRole: "A1", toolName: "aicos-x__aicos_get_document_index" }),
    ).toMatchObject({ allowed: false, toolClass: "broad_read" });
    expect(
      decideAicosToolAccess({ actorRole: "A1", toolName: "aicos-x__aicos_write_project_note" }),
    ).toMatchObject({ allowed: false, toolClass: "protected_write" });
  });

  it("lets A2 use broad/admin/protected AICOS paths", () => {
    expect(
      decideAicosToolAccess({ actorRole: "A2", toolName: "aicos-x__aicos_get_source_ref" }),
    ).toMatchObject({ allowed: true, toolClass: "broad_read" });
    expect(
      decideAicosToolAccess({ actorRole: "A2", toolName: "aicos-x__aicos_import_document" }),
    ).toMatchObject({ allowed: true, toolClass: "protected_write" });
  });

  it("models A3 as a dashboard/low-token projection, not an A2 spoof", () => {
    expect(
      decideAicosToolAccess({ actorRole: "A3", toolName: "aicos-x__aicos_get_dashboard_bundle" }),
    ).toMatchObject({ allowed: true, toolClass: "dashboard_read" });
    expect(
      decideAicosToolAccess({ actorRole: "A3", toolName: "aicos-x__aicos_get_startup_bundle" }),
    ).toMatchObject({ allowed: true, toolClass: "low_token_read" });
    expect(
      decideAicosToolAccess({ actorRole: "A3", toolName: "aicos-x__aicos_get_source_ref" }),
    ).toMatchObject({ allowed: false, toolClass: "broad_read" });
    expect(
      decideAicosToolAccess({ actorRole: "A3", toolName: "aicos-x__aicos_write_project_note" }),
    ).toMatchObject({ allowed: false, toolClass: "protected_write" });
  });

  it("adds A1-safe AICOS deny globs to bundled MCP coding/messaging profiles", () => {
    expect(resolveAicosMcpProfilePolicy("coding").deny).toEqual(
      expect.arrayContaining([
        "aicos_get_document_index",
        "*__aicos_get_document_index",
        "aicos_write_project_note",
        "*__aicos_write_project_note",
      ]),
    );
    expect(resolveAicosMcpProfilePolicy("messaging").deny).toContain("*__aicos_get_source_ref");
    expect(resolveAicosMcpProfilePolicy("full").deny).toBeUndefined();
  });
});
