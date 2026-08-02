import { describe, expect, it } from "vitest";
import type { SessionEntry } from "../../config/sessions.js";
import {
  assertExpectedExistingSession,
  ExpectedExistingSessionChangedError,
  resolveExpectedExistingSessionConstraint,
} from "./agent-expected-session.js";

const entry: SessionEntry = {
  sessionId: "session-1",
  lifecycleRevision: "revision-1",
  updatedAt: 1,
};

describe("expected existing session constraints", () => {
  it("binds backend work to both session and lifecycle identities", () => {
    expect(
      resolveExpectedExistingSessionConstraint({
        canUseInternalRuntimeHandoff: true,
        expectedExistingSessionId: "session-1",
        expectedLifecycleRevision: "revision-1",
      }),
    ).toEqual({
      ok: true,
      constraint: {
        sessionId: "session-1",
        lifecycleRevision: "revision-1",
      },
    });
  });

  it("requires a session identity for a lifecycle constraint", () => {
    expect(
      resolveExpectedExistingSessionConstraint({
        canUseInternalRuntimeHandoff: true,
        expectedLifecycleRevision: "revision-1",
      }),
    ).toEqual({
      ok: false,
      error: "expectedLifecycleRevision requires expectedExistingSessionId.",
    });
  });

  it("keeps the expected-session contract backend-only", () => {
    expect(
      resolveExpectedExistingSessionConstraint({
        canUseInternalRuntimeHandoff: false,
        expectedExistingSessionId: "session-1",
        expectedLifecycleRevision: "revision-1",
      }),
    ).toEqual({
      ok: false,
      error: "expectedExistingSessionId is reserved for backend callers.",
    });
  });

  it("accepts an exact owner and rejects a reset that retains the session id", () => {
    const constraint = {
      sessionId: "session-1",
      lifecycleRevision: "revision-1",
    };

    expect(() =>
      assertExpectedExistingSession({ constraint, entry, message: "changed" }),
    ).not.toThrow();
    expect(() =>
      assertExpectedExistingSession({
        constraint,
        entry: { ...entry, lifecycleRevision: "revision-2" },
        message: "changed",
      }),
    ).toThrow(ExpectedExistingSessionChangedError);
  });

  it("preserves legacy session-id-only constraints", () => {
    expect(() =>
      assertExpectedExistingSession({
        constraint: { sessionId: "session-1" },
        entry,
        message: "changed",
      }),
    ).not.toThrow();
  });
});
