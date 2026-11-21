import { describe, expect, it } from "vite-plus/test";
import { buildBreadcrumb, dedupeSearchMatches } from "./semanggiSearchModel";

const m = (over: Record<string, unknown>) =>
  ({
    sessionId: "s1",
    threadId: "t1",
    projectId: "p1",
    mode: "work",
    title: "T",
    source: "assistant",
    freshness: "fresh",
    ...over,
  }) as never;

describe("dedupeSearchMatches", () => {
  it("merges hits per session, best source first, stale if any hit is stale", () => {
    const rows = dedupeSearchMatches([
      m({ source: "assistant", messageId: "a" }),
      m({ source: "title", freshness: "pending" }),
      m({ sessionId: "s2", threadId: "t2" }),
    ]);
    expect(rows).toHaveLength(2);
    expect(rows[0]?.sources).toEqual(["title", "assistant"]);
    expect(rows[0]?.stale).toBe(true);
    expect(rows[1]?.stale).toBe(false);
  });
  it("filters by mode", () => {
    const rows = dedupeSearchMatches([m({}), m({ sessionId: "s2", mode: "code" })], "code");
    expect(rows.map((r) => r.sessionId)).toEqual(["s2"]);
  });
});

describe("buildBreadcrumb", () => {
  const sections = [
    { id: "a", parentId: null, name: "A" },
    { id: "b", parentId: "a", name: "B" },
  ];
  it("walks to the root", () => {
    expect(buildBreadcrumb("P", sections, "b")).toEqual(["P", "A", "B"]);
    expect(buildBreadcrumb("P", sections, null)).toEqual(["P"]);
  });
  it("survives cycles and missing parents", () => {
    expect(buildBreadcrumb("P", [{ id: "x", parentId: "x", name: "X" }], "x")).toEqual(["P", "X"]);
    expect(buildBreadcrumb("P", [{ id: "x", parentId: "gone", name: "X" }], "x")).toEqual([
      "P",
      "X",
    ]);
  });
});

describe("hasUnreadCompletion", () => {
  it("matches the native unseen-completion rule", async () => {
    const { hasUnreadCompletion } = await import("./semanggiSearchModel");
    expect(hasUnreadCompletion(null, "2026-01-01T00:00:00Z")).toBe(false);
    expect(hasUnreadCompletion("2026-01-02T00:00:00Z", null)).toBe(false);
    expect(hasUnreadCompletion("2026-01-02T00:00:00Z", "2026-01-01T00:00:00Z")).toBe(true);
    expect(hasUnreadCompletion("2026-01-01T00:00:00Z", "2026-01-02T00:00:00Z")).toBe(false);
    expect(hasUnreadCompletion("2026-01-02T00:00:00Z", "garbage")).toBe(true);
  });
});
