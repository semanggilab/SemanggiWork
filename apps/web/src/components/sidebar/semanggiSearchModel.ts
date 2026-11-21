import type { SemanggiSearchResult } from "@t3tools/contracts";

type Match = SemanggiSearchResult["matches"][number];

export interface CatalogSectionLike {
  readonly id: string;
  readonly parentId: string | null;
  readonly name: string;
}

export interface SearchRow {
  readonly sessionId: string;
  readonly threadId: string;
  readonly projectId: string;
  readonly mode: "work" | "code";
  readonly title: string;
  readonly sources: ReadonlyArray<Match["source"]>;
  readonly messageId: string | undefined;
  readonly stale: boolean;
}

const SOURCE_RANK: Record<Match["source"], number> = { title: 0, user: 1, assistant: 2 };

/** One row per session: best source first, stale if any hit is not fresh. */
export function dedupeSearchMatches(
  matches: ReadonlyArray<Match>,
  mode: "work" | "code" | "all" = "all",
): ReadonlyArray<SearchRow> {
  const rows = new Map<string, SearchRow>();
  for (const match of matches) {
    if (mode !== "all" && match.mode !== mode) continue;
    const existing = rows.get(match.sessionId);
    const stale = match.freshness !== "fresh";
    if (!existing) {
      rows.set(match.sessionId, {
        sessionId: match.sessionId,
        threadId: match.threadId,
        projectId: match.projectId,
        mode: match.mode,
        title: match.title,
        sources: [match.source],
        messageId: match.messageId,
        stale,
      });
      continue;
    }
    const sources = existing.sources.includes(match.source)
      ? existing.sources
      : [...existing.sources, match.source].toSorted((a, b) => SOURCE_RANK[a] - SOURCE_RANK[b]);
    const better = SOURCE_RANK[match.source] < SOURCE_RANK[existing.sources[0] ?? "assistant"];
    rows.set(match.sessionId, {
      ...existing,
      sources,
      messageId: better ? match.messageId : existing.messageId,
      stale: existing.stale || stale,
    });
  }
  return [...rows.values()];
}

/** Project > Section chain (root first). Cycles and missing parents end the walk. */
export function buildBreadcrumb(
  projectName: string,
  sections: ReadonlyArray<CatalogSectionLike>,
  sectionId: string | null,
): ReadonlyArray<string> {
  const byId = new Map(sections.map((section) => [section.id, section]));
  const chain: string[] = [];
  const seen = new Set<string>();
  let cursor = sectionId === null ? undefined : byId.get(sectionId);
  while (cursor && !seen.has(cursor.id)) {
    seen.add(cursor.id);
    chain.unshift(cursor.name);
    cursor = cursor.parentId === null ? undefined : byId.get(cursor.parentId);
  }
  return [projectName, ...chain];
}

/** Same rule as the native sidebar: a completion after the last visit is unread; never-visited threads are not. */
export function hasUnreadCompletion(
  completedAt: string | null | undefined,
  lastVisitedAt: string | null | undefined,
): boolean {
  if (!completedAt || !lastVisitedAt) return false;
  const completed = Date.parse(completedAt);
  if (Number.isNaN(completed)) return false;
  const visited = Date.parse(lastVisitedAt);
  return Number.isNaN(visited) ? true : completed > visited;
}
