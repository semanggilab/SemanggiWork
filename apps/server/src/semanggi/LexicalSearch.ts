import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as Result from "effect/Result";
import * as SqlClient from "effect/sql/SqlClient";
import { OrchestrationSearchThreadsInput, ThreadId } from "@t3tools/contracts";
import { messageMatcher } from "@t3tools/shared/paletteSearch";
import * as NativeAdapter from "./NativeAdapter.ts";

export class SearchScopeError extends Schema.TaggedError<SearchScopeError>()("SearchScopeError", {
  message: Schema.String,
}) {}

const decodeQuery = Schema.decodeUnknownEffect(OrchestrationSearchThreadsInput);

/** Apply the Semanggi membership scope before native ranking and candidate limits. */
export const search = Effect.fn("semanggi.lexicalSearch.search")(function* (input: {
  readonly environmentId: string;
  readonly projectId: string | null;
  readonly query: string;
  readonly limit: number;
}) {
  const sql = yield* SqlClient.SqlClient;
  const native = yield* NativeAdapter.NativeAdapter;
  if (
    !input.environmentId.trim() ||
    (input.projectId !== null && !input.projectId.trim()) ||
    !Number.isInteger(input.limit) ||
    input.limit < 1 ||
    input.limit > 50
  ) {
    return yield* new SearchScopeError({ message: "Invalid search scope or limit." });
  }
  const query = yield* decodeQuery({ query: input.query, limit: 50 }).pipe(
    Effect.mapError(() => new SearchScopeError({ message: "Invalid search query." })),
  );
  const scopeRows = yield* sql<{ thread_id: string }>`SELECT b.t3_thread_id AS thread_id
    FROM sh_thread_bindings b JOIN sh_sessions s ON s.id = b.session_id JOIN sh_projects p ON p.id = s.project_id
    WHERE b.environment_id = ${input.environmentId} AND b.binding_state = 'bound' AND p.archived_at IS NULL
      AND (${input.projectId} IS NULL OR s.project_id = ${input.projectId})
      AND NOT EXISTS (SELECT 1 FROM sh_thread_create_operations op WHERE op.session_id = s.id AND op.state != 'BOUND')`;
  const nativeResult =
    scopeRows.length === 0
      ? { matches: [] }
      : yield* native.searchThreads({
          ...query,
          threadIds: scopeRows.map(({ thread_id }) => ThreadId.make(thread_id)),
        });
  const matches: Array<{
    sessionId: string;
    threadId: string;
    projectId: string;
    mode: "work" | "code";
    title: string;
    source: "title" | "user" | "assistant";
    messageId?: string;
    freshness: "fresh" | "pending" | "unknown";
  }> = [];
  const seen = new Set<string>();
  for (const hit of nativeResult.matches) {
    if (seen.has(hit.threadId)) continue;
    const rows = yield* sql<{
      session_id: string;
      project_id: string;
      mode: "work" | "code";
    }>`
      SELECT s.id AS session_id, s.project_id, s.mode
      FROM sh_thread_bindings b JOIN sh_sessions s ON s.id = b.session_id
      JOIN sh_projects p ON p.id = s.project_id
      WHERE b.environment_id = ${input.environmentId} AND b.t3_thread_id = ${hit.threadId}
        AND b.binding_state = 'bound' AND p.archived_at IS NULL
        AND (${input.projectId} IS NULL OR s.project_id = ${input.projectId})
        AND NOT EXISTS (SELECT 1 FROM sh_thread_create_operations op WHERE op.session_id = s.id AND op.state != 'BOUND')`;
    const row = rows[0];
    if (!row) continue;
    const snapshotResult = yield* native.readThread(hit.threadId).pipe(Effect.result);
    if (Result.isFailure(snapshotResult)) continue;
    const snapshot = snapshotResult.success;
    if (
      snapshot.projection.thread.id !== hit.threadId ||
      snapshot.projection.thread.deletedAt !== null ||
      snapshot.projection.thread.title !== hit.title
    )
      continue;
    if (hit.source !== "title") {
      if (hit.messageId === undefined) continue;
      const message = snapshot.projection.messages.find((item) => item.id === hit.messageId);
      const visible = snapshot.projection.visibleTurnItems.some(
        ({ sourceThreadId, item }) =>
          sourceThreadId === hit.threadId &&
          (item.type === "user_message" || item.type === "assistant_message") &&
          item.messageId === hit.messageId,
      );
      if (
        !message ||
        message.role !== hit.source ||
        !visible ||
        messageMatcher(query.query)(message.text.replace(/!?\[([^\]\n]*)\]\([^)]*\)/g, "$1")) ===
          null
      )
        continue;
    }
    const stillScoped = yield* sql<{
      pending: number;
      cursor: number | null;
      state: string | null;
      projected_sequence: number | null;
      deleted: number | null;
    }>`SELECT
      EXISTS(SELECT 1 FROM sh_thread_refresh_queue q WHERE q.environment_id=b.environment_id AND q.thread_id=b.t3_thread_id) AS pending,
      c.cursor,c.state,pr.source_sequence AS projected_sequence,pr.deleted
      FROM sh_thread_bindings b JOIN sh_sessions s ON s.id=b.session_id
      JOIN sh_projects p ON p.id=s.project_id
      LEFT JOIN sh_ingestion_checkpoints c ON c.environment_id=b.environment_id
      LEFT JOIN sh_native_projection pr ON pr.environment_id=b.environment_id AND pr.thread_id=b.t3_thread_id
      WHERE b.environment_id=${input.environmentId}
      AND b.t3_thread_id=${hit.threadId} AND b.binding_state='bound' AND s.id=${row.session_id}
      AND s.project_id=${row.project_id} AND s.mode=${row.mode} AND p.archived_at IS NULL
      AND NOT EXISTS (SELECT 1 FROM sh_thread_create_operations op WHERE op.session_id=s.id AND op.state != 'BOUND')`;
    if (!stillScoped.length) continue;
    seen.add(hit.threadId);
    matches.push({
      sessionId: row.session_id,
      threadId: hit.threadId,
      projectId: row.project_id,
      mode: row.mode,
      title: hit.title,
      source: hit.source,
      ...(hit.messageId === undefined ? {} : { messageId: hit.messageId }),
      freshness:
        stillScoped[0]!.pending || stillScoped[0]!.state === "RECONCILING"
          ? "pending"
          : (stillScoped[0]!.state === "CATCHING_UP" || stillScoped[0]!.state === "LIVE") &&
              snapshot.snapshotSequence > 0 &&
              stillScoped[0]!.projected_sequence === snapshot.snapshotSequence &&
              (stillScoped[0]!.cursor ?? -1) >= snapshot.snapshotSequence &&
              stillScoped[0]!.deleted === 0
            ? "fresh"
            : "unknown",
    });
    if (matches.length >= input.limit) break;
  }
  const checkpoints = yield* sql<{
    state: string;
  }>`SELECT state FROM sh_ingestion_checkpoints WHERE environment_id = ${input.environmentId}`;
  return {
    matches:
      checkpoints[0]?.state === "RECONCILING"
        ? matches.map((match) => ({ ...match, freshness: "pending" as const }))
        : matches,
    coverage: "partial" as const,
    syncState:
      checkpoints[0]?.state === "RECONCILING"
        ? ("RECONCILING" as const)
        : checkpoints[0]?.state === "CATCHING_UP" || checkpoints[0]?.state === "LIVE"
          ? checkpoints[0].state === "LIVE"
            ? ("LIVE" as const)
            : ("CATCHING_UP" as const)
          : ("UNINITIALIZED" as const),
  };
});

/** Resolve the source again at open time so deleted or mismatched threads cannot be opened. */
export const openSource = Effect.fn("semanggi.lexicalSearch.openSource")(function* (input: {
  readonly environmentId: string;
  readonly projectId: string | null;
  readonly sessionId: string;
  readonly messageId?: string;
}) {
  const sql = yield* SqlClient.SqlClient;
  const native = yield* NativeAdapter.NativeAdapter;
  if (
    !input.environmentId.trim() ||
    !input.sessionId.trim() ||
    (input.messageId !== undefined && !input.messageId.trim()) ||
    (input.projectId !== null && !input.projectId.trim())
  ) {
    return yield* new SearchScopeError({ message: "Invalid search scope." });
  }
  const rows = yield* sql<{ thread_id: string; project_id: string; mode: "work" | "code" }>`
    SELECT b.t3_thread_id AS thread_id, s.project_id, s.mode FROM sh_thread_bindings b
      JOIN sh_sessions s ON s.id = b.session_id JOIN sh_projects p ON p.id = s.project_id
    WHERE b.environment_id = ${input.environmentId} AND s.id = ${input.sessionId}
      AND b.binding_state = 'bound' AND p.archived_at IS NULL
      AND (${input.projectId} IS NULL OR s.project_id = ${input.projectId})
      AND NOT EXISTS (SELECT 1 FROM sh_thread_create_operations op WHERE op.session_id = s.id AND op.state != 'BOUND')`;
  const binding = rows[0];
  if (!binding)
    return yield* new SearchScopeError({
      message: "Search result is no longer available in this scope.",
    });
  const snapshot = yield* native.readThread(ThreadId.make(binding.thread_id));
  if (
    snapshot.projection.thread.id !== binding.thread_id ||
    snapshot.projection.thread.deletedAt !== null
  ) {
    return yield* new SearchScopeError({ message: "Search source is deleted or mismatched." });
  }
  if (
    input.messageId !== undefined &&
    (!snapshot.projection.messages.some((message) => message.id === input.messageId) ||
      !snapshot.projection.visibleTurnItems.some(
        ({ sourceThreadId, item }) =>
          sourceThreadId === binding.thread_id &&
          (item.type === "user_message" || item.type === "assistant_message") &&
          item.messageId === input.messageId,
      ))
  )
    return yield* new SearchScopeError({ message: "Search message is no longer visible." });
  const stillScoped =
    yield* sql`SELECT 1 FROM sh_thread_bindings b JOIN sh_sessions s ON s.id=b.session_id
    JOIN sh_projects p ON p.id=s.project_id WHERE b.environment_id=${input.environmentId}
    AND b.t3_thread_id=${binding.thread_id} AND b.binding_state='bound' AND s.id=${input.sessionId}
    AND s.project_id=${binding.project_id} AND s.mode=${binding.mode} AND p.archived_at IS NULL
    AND NOT EXISTS (SELECT 1 FROM sh_thread_create_operations op WHERE op.session_id=s.id AND op.state != 'BOUND')`;
  if (!stillScoped.length)
    return yield* new SearchScopeError({ message: "Search scope changed while opening source." });
  return {
    sessionId: input.sessionId,
    threadId: binding.thread_id,
    projectId: binding.project_id,
    mode: binding.mode,
    sourceSequence: snapshot.snapshotSequence,
    ...(input.messageId === undefined ? {} : { messageId: input.messageId }),
  };
});
