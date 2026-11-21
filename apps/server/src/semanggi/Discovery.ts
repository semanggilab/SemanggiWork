import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";
import * as Repository from "./Repository.ts";
import * as Storage from "./Storage.ts";
import { sessionIdForThread } from "./DiscoveryIdentity.ts";
import * as Adapter from "./NativeAdapter.ts";
import { ThreadId } from "@t3tools/contracts";

export const discover = Effect.fn("semanggi.discovery.discover")(function* (input: {
  readonly environmentId: string;
  readonly threadId: string;
  readonly mode: "work" | "code";
  readonly requestId: string;
}) {
  const sql = yield* SqlClient.SqlClient;
  const binding =
    yield* sql`SELECT session_id FROM sh_thread_bindings WHERE environment_id = ${input.environmentId} AND t3_thread_id = ${input.threadId}`;
  if (binding.length) {
    yield* sql`DELETE FROM sh_discovery_pending WHERE environment_id = ${input.environmentId} AND thread_id = ${input.threadId}`;
    return;
  }
  const candidate = yield* sql<{
    state: string;
  }>`SELECT state FROM sh_thread_create_operations WHERE environment_id = ${input.environmentId} AND thread_id = ${input.threadId}`;
  if (candidate[0] && candidate[0].state !== "REJECTED") {
    yield* sql`INSERT OR IGNORE INTO sh_discovery_pending(environment_id,thread_id,mode,request_id)
      VALUES (${input.environmentId},${input.threadId},${input.mode},${input.requestId})`;
    return;
  }
  const sessionId = sessionIdForThread(input.environmentId, input.threadId);
  yield* Repository.createSession({
    id: sessionId,
    projectId: Storage.UNASSIGNED_PROJECT_ID,
    sectionId: null,
    mode: input.mode,
  });
  yield* Repository.bindThread({
    sessionId,
    environmentId: input.environmentId,
    threadId: input.threadId,
  });
  yield* sql`INSERT INTO sh_audit(actor_id,environment_id,request_id,event_type,entity_type,entity_id,revision)
    VALUES ('system:ingestion',${input.environmentId},${input.requestId},'session.discover','session',${sessionId},1)`;
  yield* sql`INSERT INTO sh_audit(actor_id,environment_id,request_id,event_type,entity_type,entity_id,revision)
    VALUES ('system:ingestion',${input.environmentId},${input.requestId},'binding.create','binding',${sessionId},1)`;
  yield* sql`DELETE FROM sh_discovery_pending WHERE environment_id = ${input.environmentId} AND thread_id = ${input.threadId}`;
});

type PendingDiscovery = {
  readonly thread_id: string;
  readonly mode: "work" | "code";
  readonly request_id: string;
};

const nextPending = Effect.fn("semanggi.discovery.nextPending")(function* (environmentId: string) {
  const sql = yield* SqlClient.SqlClient;
  const rows = yield* sql<PendingDiscovery>`
      SELECT p.thread_id,p.mode,p.request_id FROM sh_discovery_pending p
      LEFT JOIN sh_thread_create_operations op ON op.environment_id = p.environment_id AND op.thread_id = p.thread_id
      WHERE p.environment_id = ${environmentId} AND (op.state IS NULL OR op.state IN ('BOUND','REJECTED'))
      ORDER BY p.thread_id LIMIT 1`;
  return rows[0];
});

const resolve = Effect.fn("semanggi.discovery.resolve")(function* (
  environmentId: string,
  expectedThreadId?: string,
) {
  const sql = yield* SqlClient.SqlClient;
  return yield* sql.withTransaction(
    Effect.gen(function* () {
      const row = yield* nextPending(environmentId);
      if (!row || (expectedThreadId !== undefined && row.thread_id !== expectedThreadId))
        return false;
      yield* discover({
        environmentId,
        threadId: row.thread_id,
        mode: row.mode,
        requestId: row.request_id,
      });
      return true;
    }),
  );
});

/** Resolve one eligible candidate atomically. Useful to recover old databases without native reads. */
export const resolvePending = Effect.fn("semanggi.discovery.resolvePending")(function* (
  environmentId: string,
) {
  return yield* resolve(environmentId);
});

/**
 * Recheck the native thread before turning a deferred discovery into a binding.
 *
 * A delete can arrive before its committed event is replayed.  Keeping the native
 * read outside the Semanggi transaction avoids holding the local write lock during
 * I/O; `resolve` then re-selects the same candidate under that transaction.
 */
export const resolveVerifiedPending = Effect.fn("semanggi.discovery.resolveVerifiedPending")(
  function* (environmentId: string) {
    const sql = yield* SqlClient.SqlClient;
    const native = yield* Adapter.NativeAdapter;
    const pending = yield* nextPending(environmentId);
    if (!pending) return false;
    const snapshot = yield* native.readThread(ThreadId.make(pending.thread_id)).pipe(Effect.result);
    if (snapshot._tag === "Failure") return false;
    if (
      snapshot.success.projection.thread.id !== pending.thread_id ||
      snapshot.success.projection.thread.deletedAt !== null
    ) {
      yield* sql`DELETE FROM sh_discovery_pending
        WHERE environment_id=${environmentId} AND thread_id=${pending.thread_id}`;
      return false;
    }
    return yield* resolve(environmentId, pending.thread_id);
  },
);
