// @effect-diagnostics nodeBuiltinImport:off
import * as NodeCrypto from "node:crypto";
import { ThreadId, OrchestrationV2ConversationMessage } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Clock from "effect/Clock";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as Adapter from "./NativeAdapter.ts";
import * as Inbox from "./ReplayInbox.ts";

const encodeMessage = Schema.encodeSync(
  Schema.fromJsonString(Schema.toCodecJson(OrchestrationV2ConversationMessage)),
);
const encodeHashBatch = Schema.encodeSync(
  Schema.fromJsonString(
    Schema.Array(
      Schema.Struct({
        id: Schema.String,
        role: Schema.String,
        hash: Schema.String,
      }),
    ),
  ),
);

/** A full native thread snapshot is committed before its pending invalidation is removed. */
export const refreshOne = Effect.fn("semanggi.refreshProjection.refreshOne")(function* (
  environmentId: string,
) {
  const sql = yield* SqlClient.SqlClient;
  const native = yield* Adapter.NativeAdapter;
  if (!environmentId.trim())
    return yield* new Inbox.ReplayConflict({ message: "Invalid environment." });
  const checkpoint = yield* sql<{
    state: string;
  }>`SELECT state FROM sh_ingestion_checkpoints WHERE environment_id = ${environmentId}`;
  if (checkpoint[0]?.state === "RECONCILING") return { state: "RECONCILING" as const };
  const pending = yield* sql<{
    thread_id: string;
    source_sequence: number;
    attempts: number;
    force_snapshot: number;
  }>`SELECT thread_id, source_sequence, attempts, force_snapshot FROM sh_thread_refresh_queue
    WHERE environment_id = ${environmentId} AND retry_at_ms <= ${yield* Clock.currentTimeMillis}
    ORDER BY source_sequence, thread_id LIMIT 1`;
  const queued = pending[0];
  if (!queued) return { state: "EMPTY" as const };
  const snapshotResult = yield* native
    .readThread(ThreadId.make(queued.thread_id))
    .pipe(Effect.result);
  if (Result.isFailure(snapshotResult)) {
    const delay = Math.min(15_000 * 2 ** Math.min(queued.attempts, 5), 300_000);
    const retryAt = (yield* Clock.currentTimeMillis) + delay;
    yield* sql`UPDATE sh_thread_refresh_queue SET attempts = attempts + 1, retry_at_ms = ${retryAt}
      WHERE environment_id = ${environmentId} AND thread_id = ${queued.thread_id} AND source_sequence = ${queued.source_sequence}`;
    return { state: "RETRY" as const, threadId: queued.thread_id };
  }
  const snapshot = snapshotResult.success;
  if (
    snapshot.projection.thread.id !== queued.thread_id ||
    (queued.force_snapshot !== 1 && snapshot.snapshotSequence < queued.source_sequence)
  ) {
    const retryAt = (yield* Clock.currentTimeMillis) + 15_000;
    yield* sql`UPDATE sh_thread_refresh_queue SET attempts = attempts + 1, retry_at_ms = ${retryAt}
      WHERE environment_id = ${environmentId} AND thread_id = ${queued.thread_id} AND source_sequence = ${queued.source_sequence}`;
    return { state: "WAITING" as const, threadId: queued.thread_id };
  }
  const thread = snapshot.projection.thread;
  const snapshotSequence =
    queued.force_snapshot === 1
      ? Math.max(snapshot.snapshotSequence, queued.source_sequence)
      : snapshot.snapshotSequence;
  const visibleMessageIds = new Set(
    snapshot.projection.visibleTurnItems.flatMap(({ sourceThreadId, item }) =>
      sourceThreadId === queued.thread_id &&
      (item.type === "user_message" || item.type === "assistant_message")
        ? [item.messageId]
        : [],
    ),
  );
  const messages = snapshot.projection.messages.filter((message) =>
    visibleMessageIds.has(message.id),
  );
  if (
    new Set(messages.map((message) => message.id)).size !== messages.length ||
    messages.some((message) => message.threadId !== queued.thread_id)
  ) {
    return yield* new Inbox.ReplayConflict({
      message: "Native snapshot contains invalid message identities.",
    });
  }
  const hashed = messages.map((message) => ({
    id: message.id,
    role: message.role,
    hash: NodeCrypto.createHash("sha256").update(encodeMessage(message)).digest("hex"),
  }));
  return yield* sql.withTransaction(
    Effect.gen(function* () {
      const state = yield* sql<{
        state: string;
      }>`SELECT state FROM sh_ingestion_checkpoints WHERE environment_id = ${environmentId}`;
      if (state[0]?.state === "RECONCILING") return { state: "RECONCILING" as const };
      const previous = yield* sql<{
        source_sequence: number;
        title: string;
        deleted: number;
      }>`SELECT source_sequence, title, deleted FROM sh_native_projection
        WHERE environment_id = ${environmentId} AND thread_id = ${queued.thread_id}`;
      if (previous[0]?.source_sequence === snapshotSequence) {
        const priorMessages = yield* sql<{
          message_id: string;
          content_hash: string;
        }>`SELECT message_id, content_hash FROM sh_message_projection
          WHERE environment_id = ${environmentId} AND thread_id = ${queued.thread_id} AND deleted = 0 ORDER BY message_id`;
        const expectedMessages = new Map<string, string>(
          (thread.deletedAt === null ? hashed : []).map((message) => [message.id, message.hash]),
        );
        if (
          previous[0].title !== thread.title ||
          previous[0].deleted !== (thread.deletedAt === null ? 0 : 1) ||
          priorMessages.length !== expectedMessages.size ||
          priorMessages.some(
            (message) => expectedMessages.get(message.message_id) !== message.content_hash,
          )
        ) {
          return yield* new Inbox.ReplayConflict({
            message: "Native snapshot changed without advancing its source sequence.",
          });
        }
      }
      yield* sql`INSERT INTO sh_native_projection(environment_id,thread_id,source_sequence,title,deleted)
      VALUES (${environmentId},${queued.thread_id},${snapshotSequence},${thread.title},${thread.deletedAt === null ? 0 : 1})
      ON CONFLICT(environment_id,thread_id) DO UPDATE SET source_sequence=excluded.source_sequence,title=excluded.title,deleted=excluded.deleted
      WHERE excluded.source_sequence >= sh_native_projection.source_sequence`;
      yield* sql`UPDATE sh_message_projection SET deleted = 1, source_sequence = ${snapshotSequence}
      WHERE environment_id = ${environmentId} AND thread_id = ${queued.thread_id} AND source_sequence <= ${snapshotSequence}`;
      if (thread.deletedAt === null) {
        for (let offset = 0; offset < hashed.length; offset += 200) {
          const batch = encodeHashBatch(hashed.slice(offset, offset + 200));
          yield* sql`INSERT INTO sh_message_projection(environment_id,thread_id,message_id,source_sequence,role,content_hash,deleted)
          SELECT ${environmentId},${queued.thread_id},json_extract(value,'$.id'),${snapshotSequence},
            json_extract(value,'$.role'),json_extract(value,'$.hash'),0 FROM json_each(${batch}) WHERE 1
          ON CONFLICT(environment_id,thread_id,message_id) DO UPDATE SET source_sequence=excluded.source_sequence,role=excluded.role,content_hash=excluded.content_hash,deleted=0
          WHERE excluded.source_sequence >= sh_message_projection.source_sequence`;
        }
      }
      yield* sql`DELETE FROM sh_thread_refresh_queue WHERE environment_id = ${environmentId} AND thread_id = ${queued.thread_id} AND source_sequence <= ${snapshotSequence}`;
      return {
        state: "REFRESHED" as const,
        threadId: queued.thread_id,
        sequence: snapshotSequence,
      };
    }),
  );
});
