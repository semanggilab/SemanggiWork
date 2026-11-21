import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as Discovery from "./Discovery.ts";

export class ReplayConflict extends Schema.TaggedError<ReplayConflict>()("ReplayConflict", {
  message: Schema.String,
}) {}

export interface IncomingEvent {
  readonly sequence: number;
  readonly eventId: string;
  readonly hash: string;
  readonly threadId: string | null;
  readonly discovery?: { readonly mode: "work" | "code" };
  readonly cancelDiscovery?: boolean;
}

export const commitPage = Effect.fn("semanggi.replayInbox.commitPage")(function* (input: {
  readonly environmentId: string;
  readonly expectedCursor: number;
  readonly nextCursor: number;
  readonly highWater: number;
  readonly events: ReadonlyArray<IncomingEvent>;
}) {
  const sql = yield* SqlClient.SqlClient;
  if (
    !input.environmentId.trim() ||
    ![input.expectedCursor, input.nextCursor, input.highWater].every(
      (n) => Number.isSafeInteger(n) && n >= 0,
    ) ||
    input.nextCursor < input.expectedCursor ||
    input.highWater < input.nextCursor ||
    input.events.some(
      (event, index) =>
        !event.eventId.trim() ||
        !event.hash.trim() ||
        (event.threadId !== null && !event.threadId.trim()) ||
        (event.discovery !== undefined &&
          (event.threadId === null || !["work", "code"].includes(event.discovery.mode))) ||
        (event.cancelDiscovery === true &&
          (event.threadId === null || event.discovery !== undefined)) ||
        !Number.isSafeInteger(event.sequence) ||
        event.sequence <= input.expectedCursor ||
        event.sequence > input.nextCursor ||
        (index > 0 && event.sequence <= input.events[index - 1]!.sequence),
    )
  ) {
    return yield* new ReplayConflict({ message: "Invalid committed replay page." });
  }
  return yield* sql.withTransaction(
    Effect.gen(function* () {
      const checkpoints = yield* sql<{
        cursor: number;
        state: string;
      }>`SELECT cursor, state FROM sh_ingestion_checkpoints WHERE environment_id = ${input.environmentId}`;
      const checkpoint = checkpoints[0];
      if (
        (checkpoint?.cursor ?? 0) !== input.expectedCursor ||
        checkpoint?.state === "RECONCILING"
      ) {
        return yield* new ReplayConflict({
          message: "Replay checkpoint changed or requires reconciliation.",
        });
      }
      for (const event of input.events) {
        const existing = yield* sql<{
          source_sequence: number;
          event_id: string;
          event_hash: string;
        }>`SELECT source_sequence, event_id, event_hash FROM sh_inbox
        WHERE environment_id = ${input.environmentId} AND (source_sequence = ${event.sequence} OR event_id = ${event.eventId})`;
        if (
          existing.some(
            (row) =>
              row.source_sequence !== event.sequence ||
              row.event_id !== event.eventId ||
              row.event_hash !== event.hash,
          )
        ) {
          return yield* new ReplayConflict({
            message: "Source event identity or content changed.",
          });
        }
        if (!existing.length) {
          yield* sql`INSERT INTO sh_inbox(environment_id, source_sequence, event_id, event_hash)
          VALUES (${input.environmentId}, ${event.sequence}, ${event.eventId}, ${event.hash})`;
          if (event.threadId !== null) {
            if (event.cancelDiscovery === true)
              yield* sql`DELETE FROM sh_discovery_pending WHERE environment_id=${input.environmentId} AND thread_id=${event.threadId}`;
            if (event.discovery !== undefined) {
              yield* Discovery.discover({
                environmentId: input.environmentId,
                threadId: event.threadId,
                mode: event.discovery.mode,
                requestId: event.eventId,
              });
            }
            yield* sql`INSERT INTO sh_thread_refresh_queue(environment_id, thread_id, source_sequence)
            VALUES (${input.environmentId}, ${event.threadId}, ${event.sequence})
            ON CONFLICT(environment_id, thread_id) DO UPDATE SET
              force_snapshot = CASE WHEN excluded.source_sequence > source_sequence THEN 0 ELSE force_snapshot END,
              attempts = CASE WHEN excluded.source_sequence > source_sequence THEN 0 ELSE attempts END,
              retry_at_ms = CASE WHEN excluded.source_sequence > source_sequence THEN 0 ELSE retry_at_ms END,
              source_sequence = MAX(source_sequence, excluded.source_sequence)`;
          }
        }
      }
      yield* sql`INSERT INTO sh_ingestion_checkpoints(environment_id,cursor,high_water,state)
      VALUES (${input.environmentId},${input.nextCursor},${input.highWater},'CATCHING_UP')
      ON CONFLICT(environment_id) DO UPDATE SET cursor = excluded.cursor, high_water = excluded.high_water, state = 'CATCHING_UP'`;
      return { cursor: input.nextCursor, state: "CATCHING_UP" as const };
    }),
  );
});

export const markGap = Effect.fn("semanggi.replayInbox.markGap")(function* (
  environmentId: string,
  expectedCursor: number,
  highWater: number,
) {
  const sql = yield* SqlClient.SqlClient;
  if (
    !environmentId.trim() ||
    !Number.isSafeInteger(expectedCursor) ||
    expectedCursor < 0 ||
    !Number.isSafeInteger(highWater) ||
    highWater < 0
  ) {
    return yield* new ReplayConflict({ message: "Invalid gap marker." });
  }
  const rows = yield* sql<{
    cursor: number;
  }>`INSERT INTO sh_ingestion_checkpoints(environment_id,cursor,high_water,state)
    SELECT ${environmentId}, ${expectedCursor}, ${highWater}, 'RECONCILING'
    WHERE ${expectedCursor} = 0 OR EXISTS (SELECT 1 FROM sh_ingestion_checkpoints WHERE environment_id = ${environmentId} AND cursor = ${expectedCursor})
    ON CONFLICT(environment_id) DO UPDATE SET high_water = excluded.high_water, state = 'RECONCILING'
      WHERE sh_ingestion_checkpoints.cursor = ${expectedCursor} RETURNING cursor`;
  if (!rows[0]) return yield* new ReplayConflict({ message: "Replay checkpoint changed." });
  return rows[0].cursor;
});

/** A checkpoint is LIVE only when every committed invalidation has a native projection. */
export const markLiveIfIdle = Effect.fn("semanggi.replayInbox.markLiveIfIdle")(function* (
  environmentId: string,
) {
  const sql = yield* SqlClient.SqlClient;
  const rows = yield* sql`UPDATE sh_ingestion_checkpoints SET state='LIVE'
    WHERE environment_id=${environmentId} AND state='CATCHING_UP'
      AND NOT EXISTS (SELECT 1 FROM sh_thread_refresh_queue WHERE environment_id=${environmentId})
      AND NOT EXISTS (SELECT 1 FROM sh_discovery_pending WHERE environment_id=${environmentId})
      AND NOT EXISTS (SELECT 1 FROM sh_reconciliation WHERE environment_id=${environmentId})
    RETURNING cursor`;
  return rows.length > 0;
});
