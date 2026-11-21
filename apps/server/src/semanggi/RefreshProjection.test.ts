import { assert, it } from "@effect/vitest";
import { MessageId, ThreadId } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/sql/SqlClient";
import * as TestClock from "effect/testing/TestClock";
import * as Adapter from "./NativeAdapter.ts";
import * as Refresh from "./RefreshProjection.ts";
import * as Storage from "./Storage.ts";
import * as Inbox from "./ReplayInbox.ts";

it.effect("refreshes messages across multiple batches without persisting their text", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const threadId = ThreadId.make("thread");
    const at = DateTime.makeUnsafe("2026-01-01T00:00:00Z");
    const messages = Array.from({ length: 201 }, (_, index) => ({
      createdBy: "user" as const,
      creationSource: "web" as const,
      id: MessageId.make(`m${index}`),
      threadId,
      runId: null,
      nodeId: null,
      role: "user" as const,
      text: `private text ${index}`,
      attachments: [],
      streaming: false,
      createdAt: at,
      updatedAt: at,
    }));
    const native = {
      readThread: () =>
        Effect.succeed({
          snapshotSequence: 1,
          projection: {
            thread: { id: threadId, title: "Thread", deletedAt: null },
            messages,
            visibleTurnItems: messages.map(({ id }) => ({
              sourceThreadId: threadId,
              item: { type: "user_message", messageId: id },
            })),
          },
        }),
    } as unknown as Adapter.NativeAdapter["Service"];
    yield* sql`INSERT INTO sh_ingestion_checkpoints VALUES ('env',1,1,'CATCHING_UP')`;
    yield* sql`INSERT INTO sh_thread_refresh_queue(environment_id,thread_id,source_sequence) VALUES ('env','thread',1)`;
    assert.equal(
      (yield* Refresh.refreshOne("env").pipe(Effect.provideService(Adapter.NativeAdapter, native)))
        .state,
      "REFRESHED",
    );
    const rows = yield* sql<{
      message_id: string;
      content_hash: string;
    }>`SELECT message_id,content_hash FROM sh_message_projection`;
    assert.equal(rows.length, 201);
    assert.isTrue(
      rows.every((row) => row.content_hash.length === 64 && !row.content_hash.includes("private")),
    );
  }).pipe(Effect.provide(Storage.layerFromPath(":memory:"))),
);

it.effect("refreshes a reconciled thread even when its native events were pruned", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql`INSERT INTO sh_ingestion_checkpoints VALUES ('env',20,20,'CATCHING_UP')`;
    yield* sql`INSERT INTO sh_thread_refresh_queue(environment_id,thread_id,source_sequence,force_snapshot) VALUES ('env','thread',20,1)`;
    const native = {
      readThread: () =>
        Effect.succeed({
          snapshotSequence: 0,
          projection: {
            thread: { id: "thread", title: "Restored", deletedAt: null },
            messages: [],
            visibleTurnItems: [],
          },
        }),
    } as unknown as Adapter.NativeAdapter["Service"];
    const result = yield* Refresh.refreshOne("env").pipe(
      Effect.provideService(Adapter.NativeAdapter, native),
    );
    assert.deepEqual(result, { state: "REFRESHED", threadId: "thread", sequence: 20 });
    assert.deepEqual(yield* sql`SELECT source_sequence,title FROM sh_native_projection`, [
      { source_sequence: 20, title: "Restored" },
    ]);
    assert.equal((yield* sql`SELECT thread_id FROM sh_thread_refresh_queue`).length, 0);
  }).pipe(Effect.provide(Storage.layerFromPath(":memory:"))),
);

it.effect("backs off an unavailable thread so later refreshes can proceed", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql`INSERT INTO sh_ingestion_checkpoints(environment_id,cursor,high_water,state) VALUES ('env',2,2,'CATCHING_UP')`;
    yield* sql`INSERT INTO sh_thread_refresh_queue(environment_id,thread_id,source_sequence) VALUES ('env','first',1),('env','second',2)`;
    const native = {
      readThread: (threadId: string) =>
        threadId === "first"
          ? Effect.fail(new Inbox.ReplayConflict({ message: "unavailable" }))
          : Effect.succeed({
              snapshotSequence: 2,
              projection: {
                thread: { id: threadId, title: "Second", deletedAt: null },
                messages: [],
                visibleTurnItems: [],
              },
            }),
    } as unknown as Adapter.NativeAdapter["Service"];
    assert.equal(
      (yield* Refresh.refreshOne("env").pipe(Effect.provideService(Adapter.NativeAdapter, native)))
        .state,
      "RETRY",
    );
    assert.equal(
      (yield* Refresh.refreshOne("env").pipe(Effect.provideService(Adapter.NativeAdapter, native)))
        .state,
      "REFRESHED",
    );
    const rows = yield* sql<{
      thread_id: string;
      attempts: number;
    }>`SELECT thread_id,attempts FROM sh_thread_refresh_queue`;
    assert.deepEqual(rows, [{ thread_id: "first", attempts: 1 }]);
  }).pipe(Effect.provide(Storage.layerFromPath(":memory:"))),
);

it.effect("keeps a newer invalidation queued when it arrives during snapshot read", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql`INSERT INTO sh_ingestion_checkpoints(environment_id,cursor,high_water,state) VALUES ('env',1,1,'CATCHING_UP')`;
    yield* sql`INSERT INTO sh_thread_refresh_queue(environment_id,thread_id,source_sequence) VALUES ('env','thread',1)`;
    const native = {
      readThread: () =>
        sql`UPDATE sh_thread_refresh_queue SET source_sequence = 2 WHERE thread_id = 'thread'`.pipe(
          Effect.as({
            snapshotSequence: 1,
            projection: {
              thread: { id: "thread", title: "Thread", deletedAt: null },
              messages: [],
              visibleTurnItems: [],
            },
          }),
        ),
    } as unknown as Adapter.NativeAdapter["Service"];
    const result = yield* Refresh.refreshOne("env").pipe(
      Effect.provideService(Adapter.NativeAdapter, native),
    );
    assert.equal(result.state, "REFRESHED");
    const queue = yield* sql<{
      source_sequence: number;
    }>`SELECT source_sequence FROM sh_thread_refresh_queue WHERE thread_id = 'thread'`;
    assert.equal(queue[0]?.source_sequence, 2);
  }).pipe(Effect.provide(Storage.layerFromPath(":memory:"))),
);

it.effect("keeps the invalidation when projection storage fails", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql`INSERT INTO sh_ingestion_checkpoints(environment_id,cursor,high_water,state) VALUES ('env',1,1,'CATCHING_UP')`;
    yield* sql`INSERT INTO sh_thread_refresh_queue(environment_id,thread_id,source_sequence) VALUES ('env','thread',1)`;
    yield* sql`CREATE TRIGGER reject_projection BEFORE INSERT ON sh_native_projection BEGIN SELECT RAISE(ABORT, 'injected failure'); END`;
    const native = {
      readThread: () =>
        Effect.succeed({
          snapshotSequence: 1,
          projection: {
            thread: { id: "thread", title: "Thread", deletedAt: null },
            messages: [],
            visibleTurnItems: [],
          },
        }),
    } as unknown as Adapter.NativeAdapter["Service"];
    yield* Effect.flip(
      Refresh.refreshOne("env").pipe(Effect.provideService(Adapter.NativeAdapter, native)),
    );
    assert.equal((yield* sql`SELECT thread_id FROM sh_thread_refresh_queue`).length, 1);
    assert.equal((yield* sql`SELECT thread_id FROM sh_native_projection`).length, 0);
  }).pipe(Effect.provide(Storage.layerFromPath(":memory:"))),
);

it.effect(
  "refreshes complete snapshots, tombstones removed messages, and fences newer changes",
  () => {
    const at = DateTime.makeUnsafe("2026-01-01T00:00:00Z");
    const threadId = ThreadId.make("thread");
    const message = (id: string, text: string) => ({
      createdBy: "user" as const,
      creationSource: "web" as const,
      id: MessageId.make(id),
      threadId,
      runId: null,
      nodeId: null,
      role: "user" as const,
      text,
      attachments: [],
      streaming: false,
      createdAt: at,
      updatedAt: at,
    });
    let sequence = 1;
    let deletedAt: DateTime.Utc | null = null;
    let messages = [message("m1", "before"), message("m2", "remove")];
    let hiddenIds = new Set<string>();
    const native = {
      readThread: () =>
        Effect.sync(() => ({
          snapshotSequence: sequence,
          projection: {
            thread: { id: threadId, title: "Thread", deletedAt },
            messages,
            visibleTurnItems: messages
              .filter(({ id }) => !hiddenIds.has(id))
              .map(({ id }) => ({
                sourceThreadId: threadId,
                item: { type: "user_message", messageId: id },
              })),
          },
        })),
    } as unknown as Adapter.NativeAdapter["Service"];
    return Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* sql`INSERT INTO sh_ingestion_checkpoints(environment_id,cursor,high_water,state) VALUES ('env',1,1,'CATCHING_UP')`;
      yield* sql`INSERT INTO sh_thread_refresh_queue(environment_id,thread_id,source_sequence) VALUES ('env','thread',1)`;
      assert.equal((yield* Refresh.refreshOne("env")).state, "REFRESHED");
      const first = yield* sql<{
        message_id: string;
        content_hash: string;
        deleted: number;
      }>`SELECT message_id,content_hash,deleted FROM sh_message_projection ORDER BY message_id`;
      assert.equal(first.length, 2);
      yield* sql`INSERT INTO sh_thread_refresh_queue(environment_id,thread_id,source_sequence) VALUES ('env','thread',1)`;
      messages = [message("m1", "changed without event")];
      yield* Effect.flip(Refresh.refreshOne("env"));
      assert.equal((yield* sql`SELECT thread_id FROM sh_thread_refresh_queue`).length, 1);
      messages = [message("m1", "before"), message("m2", "remove")];
      assert.equal((yield* Refresh.refreshOne("env")).state, "REFRESHED");
      yield* sql`INSERT INTO sh_thread_refresh_queue(environment_id,thread_id,source_sequence) VALUES ('env','thread',3)`;
      sequence = 2;
      assert.equal((yield* Refresh.refreshOne("env")).state, "WAITING");
      yield* TestClock.adjust("16 seconds");
      sequence = 3;
      messages = [message("m1", "edited")];
      assert.equal((yield* Refresh.refreshOne("env")).state, "REFRESHED");
      const updated = yield* sql<{
        message_id: string;
        content_hash: string;
        deleted: number;
      }>`SELECT message_id,content_hash,deleted FROM sh_message_projection ORDER BY message_id`;
      assert.notEqual(updated[0]?.content_hash, first[0]?.content_hash);
      assert.equal(updated[1]?.deleted, 1);
      yield* sql`INSERT INTO sh_thread_refresh_queue(environment_id,thread_id,source_sequence) VALUES ('env','thread',4)`;
      sequence = 4;
      hiddenIds = new Set(["m1"]);
      assert.equal((yield* Refresh.refreshOne("env")).state, "REFRESHED");
      assert.equal(
        (yield* sql<{
          deleted: number;
        }>`SELECT deleted FROM sh_message_projection WHERE message_id = 'm1'`)[0]?.deleted,
        1,
      );
      yield* sql`INSERT INTO sh_thread_refresh_queue(environment_id,thread_id,source_sequence) VALUES ('env','thread',5)`;
      yield* sql`UPDATE sh_ingestion_checkpoints SET state = 'RECONCILING' WHERE environment_id = 'env'`;
      assert.equal((yield* Refresh.refreshOne("env")).state, "RECONCILING");
      yield* sql`UPDATE sh_ingestion_checkpoints SET state = 'CATCHING_UP' WHERE environment_id = 'env'`;
      sequence = 5;
      deletedAt = at;
      assert.equal((yield* Refresh.refreshOne("env")).state, "REFRESHED");
      assert.equal(
        (yield* sql<{ deleted: number }>`SELECT deleted FROM sh_native_projection`)[0]?.deleted,
        1,
      );
      assert.isTrue(
        (yield* sql<{ deleted: number }>`SELECT deleted FROM sh_message_projection`).every(
          (row) => row.deleted === 1,
        ),
      );
    }).pipe(
      Effect.provide(
        Layer.mergeAll(
          Storage.layerFromPath(":memory:"),
          Layer.succeed(Adapter.NativeAdapter, native),
        ),
      ),
    );
  },
);
