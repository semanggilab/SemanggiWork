// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/sql/SqlClient";
import * as Storage from "./Storage.ts";
import * as Inbox from "./ReplayInbox.ts";
import * as Saga from "./CreateSagaStore.ts";
import * as Discovery from "./Discovery.ts";
import * as Adapter from "./NativeAdapter.ts";

it.effect("discovers native threads once without duplicating an in-flight create saga", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* Saga.prepare({
      environmentId: "env",
      idempotencyKey: "key",
      actorId: "actor",
      requestId: "r",
      sessionId: "candidate",
      commandId: "command",
      threadId: "saga-thread",
      projectId: Storage.UNASSIGNED_PROJECT_ID,
      sectionId: null,
      mode: "work",
      requestPayload: "payload",
    });
    yield* Inbox.commitPage({
      environmentId: "env",
      expectedCursor: 0,
      nextCursor: 2,
      highWater: 2,
      events: [
        {
          sequence: 1,
          eventId: "native-created",
          hash: "h1",
          threadId: "native-thread",
          discovery: { mode: "code" },
        },
        {
          sequence: 2,
          eventId: "saga-created",
          hash: "h2",
          threadId: "saga-thread",
          discovery: { mode: "work" },
        },
      ],
    });
    const sessions = yield* sql<{
      id: string;
      mode: string;
      project_id: string;
    }>`SELECT id,mode,project_id FROM sh_sessions ORDER BY id`;
    assert.equal(sessions.length, 2);
    assert.equal(sessions.find(({ id }) => id !== "candidate")?.mode, "code");
    assert.isTrue(sessions.every(({ project_id }) => project_id === Storage.UNASSIGNED_PROJECT_ID));
    const bindings = yield* sql<{
      t3_thread_id: string;
    }>`SELECT t3_thread_id FROM sh_thread_bindings`;
    assert.deepEqual(bindings, [{ t3_thread_id: "native-thread" }]);
    yield* Inbox.commitPage({
      environmentId: "env",
      expectedCursor: 2,
      nextCursor: 3,
      highWater: 3,
      events: [
        {
          sequence: 3,
          eventId: "again",
          hash: "h3",
          threadId: "native-thread",
          discovery: { mode: "code" },
        },
      ],
    });
    assert.equal((yield* sql`SELECT id FROM sh_sessions`).length, 2);
    assert.equal(
      (yield* sql`SELECT sequence FROM sh_audit WHERE event_type = 'session.discover'`).length,
      1,
    );
    assert.equal((yield* sql`SELECT thread_id FROM sh_discovery_pending`).length, 1);
    yield* Inbox.commitPage({
      environmentId: "env",
      expectedCursor: 3,
      nextCursor: 4,
      highWater: 4,
      events: [
        {
          sequence: 4,
          eventId: "deleted",
          hash: "h4",
          threadId: "saga-thread",
          cancelDiscovery: true,
        },
      ],
    });
    yield* Saga.transition({
      environmentId: "env",
      key: "key",
      expected: "PREPARED",
      next: "REJECTED",
    });
    assert.isFalse(yield* Discovery.resolvePending("env"));
    assert.equal((yield* sql`SELECT t3_thread_id FROM sh_thread_bindings`).length, 1);
  }).pipe(Effect.provide(Storage.layerFromPath(":memory:"))),
);

it.effect(
  "does not bind a deferred discovery when the native thread was deleted before replay",
  () => {
    const native = {
      readThread: () =>
        Effect.succeed({
          projection: { thread: { id: "deleted-thread", deletedAt: "2026-11-13T00:00:00.000Z" } },
        }),
    } as unknown as Adapter.NativeAdapter["Service"];
    return Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* sql`INSERT INTO sh_discovery_pending(environment_id,thread_id,mode,request_id)
      VALUES ('env','deleted-thread','code','created')`;
      assert.isFalse(yield* Discovery.resolveVerifiedPending("env"));
      assert.equal((yield* sql`SELECT thread_id FROM sh_discovery_pending`).length, 0);
      assert.equal((yield* sql`SELECT t3_thread_id FROM sh_thread_bindings`).length, 0);
      assert.equal((yield* sql`SELECT id FROM sh_sessions`).length, 0);
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

it.effect("keeps a deferred discovery for retry when the native recheck is unavailable", () => {
  const native = {
    readThread: () => Effect.fail({ _tag: "NativeUnavailable" as const }),
  } as unknown as Adapter.NativeAdapter["Service"];
  return Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql`INSERT INTO sh_discovery_pending(environment_id,thread_id,mode,request_id)
      VALUES ('env','retry-thread','work','created')`;
    assert.isFalse(yield* Discovery.resolveVerifiedPending("env"));
    assert.deepEqual(yield* sql`SELECT thread_id FROM sh_discovery_pending`, [
      { thread_id: "retry-thread" },
    ]);
    assert.equal((yield* sql`SELECT t3_thread_id FROM sh_thread_bindings`).length, 0);
  }).pipe(
    Effect.provide(
      Layer.mergeAll(
        Storage.layerFromPath(":memory:"),
        Layer.succeed(Adapter.NativeAdapter, native),
      ),
    ),
  );
});

it.effect("a newer committed event clears refresh backoff for its thread", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* Inbox.commitPage({
      environmentId: "env",
      expectedCursor: 0,
      nextCursor: 1,
      highWater: 2,
      events: [{ sequence: 1, eventId: "e1", hash: "h1", threadId: "thread" }],
    });
    yield* sql`UPDATE sh_thread_refresh_queue SET attempts = 3, retry_at_ms = 999999 WHERE thread_id = 'thread'`;
    yield* Inbox.commitPage({
      environmentId: "env",
      expectedCursor: 1,
      nextCursor: 2,
      highWater: 2,
      events: [{ sequence: 2, eventId: "e2", hash: "h2", threadId: "thread" }],
    });
    const rows = yield* sql<{
      source_sequence: number;
      attempts: number;
      retry_at_ms: number;
    }>`SELECT source_sequence,attempts,retry_at_ms FROM sh_thread_refresh_queue`;
    assert.deepEqual(rows, [{ source_sequence: 2, attempts: 0, retry_at_ms: 0 }]);
  }).pipe(Effect.provide(Storage.layerFromPath(":memory:"))),
);

it.effect("marks an idle checkpoint LIVE and returns to catch-up for the next event", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql`INSERT INTO sh_ingestion_checkpoints VALUES ('env',2,2,'CATCHING_UP')`;
    assert.isTrue(yield* Inbox.markLiveIfIdle("env"));
    assert.deepEqual(yield* sql`SELECT state FROM sh_ingestion_checkpoints`, [{ state: "LIVE" }]);
    yield* Inbox.commitPage({
      environmentId: "env",
      expectedCursor: 2,
      nextCursor: 3,
      highWater: 3,
      events: [{ sequence: 3, eventId: "e3", hash: "h3", threadId: "thread" }],
    });
    assert.deepEqual(yield* sql`SELECT state FROM sh_ingestion_checkpoints`, [
      { state: "CATCHING_UP" },
    ]);
    assert.isFalse(yield* Inbox.markLiveIfIdle("env"));
  }).pipe(Effect.provide(Storage.layerFromPath(":memory:"))),
);

it.effect("resumes from the persisted checkpoint after reopening the database", () => {
  const dir = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "semanggi-inbox-"));
  const path = NodePath.join(dir, "semanggi.sqlite");
  return Effect.gen(function* () {
    yield* Inbox.commitPage({
      environmentId: "env",
      expectedCursor: 0,
      nextCursor: 1,
      highWater: 2,
      events: [{ sequence: 1, eventId: "e1", hash: "h1", threadId: "t" }],
    }).pipe(Effect.provide(Storage.layerFromPath(path)));
    yield* Inbox.commitPage({
      environmentId: "env",
      expectedCursor: 1,
      nextCursor: 2,
      highWater: 2,
      events: [{ sequence: 2, eventId: "e2", hash: "h2", threadId: "t" }],
    }).pipe(Effect.provide(Storage.layerFromPath(path)));
    yield* Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const checkpoint = yield* sql<{
        cursor: number;
        state: string;
      }>`SELECT cursor,state FROM sh_ingestion_checkpoints WHERE environment_id = 'env'`;
      const queue = yield* sql<{
        source_sequence: number;
      }>`SELECT source_sequence FROM sh_thread_refresh_queue WHERE environment_id = 'env' AND thread_id = 't'`;
      assert.deepEqual(checkpoint[0], { cursor: 2, state: "CATCHING_UP" });
      assert.equal(queue[0]?.source_sequence, 2);
      assert.equal((yield* sql`SELECT event_id FROM sh_inbox`).length, 2);
    }).pipe(Effect.provide(Storage.layerFromPath(path)));
  }).pipe(Effect.ensuring(Effect.sync(() => NodeFS.rmSync(dir, { recursive: true, force: true }))));
});

it.effect("commits inbox, pending refreshes and checkpoint together and fences gaps", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* Inbox.commitPage({
      environmentId: "env",
      expectedCursor: 0,
      nextCursor: 3,
      highWater: 5,
      events: [
        { sequence: 1, eventId: "e1", hash: "h1", threadId: "thread" },
        { sequence: 3, eventId: "e3", hash: "h3", threadId: "thread" },
      ],
    });
    const queue = yield* sql<{
      source_sequence: number;
    }>`SELECT source_sequence FROM sh_thread_refresh_queue WHERE thread_id = 'thread'`;
    assert.equal(queue[0]?.source_sequence, 3);
    yield* Effect.flip(
      Inbox.commitPage({
        environmentId: "env",
        expectedCursor: 0,
        nextCursor: 3,
        highWater: 5,
        events: [],
      }),
    );
    yield* Effect.flip(
      Inbox.commitPage({
        environmentId: "env",
        expectedCursor: 3,
        nextCursor: 4,
        highWater: 5,
        events: [{ sequence: 4, eventId: "e1", hash: "changed", threadId: "thread" }],
      }),
    );
    yield* sql`CREATE TRIGGER fail_replay BEFORE INSERT ON sh_inbox WHEN NEW.event_id = 'e5' BEGIN SELECT RAISE(ABORT, 'injected failure'); END`;
    yield* Effect.flip(
      Inbox.commitPage({
        environmentId: "env",
        expectedCursor: 3,
        nextCursor: 5,
        highWater: 5,
        events: [
          { sequence: 4, eventId: "e4", hash: "h4", threadId: "other" },
          { sequence: 5, eventId: "e5", hash: "h5", threadId: "other" },
        ],
      }),
    );
    assert.equal((yield* sql`SELECT event_id FROM sh_inbox WHERE event_id = 'e4'`).length, 0);
    assert.equal(
      (yield* sql`SELECT thread_id FROM sh_thread_refresh_queue WHERE thread_id = 'other'`).length,
      0,
    );
    const checkpoints = yield* sql<{
      cursor: number;
      state: string;
    }>`SELECT cursor, state FROM sh_ingestion_checkpoints WHERE environment_id = 'env'`;
    assert.equal(checkpoints[0]?.cursor, 3);
    yield* Inbox.markGap("env", 3, 10);
    yield* Effect.flip(
      Inbox.commitPage({
        environmentId: "env",
        expectedCursor: 3,
        nextCursor: 5,
        highWater: 10,
        events: [],
      }),
    );
    const gap = yield* sql<{
      cursor: number;
      state: string;
    }>`SELECT cursor, state FROM sh_ingestion_checkpoints WHERE environment_id = 'env'`;
    assert.deepEqual(gap[0], { cursor: 3, state: "RECONCILING" });
    assert.equal((yield* sql`SELECT event_id FROM sh_inbox`).length, 2);
  }).pipe(Effect.provide(Storage.layerFromPath(":memory:"))),
);
