// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/sql/SqlClient";
import * as Adapter from "./NativeAdapter.ts";
import * as Events from "../persistence/OrchestrationEventStore.ts";
import * as Folders from "../project/ManagedProjectFolders.ts";
import * as Storage from "./Storage.ts";
import * as Reconciliation from "./Reconciliation.ts";
import * as Discovery from "./Discovery.ts";
import * as Saga from "./CreateSagaStore.ts";

it.effect("publishes a generation atomically and defers threads owned by unfinished sagas", () => {
  const native = {
    listReconciliationPage: () =>
      Effect.succeed(
        ["external", "saga"].map((thread_id) => ({
          thread_id,
          project_id: "project",
          deleted_at: null,
          relationship: null,
          side_chat: null,
        })),
      ),
  } as unknown as Adapter.NativeAdapter["Service"];
  const events = {
    latestApplicationSequence: Effect.succeed(10),
    prunedThroughSequence: Effect.succeed(5),
  } as unknown as Events.OrchestrationEventStore["Service"];
  const folders = {
    isScratchProject: () => Effect.succeed(false),
  } as unknown as Folders.ManagedProjectFolders["Service"];
  return Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* Saga.prepare({
      environmentId: "env",
      idempotencyKey: "key",
      actorId: "actor",
      requestId: "r",
      sessionId: "candidate",
      commandId: "command",
      threadId: "saga",
      projectId: Storage.UNASSIGNED_PROJECT_ID,
      sectionId: null,
      mode: "work",
      requestPayload: "payload",
    });
    yield* sql`INSERT INTO sh_ingestion_checkpoints VALUES ('env',1,10,'RECONCILING')`;
    yield* sql`CREATE TRIGGER reject_binding BEFORE INSERT ON sh_thread_bindings BEGIN SELECT RAISE(ABORT, 'injected failure'); END`;
    yield* Effect.flip(Reconciliation.reconcilePage("env", 10));
    assert.equal((yield* sql`SELECT id FROM sh_sessions`).length, 1);
    assert.equal((yield* sql`SELECT thread_id FROM sh_discovery_pending`).length, 0);
    assert.deepEqual(yield* sql`SELECT cursor,state FROM sh_ingestion_checkpoints`, [
      { cursor: 1, state: "RECONCILING" },
    ]);
    yield* sql`DROP TRIGGER reject_binding`;
    assert.equal((yield* Reconciliation.reconcilePage("env", 10)).state, "COMPLETE");
    assert.deepEqual(yield* sql`SELECT t3_thread_id FROM sh_thread_bindings`, [
      { t3_thread_id: "external" },
    ]);
    assert.deepEqual(yield* sql`SELECT thread_id FROM sh_discovery_pending`, [
      { thread_id: "saga" },
    ]);
    assert.equal(
      (yield* sql`SELECT event_type FROM sh_audit WHERE event_type='session.discover'`).length,
      1,
    );
    yield* Saga.transition({
      environmentId: "env",
      key: "key",
      expected: "PREPARED",
      next: "REJECTED",
    });
    assert.isTrue(yield* Discovery.resolvePending("env"));
    assert.equal((yield* sql`SELECT session_id FROM sh_thread_bindings`).length, 2);
  }).pipe(
    Effect.provide(
      Layer.mergeAll(
        Storage.layerFromPath(":memory:"),
        Layer.succeed(Adapter.NativeAdapter, native),
        Layer.succeed(Events.OrchestrationEventStore, events),
        Layer.succeed(Folders.ManagedProjectFolders, folders),
      ),
    ),
  );
});

it.effect("discards a snapshot page if source events are pruned while it is read", () => {
  let calls = 0;
  const native = {
    listReconciliationPage: () =>
      Effect.succeed([
        {
          thread_id: "thread",
          project_id: "project",
          deleted_at: null,
          relationship: null,
          side_chat: null,
        },
      ]),
  } as unknown as Adapter.NativeAdapter["Service"];
  const events = {
    latestApplicationSequence: Effect.succeed(10),
    prunedThroughSequence: Effect.sync(() => (++calls === 1 ? 5 : 11)),
  } as unknown as Events.OrchestrationEventStore["Service"];
  const folders = {
    isScratchProject: () => Effect.succeed(false),
  } as unknown as Folders.ManagedProjectFolders["Service"];
  return Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql`INSERT INTO sh_ingestion_checkpoints VALUES ('env',1,10,'RECONCILING')`;
    assert.deepEqual(yield* Reconciliation.reconcilePage("env", 10), { state: "RESTART" });
    assert.equal((yield* sql`SELECT session_id FROM sh_thread_bindings`).length, 0);
    assert.equal((yield* sql`SELECT environment_id FROM sh_reconciliation`).length, 0);
    assert.deepEqual(yield* sql`SELECT cursor,state FROM sh_ingestion_checkpoints`, [
      { cursor: 1, state: "RECONCILING" },
    ]);
  }).pipe(
    Effect.provide(
      Layer.mergeAll(
        Storage.layerFromPath(":memory:"),
        Layer.succeed(Adapter.NativeAdapter, native),
        Layer.succeed(Events.OrchestrationEventStore, events),
        Layer.succeed(Folders.ManagedProjectFolders, folders),
      ),
    ),
  );
});

it.effect("persists a partially scanned generation across database restarts", () => {
  const dir = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "semanggi-reconcile-"));
  const path = NodePath.join(dir, "semanggi.sqlite");
  const native = {
    listReconciliationPage: (after: string, limit: number) =>
      Effect.succeed(
        ["a", "b"]
          .filter((id) => id > after)
          .slice(0, limit)
          .map((id) => ({
            thread_id: id,
            project_id: "project",
            deleted_at: null,
            relationship: null,
            side_chat: null,
          })),
      ),
  } as unknown as Adapter.NativeAdapter["Service"];
  const events = {
    latestApplicationSequence: Effect.succeed(10),
    prunedThroughSequence: Effect.succeed(5),
  } as unknown as Events.OrchestrationEventStore["Service"];
  const folders = {
    isScratchProject: () => Effect.succeed(false),
  } as unknown as Folders.ManagedProjectFolders["Service"];
  const layer = () =>
    Layer.mergeAll(
      Storage.layerFromPath(path),
      Layer.succeed(Adapter.NativeAdapter, native),
      Layer.succeed(Events.OrchestrationEventStore, events),
      Layer.succeed(Folders.ManagedProjectFolders, folders),
    );
  return Effect.gen(function* () {
    yield* Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* sql`INSERT INTO sh_ingestion_checkpoints VALUES ('env',1,10,'RECONCILING')`;
      assert.equal((yield* Reconciliation.reconcilePage("env", 1)).state, "SCANNING");
    }).pipe(Effect.provide(layer()));
    yield* Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      assert.deepEqual(yield* Reconciliation.reconcilePage("env", 1), {
        state: "COMPLETE",
        cursor: 10,
        count: 1,
      });
      assert.equal((yield* sql`SELECT session_id FROM sh_thread_bindings`).length, 2);
    }).pipe(Effect.provide(layer()));
  }).pipe(Effect.ensuring(Effect.sync(() => NodeFS.rmSync(dir, { recursive: true, force: true }))));
});

it.effect(
  "resumes pages after a gap, restarts if the replay boundary is pruned, and never deletes unseen data",
  () => {
    let pruned = 5;
    let latest = 10;
    const sourceRows = ["a", "b", "c"].map((thread_id) => ({
      thread_id,
      project_id: "project",
      deleted_at: null,
      relationship: null,
      side_chat: null,
    }));
    const native = {
      listReconciliationPage: (after: string, limit: number) =>
        Effect.succeed(sourceRows.filter((row) => row.thread_id > after).slice(0, limit)),
    } as unknown as Adapter.NativeAdapter["Service"];
    const events = {
      get latestApplicationSequence() {
        return Effect.sync(() => latest);
      },
      get prunedThroughSequence() {
        return Effect.sync(() => pruned);
      },
    } as unknown as Events.OrchestrationEventStore["Service"];
    const folders = {
      isScratchProject: () => Effect.succeed(false),
    } as unknown as Folders.ManagedProjectFolders["Service"];
    return Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* sql`INSERT INTO sh_ingestion_checkpoints VALUES ('env',1,10,'RECONCILING')`;
      yield* sql`INSERT INTO sh_native_projection VALUES ('env','unseen',1,'keep',0)`;
      assert.equal((yield* Reconciliation.reconcilePage("env", 1)).state, "SCANNING");
      assert.deepEqual(yield* sql`SELECT after_thread_id FROM sh_reconciliation`, [
        { after_thread_id: "a" },
      ]);
      assert.equal((yield* sql`SELECT session_id FROM sh_thread_bindings`).length, 0);
      assert.equal((yield* sql`SELECT thread_id FROM sh_thread_refresh_queue`).length, 0);
      assert.equal(
        (yield* sql`SELECT thread_id FROM sh_native_projection WHERE thread_id='unseen'`).length,
        1,
      );
      latest = 20;
      pruned = 15;
      assert.equal((yield* Reconciliation.reconcilePage("env", 1)).state, "SCANNING");
      assert.deepEqual(yield* sql`SELECT base_sequence,after_thread_id FROM sh_reconciliation`, [
        { base_sequence: 20, after_thread_id: "a" },
      ]);
      assert.equal((yield* Reconciliation.reconcilePage("env", 1)).state, "SCANNING");
      assert.deepEqual(yield* Reconciliation.reconcilePage("env", 1), {
        state: "COMPLETE",
        cursor: 20,
        count: 1,
      });
      assert.deepEqual(yield* sql`SELECT cursor,state FROM sh_ingestion_checkpoints`, [
        { cursor: 20, state: "CATCHING_UP" },
      ]);
      assert.equal(
        (yield* sql`SELECT thread_id FROM sh_native_projection WHERE thread_id='unseen'`).length,
        1,
      );
      assert.equal((yield* sql`SELECT session_id FROM sh_thread_bindings`).length, 3);
      assert.deepEqual(
        yield* sql`SELECT thread_id,source_sequence,force_snapshot FROM sh_thread_refresh_queue ORDER BY thread_id`,
        [
          { thread_id: "a", source_sequence: 20, force_snapshot: 1 },
          { thread_id: "b", source_sequence: 20, force_snapshot: 1 },
          { thread_id: "c", source_sequence: 20, force_snapshot: 1 },
        ],
      );
      yield* sql`UPDATE sh_ingestion_checkpoints SET cursor=30,state='RECONCILING' WHERE environment_id='env'`;
      assert.deepEqual(yield* Reconciliation.reconcilePage("env", 1), { state: "SOURCE_REWOUND" });
      assert.deepEqual(yield* sql`SELECT cursor,state FROM sh_ingestion_checkpoints`, [
        { cursor: 30, state: "RECONCILING" },
      ]);
    }).pipe(
      Effect.provide(
        Layer.mergeAll(
          Storage.layerFromPath(":memory:"),
          Layer.succeed(Adapter.NativeAdapter, native),
          Layer.succeed(Events.OrchestrationEventStore, events),
          Layer.succeed(Folders.ManagedProjectFolders, folders),
        ),
      ),
    );
  },
);

it.effect("discovers a thread after its competing create operation is rejected", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* Saga.prepare({
      environmentId: "env",
      idempotencyKey: "key",
      actorId: "actor",
      requestId: "r",
      sessionId: "candidate",
      commandId: "command",
      threadId: "thread",
      projectId: Storage.UNASSIGNED_PROJECT_ID,
      sectionId: null,
      mode: "work",
      requestPayload: "payload",
    });
    yield* Discovery.discover({
      environmentId: "env",
      threadId: "thread",
      mode: "code",
      requestId: "event",
    });
    assert.equal((yield* sql`SELECT thread_id FROM sh_discovery_pending`).length, 1);
    assert.isFalse(yield* Discovery.resolvePending("env"));
    yield* Saga.transition({
      environmentId: "env",
      key: "key",
      expected: "PREPARED",
      next: "REJECTED",
    });
    assert.isTrue(yield* Discovery.resolvePending("env"));
    assert.equal((yield* sql`SELECT thread_id FROM sh_discovery_pending`).length, 0);
    assert.deepEqual(
      yield* sql`SELECT s.mode,b.t3_thread_id FROM sh_thread_bindings b JOIN sh_sessions s ON s.id=b.session_id`,
      [{ mode: "code", t3_thread_id: "thread" }],
    );
  }).pipe(Effect.provide(Storage.layerFromPath(":memory:"))),
);
