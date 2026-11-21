// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";
import * as Storage from "./Storage.ts";
import * as Saga from "./CreateSagaStore.ts";
import * as Repository from "./Repository.ts";
import * as TestClock from "effect/testing/TestClock";

it.effect("rotates pending recovery work and prevents moving unconfirmed sessions", () =>
  Effect.gen(function* () {
    const input = {
      environmentId: "env",
      idempotencyKey: "one",
      actorId: "actor",
      requestId: "r",
      sessionId: "s1",
      commandId: "c1",
      threadId: "t1",
      projectId: Storage.UNASSIGNED_PROJECT_ID,
      sectionId: null,
      mode: "work" as const,
      requestPayload: "payload",
    };
    yield* Saga.prepare(input);
    yield* Saga.prepare({
      ...input,
      idempotencyKey: "two",
      sessionId: "s2",
      commandId: "c2",
      threadId: "t2",
    });
    yield* Repository.createProject({ id: "other", name: "Other" });
    yield* Effect.flip(
      Repository.moveSession({
        sessionId: "s1",
        projectId: "other",
        sectionId: null,
        expectedRevision: 1,
      }),
    );
    assert.isNull(yield* Saga.nextRecovery("env"));
    yield* TestClock.adjust("31 seconds");
    assert.equal((yield* Saga.nextRecovery("env"))?.sessionId, "s1");
    assert.equal((yield* Saga.nextRecovery("env"))?.sessionId, "s2");
    assert.equal((yield* Saga.nextRecovery("env"))?.sessionId, "s1");
    assert.isNull(yield* Saga.nextRecovery("other"));
  }).pipe(Effect.provide(Storage.layerFromPath(":memory:"))),
);

it.effect(
  "persists create intent, unknown delivery and binding without duplicates across restarts",
  () => {
    const dir = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "semanggi-saga-"));
    const path = NodePath.join(dir, "semanggi.sqlite");
    const input = {
      environmentId: "env",
      idempotencyKey: "key",
      actorId: "actor",
      requestId: "request",
      sessionId: "session",
      commandId: "command",
      threadId: "thread",
      projectId: Storage.UNASSIGNED_PROJECT_ID,
      sectionId: null,
      mode: "work" as const,
      requestPayload: "native-create-payload",
    };
    return Effect.gen(function* () {
      assert.equal(
        (yield* Saga.prepare(input).pipe(Effect.provide(Storage.layerFromPath(path)))).state,
        "PREPARED",
      );
      assert.equal(
        (yield* Repository.listSessions(Storage.UNASSIGNED_PROJECT_ID).pipe(
          Effect.provide(Storage.layerFromPath(path)),
        )).length,
        0,
      );
      yield* Effect.flip(
        Saga.prepare({ ...input, requestPayload: "different" }).pipe(
          Effect.provide(Storage.layerFromPath(path)),
        ),
      );
      assert.equal(
        (yield* Saga.transition({
          environmentId: "env",
          key: "key",
          expected: "PREPARED",
          next: "DISPATCHED",
        }).pipe(Effect.provide(Storage.layerFromPath(path)))).state,
        "DISPATCHED",
      );
      assert.equal(
        (yield* Saga.transition({
          environmentId: "env",
          key: "key",
          expected: "DISPATCHED",
          next: "NEEDS_RECONCILIATION",
        }).pipe(Effect.provide(Storage.layerFromPath(path)))).state,
        "NEEDS_RECONCILIATION",
      );
      yield* Effect.flip(
        Saga.transition({
          environmentId: "env",
          key: "key",
          expected: "NEEDS_RECONCILIATION",
          next: "DISPATCHED",
        }).pipe(Effect.provide(Storage.layerFromPath(path))),
      );
      assert.equal(
        (yield* Saga.transition({
          environmentId: "env",
          key: "key",
          expected: "NEEDS_RECONCILIATION",
          next: "NATIVE_CONFIRMED",
          nativeSequence: 42,
        }).pipe(Effect.provide(Storage.layerFromPath(path)))).state,
        "NATIVE_CONFIRMED",
      );
      assert.equal(
        (yield* Saga.bindConfirmed("env", "key").pipe(Effect.provide(Storage.layerFromPath(path))))
          .state,
        "BOUND",
      );
      assert.equal(
        (yield* Repository.listSessions(Storage.UNASSIGNED_PROJECT_ID).pipe(
          Effect.provide(Storage.layerFromPath(path)),
        )).length,
        1,
      );
      assert.equal(
        (yield* Saga.bindConfirmed("env", "key").pipe(Effect.provide(Storage.layerFromPath(path))))
          .state,
        "BOUND",
      );
      assert.equal(
        (yield* Saga.prepare({ ...input, requestId: "retry" }).pipe(
          Effect.provide(Storage.layerFromPath(path)),
        )).native_sequence,
        42,
      );
      yield* Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        const sessions = yield* sql`SELECT id FROM sh_sessions WHERE id = 'session'`;
        const bindings =
          yield* sql`SELECT session_id FROM sh_thread_bindings WHERE t3_thread_id = 'thread'`;
        const audits =
          yield* sql`SELECT sequence FROM sh_audit WHERE event_type = 'binding.create'`;
        assert.equal(sessions.length, 1);
        assert.equal(bindings.length, 1);
        assert.equal(audits.length, 1);
      }).pipe(Effect.provide(Storage.layerFromPath(path)));
    }).pipe(
      Effect.ensuring(Effect.sync(() => NodeFS.rmSync(dir, { recursive: true, force: true }))),
    );
  },
);
