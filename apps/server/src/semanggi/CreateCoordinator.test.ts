import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as NativeAdapter from "./NativeAdapter.ts";
import * as CreateCoordinator from "./CreateCoordinator.ts";
import * as Storage from "./Storage.ts";
import type * as CommandReceipts from "../orchestration-v2/CommandReceiptStore.ts";
import * as SqlClient from "effect/sql/SqlClient";
import * as Saga from "./CreateSagaStore.ts";
import * as Recovery from "./CreateRecovery.ts";
import * as TestClock from "effect/testing/TestClock";

it.effect(
  "does not redispatch an unknown create and recovers only from the matching receipt",
  () => {
    let dispatches = 0;
    let receiptVisible = false;
    let receiptThreadId = "other-thread";
    const adapter = {
      dispatch: () =>
        Effect.sync(() => {
          dispatches++;
          return { sequence: 4, storedEvents: [] };
        }),
      lookupCommand: () =>
        Effect.sync(() =>
          receiptVisible
            ? Option.some({
                threadId: receiptThreadId,
                commandId: "command",
                commandType: "thread.create",
                status: "accepted",
                resultSequence: 4,
              } as CommandReceipts.CommandReceiptV2)
            : Option.none(),
        ),
    } as unknown as NativeAdapter.NativeAdapter["Service"];
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
      requestPayload:
        '{"type":"thread.create","commandId":"command","threadId":"thread","projectId":"native-project","title":"Thread","modelSelection":{"instanceId":"instance","model":"gpt-test"},"runtimeMode":"full-access","interactionMode":"default","branch":null,"worktreePath":null,"createdBy":"user","creationSource":"web"}',
    };
    return Effect.gen(function* () {
      const first = yield* CreateCoordinator.createOrRecover(input);
      assert.equal(first.state, "NEEDS_RECONCILIATION");
      assert.equal(dispatches, 1);
      const repeated = yield* CreateCoordinator.createOrRecover(input);
      assert.equal(repeated.state, "NEEDS_RECONCILIATION");
      assert.equal(dispatches, 1);
      receiptVisible = true;
      assert.equal((yield* CreateCoordinator.createOrRecover(input)).state, "NEEDS_RECONCILIATION");
      assert.equal(dispatches, 1);
      receiptThreadId = "thread";
      const recovered = yield* CreateCoordinator.createOrRecover(input);
      assert.equal(recovered.state, "BOUND");
      assert.equal(recovered.native_sequence, 4);
      assert.equal(dispatches, 1);
      assert.equal((yield* CreateCoordinator.createOrRecover(input)).state, "BOUND");
      assert.equal(dispatches, 1);
    }).pipe(
      Effect.provide(
        Layer.mergeAll(
          Storage.layerFromPath(":memory:"),
          Layer.succeed(NativeAdapter.NativeAdapter, adapter),
        ),
      ),
    );
  },
);

it.effect("records a definitive native rejection without creating a binding", () => {
  const adapter = {
    dispatch: () => Effect.die("simulated delivery interruption"),
    lookupCommand: () =>
      Effect.succeedSome({
        threadId: "thread",
        commandId: "command",
        commandType: "thread.create",
        status: "rejected",
        resultSequence: 0,
      } as CommandReceipts.CommandReceiptV2),
  } as unknown as NativeAdapter.NativeAdapter["Service"];
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
    requestPayload:
      '{"type":"thread.create","commandId":"command","threadId":"thread","projectId":"native-project","title":"Thread","modelSelection":{"instanceId":"instance","model":"gpt-test"},"runtimeMode":"full-access","interactionMode":"default","branch":null,"worktreePath":null,"createdBy":"user","creationSource":"web"}',
  };
  return Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* Saga.prepare(input);
    yield* Saga.transition({
      environmentId: "env",
      key: "key",
      expected: "PREPARED",
      next: "DISPATCHED",
    });
    assert.equal((yield* CreateCoordinator.createOrRecover(input)).state, "REJECTED");
    assert.equal((yield* sql`SELECT session_id FROM sh_thread_bindings`).length, 0);
  }).pipe(
    Effect.provide(
      Layer.mergeAll(
        Storage.layerFromPath(":memory:"),
        Layer.succeed(NativeAdapter.NativeAdapter, adapter),
      ),
    ),
  );
});

it.effect("rejects an already-used native command ID before dispatch", () => {
  let dispatches = 0;
  const adapter = {
    dispatch: () =>
      Effect.sync(() => {
        dispatches++;
        return { sequence: 1, storedEvents: [] };
      }),
    lookupCommand: () =>
      Effect.succeedSome({
        threadId: "thread",
        commandId: "command",
        commandType: "thread.create",
        status: "accepted",
        resultSequence: 1,
      } as CommandReceipts.CommandReceiptV2),
  } as unknown as NativeAdapter.NativeAdapter["Service"];
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
    requestPayload:
      '{"type":"thread.create","commandId":"command","threadId":"thread","projectId":"native-project","title":"Thread","modelSelection":{"instanceId":"instance","model":"gpt-test"},"runtimeMode":"full-access","interactionMode":"default","branch":null,"worktreePath":null,"createdBy":"user","creationSource":"web"}',
  };
  return Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    assert.equal((yield* CreateCoordinator.createOrRecover(input)).state, "REJECTED");
    assert.equal(dispatches, 0);
    assert.equal((yield* sql`SELECT session_id FROM sh_thread_bindings`).length, 0);
  }).pipe(
    Effect.provide(
      Layer.mergeAll(
        Storage.layerFromPath(":memory:"),
        Layer.succeed(NativeAdapter.NativeAdapter, adapter),
      ),
    ),
  );
});

it.effect("recovers an aged prepared command once and then leaves it complete", () => {
  let dispatches = 0;
  const adapter = {
    dispatch: () =>
      Effect.sync(() => {
        dispatches++;
        return { sequence: 4, storedEvents: [] };
      }),
    lookupCommand: () =>
      Effect.sync(() =>
        dispatches === 0
          ? Option.none()
          : Option.some({
              threadId: "thread",
              commandId: "command",
              commandType: "thread.create",
              status: "accepted",
              resultSequence: 4,
            } as CommandReceipts.CommandReceiptV2),
      ),
  } as unknown as NativeAdapter.NativeAdapter["Service"];
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
    requestPayload:
      '{"type":"thread.create","commandId":"command","threadId":"thread","projectId":"native-project","title":"Thread","modelSelection":{"instanceId":"instance","model":"gpt-test"},"runtimeMode":"full-access","interactionMode":"default","branch":null,"worktreePath":null,"createdBy":"user","creationSource":"web"}',
  };
  return Effect.gen(function* () {
    yield* Saga.prepare(input);
    assert.equal((yield* Recovery.recoverNext("env")).state, "IDLE");
    yield* TestClock.adjust("31 seconds");
    assert.equal((yield* Recovery.recoverNext("env")).state, "BOUND");
    assert.equal(dispatches, 1);
    assert.equal((yield* Recovery.recoverNext("env")).state, "IDLE");
    assert.equal(dispatches, 1);
  }).pipe(
    Effect.provide(
      Layer.mergeAll(
        Storage.layerFromPath(":memory:"),
        Layer.succeed(NativeAdapter.NativeAdapter, adapter),
      ),
    ),
  );
});
