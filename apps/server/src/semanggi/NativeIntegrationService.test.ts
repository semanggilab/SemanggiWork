import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as EventStore from "../persistence/OrchestrationEventStore.ts";
import * as Adapter from "./NativeAdapter.ts";
import * as Integration from "./NativeIntegrationService.ts";
import * as Storage from "./Storage.ts";
import * as Repository from "./Repository.ts";
import * as ManagedProjectFolders from "../project/ManagedProjectFolders.ts";

const folders = {
  isScratchProject: () => Effect.succeed(false),
} as unknown as ManagedProjectFolders.ManagedProjectFolders["Service"];

it.effect("checks environment and input fields before public search and source opening", () => {
  const native = {
    searchThreads: () =>
      Effect.succeed({
        matches: [{ threadId: "thread", projectId: "native", title: "Result", source: "title" }],
      }),
    readThread: () =>
      Effect.succeed({
        snapshotSequence: 5,
        projection: {
          thread: { id: "thread", title: "Result", deletedAt: null },
          messages: [],
          visibleTurnItems: [],
        },
      }),
  } as unknown as Adapter.NativeAdapter["Service"];
  const events = {} as EventStore.OrchestrationEventStore["Service"];
  const layer = Integration.layer.pipe(
    Layer.provideMerge(Storage.layerFromPath(":memory:")),
    Layer.provide(
      Layer.mergeAll(
        Layer.succeed(Adapter.NativeAdapter, native),
        Layer.succeed(EventStore.OrchestrationEventStore, events),
        Layer.succeed(ManagedProjectFolders.ManagedProjectFolders, folders),
      ),
    ),
  );
  return Effect.gen(function* () {
    yield* Repository.createSession({
      id: "session",
      projectId: Storage.UNASSIGNED_PROJECT_ID,
      sectionId: null,
      mode: "code",
    });
    yield* Repository.bindThread({
      sessionId: "session",
      environmentId: "env",
      threadId: "thread",
    });
    const integration = yield* Integration.NativeIntegrationService;
    const input = { environmentId: "env", projectId: null, query: "result", limit: 10 };
    assert.equal(
      (yield* Effect.flip(integration.searchPublic("other", input))).reason,
      "scope_mismatch",
    );
    assert.equal(
      (yield* Effect.flip(integration.searchPublic("env", { ...input, extra: true }))).reason,
      "invalid_request",
    );
    const result = yield* integration.searchPublic("env", input);
    assert.equal(result.coverage, "partial");
    assert.equal(result.matches[0]?.sessionId, "session");
    const opened = yield* integration.openSearchSourcePublic("env", {
      environmentId: "env",
      projectId: null,
      sessionId: "session",
    });
    assert.equal(opened.threadId, "thread");
    assert.equal(opened.mode, "code");
    assert.equal(
      (yield* Effect.flip(
        integration.openSearchSourcePublic("other", {
          environmentId: "env",
          projectId: null,
          sessionId: "session",
        }),
      )).reason,
      "scope_mismatch",
    );
  }).pipe(Effect.provide(layer));
});

it.effect("exposes unknown create status only to its authenticated actor and scope", () => {
  let dispatches = 0;
  const native = {
    dispatch: () =>
      Effect.sync(() => {
        dispatches++;
        return { sequence: 1, storedEvents: [] };
      }),
    lookupCommand: () => Effect.succeedNone,
  } as unknown as Adapter.NativeAdapter["Service"];
  const events = {} as EventStore.OrchestrationEventStore["Service"];
  const layer = Integration.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        Storage.layerFromPath(":memory:"),
        Layer.succeed(Adapter.NativeAdapter, native),
        Layer.succeed(EventStore.OrchestrationEventStore, events),
        Layer.succeed(ManagedProjectFolders.ManagedProjectFolders, folders),
      ),
    ),
  );
  const input = {
    requestId: "request",
    idempotencyKey: "key",
    scope: { environmentId: "env", projectId: Storage.UNASSIGNED_PROJECT_ID },
    sessionId: "session",
    commandId: "command",
    threadId: "thread",
    sectionId: null,
    mode: "work" as const,
    nativeCommandJson:
      '{"type":"thread.create","commandId":"command","threadId":"thread","projectId":"native-project","title":"Thread","modelSelection":{"instanceId":"instance","model":"gpt-test"},"runtimeMode":"full-access","interactionMode":"default","branch":null,"worktreePath":null,"createdBy":"user","creationSource":"web"}',
  };
  return Effect.gen(function* () {
    const integration = yield* Integration.NativeIntegrationService;
    const forged = yield* Effect.flip(
      integration.startCreate("actor", "env", { ...input, actorId: "forged" }),
    );
    assert.equal(forged.reason, "invalid_request");
    const foreign = yield* Effect.flip(integration.startCreate("actor", "other", input));
    assert.equal(foreign.reason, "scope_mismatch");
    const first = yield* integration.startCreate("actor", "env", input);
    assert.equal(first.state, "NEEDS_RECONCILIATION");
    assert.equal(dispatches, 1);
    const status = yield* integration.getCreateStatus("actor", "env", {
      scope: input.scope,
      idempotencyKey: "key",
    });
    assert.deepEqual(status, first);
    assert.isFalse("request_payload" in status);
    const pending = yield* integration.listPendingCreates("actor", "env", {
      scope: input.scope,
      limit: 1,
    });
    assert.deepEqual(pending, {
      operations: [{ idempotencyKey: "key", status: first }],
      hasMore: false,
    });
    assert.deepEqual(
      yield* integration.listPendingCreates("other", "env", { scope: input.scope, limit: 10 }),
      { operations: [], hasMore: false },
    );
    assert.deepEqual(
      yield* integration.listPendingCreates("actor", "env", {
        scope: { ...input.scope, projectId: "other" },
        limit: 10,
      }),
      { operations: [], hasMore: false },
    );
    assert.equal(
      (yield* Effect.flip(
        integration.listPendingCreates("actor", "other", { scope: input.scope, limit: 10 }),
      )).reason,
      "scope_mismatch",
    );
    assert.equal(
      (yield* Effect.flip(
        integration.listPendingCreates("actor", "env", { scope: input.scope, limit: 0 }),
      )).reason,
      "invalid_request",
    );
    assert.equal(
      (yield* integration.listPendingCreates("actor", "env", {
        scope: input.scope,
        limit: 10,
        afterKey: "key",
      })).operations.length,
      0,
    );
    yield* Effect.flip(
      integration.getCreateStatus("other", "env", { scope: input.scope, idempotencyKey: "key" }),
    );
    yield* Effect.flip(
      integration.getCreateStatus("actor", "env", {
        scope: { ...input.scope, projectId: "other" },
        idempotencyKey: "key",
      }),
    );
    assert.equal(
      (yield* integration.startCreate("actor", "env", input)).state,
      "NEEDS_RECONCILIATION",
    );
    assert.equal(dispatches, 1);
    yield* integration.startCreate("actor", "env", {
      ...input,
      idempotencyKey: "key2",
      sessionId: "session2",
      commandId: "command2",
      threadId: "thread2",
      nativeCommandJson: input.nativeCommandJson
        .replace('"commandId":"command"', '"commandId":"command2"')
        .replace('"threadId":"thread"', '"threadId":"thread2"'),
    });
    const page = yield* integration.listPendingCreates("actor", "env", {
      scope: input.scope,
      limit: 1,
    });
    assert.isTrue(page.hasMore);
    const next = yield* integration.listPendingCreates("actor", "env", {
      scope: input.scope,
      limit: 1,
      afterKey: page.operations[0]!.idempotencyKey,
    });
    assert.equal(next.operations[0]?.idempotencyKey, "key2");
    assert.isFalse(next.hasMore);
  }).pipe(Effect.provide(layer));
});
