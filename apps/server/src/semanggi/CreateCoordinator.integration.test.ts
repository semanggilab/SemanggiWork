// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import { assert, it } from "@effect/vitest";
import {
  EventId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ThreadId,
  OrchestrationV2Command,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/sql/SqlClient";
import * as Schema from "effect/Schema";
import * as NodeServices from "@effect/platform-node/NodeServices";
import type * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as T3Sqlite from "../persistence/Sqlite.ts";
import * as NativeReceipts from "../orchestration-v2/CommandReceiptStore.ts";
import * as Orchestrator from "../orchestration-v2/Orchestrator.ts";
import * as ProjectStore from "../orchestration-v2/ProjectStore.ts";
import * as ThreadSearch from "../orchestration-v2/ThreadSearch.ts";
import * as ProviderRegistry from "../orchestration-v2/ProviderAdapterRegistry.ts";
import { CodexProviderCapabilitiesV2 } from "../orchestration-v2/Adapters/CodexAdapterV2.ts";
import * as Harness from "../orchestration-v2/testkit/ProviderReplayHarness.ts";
import * as Adapter from "./NativeAdapter.ts";
import * as Coordinator from "./CreateCoordinator.ts";
import * as Storage from "./Storage.ts";
import * as Search from "./LexicalSearch.ts";
import * as Saga from "./CreateSagaStore.ts";
import * as DateTime from "effect/DateTime";
import * as EventSink from "../orchestration-v2/EventSink.ts";
import * as Events from "../persistence/OrchestrationEventStore.ts";
import * as Folders from "../project/ManagedProjectFolders.ts";
import * as Reconciliation from "./Reconciliation.ts";
import * as Refresh from "./RefreshProjection.ts";
import * as Ingestion from "./ReplayIngestion.ts";
import * as Service from "./Service.ts";

const instanceId = ProviderInstanceId.make("codex");
const decodeNativeCommand = Schema.decodeEffect(Schema.fromJsonString(OrchestrationV2Command));
const provider = {
  instanceId,
  driver: ProviderDriverKind.make("codex"),
  getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
  planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" as const }),
  openSession: () => Effect.die("No provider process needed for thread creation"),
} as ProviderAdapter.ProviderAdapterV2["Service"];
const database = T3Sqlite.layerMemory;
const nativeLayer = Layer.mergeAll(
  Harness.layerWithRegistry(
    { name: "semanggi-native-create" },
    ProviderRegistry.layerFromAdapters([provider]),
    { databaseLayer: database, runEffectWorker: false },
  ),
  NativeReceipts.layer.pipe(Layer.provide(database)),
  ProjectStore.layer.pipe(Layer.provide(database)),
  ThreadSearch.layer.pipe(Layer.provide(database)),
  Events.layer.pipe(Layer.provide(database)),
  database,
);

it.effect.each(["PREPARED", "DISPATCHED_BEFORE_SEND", "NATIVE_CONFIRMED", "BOUND"] as const)(
  "recovers a native create across database restart at %s",
  (crashPoint) => {
    const dir = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "semanggi-create-stage-"));
    const nativePath = NodePath.join(dir, "native.sqlite");
    const semanggiPath = NodePath.join(dir, "semanggi.sqlite");
    const nativeDb = () => {
      const db = T3Sqlite.layerFromPath(nativePath).pipe(Layer.provide(NodeServices.layer));
      return Layer.mergeAll(
        Harness.layerWithRegistry(
          { name: "semanggi-create-stage" },
          ProviderRegistry.layerFromAdapters([provider]),
          { databaseLayer: db, runEffectWorker: false },
        ),
        NativeReceipts.layer.pipe(Layer.provide(db)),
      );
    };
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
        '{"type":"thread.create","commandId":"command","threadId":"thread","projectId":"native-project","title":"Thread","modelSelection":{"instanceId":"codex","model":"gpt-5.1-codex"},"runtimeMode":"full-access","interactionMode":"default","branch":null,"worktreePath":null,"createdBy":"user","creationSource":"web"}',
    };
    const recover = Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const receipts = yield* NativeReceipts.CommandReceiptStoreV2;
      const adapter = Adapter.NativeAdapter.of({
        dispatch: orchestrator.dispatch,
        lookupCommand: receipts.getByCommandId,
        readThread: orchestrator.getThreadSnapshot,
        readHistory: orchestrator.getThreadHistoryPage,
        searchThreads: () => Effect.die("Search is not used"),
        listReconciliationPage: () => Effect.die("Reconciliation is not used"),
        capabilities: {
          create: true,
          commandReceiptLookup: true,
          historyPagination: true,
          lexicalSearch: true,
          durableBridge: false,
        },
      });
      return yield* Coordinator.createOrRecover(input).pipe(
        Effect.provide(
          Layer.mergeAll(
            Storage.layerFromPath(semanggiPath),
            Layer.succeed(Adapter.NativeAdapter, adapter),
          ),
        ),
      );
    });
    return Effect.gen(function* () {
      yield* Saga.prepare(input).pipe(Effect.provide(Storage.layerFromPath(semanggiPath)));
      if (crashPoint === "DISPATCHED_BEFORE_SEND" || crashPoint === "NATIVE_CONFIRMED") {
        yield* Saga.transition({
          environmentId: "env",
          key: "key",
          expected: "PREPARED",
          next: "DISPATCHED",
        }).pipe(Effect.provide(Storage.layerFromPath(semanggiPath)));
      }
      if (crashPoint === "NATIVE_CONFIRMED") {
        const sequence = yield* Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          return (yield* orchestrator.dispatch(yield* decodeNativeCommand(input.requestPayload)))
            .sequence;
        }).pipe(Effect.provide(nativeDb()));
        yield* Saga.transition({
          environmentId: "env",
          key: "key",
          expected: "DISPATCHED",
          next: "NATIVE_CONFIRMED",
          nativeSequence: sequence,
        }).pipe(Effect.provide(Storage.layerFromPath(semanggiPath)));
      }
      if (crashPoint === "BOUND")
        assert.equal((yield* recover.pipe(Effect.provide(nativeDb()))).state, "BOUND");
      const afterRestart = yield* recover.pipe(Effect.provide(nativeDb()));
      assert.equal(
        afterRestart.state,
        crashPoint === "DISPATCHED_BEFORE_SEND" ? "NEEDS_RECONCILIATION" : "BOUND",
      );
      const retry = yield* recover.pipe(Effect.provide(nativeDb()));
      assert.equal(retry.state, afterRestart.state);
      yield* Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        assert.equal((yield* sql`SELECT id FROM sh_sessions`).length, 1);
        assert.equal(
          (yield* sql`SELECT session_id FROM sh_thread_bindings`).length,
          crashPoint === "DISPATCHED_BEFORE_SEND" ? 0 : 1,
        );
        assert.equal(
          (yield* sql`SELECT sequence FROM sh_audit WHERE event_type='binding.create'`).length,
          crashPoint === "DISPATCHED_BEFORE_SEND" ? 0 : 1,
        );
      }).pipe(Effect.provide(Storage.layerFromPath(semanggiPath)));
    }).pipe(
      Effect.ensuring(Effect.sync(() => NodeFS.rmSync(dir, { recursive: true, force: true }))),
    );
  },
);

it.effect("recovers a committed native create after both databases reopen", () => {
  const dir = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "semanggi-native-restart-"));
  const nativePath = NodePath.join(dir, "native.sqlite");
  const semanggiPath = NodePath.join(dir, "semanggi.sqlite");
  const makeNativeLayer = () => {
    const db = T3Sqlite.layerFromPath(nativePath).pipe(Layer.provide(NodeServices.layer));
    return Layer.mergeAll(
      Harness.layerWithRegistry(
        { name: "semanggi-native-restart" },
        ProviderRegistry.layerFromAdapters([provider]),
        { databaseLayer: db, runEffectWorker: false },
      ),
      NativeReceipts.layer.pipe(Layer.provide(db)),
      ProjectStore.layer.pipe(Layer.provide(db)),
      ThreadSearch.layer.pipe(Layer.provide(db)),
      Events.layer.pipe(Layer.provide(db)),
    );
  };
  const input = {
    environmentId: "env",
    idempotencyKey: "key",
    actorId: "actor",
    requestId: "request",
    sessionId: "session",
    commandId: "native-command",
    threadId: "native-thread",
    projectId: "semanggi-project",
    sectionId: null,
    mode: "work" as const,
    requestPayload:
      '{"type":"thread.create","commandId":"native-command","threadId":"native-thread","projectId":"native-project","title":"Thread","modelSelection":{"instanceId":"codex","model":"gpt-5.1-codex"},"runtimeMode":"full-access","interactionMode":"default","branch":null,"worktreePath":null,"createdBy":"user","creationSource":"web"}',
  };
  return Effect.gen(function* () {
    yield* Effect.gen(function* () {
      const service = yield* Service.SemanggiService;
      yield* service.createProject(
        { actorId: "vertical-slice", environmentId: "env", requestId: "project-create" },
        { id: "semanggi-project", name: "Semanggi M1" },
      );
    }).pipe(
      Effect.provide(Service.layer.pipe(Layer.provideMerge(Storage.layerFromPath(semanggiPath)))),
    );
    yield* Saga.prepare(input).pipe(Effect.provide(Storage.layerFromPath(semanggiPath)));
    yield* Saga.transition({
      environmentId: "env",
      key: "key",
      expected: "PREPARED",
      next: "DISPATCHED",
    }).pipe(Effect.provide(Storage.layerFromPath(semanggiPath)));
    const originalSequence = yield* Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const projects = yield* ProjectStore.ProjectStoreV2;
      const projectId = ProjectId.make("native-project");
      yield* projects.apply({
        sequence: 0,
        eventId: EventId.make("restart-project"),
        aggregateKind: "project",
        aggregateId: projectId,
        occurredAt: "2026-01-01T00:00:00Z",
        commandId: null,
        causationEventId: null,
        correlationId: null,
        metadata: {},
        type: "project.created",
        payload: {
          projectId,
          title: "Native",
          workspaceRoot: "/tmp/native-project",
          defaultModelSelection: null,
          scripts: [],
          createdAt: "2026-01-01T00:00:00Z",
          updatedAt: "2026-01-01T00:00:00Z",
        },
      });
      const command = yield* decodeNativeCommand(input.requestPayload);
      return (yield* orchestrator.dispatch(command)).sequence;
    }).pipe(Effect.provide(makeNativeLayer()));
    yield* Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const receipts = yield* NativeReceipts.CommandReceiptStoreV2;
      const native = Adapter.NativeAdapter.of({
        dispatch: orchestrator.dispatch,
        lookupCommand: receipts.getByCommandId,
        readThread: orchestrator.getThreadSnapshot,
        readHistory: orchestrator.getThreadHistoryPage,
        searchThreads: () => Effect.die("Search is not used"),
        listReconciliationPage: () => Effect.die("Reconciliation is not used"),
        capabilities: {
          create: true,
          commandReceiptLookup: true,
          historyPagination: true,
          lexicalSearch: true,
          durableBridge: false,
        },
      });
      const recovered = yield* Coordinator.createOrRecover(input).pipe(
        Effect.provide(
          Layer.mergeAll(
            Storage.layerFromPath(semanggiPath),
            Layer.succeed(Adapter.NativeAdapter, native),
          ),
        ),
      );
      assert.equal(recovered.state, "BOUND");
      assert.equal(recovered.native_sequence, originalSequence);
      assert.equal(
        yield* orchestrator.getThreadEventSequence(ThreadId.make("native-thread")),
        originalSequence,
      );
    }).pipe(Effect.provide(makeNativeLayer()));
    yield* Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const command = yield* decodeNativeCommand(
        '{"type":"message.dispatch","commandId":"message-command","threadId":"native-thread","messageId":"message-one","text":"unique semanggi search token","attachments":[],"dispatchMode":{"type":"defer_start"},"createdBy":"user","creationSource":"web"}',
      );
      yield* orchestrator.dispatch(command);
    }).pipe(Effect.provide(makeNativeLayer()));
    yield* Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const sink = yield* EventSink.EventSinkV2;
      const events = yield* Events.OrchestrationEventStore;
      const native = {
        readThread: orchestrator.getThreadSnapshot,
      } as unknown as Adapter.NativeAdapter["Service"];
      const folders = {
        isScratchProject: () => Effect.succeed(false),
      } as unknown as Folders.ManagedProjectFolders["Service"];
      yield* Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        yield* Ingestion.ingestNextPage({ environmentId: "env", limit: 100 });
        assert.equal((yield* Refresh.refreshOne("env")).state, "REFRESHED");
        const previous = yield* sql<{
          content_hash: string;
        }>`SELECT content_hash FROM sh_message_projection WHERE message_id='message-one'`;
        const message = (yield* orchestrator.getThreadProjection(
          ThreadId.make("native-thread"),
        )).messages.find(({ id }) => id === "message-one")!;
        yield* sink.write({
          events: [
            {
              id: EventId.make("edited-message"),
              type: "message.updated",
              threadId: ThreadId.make("native-thread"),
              occurredAt: yield* DateTime.now,
              payload: { ...message, text: "revised semanggi search token" },
            },
          ],
        });
        yield* Ingestion.ingestNextPage({ environmentId: "env", limit: 100 });
        assert.equal((yield* Refresh.refreshOne("env")).state, "REFRESHED");
        const edited = yield* sql<{
          content_hash: string;
        }>`SELECT content_hash FROM sh_message_projection WHERE message_id='message-one'`;
        assert.notEqual(edited[0]?.content_hash, previous[0]?.content_hash);
      }).pipe(
        Effect.provide(
          Layer.mergeAll(
            Storage.layerFromPath(semanggiPath),
            Layer.succeed(Adapter.NativeAdapter, native),
            Layer.succeed(Events.OrchestrationEventStore, events),
            Layer.succeed(Folders.ManagedProjectFolders, folders),
          ),
        ),
      );
    }).pipe(Effect.provide(makeNativeLayer()));
    yield* Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const search = yield* ThreadSearch.ThreadSearch;
      const sink = yield* EventSink.EventSinkV2;
      const events = yield* Events.OrchestrationEventStore;
      const folders = {
        isScratchProject: () => Effect.succeed(false),
      } as unknown as Folders.ManagedProjectFolders["Service"];
      const native = {
        readThread: orchestrator.getThreadSnapshot,
        searchThreads: search.searchThreads,
      } as unknown as Adapter.NativeAdapter["Service"];
      yield* Effect.gen(function* () {
        const results = yield* Search.search({
          environmentId: "env",
          projectId: "semanggi-project",
          query: "semanggi search token",
          limit: 10,
        });
        assert.equal(results.matches[0]?.sessionId, "session");
        assert.equal(results.matches[0]?.messageId, "message-one");
        assert.equal(
          (yield* Search.openSource({
            environmentId: "env",
            projectId: "semanggi-project",
            sessionId: "session",
            messageId: "message-one",
          })).threadId,
          "native-thread",
        );
        const sql = yield* SqlClient.SqlClient;
        const projection = yield* orchestrator.getThreadProjection(ThreadId.make("native-thread"));
        const run = projection.runs.find((item) => item.userMessageId === "message-one");
        assert.isDefined(run);
        const now = yield* DateTime.now;
        yield* sink.write({
          events: [
            {
              id: EventId.make("run-rolled-back"),
              type: "run.updated",
              threadId: ThreadId.make("native-thread"),
              occurredAt: now,
              payload: { ...run!, status: "rolled_back", completedAt: now },
            },
          ],
        });
        yield* Ingestion.ingestNextPage({ environmentId: "env", limit: 100 });
        assert.equal((yield* Refresh.refreshOne("env")).state, "REFRESHED");
        assert.deepEqual(
          yield* sql`SELECT deleted FROM sh_message_projection WHERE message_id='message-one'`,
          [{ deleted: 1 }],
        );
        assert.equal(
          (yield* Search.search({
            environmentId: "env",
            projectId: "semanggi-project",
            query: "semanggi search token",
            limit: 10,
          })).matches.length,
          0,
        );
        yield* Effect.flip(
          Search.openSource({
            environmentId: "env",
            projectId: "semanggi-project",
            sessionId: "session",
            messageId: "message-one",
          }),
        );
      }).pipe(
        Effect.provide(
          Layer.mergeAll(
            Storage.layerFromPath(semanggiPath),
            Layer.succeed(Adapter.NativeAdapter, native),
            Layer.succeed(Events.OrchestrationEventStore, events),
            Layer.succeed(Folders.ManagedProjectFolders, folders),
          ),
        ),
      );
    }).pipe(Effect.provide(makeNativeLayer()));
    yield* Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      assert.equal((yield* sql`SELECT session_id FROM sh_thread_bindings`).length, 1);
      assert.deepEqual(yield* sql`SELECT id,project_id FROM sh_sessions`, [
        { id: "session", project_id: "semanggi-project" },
      ]);
    }).pipe(Effect.provide(Storage.layerFromPath(semanggiPath)));
  }).pipe(Effect.ensuring(Effect.sync(() => NodeFS.rmSync(dir, { recursive: true, force: true }))));
});

it.effect("creates a native T3 thread and binds its receipt in a separate database", () =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const projects = yield* ProjectStore.ProjectStoreV2;
    const nativeSql = yield* SqlClient.SqlClient;
    const nativeProjectId = ProjectId.make("native-project");
    yield* projects.apply({
      sequence: 0,
      eventId: EventId.make("native-project-create"),
      aggregateKind: "project",
      aggregateId: nativeProjectId,
      occurredAt: "2026-01-01T00:00:00Z",
      commandId: null,
      causationEventId: null,
      correlationId: null,
      metadata: {},
      type: "project.created",
      payload: {
        projectId: nativeProjectId,
        title: "Native",
        workspaceRoot: "/tmp/native-project",
        defaultModelSelection: null,
        scripts: [],
        createdAt: "2026-01-01T00:00:00Z",
        updatedAt: "2026-01-01T00:00:00Z",
      },
    });
    const native = yield* Adapter.NativeAdapter.pipe(Effect.provide(Adapter.layer));
    const events = yield* Events.OrchestrationEventStore;
    const folders = {
      isScratchProject: () => Effect.succeed(false),
    } as unknown as Folders.ManagedProjectFolders["Service"];
    const input = {
      environmentId: "env",
      idempotencyKey: "key",
      actorId: "actor",
      requestId: "request",
      sessionId: "session",
      commandId: "native-command",
      threadId: "native-thread",
      projectId: Storage.UNASSIGNED_PROJECT_ID,
      sectionId: null,
      mode: "work" as const,
      requestPayload:
        '{"type":"thread.create","commandId":"native-command","threadId":"native-thread","projectId":"native-project","title":"Thread","modelSelection":{"instanceId":"codex","model":"gpt-5.1-codex"},"runtimeMode":"full-access","interactionMode":"default","branch":null,"worktreePath":null,"createdBy":"user","creationSource":"web"}',
    };
    yield* Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const created = yield* Coordinator.createOrRecover(input);
      assert.equal(created.state, "BOUND");
      assert.equal((yield* Coordinator.createOrRecover(input)).state, "BOUND");
      const bindings = yield* sql<{
        session_id: string;
        t3_thread_id: string;
      }>`SELECT session_id,t3_thread_id FROM sh_thread_bindings`;
      assert.deepEqual(bindings, [{ session_id: "session", t3_thread_id: "native-thread" }]);
      const results = yield* Search.search({
        environmentId: "env",
        projectId: null,
        query: "Thread",
        limit: 10,
      });
      assert.equal(results.matches[0]?.sessionId, "session");
      const opened = yield* Search.openSource({
        environmentId: "env",
        projectId: null,
        sessionId: "session",
      });
      assert.equal(opened.threadId, "native-thread");
      const highWater = yield* events.latestApplicationSequence;
      yield* nativeSql`INSERT INTO orchestration_v2_projection_metadata(projection_name,schema_version,last_sequence,updated_at)
        VALUES (${Events.EVENT_LOG_PRUNED_THROUGH},2,${highWater},'2026-01-01T00:00:00Z')`;
      yield* nativeSql`DELETE FROM orchestration_events WHERE sequence <= ${highWater}`;
      assert.equal(
        (yield* Ingestion.ingestNextPage({ environmentId: "env", limit: 100 })).state,
        "RECONCILING",
      );
      assert.equal((yield* Reconciliation.reconcilePage("env", 1)).state, "COMPLETE");
      assert.equal((yield* Refresh.refreshOne("env")).state, "REFRESHED");
      assert.deepEqual(yield* sql`SELECT thread_id,title FROM sh_native_projection`, [
        { thread_id: "native-thread", title: "Thread" },
      ]);
      assert.equal((yield* sql`SELECT session_id FROM sh_thread_bindings`).length, 1);
      const deletion = yield* decodeNativeCommand(
        '{"type":"thread.delete","commandId":"delete-command","threadId":"native-thread"}',
      );
      yield* native.dispatch(deletion);
      yield* Ingestion.ingestNextPage({ environmentId: "env", limit: 100 });
      assert.equal((yield* Refresh.refreshOne("env")).state, "REFRESHED");
      assert.deepEqual(yield* sql`SELECT thread_id,deleted FROM sh_native_projection`, [
        { thread_id: "native-thread", deleted: 1 },
      ]);
      yield* Effect.flip(
        Search.openSource({ environmentId: "env", projectId: null, sessionId: "session" }),
      );
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
    const snapshot = yield* orchestrator.getThreadSnapshot(ThreadId.make("native-thread"));
    assert.equal(snapshot.projection.thread.title, "Thread");
  }).pipe(Effect.provide(nativeLayer)),
);
