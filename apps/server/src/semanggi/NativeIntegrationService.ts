import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as SqlClient from "effect/sql/SqlClient";
import * as ServerConfig from "../config.ts";
import * as NativeAdapter from "./NativeAdapter.ts";
import * as Coordinator from "./CreateCoordinator.ts";
import * as Storage from "./Storage.ts";
import * as Search from "./LexicalSearch.ts";
import * as Ingestion from "./ReplayIngestion.ts";
import * as EventStore from "../persistence/OrchestrationEventStore.ts";
import * as Refresh from "./RefreshProjection.ts";
import {
  SemanggiCreateThreadInput,
  SemanggiCreateStatusInput,
  SemanggiPendingCreatesInput,
  SemanggiRequestError,
  SemanggiSearchInput,
  SemanggiOpenSourceInput,
  type SemanggiCreateStatus,
  type SemanggiPendingCreatesResult,
  type SemanggiSearchResult,
  type SemanggiOpenSourceResult,
} from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import * as Saga from "./CreateSagaStore.ts";
import * as Recovery from "./CreateRecovery.ts";
import * as ManagedProjectFolders from "../project/ManagedProjectFolders.ts";
import * as Reconciliation from "./Reconciliation.ts";
import * as Discovery from "./Discovery.ts";
import * as Inbox from "./ReplayInbox.ts";

const decodeCreate = Schema.decodeUnknownEffect(SemanggiCreateThreadInput, {
  onExcessProperty: "error",
});
const decodeStatus = Schema.decodeUnknownEffect(SemanggiCreateStatusInput, {
  onExcessProperty: "error",
});
const decodePending = Schema.decodeUnknownEffect(SemanggiPendingCreatesInput, {
  onExcessProperty: "error",
});
const isSagaConflict = Schema.is(Saga.CreateSagaConflict);
const isSearchScopeError = Schema.is(Search.SearchScopeError);
const decodeSearch = Schema.decodeUnknownEffect(SemanggiSearchInput, { onExcessProperty: "error" });
const decodeOpenSource = Schema.decodeUnknownEffect(SemanggiOpenSourceInput, {
  onExcessProperty: "error",
});

export class NativeIntegrationService extends Context.Service<
  NativeIntegrationService,
  {
    readonly createOrRecover: (
      input: Parameters<typeof Coordinator.createOrRecover>[0],
    ) => Effect.Effect<
      Effect.Success<ReturnType<typeof Coordinator.createOrRecover>>,
      Effect.Error<ReturnType<typeof Coordinator.createOrRecover>>
    >;
    readonly startCreate: (
      actorId: string,
      environmentId: string,
      input: unknown,
    ) => Effect.Effect<SemanggiCreateStatus, SemanggiRequestError>;
    readonly getCreateStatus: (
      actorId: string,
      environmentId: string,
      input: unknown,
    ) => Effect.Effect<SemanggiCreateStatus, SemanggiRequestError>;
    readonly listPendingCreates: (
      actorId: string,
      environmentId: string,
      input: unknown,
    ) => Effect.Effect<SemanggiPendingCreatesResult, SemanggiRequestError>;
    readonly recoverNextCreate: (
      environmentId: string,
    ) => Effect.Effect<
      Effect.Success<ReturnType<typeof Recovery.recoverNext>>,
      Effect.Error<ReturnType<typeof Recovery.recoverNext>>
    >;
    readonly search: (
      input: Parameters<typeof Search.search>[0],
    ) => Effect.Effect<
      Effect.Success<ReturnType<typeof Search.search>>,
      Effect.Error<ReturnType<typeof Search.search>>
    >;
    readonly searchPublic: (
      environmentId: string,
      input: unknown,
    ) => Effect.Effect<SemanggiSearchResult, SemanggiRequestError>;
    readonly openSearchSourcePublic: (
      environmentId: string,
      input: unknown,
    ) => Effect.Effect<SemanggiOpenSourceResult, SemanggiRequestError>;
    readonly openSearchSource: (
      input: Parameters<typeof Search.openSource>[0],
    ) => Effect.Effect<
      Effect.Success<ReturnType<typeof Search.openSource>>,
      Effect.Error<ReturnType<typeof Search.openSource>>
    >;
    readonly ingestNextPage: (
      input: Parameters<typeof Ingestion.ingestNextPage>[0],
    ) => Effect.Effect<
      Effect.Success<ReturnType<typeof Ingestion.ingestNextPage>>,
      Effect.Error<ReturnType<typeof Ingestion.ingestNextPage>>
    >;
    readonly refreshOne: (
      environmentId: string,
    ) => Effect.Effect<
      Effect.Success<ReturnType<typeof Refresh.refreshOne>>,
      Effect.Error<ReturnType<typeof Refresh.refreshOne>>
    >;
    readonly reconcilePage: (
      environmentId: string,
      limit: number,
    ) => Effect.Effect<
      Effect.Success<ReturnType<typeof Reconciliation.reconcilePage>>,
      Effect.Error<ReturnType<typeof Reconciliation.reconcilePage>>
    >;
    readonly resolvePendingDiscovery: (
      environmentId: string,
    ) => Effect.Effect<
      Effect.Success<ReturnType<typeof Discovery.resolveVerifiedPending>>,
      Effect.Error<ReturnType<typeof Discovery.resolveVerifiedPending>>
    >;
    readonly markLiveIfIdle: (
      environmentId: string,
    ) => Effect.Effect<
      Effect.Success<ReturnType<typeof Inbox.markLiveIfIdle>>,
      Effect.Error<ReturnType<typeof Inbox.markLiveIfIdle>>
    >;
  }
>()("t3/semanggi/NativeIntegrationService") {}

const make = Effect.gen(function* () {
  const native = yield* NativeAdapter.NativeAdapter;
  const sql = yield* SqlClient.SqlClient;
  const events = yield* EventStore.OrchestrationEventStore;
  const folders = yield* ManagedProjectFolders.ManagedProjectFolders;
  return NativeIntegrationService.of({
    createOrRecover: Effect.fn("NativeIntegrationService.createOrRecover")((input) =>
      Coordinator.createOrRecover(input).pipe(
        Effect.provideService(NativeAdapter.NativeAdapter, native),
        Effect.provideService(SqlClient.SqlClient, sql),
      ),
    ),
    startCreate: Effect.fn("NativeIntegrationService.startCreate")(
      function* (actorId, environmentId, raw) {
        const input = yield* decodeCreate(raw).pipe(
          Effect.mapError(() => new SemanggiRequestError({ reason: "invalid_request" })),
        );
        if (!actorId.trim() || input.scope.environmentId !== environmentId) {
          return yield* new SemanggiRequestError({ reason: "scope_mismatch" });
        }
        const operation = yield* Coordinator.createOrRecover({
          actorId,
          environmentId,
          requestId: input.requestId,
          idempotencyKey: input.idempotencyKey,
          projectId: input.scope.projectId,
          sectionId: input.sectionId,
          sessionId: input.sessionId,
          commandId: input.commandId,
          threadId: input.threadId,
          mode: input.mode,
          requestPayload: input.nativeCommandJson,
        }).pipe(
          Effect.provideService(NativeAdapter.NativeAdapter, native),
          Effect.provideService(SqlClient.SqlClient, sql),
          Effect.mapError(
            (error) =>
              new SemanggiRequestError({
                reason: isSagaConflict(error) ? "conflict" : "storage_error",
              }),
          ),
        );
        return {
          state: operation.state,
          sessionId: operation.session_id,
          threadId: operation.thread_id,
          revision: operation.revision,
        };
      },
    ),
    getCreateStatus: Effect.fn("NativeIntegrationService.getCreateStatus")(
      function* (actorId, environmentId, raw) {
        const input = yield* decodeStatus(raw).pipe(
          Effect.mapError(() => new SemanggiRequestError({ reason: "invalid_request" })),
        );
        if (!actorId.trim() || input.scope.environmentId !== environmentId) {
          return yield* new SemanggiRequestError({ reason: "scope_mismatch" });
        }
        const rows = yield* sql<{
          state: SemanggiCreateStatus["state"];
          session_id: string;
          thread_id: string;
          revision: number;
        }>`
        SELECT op.state, op.session_id, op.thread_id, op.revision FROM sh_thread_create_operations op
        JOIN sh_sessions s ON s.id = op.session_id
        WHERE op.environment_id = ${environmentId} AND op.idempotency_key = ${input.idempotencyKey}
          AND op.actor_id = ${actorId} AND s.project_id = ${input.scope.projectId}`.pipe(
          Effect.mapError(() => new SemanggiRequestError({ reason: "storage_error" })),
        );
        if (!rows[0]) return yield* new SemanggiRequestError({ reason: "invalid_request" });
        return {
          state: rows[0].state,
          sessionId: rows[0].session_id,
          threadId: rows[0].thread_id,
          revision: rows[0].revision,
        };
      },
    ),
    listPendingCreates: Effect.fn("NativeIntegrationService.listPendingCreates")(
      function* (actorId, environmentId, raw) {
        const input = yield* decodePending(raw).pipe(
          Effect.mapError(() => new SemanggiRequestError({ reason: "invalid_request" })),
        );
        if (!actorId.trim() || input.scope.environmentId !== environmentId)
          return yield* new SemanggiRequestError({ reason: "scope_mismatch" });
        const rows = yield* sql<{
          idempotency_key: string;
          state: SemanggiCreateStatus["state"];
          session_id: string;
          thread_id: string;
          revision: number;
        }>`
        SELECT op.idempotency_key,op.state,op.session_id,op.thread_id,op.revision FROM sh_thread_create_operations op
        JOIN sh_sessions s ON s.id=op.session_id
        WHERE op.environment_id=${environmentId} AND op.actor_id=${actorId} AND s.project_id=${input.scope.projectId}
          AND op.state IN ('PREPARED','DISPATCHED','NATIVE_CONFIRMED','NEEDS_RECONCILIATION')
          AND op.idempotency_key > ${input.afterKey ?? ""}
        ORDER BY op.idempotency_key LIMIT ${input.limit + 1}`.pipe(
          Effect.mapError(() => new SemanggiRequestError({ reason: "storage_error" })),
        );
        return {
          operations: rows.slice(0, input.limit).map((row) => ({
            idempotencyKey: row.idempotency_key,
            status: {
              state: row.state,
              sessionId: row.session_id,
              threadId: row.thread_id,
              revision: row.revision,
            },
          })),
          hasMore: rows.length > input.limit,
        };
      },
    ),
    recoverNextCreate: Effect.fn("NativeIntegrationService.recoverNextCreate")((environmentId) =>
      Recovery.recoverNext(environmentId).pipe(
        Effect.provideService(NativeAdapter.NativeAdapter, native),
        Effect.provideService(SqlClient.SqlClient, sql),
      ),
    ),
    searchPublic: Effect.fn("NativeIntegrationService.searchPublic")(
      function* (environmentId, raw) {
        const input = yield* decodeSearch(raw).pipe(
          Effect.mapError(() => new SemanggiRequestError({ reason: "invalid_request" })),
        );
        if (input.environmentId !== environmentId)
          return yield* new SemanggiRequestError({ reason: "scope_mismatch" });
        return yield* Search.search(input).pipe(
          Effect.provideService(NativeAdapter.NativeAdapter, native),
          Effect.provideService(SqlClient.SqlClient, sql),
          Effect.mapError(
            (error) =>
              new SemanggiRequestError({
                reason: isSearchScopeError(error) ? "invalid_request" : "storage_error",
              }),
          ),
        );
      },
    ),
    openSearchSourcePublic: Effect.fn("NativeIntegrationService.openSearchSourcePublic")(
      function* (environmentId, raw) {
        const input = yield* decodeOpenSource(raw).pipe(
          Effect.mapError(() => new SemanggiRequestError({ reason: "invalid_request" })),
        );
        if (input.environmentId !== environmentId)
          return yield* new SemanggiRequestError({ reason: "scope_mismatch" });
        return yield* Search.openSource(input).pipe(
          Effect.provideService(NativeAdapter.NativeAdapter, native),
          Effect.provideService(SqlClient.SqlClient, sql),
          Effect.mapError(
            (error) =>
              new SemanggiRequestError({
                reason: isSearchScopeError(error) ? "invalid_request" : "storage_error",
              }),
          ),
        );
      },
    ),
    search: Effect.fn("NativeIntegrationService.search")((input) =>
      Search.search(input).pipe(
        Effect.provideService(NativeAdapter.NativeAdapter, native),
        Effect.provideService(SqlClient.SqlClient, sql),
      ),
    ),
    openSearchSource: Effect.fn("NativeIntegrationService.openSearchSource")((input) =>
      Search.openSource(input).pipe(
        Effect.provideService(NativeAdapter.NativeAdapter, native),
        Effect.provideService(SqlClient.SqlClient, sql),
      ),
    ),
    ingestNextPage: Effect.fn("NativeIntegrationService.ingestNextPage")((input) =>
      Ingestion.ingestNextPage(input).pipe(
        Effect.provideService(EventStore.OrchestrationEventStore, events),
        Effect.provideService(ManagedProjectFolders.ManagedProjectFolders, folders),
        Effect.provideService(SqlClient.SqlClient, sql),
      ),
    ),
    refreshOne: Effect.fn("NativeIntegrationService.refreshOne")((environmentId) =>
      Refresh.refreshOne(environmentId).pipe(
        Effect.provideService(NativeAdapter.NativeAdapter, native),
        Effect.provideService(SqlClient.SqlClient, sql),
      ),
    ),
    reconcilePage: Effect.fn("NativeIntegrationService.reconcilePage")((environmentId, limit) =>
      Reconciliation.reconcilePage(environmentId, limit).pipe(
        Effect.provideService(NativeAdapter.NativeAdapter, native),
        Effect.provideService(EventStore.OrchestrationEventStore, events),
        Effect.provideService(ManagedProjectFolders.ManagedProjectFolders, folders),
        Effect.provideService(SqlClient.SqlClient, sql),
      ),
    ),
    resolvePendingDiscovery: Effect.fn("NativeIntegrationService.resolvePendingDiscovery")(
      (environmentId) =>
        Discovery.resolveVerifiedPending(environmentId).pipe(
          Effect.provideService(NativeAdapter.NativeAdapter, native),
          Effect.provideService(SqlClient.SqlClient, sql),
        ),
    ),
    markLiveIfIdle: Effect.fn("NativeIntegrationService.markLiveIfIdle")((environmentId) =>
      Inbox.markLiveIfIdle(environmentId).pipe(Effect.provideService(SqlClient.SqlClient, sql)),
    ),
  });
});

export const layer = Layer.effect(NativeIntegrationService, make);

export const layerConfig = Layer.unwrap(
  Effect.gen(function* () {
    const config = yield* ServerConfig.ServerConfig;
    const path = yield* Path.Path;
    return layer.pipe(
      Layer.provide(Storage.layerFromPath(path.join(config.stateDir, "semanggi.sqlite"))),
    );
  }),
);
