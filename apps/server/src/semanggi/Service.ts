import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as SqlClient from "effect/sql/SqlClient";
import type * as SqlError from "effect/sql/SqlError";
import * as Schema from "effect/Schema";
import { SemanggiRequestError, SemanggiSessionMove } from "@t3tools/contracts";
import * as Repository from "./Repository.ts";
import * as Storage from "./Storage.ts";
import * as ServerConfig from "../config.ts";

const decodeMove = Schema.decodeUnknownEffect(SemanggiSessionMove, { onExcessProperty: "error" });
const isValidationError = Schema.is(Repository.SemanggiValidationError);

type Bound<F extends (...args: never[]) => Effect.Effect<unknown, unknown, SqlClient.SqlClient>> = (
  ...args: Parameters<F>
) => Effect.Effect<Effect.Success<ReturnType<F>>, Effect.Error<ReturnType<F>>>;

/** Supplied by the authenticated transport or an explicitly identified internal worker. */
export interface AuditContext {
  readonly actorId: string;
  readonly environmentId: string;
  readonly requestId: string;
}

type Audited<F extends (...args: never[]) => Effect.Effect<unknown, unknown, SqlClient.SqlClient>> =
  (
    context: AuditContext,
    ...args: Parameters<F>
  ) => Effect.Effect<
    Effect.Success<ReturnType<F>>,
    Effect.Error<ReturnType<F>> | SqlError.SqlError | Repository.SemanggiValidationError
  >;

export class SemanggiService extends Context.Service<
  SemanggiService,
  {
    readonly listProjects: Bound<typeof Repository.listProjects>;
    readonly listSections: Bound<typeof Repository.listSections>;
    readonly listSessions: Bound<typeof Repository.listSessions>;
    readonly listCatalogSessions: Bound<typeof Repository.listCatalogSessions>;
    readonly findSessionByThread: Bound<typeof Repository.findSessionByThread>;
    readonly createProject: Audited<typeof Repository.createProject>;
    readonly renameProject: Audited<typeof Repository.renameProject>;
    readonly setProjectArchived: Audited<typeof Repository.setProjectArchived>;
    readonly createSection: Audited<typeof Repository.createSection>;
    readonly renameSection: Audited<typeof Repository.renameSection>;
    readonly moveSectionSubtree: Audited<typeof Repository.moveSectionSubtree>;
    readonly reorderSection: Audited<typeof Repository.reorderSection>;
    readonly deleteLeafSection: Audited<typeof Repository.deleteLeafSection>;
    readonly listWorkspaceLinks: Bound<typeof Repository.listWorkspaceLinks>;
    readonly linkWorkspace: Audited<typeof Repository.linkWorkspace>;
    readonly unlinkWorkspace: Audited<typeof Repository.unlinkWorkspace>;
    readonly createSession: Audited<typeof Repository.createSession>;
    readonly bindThread: Audited<typeof Repository.bindThread>;
    readonly executeSessionMove: Bound<typeof Repository.executeSessionMove>;
    readonly moveSession: (
      actorId: string,
      environmentId: string,
      input: unknown,
    ) => Effect.Effect<{ readonly revision: number }, SemanggiRequestError>;
  }
>()("t3/semanggi/Service/SemanggiService") {}

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const audited = <A, E>(
    context: AuditContext,
    event: string,
    entityType: string,
    entityId: string,
    operation: Effect.Effect<A, E, SqlClient.SqlClient>,
    revision: (result: A) => number | null,
  ) =>
    sql
      .withTransaction(
        Effect.gen(function* () {
          if (
            !context.actorId.trim() ||
            !context.environmentId.trim() ||
            !context.requestId.trim()
          ) {
            return yield* new Repository.SemanggiValidationError({
              message: "Audit context must identify actor, environment and request.",
            });
          }
          const result = yield* operation;
          const version = revision(result);
          if (version !== null) {
            yield* sql`INSERT INTO sh_audit(actor_id, environment_id, request_id, event_type, entity_type, entity_id, revision)
          VALUES (${context.actorId}, ${context.environmentId}, ${context.requestId}, ${event}, ${entityType}, ${entityId}, ${version})`;
          }
          return result;
        }),
      )
      .pipe(Effect.provideService(SqlClient.SqlClient, sql));
  return SemanggiService.of({
    listProjects: Effect.fn("SemanggiService.listProjects")(() =>
      Repository.listProjects().pipe(Effect.provideService(SqlClient.SqlClient, sql)),
    ),
    listSections: Effect.fn("SemanggiService.listSections")((input) =>
      Repository.listSections(input).pipe(Effect.provideService(SqlClient.SqlClient, sql)),
    ),
    listSessions: Effect.fn("SemanggiService.listSessions")((input) =>
      Repository.listSessions(input).pipe(Effect.provideService(SqlClient.SqlClient, sql)),
    ),
    listCatalogSessions: Effect.fn("SemanggiService.listCatalogSessions")((environmentId) =>
      Repository.listCatalogSessions(environmentId).pipe(
        Effect.provideService(SqlClient.SqlClient, sql),
      ),
    ),
    findSessionByThread: Effect.fn("SemanggiService.findSessionByThread")(
      (environmentId, threadId) =>
        Repository.findSessionByThread(environmentId, threadId).pipe(
          Effect.provideService(SqlClient.SqlClient, sql),
        ),
    ),
    createProject: Effect.fn("SemanggiService.createProject")((context, input) =>
      audited(
        context,
        "project.create",
        "project",
        input.id,
        Repository.createProject(input),
        () => 1,
      ),
    ),
    renameProject: Effect.fn("SemanggiService.renameProject")((context, input) =>
      audited(
        context,
        "project.rename",
        "project",
        input.id,
        Repository.renameProject(input),
        (revision) => revision,
      ),
    ),
    setProjectArchived: Effect.fn("SemanggiService.setProjectArchived")((context, input) =>
      audited(
        context,
        input.archived ? "project.archive" : "project.restore",
        "project",
        input.id,
        Repository.setProjectArchived(input),
        (revision) => revision,
      ),
    ),
    createSection: Effect.fn("SemanggiService.createSection")((context, input) =>
      audited(
        context,
        "section.create",
        "section",
        input.id,
        Repository.createSection(input),
        () => 1,
      ),
    ),
    renameSection: Effect.fn("SemanggiService.renameSection")((context, input) =>
      audited(
        context,
        "section.rename",
        "section",
        input.id,
        Repository.renameSection(input),
        (revision) => revision,
      ),
    ),
    moveSectionSubtree: Effect.fn("SemanggiService.moveSectionSubtree")((context, input) =>
      audited(
        context,
        "section.move_subtree",
        "section",
        input.id,
        Repository.moveSectionSubtree(input),
        (result) => result.revision,
      ),
    ),
    reorderSection: Effect.fn("SemanggiService.reorderSection")((context, input) =>
      audited(
        context,
        "section.reorder",
        "section",
        input.id,
        Repository.reorderSection(input),
        (result) => result.revision,
      ),
    ),
    deleteLeafSection: Effect.fn("SemanggiService.deleteLeafSection")((context, input) =>
      audited(
        context,
        "section.delete",
        "section",
        input.id,
        Effect.gen(function* () {
          const sessions = yield* Repository.deleteLeafSection(input);
          for (const session of sessions) {
            yield* sql`INSERT INTO sh_audit(actor_id, environment_id, request_id, event_type, entity_type, entity_id, revision)
            VALUES (${context.actorId}, ${context.environmentId}, ${context.requestId}, 'session.move_to_root', 'session', ${session.id}, ${session.revision})`;
          }
          return sessions;
        }),
        () => input.expectedRevision,
      ),
    ),
    listWorkspaceLinks: Effect.fn("SemanggiService.listWorkspaceLinks")((environmentId) =>
      Repository.listWorkspaceLinks(environmentId).pipe(
        Effect.provideService(SqlClient.SqlClient, sql),
      ),
    ),
    linkWorkspace: Effect.fn("SemanggiService.linkWorkspace")((context, input) =>
      context.environmentId !== input.environmentId
        ? Effect.fail(
            new Repository.SemanggiValidationError({
              message: "Workspace environment does not match audit context.",
            }),
          )
        : audited(
            context,
            "workspace.link",
            "project",
            input.projectId,
            Repository.linkWorkspace(input),
            () => 1,
          ),
    ),
    unlinkWorkspace: Effect.fn("SemanggiService.unlinkWorkspace")((context, input) =>
      context.environmentId !== input.environmentId
        ? Effect.fail(
            new Repository.SemanggiValidationError({
              message: "Workspace environment does not match audit context.",
            }),
          )
        : audited(
            context,
            "workspace.unlink",
            "project",
            input.projectId,
            Repository.unlinkWorkspace(input),
            () => 1,
          ),
    ),
    createSession: Effect.fn("SemanggiService.createSession")((context, input) =>
      audited(
        context,
        "session.create",
        "session",
        input.id,
        Repository.createSession(input),
        () => 1,
      ),
    ),
    bindThread: Effect.fn("SemanggiService.bindThread")((context, input) =>
      context.environmentId !== input.environmentId
        ? Effect.fail(
            new Repository.SemanggiValidationError({
              message: "Binding environment does not match audit context.",
            }),
          )
        : audited(
            context,
            "binding.create",
            "binding",
            input.sessionId,
            Repository.bindThread(input),
            (created) => (created ? 1 : null),
          ),
    ),
    executeSessionMove: Effect.fn("SemanggiService.executeSessionMove")((actor, input) =>
      Repository.executeSessionMove(actor, input).pipe(
        Effect.provideService(SqlClient.SqlClient, sql),
      ),
    ),
    moveSession: Effect.fn("SemanggiService.moveSession")(function* (actorId, environmentId, raw) {
      const input = yield* decodeMove(raw).pipe(
        Effect.mapError(() => new SemanggiRequestError({ reason: "invalid_request" })),
      );
      if (
        !actorId.trim() ||
        input.scope.environmentId !== environmentId ||
        input.scope.projectId !== input.operation.projectId
      ) {
        return yield* new SemanggiRequestError({ reason: "scope_mismatch" });
      }
      const revision = yield* Repository.executeSessionMove(actorId, input).pipe(
        Effect.provideService(SqlClient.SqlClient, sql),
        Effect.mapError(
          (error) =>
            new SemanggiRequestError({
              reason:
                error instanceof Repository.SemanggiRevisionConflict
                  ? "conflict"
                  : isValidationError(error)
                    ? "invalid_request"
                    : "storage_error",
            }),
        ),
      );
      return { revision };
    }),
  });
});

export const layer = Layer.effect(SemanggiService, make);

export const layerConfig = Layer.unwrap(
  Effect.gen(function* () {
    const { stateDir } = yield* ServerConfig.ServerConfig;
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    yield* fs.makeDirectory(stateDir, { recursive: true });
    return layer.pipe(Layer.provide(Storage.layerFromPath(path.join(stateDir, "semanggi.sqlite"))));
  }),
);
