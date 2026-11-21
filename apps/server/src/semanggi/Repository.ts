// @effect-diagnostics nodeBuiltinImport:off
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import type { SemanggiSessionMove } from "@t3tools/contracts";
import * as NodeCrypto from "node:crypto";
import { normalizeName } from "./casefold.ts";

export class SemanggiValidationError extends Schema.TaggedError<SemanggiValidationError>()(
  "SemanggiValidationError",
  { message: Schema.String },
) {}

export class SemanggiRevisionConflict extends Error {
  readonly _tag = "SemanggiRevisionConflict";
  constructor() {
    super("The revision changed or the record no longer exists.");
  }
}

export const listProjects = Effect.fn("semanggi.listProjects")(function* () {
  const sql = yield* SqlClient.SqlClient;
  return yield* sql<{ id: string; name: string; revision: number }>`
    SELECT id, name, revision FROM sh_projects WHERE archived_at IS NULL ORDER BY normalized_name, id`;
});

export const listSections = Effect.fn("semanggi.listSections")(function* (projectId: string) {
  const sql = yield* SqlClient.SqlClient;
  return yield* sql<{
    id: string;
    parent_id: string | null;
    name: string;
    depth: number;
    revision: number;
  }>`
    SELECT id, parent_id, name, depth, revision FROM sh_sections
    WHERE project_id = ${projectId} ORDER BY depth, position, normalized_name, id`;
});

export const listSessions = Effect.fn("semanggi.listSessions")(function* (projectId: string) {
  const sql = yield* SqlClient.SqlClient;
  return yield* sql<{ id: string; section_id: string | null; mode: string; revision: number }>`
    SELECT s.id, s.section_id, s.mode, s.revision FROM sh_sessions s WHERE s.project_id = ${projectId}
      AND NOT EXISTS (SELECT 1 FROM sh_thread_create_operations op WHERE op.session_id = s.id AND op.state != 'BOUND')
    ORDER BY s.id`;
});

export const listCatalogSessions = Effect.fn("semanggi.listCatalogSessions")(function* (
  environmentId: string,
) {
  const sql = yield* SqlClient.SqlClient;
  return yield* sql<{
    id: string;
    project_id: string;
    section_id: string | null;
    mode: "work" | "code";
    revision: number;
    thread_id: string;
  }>`SELECT s.id, s.project_id, s.section_id, s.mode, s.revision, b.t3_thread_id AS thread_id
    FROM sh_sessions s JOIN sh_thread_bindings b ON b.session_id = s.id
    WHERE b.environment_id = ${environmentId} AND b.binding_state = 'bound'
    ORDER BY s.id`;
});

export const findSessionByThread = Effect.fn("semanggi.findSessionByThread")(function* (
  environmentId: string,
  threadId: string,
) {
  const sql = yield* SqlClient.SqlClient;
  const rows = yield* sql<{ session_id: string; binding_state: string; revision: number }>`
    SELECT session_id, binding_state, revision FROM sh_thread_bindings
    WHERE environment_id = ${environmentId} AND t3_thread_id = ${threadId}`;
  return rows[0] ?? null;
});

export const bindThread = Effect.fn("semanggi.bindThread")(function* (input: {
  readonly sessionId: string;
  readonly environmentId: string;
  readonly threadId: string;
}) {
  const sql = yield* SqlClient.SqlClient;
  const inserted = yield* sql<{
    session_id: string;
  }>`INSERT INTO sh_thread_bindings(session_id, environment_id, t3_thread_id, binding_state)
    VALUES (${input.sessionId}, ${input.environmentId}, ${input.threadId}, 'bound')
    ON CONFLICT(session_id) DO NOTHING RETURNING session_id`;
  const bindings = yield* sql<{ environment_id: string; t3_thread_id: string }>`
    SELECT environment_id, t3_thread_id FROM sh_thread_bindings WHERE session_id = ${input.sessionId}`;
  if (
    bindings[0]?.environment_id !== input.environmentId ||
    bindings[0]?.t3_thread_id !== input.threadId
  ) {
    return yield* new SemanggiValidationError({
      message: "Session is already bound to another thread.",
    });
  }
  return inserted.length > 0;
});

export const createProject = Effect.fn("semanggi.createProject")(function* (input: {
  readonly id: string;
  readonly name: string;
}) {
  const sql = yield* SqlClient.SqlClient;
  const name = input.name.normalize("NFC").trim();
  if (!name)
    return yield* new SemanggiValidationError({ message: "Project name cannot be empty." });
  yield* sql`INSERT INTO sh_projects(id, name, normalized_name) VALUES (${input.id}, ${name}, ${normalizeName(name)})`;
});

export const renameProject = Effect.fn("semanggi.renameProject")(function* (input: {
  readonly id: string;
  readonly name: string;
  readonly expectedRevision: number;
}) {
  const sql = yield* SqlClient.SqlClient;
  const name = input.name.normalize("NFC").trim();
  if (!name)
    return yield* new SemanggiValidationError({ message: "Project name cannot be empty." });
  const rows = yield* sql<{
    revision: number;
  }>`UPDATE sh_projects SET name = ${name}, normalized_name = ${normalizeName(name)}, revision = revision + 1
    WHERE id = ${input.id} AND revision = ${input.expectedRevision} RETURNING revision`;
  if (!rows[0]) return yield* Effect.fail(new SemanggiRevisionConflict());
  return rows[0].revision;
});

export const setProjectArchived = Effect.fn("semanggi.setProjectArchived")(function* (input: {
  readonly id: string;
  readonly archived: boolean;
  readonly expectedRevision: number;
}) {
  const sql = yield* SqlClient.SqlClient;
  const rows = yield* sql<{ revision: number }>`UPDATE sh_projects
    SET archived_at = CASE WHEN ${input.archived ? 1 : 0} = 1 THEN strftime('%Y-%m-%dT%H:%M:%fZ', 'now') ELSE NULL END,
    revision = revision + 1 WHERE id = ${input.id} AND revision = ${input.expectedRevision} RETURNING revision`;
  if (!rows[0]) return yield* Effect.fail(new SemanggiRevisionConflict());
  return rows[0].revision;
});

export const createSection = Effect.fn("semanggi.createSection")(function* (input: {
  readonly id: string;
  readonly projectId: string;
  readonly parentId: string | null;
  readonly name: string;
}) {
  const sql = yield* SqlClient.SqlClient;
  const name = input.name.normalize("NFC").trim();
  if (!name)
    return yield* new SemanggiValidationError({ message: "Section name cannot be empty." });
  return yield* sql.withTransaction(
    Effect.gen(function* () {
      const parents =
        input.parentId === null
          ? []
          : yield* sql<{
              depth: number;
            }>`SELECT depth FROM sh_sections WHERE id = ${input.parentId} AND project_id = ${input.projectId}`;
      if (input.parentId !== null && parents.length === 0) {
        return yield* new SemanggiValidationError({
          message: "Section parent does not belong to this project.",
        });
      }
      const depth = input.parentId === null ? 1 : parents[0]!.depth + 1;
      if (depth > 4)
        return yield* new SemanggiValidationError({ message: "Section depth cannot exceed four." });
      yield* sql`INSERT INTO sh_sections(id, project_id, parent_id, name, normalized_name, depth, position)
        VALUES (${input.id}, ${input.projectId}, ${input.parentId}, ${name}, ${normalizeName(name)}, ${depth},
          COALESCE((SELECT MAX(position) + 1 FROM sh_sections WHERE project_id = ${input.projectId}
            AND parent_id IS ${input.parentId}), 1))`;
    }),
  );
});

export const renameSection = Effect.fn("semanggi.renameSection")(function* (input: {
  readonly id: string;
  readonly name: string;
  readonly expectedRevision: number;
}) {
  const sql = yield* SqlClient.SqlClient;
  const name = input.name.normalize("NFC").trim();
  if (!name)
    return yield* new SemanggiValidationError({ message: "Section name cannot be empty." });
  const rows = yield* sql<{
    revision: number;
  }>`UPDATE sh_sections SET name = ${name}, normalized_name = ${normalizeName(name)}, revision = revision + 1
    WHERE id = ${input.id} AND revision = ${input.expectedRevision} RETURNING revision`;
  if (!rows[0]) return yield* Effect.fail(new SemanggiRevisionConflict());
  return rows[0].revision;
});

/** Moves one section and its descendants atomically; only a current root revision can move. */
export const moveSectionSubtree = Effect.fn("semanggi.moveSectionSubtree")(function* (input: {
  readonly id: string;
  readonly parentId: string | null;
  readonly expectedRevision: number;
}) {
  const sql = yield* SqlClient.SqlClient;
  return yield* sql.withTransaction(
    Effect.gen(function* () {
      const roots = yield* sql<{
        project_id: string;
        depth: number;
      }>`SELECT project_id, depth FROM sh_sections WHERE id = ${input.id} AND revision = ${input.expectedRevision}`;
      const root = roots[0];
      if (!root) return yield* Effect.fail(new SemanggiRevisionConflict());
      const parents =
        input.parentId === null
          ? []
          : yield* sql<{
              project_id: string;
              depth: number;
            }>`SELECT project_id, depth FROM sh_sections WHERE id = ${input.parentId}`;
      const parent = parents[0];
      if (parent && parent.project_id !== root.project_id) {
        return yield* new SemanggiValidationError({
          message: "Section parent does not belong to this project.",
        });
      }
      const descendants = yield* sql<{
        id: string;
        depth: number;
      }>`WITH RECURSIVE subtree(id, depth) AS (
        SELECT id, depth FROM sh_sections WHERE id = ${input.id}
        UNION ALL SELECT child.id, child.depth FROM sh_sections child JOIN subtree ON child.parent_id = subtree.id
      ) SELECT id, depth FROM subtree`;
      if (input.parentId !== null && descendants.some(({ id }) => id === input.parentId)) {
        return yield* new SemanggiValidationError({
          message: "Section cannot be moved into its own subtree.",
        });
      }
      const nextRootDepth = parent ? parent.depth + 1 : 1;
      const depthDelta = nextRootDepth - root.depth;
      if (descendants.some(({ depth }) => depth + depthDelta > 4)) {
        return yield* new SemanggiValidationError({ message: "Section depth cannot exceed four." });
      }
      const nextPositionRows = yield* sql<{
        next_position: number;
      }>`SELECT COALESCE(MAX(position) + 1, 1) AS next_position
        FROM sh_sections WHERE project_id = ${root.project_id} AND parent_id IS ${input.parentId} AND id != ${input.id}`;
      const nextPosition = nextPositionRows[0]?.next_position ?? 1;
      for (const descendant of descendants) {
        yield* sql`UPDATE sh_sections SET
          parent_id = CASE WHEN id = ${input.id} THEN ${input.parentId} ELSE parent_id END,
          depth = depth + ${depthDelta},
          position = CASE WHEN id = ${input.id} THEN ${nextPosition} ELSE position END,
          revision = revision + 1
          WHERE id = ${descendant.id}`;
      }
      return { revision: input.expectedRevision + 1 };
    }),
  );
});

export const deleteLeafSection = Effect.fn("semanggi.deleteLeafSection")(function* (input: {
  readonly id: string;
  readonly expectedRevision: number;
}) {
  const sql = yield* SqlClient.SqlClient;
  return yield* sql.withTransaction(
    Effect.gen(function* () {
      const sections = yield* sql<{
        id: string;
      }>`SELECT id FROM sh_sections WHERE id = ${input.id} AND revision = ${input.expectedRevision}`;
      if (!sections[0]) return yield* Effect.fail(new SemanggiRevisionConflict());
      const children = yield* sql<{
        id: string;
      }>`SELECT id FROM sh_sections WHERE parent_id = ${input.id} LIMIT 1`;
      if (children.length)
        return yield* new SemanggiValidationError({ message: "Section has children." });
      const moved = yield* sql<{
        id: string;
        revision: number;
      }>`UPDATE sh_sessions SET section_id = NULL, revision = revision + 1 WHERE section_id = ${input.id} RETURNING id, revision`;
      yield* sql`DELETE FROM sh_sections WHERE id = ${input.id}`;
      return moved;
    }),
  );
});

export const createSession = Effect.fn("semanggi.createSession")(function* (input: {
  readonly id: string;
  readonly projectId: string;
  readonly sectionId: string | null;
  readonly mode: "work" | "code";
}) {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`INSERT INTO sh_sessions(id, project_id, section_id, mode)
    VALUES (${input.id}, ${input.projectId}, ${input.sectionId}, ${input.mode})`;
});

export const moveSession = Effect.fn("semanggi.moveSession")(function* (input: {
  readonly sessionId: string;
  readonly projectId: string;
  readonly sectionId: string | null;
  readonly expectedRevision: number;
}) {
  const sql = yield* SqlClient.SqlClient;
  const rows = yield* sql<{ revision: number }>`UPDATE sh_sessions
    SET project_id = ${input.projectId}, section_id = ${input.sectionId}, revision = revision + 1
    WHERE id = ${input.sessionId} AND revision = ${input.expectedRevision} RETURNING revision`;
  if (rows.length === 0) return yield* Effect.fail(new SemanggiRevisionConflict());
  return rows[0]!.revision;
});

/** The caller must obtain actorId from the authenticated server session. */
export const executeSessionMove = Effect.fn("semanggi.executeSessionMove")(function* (
  actorId: string,
  input: SemanggiSessionMove,
) {
  const sql = yield* SqlClient.SqlClient;
  if (
    !actorId.trim() ||
    !input.scope.environmentId.trim() ||
    !input.scope.projectId.trim() ||
    !input.idempotencyKey.trim() ||
    input.scope.projectId !== input.operation.projectId
  ) {
    return yield* new SemanggiValidationError({ message: "Invalid actor or mutation scope." });
  }
  // Length-prefix the canonical fields so separators inside identifiers cannot collide.
  const identity = NodeCrypto.createHash("sha256")
    .update(
      [
        input.expectedRevision,
        input.operation.type,
        input.operation.sessionId,
        input.operation.projectId,
        input.operation.sectionId,
      ]
        .map((value) => (value === null ? "-1:" : `${String(value).length}:${String(value)}`))
        .join(""),
    )
    .digest("hex");
  return yield* sql.withTransaction(
    Effect.gen(function* () {
      const binding = yield* sql<{
        environment_id: string;
      }>`SELECT environment_id FROM sh_thread_bindings WHERE session_id = ${input.operation.sessionId}`;
      if (binding[0] && binding[0].environment_id !== input.scope.environmentId) {
        return yield* new SemanggiValidationError({
          message: "Session belongs to another environment.",
        });
      }
      const existing = yield* sql<{
        actor_id: string;
        request_identity: string;
        result_revision: number;
      }>`
      SELECT actor_id, request_identity, result_revision FROM sh_commands
      WHERE environment_id = ${input.scope.environmentId} AND project_id = ${input.scope.projectId}
      AND idempotency_key = ${input.idempotencyKey}`;
      if (existing[0]) {
        if (existing[0].actor_id !== actorId || existing[0].request_identity !== identity) {
          return yield* new SemanggiValidationError({
            message: "Idempotency key was already used for a different request.",
          });
        }
        return existing[0].result_revision;
      }
      const revision = yield* moveSession({
        ...input.operation,
        expectedRevision: input.expectedRevision,
      });
      yield* sql`INSERT INTO sh_commands(environment_id, project_id, idempotency_key, actor_id, request_identity, result_revision)
      VALUES (${input.scope.environmentId}, ${input.scope.projectId}, ${input.idempotencyKey}, ${actorId}, ${identity}, ${revision})`;
      yield* sql`INSERT INTO sh_audit(actor_id, environment_id, request_id, event_type, entity_type, entity_id, revision)
      VALUES (${actorId}, ${input.scope.environmentId}, ${input.requestId}, 'session.move', 'session', ${input.operation.sessionId}, ${revision})`;
      return revision;
    }),
  );
});

export const listWorkspaceLinks = Effect.fn("semanggi.listWorkspaceLinks")(function* (
  environmentId: string,
) {
  const sql = yield* SqlClient.SqlClient;
  return yield* sql<{ project_id: string; t3_project_id: string; is_default: number }>`
    SELECT l.project_id, l.t3_project_id, l.is_default FROM sh_workspace_links l
    JOIN sh_projects p ON p.id = l.project_id AND p.archived_at IS NULL
    WHERE l.environment_id = ${environmentId} ORDER BY l.project_id, l.t3_project_id`;
});

/** Idempotent. The first link of a Project in an environment becomes its default; `isDefault` moves it. */
export const linkWorkspace = Effect.fn("semanggi.linkWorkspace")(function* (input: {
  readonly projectId: string;
  readonly environmentId: string;
  readonly t3ProjectId: string;
  readonly isDefault: boolean;
}) {
  const sql = yield* SqlClient.SqlClient;
  const project = yield* sql<{ archived_at: string | null }>`
    SELECT archived_at FROM sh_projects WHERE id = ${input.projectId}`;
  if (!project[0] || project[0].archived_at !== null) {
    return yield* new SemanggiValidationError({ message: "Project is missing or archived." });
  }
  const existing = yield* sql<{ is_default: number }>`
    SELECT is_default FROM sh_workspace_links WHERE project_id = ${input.projectId}
    AND environment_id = ${input.environmentId} AND t3_project_id = ${input.t3ProjectId}`;
  const others = yield* sql<{ n: number }>`
    SELECT COUNT(*) AS n FROM sh_workspace_links WHERE project_id = ${input.projectId}
    AND environment_id = ${input.environmentId} AND t3_project_id != ${input.t3ProjectId}`;
  const makeDefault = input.isDefault || (others[0]?.n ?? 0) === 0 || existing[0]?.is_default === 1;
  if (makeDefault) {
    yield* sql`UPDATE sh_workspace_links SET is_default = 0 WHERE project_id = ${input.projectId}
      AND environment_id = ${input.environmentId} AND t3_project_id != ${input.t3ProjectId}`;
  }
  yield* sql`INSERT INTO sh_workspace_links(project_id, environment_id, t3_project_id, is_default)
    VALUES (${input.projectId}, ${input.environmentId}, ${input.t3ProjectId}, ${makeDefault ? 1 : 0})
    ON CONFLICT(project_id, environment_id, t3_project_id) DO UPDATE SET is_default = excluded.is_default`;
});

/** Removes the link; if it was the default, the lowest remaining t3_project_id becomes default. */
export const unlinkWorkspace = Effect.fn("semanggi.unlinkWorkspace")(function* (input: {
  readonly projectId: string;
  readonly environmentId: string;
  readonly t3ProjectId: string;
}) {
  const sql = yield* SqlClient.SqlClient;
  const removed = yield* sql<{ is_default: number }>`DELETE FROM sh_workspace_links
    WHERE project_id = ${input.projectId} AND environment_id = ${input.environmentId}
    AND t3_project_id = ${input.t3ProjectId} RETURNING is_default`;
  if (removed[0]?.is_default === 1) {
    yield* sql`UPDATE sh_workspace_links SET is_default = 1 WHERE rowid = (
      SELECT rowid FROM sh_workspace_links WHERE project_id = ${input.projectId}
      AND environment_id = ${input.environmentId} ORDER BY t3_project_id LIMIT 1)`;
  }
});

/** Swaps a Section with its previous/next sibling. Siblings are renumbered so ties cannot stall a move. */
export const reorderSection = Effect.fn("semanggi.reorderSection")(function* (input: {
  readonly id: string;
  readonly direction: "up" | "down";
  readonly expectedRevision: number;
}) {
  const sql = yield* SqlClient.SqlClient;
  return yield* sql.withTransaction(
    Effect.gen(function* () {
      const rows = yield* sql<{ project_id: string; parent_id: string | null }>`
        SELECT project_id, parent_id FROM sh_sections
        WHERE id = ${input.id} AND revision = ${input.expectedRevision}`;
      const target = rows[0];
      if (!target) return yield* Effect.fail(new SemanggiRevisionConflict());
      const siblings = yield* sql<{ id: string }>`SELECT id FROM sh_sections
        WHERE project_id = ${target.project_id} AND parent_id IS ${target.parent_id}
        ORDER BY position, normalized_name, id`;
      const index = siblings.findIndex(({ id }) => id === input.id);
      const swapWith = input.direction === "up" ? index - 1 : index + 1;
      const neighbor = siblings[swapWith];
      if (!neighbor) {
        return yield* new SemanggiValidationError({ message: "Section is already at the edge." });
      }
      const order = siblings.map(({ id }) => id);
      order[index] = neighbor.id;
      order[swapWith] = input.id;
      for (const [position, id] of order.entries()) {
        const bump = id === input.id || id === neighbor.id ? 1 : 0;
        yield* sql`UPDATE sh_sections SET position = ${position + 1}, revision = revision + ${bump}
          WHERE id = ${id}`;
      }
      return { revision: input.expectedRevision + 1 };
    }),
  );
});
