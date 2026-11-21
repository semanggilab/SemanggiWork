// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeSqlite from "node:sqlite";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";
import { layerFromPath, UNASSIGNED_PROJECT_ID } from "./Storage.ts";
import * as Repository from "./Repository.ts";
import { sessionIdForThread } from "./DiscoveryIdentity.ts";

it.effect(
  "upgrades an interrupted reconciliation generation without losing its staged identities",
  () => {
    const dir = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "semanggi-reconcile-upgrade-"));
    const path = NodePath.join(dir, "semanggi.sqlite");
    return Effect.gen(function* () {
      yield* Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        yield* sql`INSERT INTO sh_reconciliation_items(environment_id,thread_id,mode,session_id) VALUES ('env','thread','work',${sessionIdForThread("env", "thread")})`;
      }).pipe(Effect.provide(layerFromPath(path)));
      yield* Effect.sync(() => {
        const db = new NodeSqlite.DatabaseSync(path);
        db.exec(
          "ALTER TABLE sh_reconciliation_items DROP COLUMN will_create; ALTER TABLE sh_reconciliation_items DROP COLUMN session_id; ALTER TABLE sh_ingestion_checkpoints RENAME TO sh_ingestion_checkpoints_v15_seed; CREATE TABLE sh_ingestion_checkpoints (environment_id TEXT PRIMARY KEY, cursor INTEGER NOT NULL CHECK (cursor >= 0), high_water INTEGER NOT NULL CHECK (high_water >= 0), state TEXT NOT NULL CHECK (state IN ('CATCHING_UP', 'RECONCILING'))); INSERT INTO sh_ingestion_checkpoints SELECT environment_id,cursor,high_water,state FROM sh_ingestion_checkpoints_v15_seed; DROP TABLE sh_ingestion_checkpoints_v15_seed; DELETE FROM sh_migrations WHERE version IN (14,15,16,17,18)",
        );
        db.close();
      });
      yield* Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        const rows = yield* sql<{
          session_id: string;
          will_create: number;
        }>`SELECT session_id,will_create FROM sh_reconciliation_items WHERE environment_id='env' AND thread_id='thread'`;
        assert.deepEqual(rows, [
          { session_id: sessionIdForThread("env", "thread"), will_create: 0 },
        ]);
      }).pipe(Effect.provide(layerFromPath(path)));
    }).pipe(
      Effect.ensuring(Effect.sync(() => NodeFS.rmSync(dir, { recursive: true, force: true }))),
    );
  },
);

it.effect("initializes the separate domain store and enforces binding uniqueness", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const projects = yield* sql<{ id: string }>`SELECT id FROM sh_projects`;
    assert.deepEqual(
      projects.map((project) => project.id),
      [UNASSIGNED_PROJECT_ID],
    );
    yield* sql`INSERT INTO sh_sessions (id, project_id, mode) VALUES ('s1', ${UNASSIGNED_PROJECT_ID}, 'work')`;
    yield* sql`INSERT INTO sh_sessions (id, project_id, mode) VALUES ('s2', ${UNASSIGNED_PROJECT_ID}, 'code')`;
    yield* sql`INSERT INTO sh_thread_bindings (session_id, environment_id, t3_thread_id, binding_state) VALUES ('s1', 'env', 'thread', 'bound')`;
    const duplicate = yield* Effect.flip(
      sql`INSERT INTO sh_thread_bindings (session_id, environment_id, t3_thread_id, binding_state) VALUES ('s2', 'env', 'thread', 'bound')`,
    );
    assert.isDefined(duplicate);
    yield* sql`INSERT INTO sh_projects(id, name, normalized_name) VALUES ('other', 'Other', 'other')`;
    yield* sql`INSERT INTO sh_sections(id, project_id, name, normalized_name, depth) VALUES ('root', 'other', 'Root', 'root', 1)`;
    const invalidSection = yield* Effect.flip(
      sql`UPDATE sh_sessions SET section_id = 'root' WHERE id = 's1'`,
    );
    assert.isDefined(invalidSection);
    const invalidParent = yield* Effect.flip(
      sql`INSERT INTO sh_sections(id, project_id, parent_id, name, normalized_name, depth) VALUES ('child', ${UNASSIGNED_PROJECT_ID}, 'root', 'Child', 'child', 2)`,
    );
    assert.isDefined(invalidParent);
  }).pipe(Effect.provide(layerFromPath(":memory:"))),
);

it.effect("moves sessions with revision checking and rejects invalid section destinations", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* Repository.createProject({ id: "p1", name: " First " });
    yield* Repository.createProject({ id: "p2", name: "Second" });
    yield* Repository.createSection({
      id: "section",
      projectId: "p2",
      parentId: null,
      name: "Research",
    });
    yield* Repository.createSession({
      id: "session",
      projectId: "p1",
      sectionId: null,
      mode: "work",
    });
    const revision = yield* Repository.moveSession({
      sessionId: "session",
      projectId: "p2",
      sectionId: "section",
      expectedRevision: 1,
    });
    assert.equal(revision, 2);
    const stale = yield* Effect.flip(
      Repository.moveSession({
        sessionId: "session",
        projectId: "p1",
        sectionId: null,
        expectedRevision: 1,
      }),
    );
    assert.instanceOf(stale, Repository.SemanggiRevisionConflict);
    const invalid = yield* Effect.flip(
      Repository.moveSession({
        sessionId: "session",
        projectId: "p1",
        sectionId: "section",
        expectedRevision: 2,
      }),
    );
    assert.isDefined(invalid);
    const rows = yield* sql<{
      project_id: string;
      revision: number;
    }>`SELECT project_id, revision FROM sh_sessions WHERE id = 'session'`;
    assert.equal(rows[0]?.project_id, "p2");
    assert.equal(rows[0]?.revision, 2);
  }).pipe(Effect.provide(layerFromPath(":memory:"))),
);

it.effect("rolls back failed transactions and protects Unassigned", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const failed = yield* Effect.flip(
      sql.withTransaction(
        Effect.gen(function* () {
          yield* Repository.createProject({ id: "rollback", name: "Rollback" });
          return yield* new Repository.SemanggiValidationError({ message: "abort transaction" });
        }),
      ),
    );
    assert.equal(failed.message, "abort transaction");
    const projects = yield* sql<{ id: string }>`SELECT id FROM sh_projects WHERE id = 'rollback'`;
    assert.equal(projects.length, 0);
    yield* Effect.flip(sql`DELETE FROM sh_projects WHERE id = ${UNASSIGNED_PROJECT_ID}`);
    yield* Effect.flip(
      sql`UPDATE sh_projects SET id = 'replacement' WHERE id = ${UNASSIGNED_PROJECT_ID}`,
    );
    yield* Effect.flip(
      sql`UPDATE sh_projects SET archived_at = 'now' WHERE id = ${UNASSIGNED_PROJECT_ID}`,
    );
    const integrity = yield* sql<{ integrity_check: string }>`PRAGMA integrity_check`;
    assert.equal(integrity[0]?.integrity_check, "ok");
    const foreignKeys = yield* sql`PRAGMA foreign_key_check`;
    assert.equal(foreignKeys.length, 0);
  }).pipe(Effect.provide(layerFromPath(":memory:"))),
);

it.effect("enforces section depth, sibling uniqueness and binding identity", () =>
  Effect.gen(function* () {
    yield* Repository.createProject({ id: "p", name: "Project" });
    let parentId: string | null = null;
    for (let depth = 1; depth <= 4; depth++) {
      const id = `s${depth}`;
      yield* Repository.createSection({ id, projectId: "p", parentId, name: `Level ${depth}` });
      parentId = id;
    }
    yield* Effect.flip(
      Repository.createSection({ id: "s5", projectId: "p", parentId, name: "Too deep" }),
    );
    yield* Effect.flip(
      Repository.createSection({
        id: "duplicate",
        projectId: "p",
        parentId: null,
        name: "level 1",
      }),
    );
    yield* Repository.createSession({ id: "one", projectId: "p", sectionId: "s4", mode: "work" });
    yield* Repository.createSession({ id: "two", projectId: "p", sectionId: null, mode: "code" });
    yield* Repository.bindThread({ sessionId: "one", environmentId: "env", threadId: "thread" });
    assert.equal((yield* Repository.findSessionByThread("env", "thread"))?.session_id, "one");
    assert.isNull(yield* Repository.findSessionByThread("other", "thread"));
    yield* Repository.bindThread({ sessionId: "one", environmentId: "env", threadId: "thread" });
    yield* Effect.flip(
      Repository.bindThread({ sessionId: "one", environmentId: "env", threadId: "other" }),
    );
    yield* Effect.flip(
      Repository.bindThread({ sessionId: "two", environmentId: "env", threadId: "thread" }),
    );
  }).pipe(Effect.provide(layerFromPath(":memory:"))),
);

it.effect(
  "rejects cycles, cross-project references, invalid revisions and non-sibling name collisions",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* Repository.createProject({ id: "p", name: "Project" });
      yield* Repository.createProject({ id: "other", name: "Other" });
      yield* Repository.createSection({ id: "a", projectId: "p", parentId: null, name: "A" });
      yield* Repository.createSection({ id: "b", projectId: "p", parentId: "a", name: "Straße" });
      yield* Effect.flip(
        Repository.createSection({
          id: "duplicate",
          projectId: "p",
          parentId: "a",
          name: "STRASSE",
        }),
      );
      yield* Repository.createSection({
        id: "different-parent",
        projectId: "p",
        parentId: null,
        name: "STRASSE",
      });
      yield* Effect.flip(sql`UPDATE sh_sections SET parent_id = 'b', depth = 3 WHERE id = 'a'`);
      yield* Effect.flip(sql`UPDATE sh_sections SET parent_id = id, depth = 2 WHERE id = 'a'`);
      yield* Effect.flip(sql`UPDATE sh_sections SET project_id = 'other' WHERE id = 'a'`);
      yield* Effect.flip(
        sql`INSERT INTO sh_sessions(id,project_id,mode,revision) VALUES ('invalid', 'missing', 'work', 1)`,
      );
      yield* Effect.flip(
        sql`INSERT INTO sh_sessions(id,project_id,mode,revision) VALUES ('invalid', 'p', 'work', 0)`,
      );
      yield* Effect.flip(
        sql`INSERT INTO sh_sessions(id,project_id,section_id,mode) VALUES ('invalid', 'other', 'a', 'work')`,
      );
      const integrity = yield* sql<{ integrity_check: string }>`PRAGMA integrity_check`;
      assert.equal(integrity[0]?.integrity_check, "ok");
      assert.equal((yield* sql`PRAGMA foreign_key_check`).length, 0);
    }).pipe(Effect.provide(layerFromPath(":memory:"))),
);

it.effect("uses Unicode case folding and normalization for project names", () =>
  Effect.gen(function* () {
    yield* Repository.createProject({ id: "one", name: "Straße" });
    yield* Effect.flip(Repository.createProject({ id: "two", name: "STRASSE" }));
    yield* Repository.createProject({ id: "three", name: "Café" });
    yield* Effect.flip(Repository.createProject({ id: "four", name: "Cafe\u0301" }));
    yield* Repository.createProject({ id: "five", name: "ΟΣ" });
    yield* Effect.flip(Repository.createProject({ id: "six", name: "ος" }));
  }).pipe(Effect.provide(layerFromPath(":memory:"))),
);

it.effect("moves a section subtree atomically and rejects stale, cyclic, and too-deep moves", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* Repository.createProject({ id: "p", name: "Project" });
    yield* Repository.createSection({ id: "a", projectId: "p", parentId: null, name: "A" });
    yield* Repository.createSection({ id: "b", projectId: "p", parentId: "a", name: "B" });
    yield* Repository.createSection({ id: "c", projectId: "p", parentId: "b", name: "C" });
    yield* Repository.createSection({
      id: "target",
      projectId: "p",
      parentId: null,
      name: "Target",
    });
    assert.deepEqual(
      yield* Repository.moveSectionSubtree({ id: "a", parentId: "target", expectedRevision: 1 }),
      { revision: 2 },
    );
    const tree = yield* sql<{
      id: string;
      parent_id: string | null;
      depth: number;
    }>`SELECT id,parent_id,depth FROM sh_sections WHERE id IN ('a','b','c') ORDER BY id`;
    assert.deepEqual(tree, [
      { id: "a", parent_id: "target", depth: 2 },
      { id: "b", parent_id: "a", depth: 3 },
      { id: "c", parent_id: "b", depth: 4 },
    ]);
    yield* Effect.flip(
      Repository.moveSectionSubtree({ id: "a", parentId: null, expectedRevision: 1 }),
    );
    yield* Effect.flip(
      Repository.moveSectionSubtree({ id: "a", parentId: "c", expectedRevision: 2 }),
    );
    yield* Effect.flip(
      Repository.moveSectionSubtree({ id: "target", parentId: "c", expectedRevision: 1 }),
    );
  }).pipe(Effect.provide(layerFromPath(":memory:"))),
);

it.effect("updates projects with revisions and returns sessions to root when deleting a leaf", () =>
  Effect.gen(function* () {
    yield* Repository.createProject({ id: "p", name: "Before" });
    assert.equal(
      yield* Repository.renameProject({ id: "p", name: "After", expectedRevision: 1 }),
      2,
    );
    yield* Effect.flip(Repository.renameProject({ id: "p", name: "Stale", expectedRevision: 1 }));
    assert.equal(
      yield* Repository.setProjectArchived({ id: "p", archived: true, expectedRevision: 2 }),
      3,
    );
    assert.isFalse((yield* Repository.listProjects()).some(({ id }) => id === "p"));
    assert.equal(
      yield* Repository.setProjectArchived({ id: "p", archived: false, expectedRevision: 3 }),
      4,
    );
    yield* Repository.createSection({ id: "root", projectId: "p", parentId: null, name: "Root" });
    yield* Repository.createSection({
      id: "child",
      projectId: "p",
      parentId: "root",
      name: "Child",
    });
    yield* Repository.createSession({ id: "s", projectId: "p", sectionId: "child", mode: "work" });
    yield* Effect.flip(Repository.deleteLeafSection({ id: "root", expectedRevision: 1 }));
    assert.equal(
      yield* Repository.renameSection({ id: "child", name: "Renamed", expectedRevision: 1 }),
      2,
    );
    yield* Repository.deleteLeafSection({ id: "child", expectedRevision: 2 });
    const session = (yield* Repository.listSessions("p"))[0];
    assert.isNull(session?.section_id);
    assert.equal(session?.revision, 2);
  }).pipe(Effect.provide(layerFromPath(":memory:"))),
);

it.effect("records a session move atomically and prevents idempotency key reuse", () =>
  Effect.gen(function* () {
    yield* Repository.createProject({ id: "destination", name: "Destination" });
    yield* Repository.createSession({
      id: "session",
      projectId: UNASSIGNED_PROJECT_ID,
      sectionId: null,
      mode: "work",
    });
    const input = {
      requestId: "request",
      idempotencyKey: "key",
      scope: { environmentId: "env", projectId: "destination" },
      expectedRevision: 1,
      operation: {
        type: "session.move" as const,
        sessionId: "session",
        projectId: "destination",
        sectionId: null,
      },
    };
    assert.equal(yield* Repository.executeSessionMove("actor", input), 2);
    assert.equal(
      yield* Repository.executeSessionMove("actor", { ...input, requestId: "retry" }),
      2,
    );
    yield* Effect.flip(Repository.executeSessionMove("different-actor", input));
    yield* Effect.flip(Repository.executeSessionMove("actor", { ...input, expectedRevision: 2 }));
    yield* Effect.flip(
      Repository.executeSessionMove("actor", {
        ...input,
        scope: { ...input.scope, projectId: "other" },
      }),
    );
    const sessions = yield* Repository.listSessions("destination");
    assert.equal(sessions[0]?.revision, 2);
    const sql = yield* SqlClient.SqlClient;
    const audit = yield* sql<{
      actor_id: string;
      revision: number;
    }>`SELECT actor_id, revision FROM sh_audit`;
    assert.deepEqual(audit, [{ actor_id: "actor", revision: 2 }]);
  }).pipe(Effect.provide(layerFromPath(":memory:"))),
);

it.effect("rolls back the session change and receipt when audit insertion fails", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* Repository.createProject({ id: "destination", name: "Destination" });
    yield* Repository.createSession({
      id: "session",
      projectId: UNASSIGNED_PROJECT_ID,
      sectionId: null,
      mode: "work",
    });
    yield* sql`CREATE TRIGGER fail_audit BEFORE INSERT ON sh_audit BEGIN SELECT RAISE(ABORT, 'injected audit failure'); END`;
    yield* Effect.flip(
      Repository.executeSessionMove("actor", {
        requestId: "r",
        idempotencyKey: "k",
        scope: { environmentId: "env", projectId: "destination" },
        expectedRevision: 1,
        operation: {
          type: "session.move",
          sessionId: "session",
          projectId: "destination",
          sectionId: null,
        },
      }),
    );
    const sessions = yield* Repository.listSessions(UNASSIGNED_PROJECT_ID);
    assert.equal(sessions[0]?.revision, 1);
    const receipts = yield* sql`SELECT * FROM sh_commands`;
    assert.equal(receipts.length, 0);
  }).pipe(Effect.provide(layerFromPath(":memory:"))),
);

it.effect("reopens a migrated on-disk profile and rejects a future migration", () => {
  const dir = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "semanggi-storage-"));
  const path = NodePath.join(dir, "semanggi.sqlite");
  return Effect.gen(function* () {
    yield* Repository.createProject({ id: "persisted", name: "Persisted" }).pipe(
      Effect.provide(layerFromPath(path)),
    );
    const projects = yield* Repository.listProjects().pipe(Effect.provide(layerFromPath(path)));
    assert.isTrue(projects.some(({ id }) => id === "persisted"));
    // Recreate the preceding schema version while retaining user data.
    yield* Effect.sync(() => {
      const db = new NodeSqlite.DatabaseSync(path);
      db.exec(
        "DROP TRIGGER sh_pending_session_no_move; DROP TABLE sh_reconciliation_items; DROP TABLE sh_reconciliation; DROP TABLE sh_discovery_pending; DROP TABLE sh_message_projection; DROP TABLE sh_native_projection; DROP TABLE sh_thread_refresh_queue; DROP TABLE sh_inbox; DROP TABLE sh_ingestion_checkpoints; DROP TABLE sh_thread_create_operations; DROP TABLE sh_commands; DROP TABLE sh_audit; DELETE FROM sh_migrations WHERE version > 1",
      );
      db.close();
    });
    yield* Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const versions = yield* sql<{
        version: number;
      }>`SELECT version FROM sh_migrations ORDER BY version`;
      assert.deepEqual(
        versions.map(({ version }) => version),
        [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17],
      );
      assert.isTrue((yield* Repository.listProjects()).some(({ id }) => id === "persisted"));
      const commands = yield* sql`SELECT * FROM sh_commands`;
      assert.equal(commands.length, 0);
    }).pipe(Effect.provide(layerFromPath(path)));
    yield* Effect.sync(() => {
      const db = new NodeSqlite.DatabaseSync(path);
      db.exec("INSERT INTO sh_migrations(version) VALUES (999)");
      db.close();
    });
    const result = yield* Effect.flip(
      Repository.listProjects().pipe(Effect.provide(layerFromPath(path))),
    );
    assert.match(String(result), /Unsupported Semanggi database migration version/);
  }).pipe(Effect.ensuring(Effect.sync(() => NodeFS.rmSync(dir, { recursive: true, force: true }))));
});

it.effect(
  "rolls back an upgrade when Unicode case folding exposes existing duplicate names",
  () => {
    const dir = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "semanggi-conflict-"));
    const path = NodePath.join(dir, "semanggi.sqlite");
    return Effect.gen(function* () {
      yield* Repository.listProjects().pipe(Effect.provide(layerFromPath(path)));
      yield* Effect.sync(() => {
        const db = new NodeSqlite.DatabaseSync(path);
        db.exec(
          "DROP TRIGGER sh_pending_session_no_move; DROP TABLE sh_reconciliation_items; DROP TABLE sh_reconciliation; DROP TABLE sh_discovery_pending; DROP TABLE sh_message_projection; DROP TABLE sh_native_projection; DROP TABLE sh_thread_refresh_queue; DROP TABLE sh_inbox; DROP TABLE sh_ingestion_checkpoints; DROP TABLE sh_thread_create_operations; DROP TABLE sh_audit; DELETE FROM sh_migrations WHERE version > 2; INSERT INTO sh_projects(id,name,normalized_name) VALUES ('a','Straße','straße'),('b','STRASSE','strasse')",
        );
        db.close();
      });
      yield* Effect.flip(Repository.listProjects().pipe(Effect.provide(layerFromPath(path))));
      yield* Effect.sync(() => {
        const db = new NodeSqlite.DatabaseSync(path);
        const versions = db.prepare("SELECT version FROM sh_migrations ORDER BY version").all();
        const projects = db
          .prepare("SELECT count(*) AS count FROM sh_projects WHERE id IN ('a','b')")
          .get();
        assert.deepEqual(
          versions.map((row) => row.version),
          [1, 2],
        );
        assert.equal(projects?.count, 2);
        db.close();
      });
    }).pipe(
      Effect.ensuring(Effect.sync(() => NodeFS.rmSync(dir, { recursive: true, force: true }))),
    );
  },
);

it.effect("upgrades the v14 checkpoint constraint and permits LIVE only after migration", () => {
  const dir = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "semanggi-live-upgrade-"));
  const path = NodePath.join(dir, "semanggi.sqlite");
  return Effect.gen(function* () {
    yield* Repository.listProjects().pipe(Effect.provide(layerFromPath(path)));
    yield* Effect.sync(() => {
      const db = new NodeSqlite.DatabaseSync(path);
      db.exec(
        "ALTER TABLE sh_ingestion_checkpoints RENAME TO sh_ingestion_checkpoints_v15_seed; CREATE TABLE sh_ingestion_checkpoints (environment_id TEXT PRIMARY KEY, cursor INTEGER NOT NULL CHECK (cursor >= 0), high_water INTEGER NOT NULL CHECK (high_water >= 0), state TEXT NOT NULL CHECK (state IN ('CATCHING_UP', 'RECONCILING'))); INSERT INTO sh_ingestion_checkpoints VALUES ('env',4,4,'CATCHING_UP'); DROP TABLE sh_ingestion_checkpoints_v15_seed; DELETE FROM sh_migrations WHERE version IN (15,16,17,18)",
      );
      db.close();
    });
    yield* Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* sql`UPDATE sh_ingestion_checkpoints SET state='LIVE' WHERE environment_id='env'`;
      assert.deepEqual(yield* sql`SELECT cursor,state FROM sh_ingestion_checkpoints`, [
        { cursor: 4, state: "LIVE" },
      ]);
    }).pipe(Effect.provide(layerFromPath(path)));
  }).pipe(Effect.ensuring(Effect.sync(() => NodeFS.rmSync(dir, { recursive: true, force: true }))));
});
