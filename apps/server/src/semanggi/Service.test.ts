import { assert, it } from "@effect/vitest";
// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import * as NodeOS from "node:os";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Service from "./Service.ts";
import * as Storage from "./Storage.ts";
import * as ServerConfig from "../config.ts";
import * as RpcAuthorization from "../auth/RpcAuthorization.ts";
import { AuthOrchestrationOperateScope, WS_METHODS, WsRpcGroup } from "@t3tools/contracts";
import * as RpcTest from "effect/rpc/RpcTest";
import * as SqlClient from "effect/sql/SqlClient";

const actor = { actorId: "actor", environmentId: "env", requestId: "test-request" };

it.effect("retains actor and revision audit across reopening the database", () => {
  const dir = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "semanggi-audit-"));
  const filename = NodePath.join(dir, "semanggi.sqlite");
  const persistent = () => Service.layer.pipe(Layer.provideMerge(Storage.layerFromPath(filename)));
  return Effect.gen(function* () {
    yield* Effect.gen(function* () {
      const service = yield* Service.SemanggiService;
      yield* service.createProject(actor, { id: "p", name: "Project" });
      yield* service.renameProject(actor, { id: "p", name: "Updated", expectedRevision: 1 });
    }).pipe(Effect.provide(persistent()));
    yield* Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const service = yield* Service.SemanggiService;
      const events = yield* sql<{
        event_type: string;
        actor_id: string;
        revision: number;
      }>`SELECT event_type, actor_id, revision FROM sh_audit ORDER BY sequence`;
      assert.deepEqual(events, [
        { event_type: "project.create", actor_id: "actor", revision: 1 },
        { event_type: "project.rename", actor_id: "actor", revision: 2 },
      ]);
      assert.equal((yield* service.listProjects()).find(({ id }) => id === "p")?.revision, 2);
    }).pipe(Effect.provide(persistent()));
  }).pipe(Effect.ensuring(Effect.sync(() => NodeFS.rmSync(dir, { recursive: true, force: true }))));
});

const auditedLayer = Service.layer.pipe(Layer.provideMerge(Storage.layerFromPath(":memory:")));

it.effect("audits domain changes and avoids an extra event on a binding retry", () =>
  Effect.gen(function* () {
    const service = yield* Service.SemanggiService;
    const sql = yield* SqlClient.SqlClient;
    yield* service.createProject(actor, { id: "p", name: "Project" });
    yield* service.renameProject(actor, { id: "p", name: "Renamed", expectedRevision: 1 });
    yield* service.setProjectArchived(actor, { id: "p", archived: true, expectedRevision: 2 });
    yield* service.setProjectArchived(actor, { id: "p", archived: false, expectedRevision: 3 });
    yield* service.createSection(actor, {
      id: "section",
      projectId: "p",
      parentId: null,
      name: "Section",
    });
    yield* service.renameSection(actor, {
      id: "section",
      name: "New section",
      expectedRevision: 1,
    });
    yield* service.createSession(actor, {
      id: "session",
      projectId: "p",
      sectionId: "section",
      mode: "work",
    });
    assert.isTrue(
      yield* service.bindThread(actor, {
        sessionId: "session",
        environmentId: "env",
        threadId: "thread",
      }),
    );
    assert.isFalse(
      yield* service.bindThread(actor, {
        sessionId: "session",
        environmentId: "env",
        threadId: "thread",
      }),
    );
    yield* service.deleteLeafSection(actor, { id: "section", expectedRevision: 2 });
    const events = yield* sql<{
      event_type: string;
      entity_type: string;
      entity_id: string;
      revision: number;
      actor_id: string;
      environment_id: string;
      request_id: string;
    }>`
      SELECT event_type, entity_type, entity_id, revision, actor_id, environment_id, request_id FROM sh_audit ORDER BY sequence`;
    assert.deepEqual(
      events.map(({ event_type, revision }) => [event_type, revision]),
      [
        ["project.create", 1],
        ["project.rename", 2],
        ["project.archive", 3],
        ["project.restore", 4],
        ["section.create", 1],
        ["section.rename", 2],
        ["session.create", 1],
        ["binding.create", 1],
        ["session.move_to_root", 2],
        ["section.delete", 2],
      ],
    );
    assert.isTrue(
      events.every(
        (event) =>
          event.actor_id === actor.actorId &&
          event.environment_id === actor.environmentId &&
          event.request_id === actor.requestId,
      ),
    );
    assert.deepEqual(
      events.slice(-2).map(({ entity_type, entity_id }) => [entity_type, entity_id]),
      [
        ["session", "session"],
        ["section", "section"],
      ],
    );
  }).pipe(Effect.provide(auditedLayer)),
);

it.effect("rolls back mutations and dependent session changes when auditing fails", () =>
  Effect.gen(function* () {
    const service = yield* Service.SemanggiService;
    const sql = yield* SqlClient.SqlClient;
    yield* service.createProject(actor, { id: "p", name: "Project" });
    yield* service.createSection(actor, {
      id: "section",
      projectId: "p",
      parentId: null,
      name: "Section",
    });
    yield* service.createSession(actor, {
      id: "s",
      projectId: "p",
      sectionId: "section",
      mode: "work",
    });
    yield* sql`CREATE TRIGGER reject_section_delete_audit BEFORE INSERT ON sh_audit WHEN NEW.event_type IN ('section.delete', 'project.rename', 'binding.create') BEGIN SELECT RAISE(ABORT, 'injected failure'); END`;
    yield* Effect.flip(
      service.renameProject(actor, { id: "p", name: "New name", expectedRevision: 1 }),
    );
    yield* Effect.flip(
      service.bindThread(actor, { sessionId: "s", environmentId: "env", threadId: "thread" }),
    );
    assert.isNull(yield* service.findSessionByThread("env", "thread"));
    yield* Effect.flip(service.deleteLeafSection(actor, { id: "section", expectedRevision: 1 }));
    assert.equal((yield* service.listSections("p"))[0]?.id, "section");
    assert.equal((yield* service.listSessions("p"))[0]?.section_id, "section");
    assert.equal((yield* service.listSessions("p"))[0]?.revision, 1);
    const events = yield* sql<{
      event_type: string;
    }>`SELECT event_type FROM sh_audit ORDER BY sequence`;
    assert.deepEqual(
      events.map(({ event_type }) => event_type),
      ["project.create", "section.create", "session.create"],
    );
    yield* Effect.flip(
      service.renameProject(
        { ...actor, actorId: " " },
        { id: "p", name: "Invalid", expectedRevision: 1 },
      ),
    );
    yield* Effect.flip(
      service.bindThread(
        { ...actor, environmentId: "other" },
        { sessionId: "s", environmentId: "env", threadId: "thread" },
      ),
    );
    assert.equal((yield* service.listProjects()).find(({ id }) => id === "p")?.revision, 1);
  }).pipe(Effect.provide(auditedLayer)),
);

it.effect(
  "allows only one concurrent update for the same revision and records one audit event",
  () =>
    Effect.gen(function* () {
      const service = yield* Service.SemanggiService;
      const sql = yield* SqlClient.SqlClient;
      yield* service.createProject(actor, { id: "p", name: "Project" });
      const results = yield* Effect.all(
        [
          Effect.exit(
            service.renameProject(actor, { id: "p", name: "First", expectedRevision: 1 }),
          ),
          Effect.exit(
            service.renameProject(actor, { id: "p", name: "Second", expectedRevision: 1 }),
          ),
        ],
        { concurrency: "unbounded" },
      );
      assert.equal(results.filter((result) => result._tag === "Success").length, 1);
      assert.equal(results.filter((result) => result._tag === "Failure").length, 1);
      const events = yield* sql<{
        event_type: string;
        revision: number;
      }>`SELECT event_type, revision FROM sh_audit ORDER BY sequence`;
      assert.deepEqual(events, [
        { event_type: "project.create", revision: 1 },
        { event_type: "project.rename", revision: 2 },
      ]);
    }).pipe(Effect.provide(auditedLayer)),
);

it.effect("opens the Semanggi profile separately from the T3 database", () =>
  Effect.gen(function* () {
    const service = yield* Service.SemanggiService;
    const config = yield* ServerConfig.ServerConfig;
    yield* service.createProject(actor, { id: "local", name: "Local" });
    assert.isTrue(NodeFS.existsSync(NodePath.join(config.stateDir, "semanggi.sqlite")));
    assert.isFalse(NodeFS.existsSync(config.dbPath));
  }).pipe(
    Effect.provide(
      Service.layerConfig.pipe(
        Layer.provideMerge(ServerConfig.layerTest(process.cwd(), { prefix: "semanggi-service-" })),
        Layer.provide(NodeServices.layer),
      ),
    ),
  ),
);

it.effect("uses the authenticated actor and rejects forged fields and foreign environments", () =>
  Effect.gen(function* () {
    const service = yield* Service.SemanggiService;
    yield* service.createProject(actor, { id: "project", name: "Project" });
    yield* service.createSession(actor, {
      id: "session",
      projectId: Storage.UNASSIGNED_PROJECT_ID,
      sectionId: null,
      mode: "work",
    });
    yield* service.bindThread(actor, {
      sessionId: "session",
      environmentId: "env",
      threadId: "thread",
    });
    const input = {
      requestId: "r",
      idempotencyKey: "k",
      scope: { environmentId: "env", projectId: "project" },
      expectedRevision: 1,
      operation: {
        type: "session.move",
        sessionId: "session",
        projectId: "project",
        sectionId: null,
      },
    };
    const forged = yield* Effect.flip(
      service.moveSession("actor", "env", { ...input, actorId: "other" }),
    );
    assert.equal(forged.reason, "invalid_request");
    const foreign = yield* Effect.flip(service.moveSession("actor", "other", input));
    assert.equal(foreign.reason, "scope_mismatch");
    const wrongProject = yield* Effect.flip(
      service.moveSession("actor", "env", {
        ...input,
        scope: { ...input.scope, projectId: "other" },
      }),
    );
    assert.equal(wrongProject.reason, "scope_mismatch");
    assert.deepEqual(yield* service.moveSession("actor", "env", input), { revision: 2 });
    assert.deepEqual(yield* service.moveSession("actor", "env", input), { revision: 2 });
    const otherActor = yield* Effect.flip(service.moveSession("other", "env", input));
    assert.equal(otherActor.reason, "invalid_request");
    const stale = yield* Effect.flip(
      service.moveSession("actor", "env", { ...input, idempotencyKey: "new" }),
    );
    assert.equal(stale.reason, "conflict");
  }).pipe(Effect.provide(Service.layer.pipe(Layer.provide(Storage.layerFromPath(":memory:"))))),
);

it.effect("executes an authorized RPC mutation and replays its receipt", () => {
  const group = WsRpcGroup.omit(
    ...[...WsRpcGroup.requests.keys()].filter(
      (
        tag,
      ): tag is Exclude<
        keyof typeof RpcAuthorization.RPC_REQUIRED_SCOPES,
        typeof WS_METHODS.semanggiSessionMove
      > => tag !== WS_METHODS.semanggiSessionMove,
    ),
  );
  return Effect.gen(function* () {
    const service = yield* Service.SemanggiService;
    yield* service.createProject(actor, { id: "project", name: "Project" });
    yield* service.createSession(actor, {
      id: "session",
      projectId: Storage.UNASSIGNED_PROJECT_ID,
      sectionId: null,
      mode: "work",
    });
    const client = yield* RpcTest.makeClient(group).pipe(
      Effect.provide(
        Layer.mergeAll(
          group.toLayerHandler(WS_METHODS.semanggiSessionMove, (input) =>
            service.moveSession("actor", "env", input),
          ),
          RpcAuthorization.layer([AuthOrchestrationOperateScope]),
        ),
      ),
    );
    const input = {
      requestId: "r",
      idempotencyKey: "k",
      scope: { environmentId: "env", projectId: "project" },
      expectedRevision: 1,
      operation: {
        type: "session.move",
        sessionId: "session",
        projectId: "project",
        sectionId: null,
      },
    };
    assert.deepEqual(yield* client[WS_METHODS.semanggiSessionMove](input), { revision: 2 });
    assert.deepEqual(yield* client[WS_METHODS.semanggiSessionMove](input), { revision: 2 });
  }).pipe(
    Effect.scoped,
    Effect.provide(Service.layer.pipe(Layer.provide(Storage.layerFromPath(":memory:")))),
  );
});

it.effect(
  "links workspaces with a single default, promotes on unlink, and rejects archived projects",
  () =>
    Effect.gen(function* () {
      const service = yield* Service.SemanggiService;
      const sql = yield* SqlClient.SqlClient;
      yield* service.createProject(actor, { id: "wp", name: "Workspace project" });
      const link = (t3ProjectId: string, isDefault: boolean) =>
        service.linkWorkspace(actor, {
          projectId: "wp",
          environmentId: "env",
          t3ProjectId,
          isDefault,
        });
      yield* link("b", false);
      yield* link("a", false);
      yield* link("a", false); // idempotent retry
      const read = () => service.listWorkspaceLinks("env");
      assert.deepEqual(
        (yield* read()).map((row) => [row.t3_project_id, row.is_default]),
        [
          ["a", 0],
          ["b", 1],
        ],
      );
      yield* link("a", true);
      assert.deepEqual(
        (yield* read()).map((row) => [row.t3_project_id, row.is_default]),
        [
          ["a", 1],
          ["b", 0],
        ],
      );
      yield* service.unlinkWorkspace(actor, {
        projectId: "wp",
        environmentId: "env",
        t3ProjectId: "a",
      });
      assert.deepEqual(
        (yield* read()).map((row) => [row.t3_project_id, row.is_default]),
        [["b", 1]],
      );
      // Another environment is unaffected, and a foreign audit context is refused.
      assert.deepEqual(yield* service.listWorkspaceLinks("other"), []);
      const forged = yield* Effect.flip(
        service.linkWorkspace(actor, {
          projectId: "wp",
          environmentId: "other",
          t3ProjectId: "x",
          isDefault: false,
        }),
      );
      assert.equal(forged._tag, "SemanggiValidationError");
      yield* service.setProjectArchived(actor, { id: "wp", archived: true, expectedRevision: 1 });
      const archived = yield* Effect.flip(link("c", false));
      assert.equal(archived._tag, "SemanggiValidationError");
      assert.deepEqual(yield* read(), []); // archived projects are not listed
      const events = yield* sql<{
        event_type: string;
      }>`SELECT event_type FROM sh_audit WHERE event_type LIKE 'workspace.%'`;
      assert.equal(events.length, 5);
    }).pipe(Effect.provide(auditedLayer)),
);

it.effect("reorders sibling sections atomically and rejects stale revisions and edge moves", () =>
  Effect.gen(function* () {
    const service = yield* Service.SemanggiService;
    yield* service.createProject(actor, { id: "rp", name: "Reorder" });
    for (const id of ["s1", "s2", "s3"]) {
      yield* service.createSection(actor, { id, projectId: "rp", parentId: null, name: id });
    }
    const order = () =>
      service
        .listSections("rp")
        .pipe(Effect.map((rows) => rows.map(({ id, revision }) => [id, revision])));
    assert.deepEqual(yield* order(), [
      ["s1", 1],
      ["s2", 1],
      ["s3", 1],
    ]);
    const moved = yield* service.reorderSection(actor, {
      id: "s3",
      direction: "up",
      expectedRevision: 1,
    });
    assert.equal(moved.revision, 2);
    assert.deepEqual(yield* order(), [
      ["s1", 1],
      ["s3", 2],
      ["s2", 2],
    ]);
    const stale = yield* Effect.flip(
      service.reorderSection(actor, { id: "s3", direction: "up", expectedRevision: 1 }),
    );
    assert.equal(stale._tag, "SemanggiRevisionConflict");
    yield* service.reorderSection(actor, { id: "s3", direction: "up", expectedRevision: 2 });
    const edge = yield* Effect.flip(
      service.reorderSection(actor, { id: "s3", direction: "up", expectedRevision: 3 }),
    );
    assert.equal(edge._tag, "SemanggiValidationError");
    assert.deepEqual(
      (yield* order()).map(([id]) => id),
      ["s3", "s1", "s2"],
    );
  }).pipe(Effect.provide(auditedLayer)),
);
