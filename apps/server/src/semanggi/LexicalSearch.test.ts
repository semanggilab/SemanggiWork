// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/sql/SqlClient";
import * as Adapter from "./NativeAdapter.ts";
import * as Repository from "./Repository.ts";
import * as Search from "./LexicalSearch.ts";
import * as Storage from "./Storage.ts";

it.effect("does not return a result when its session moves while the native snapshot is read", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* Repository.createProject({ id: "p", name: "Project" });
    yield* Repository.createProject({ id: "q", name: "Other" });
    yield* Repository.createSession({
      id: "session",
      projectId: "p",
      sectionId: null,
      mode: "code",
    });
    yield* Repository.bindThread({
      sessionId: "session",
      environmentId: "env",
      threadId: "thread",
    });
    const native = {
      searchThreads: () =>
        Effect.succeed({ matches: [{ threadId: "thread", title: "Result", source: "title" }] }),
      readThread: () =>
        sql`UPDATE sh_sessions SET project_id='q' WHERE id='session'`.pipe(
          Effect.as({
            snapshotSequence: 1,
            projection: { thread: { id: "thread", title: "Result", deletedAt: null } },
          }),
        ),
    } as unknown as Adapter.NativeAdapter["Service"];
    const result = yield* Search.search({
      environmentId: "env",
      projectId: "p",
      query: "Result",
      limit: 10,
    }).pipe(Effect.provideService(Adapter.NativeAdapter, native));
    assert.deepEqual(result.matches, []);
  }).pipe(Effect.provide(Storage.layerFromPath(":memory:"))),
);

it.effect("resolves a search result to its native thread after reopening storage", () => {
  const dir = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "semanggi-search-"));
  const path = NodePath.join(dir, "semanggi.sqlite");
  const native = {
    searchThreads: () =>
      Effect.succeed({
        matches: [
          { threadId: "thread", projectId: "native-project", title: "Result", source: "title" },
        ],
      }),
    readThread: () =>
      Effect.succeed({
        snapshotSequence: 3,
        projection: { thread: { id: "thread", title: "Result", deletedAt: null } },
      }),
  } as unknown as Adapter.NativeAdapter["Service"];
  const layer = () =>
    Layer.mergeAll(Storage.layerFromPath(path), Layer.succeed(Adapter.NativeAdapter, native));
  return Effect.gen(function* () {
    yield* Effect.gen(function* () {
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
    }).pipe(Effect.provide(layer()));
    yield* Effect.gen(function* () {
      const results = yield* Search.search({
        environmentId: "env",
        projectId: null,
        query: "result",
        limit: 10,
      });
      assert.equal(results.matches[0]?.sessionId, "session");
      const source = yield* Search.openSource({
        environmentId: "env",
        projectId: null,
        sessionId: "session",
      });
      assert.equal(source.threadId, "thread");
      assert.equal(source.mode, "code");
    }).pipe(Effect.provide(layer()));
  }).pipe(Effect.ensuring(Effect.sync(() => NodeFS.rmSync(dir, { recursive: true, force: true }))));
});

it.effect(
  "returns only bound threads in scope across both modes and signals incomplete coverage",
  () => {
    let deleted = false;
    let messageText = "query from current snapshot";
    let messageVisible = true;
    let messageAvailable = true;
    let searchedThreadIds: ReadonlyArray<string> = [];
    const native = {
      readThread: (threadId: string) =>
        Effect.sync(() => ({
          snapshotSequence: 42,
          projection: {
            thread: {
              id: threadId,
              title: threadId === "work-thread" ? "Work result" : "Code result",
              deletedAt: deleted ? "deleted" : null,
            },
            messages: messageAvailable
              ? [{ id: "message", role: "assistant", text: messageText }]
              : [],
            visibleTurnItems: messageVisible
              ? [
                  {
                    sourceThreadId: threadId,
                    item: { type: "assistant_message", messageId: "message" },
                  },
                ]
              : [],
          },
        })),
      searchThreads: (input: { readonly threadIds?: ReadonlyArray<string> }) =>
        Effect.sync(() => {
          searchedThreadIds = input.threadIds ?? [];
          return {
            matches: [
              {
                threadId: "work-thread",
                projectId: "native",
                title: "Work result",
                source: "title",
              },
              { threadId: "work-thread", projectId: "native", title: "Duplicate", source: "user" },
              {
                threadId: "code-thread",
                projectId: "native",
                title: "Code result",
                source: "assistant",
                messageId: "stale-message",
              },
              {
                threadId: "code-thread",
                projectId: "native",
                title: "Code result",
                source: "assistant",
                messageId: "message",
                snippet: "never return this",
              },
              {
                threadId: "other-thread",
                projectId: "native",
                title: "Other environment",
                source: "title",
              },
            ],
          };
        }),
    } as unknown as Adapter.NativeAdapter["Service"];
    return Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* Repository.createProject({ id: "p", name: "Project" });
      yield* Repository.createSession({
        id: "work",
        projectId: "p",
        sectionId: null,
        mode: "work",
      });
      yield* Repository.createSession({
        id: "code",
        projectId: "p",
        sectionId: null,
        mode: "code",
      });
      yield* Repository.createSession({
        id: "other",
        projectId: "p",
        sectionId: null,
        mode: "work",
      });
      yield* Repository.bindThread({
        sessionId: "work",
        environmentId: "env",
        threadId: "work-thread",
      });
      yield* Repository.bindThread({
        sessionId: "code",
        environmentId: "env",
        threadId: "code-thread",
      });
      yield* Repository.bindThread({
        sessionId: "other",
        environmentId: "other",
        threadId: "other-thread",
      });
      yield* sql`INSERT INTO sh_thread_refresh_queue(environment_id,thread_id,source_sequence) VALUES ('env','code-thread',2)`;
      const results = yield* Search.search({
        environmentId: "env",
        projectId: "p",
        query: "query",
        limit: 20,
      });
      assert.equal(results.coverage, "partial");
      assert.sameMembers([...searchedThreadIds], ["work-thread", "code-thread"]);
      assert.equal(results.syncState, "UNINITIALIZED");
      assert.deepEqual(
        results.matches.map(({ sessionId, mode, freshness }) => [sessionId, mode, freshness]),
        [
          ["work", "work", "unknown"],
          ["code", "code", "pending"],
        ],
      );
      assert.isFalse("snippet" in results.matches[1]!);
      assert.equal(results.matches[1]?.messageId, "message");
      yield* sql`INSERT INTO sh_ingestion_checkpoints VALUES ('env',42,42,'CATCHING_UP')`;
      yield* sql`INSERT INTO sh_native_projection VALUES ('env','work-thread',42,'Work result',0)`;
      assert.equal(
        (yield* Search.search({ environmentId: "env", projectId: "p", query: "query", limit: 20 }))
          .matches[0]?.freshness,
        "fresh",
      );
      yield* sql`UPDATE sh_ingestion_checkpoints SET state='LIVE' WHERE environment_id='env'`;
      const live = yield* Search.search({
        environmentId: "env",
        projectId: "p",
        query: "query",
        limit: 20,
      });
      assert.equal(live.syncState, "LIVE");
      assert.equal(live.matches[0]?.freshness, "fresh");
      yield* sql`UPDATE sh_ingestion_checkpoints SET state='CATCHING_UP' WHERE environment_id='env'`;
      yield* sql`INSERT INTO sh_thread_refresh_queue(environment_id,thread_id,source_sequence) VALUES ('env','work-thread',43)`;
      assert.equal(
        (yield* Search.search({ environmentId: "env", projectId: "p", query: "query", limit: 20 }))
          .matches[0]?.freshness,
        "pending",
      );
      yield* sql`DELETE FROM sh_thread_refresh_queue WHERE thread_id='work-thread'`;
      yield* sql`UPDATE sh_native_projection SET source_sequence=41 WHERE thread_id='work-thread'`;
      assert.equal(
        (yield* Search.search({ environmentId: "env", projectId: "p", query: "query", limit: 20 }))
          .matches[0]?.freshness,
        "unknown",
      );
      yield* sql`UPDATE sh_ingestion_checkpoints SET state='RECONCILING' WHERE environment_id='env'`;
      assert.equal(
        (yield* Search.search({ environmentId: "env", projectId: "p", query: "query", limit: 20 }))
          .matches[0]?.freshness,
        "pending",
      );
      yield* sql`UPDATE sh_ingestion_checkpoints SET state='CATCHING_UP' WHERE environment_id='env'`;
      messageText = "content was changed";
      assert.deepEqual(
        (yield* Search.search({
          environmentId: "env",
          projectId: "p",
          query: "query",
          limit: 20,
        })).matches.map(({ sessionId }) => sessionId),
        ["work"],
      );
      messageText = "query from current snapshot";
      messageVisible = false;
      assert.deepEqual(
        (yield* Search.search({
          environmentId: "env",
          projectId: "p",
          query: "query",
          limit: 20,
        })).matches.map(({ sessionId }) => sessionId),
        ["work"],
      );
      yield* Effect.flip(
        Search.openSource({
          environmentId: "env",
          projectId: "p",
          sessionId: "code",
          messageId: "message",
        }),
      );
      messageVisible = true;
      assert.equal(
        (yield* Search.openSource({
          environmentId: "env",
          projectId: "p",
          sessionId: "code",
          messageId: "message",
        })).messageId,
        "message",
      );
      messageAvailable = false;
      yield* Effect.flip(
        Search.openSource({
          environmentId: "env",
          projectId: "p",
          sessionId: "code",
          messageId: "message",
        }),
      );
      messageAvailable = true;
      assert.deepEqual(
        yield* Search.openSource({ environmentId: "env", projectId: "p", sessionId: "code" }),
        {
          sessionId: "code",
          threadId: "code-thread",
          projectId: "p",
          mode: "code",
          sourceSequence: 42,
        },
      );
      yield* Effect.flip(
        Search.openSource({ environmentId: "other", projectId: "p", sessionId: "code" }),
      );
      deleted = true;
      yield* Effect.flip(
        Search.openSource({ environmentId: "env", projectId: "p", sessionId: "code" }),
      );
      assert.equal(
        (yield* Search.search({
          environmentId: "env",
          projectId: "another",
          query: "query",
          limit: 20,
        })).matches.length,
        0,
      );
      yield* Effect.flip(
        Search.search({ environmentId: "env", projectId: null, query: "x", limit: 20 }),
      );
    }).pipe(
      Effect.provide(
        Layer.mergeAll(
          Storage.layerFromPath(":memory:"),
          Layer.succeed(Adapter.NativeAdapter, native),
        ),
      ),
    );
  },
);
