import { assert, it } from "@effect/vitest";
import {
  EventId,
  MessageId,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
  type OrchestrationV2DomainEvent,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/sql/SqlClient";
import * as EventStore from "../persistence/OrchestrationEventStore.ts";
import * as T3Sqlite from "../persistence/Sqlite.ts";
import * as Storage from "./Storage.ts";
import * as Ingestion from "./ReplayIngestion.ts";
import * as ManagedProjectFolders from "../project/ManagedProjectFolders.ts";

const folders = {
  isScratchProject: (projectId: string) => Effect.succeed(projectId === "scratch"),
} as unknown as ManagedProjectFolders.ManagedProjectFolders["Service"];

it.effect(
  "discovers native Work and Code threads from committed events, excluding side chats",
  () =>
    Effect.gen(function* () {
      const source = yield* EventStore.OrchestrationEventStore;
      const sql = yield* SqlClient.SqlClient;
      const at = DateTime.makeUnsafe("2026-01-01T00:00:00Z");
      const provider = ProviderInstanceId.make("codex");
      const created = (
        id: string,
        project: string,
        sideChat = false,
      ): OrchestrationV2DomainEvent => {
        const threadId = ThreadId.make(id);
        const parent = ThreadId.make("parent");
        return {
          id: EventId.make(`created:${id}`),
          type: "thread.created",
          threadId,
          providerInstanceId: provider,
          occurredAt: at,
          payload: {
            createdBy: "user",
            creationSource: "web",
            id: threadId,
            projectId: ProjectId.make(project),
            title: id,
            providerInstanceId: provider,
            modelSelection: { instanceId: provider, model: "gpt-5" },
            runtimeMode: "full-access",
            interactionMode: "default",
            branch: null,
            worktreePath: null,
            linkedPullRequest: null,
            branchPullRequest: null,
            activeProviderThreadId: null,
            lineage: sideChat
              ? {
                  parentThreadId: parent,
                  relationshipToParent: "fork",
                  rootThreadId: parent,
                  sideChat: true,
                }
              : { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId },
            forkedFrom: null,
            createdAt: at,
            updatedAt: at,
            archivedAt: null,
            settledOverride: null,
            settledAt: null,
            lastVisitedAt: null,
            deletedAt: null,
          },
        };
      };
      yield* source.appendAgentEvents({
        events: [
          created("work", "scratch"),
          created("code", "project"),
          created("side", "project", true),
        ],
      });
      yield* Ingestion.ingestNextPage({ environmentId: "env", limit: 10 });
      const rows = yield* sql<{
        t3_thread_id: string;
        mode: string;
      }>`SELECT b.t3_thread_id,s.mode FROM sh_thread_bindings b JOIN sh_sessions s ON s.id = b.session_id ORDER BY b.t3_thread_id`;
      assert.deepEqual(rows, [
        { t3_thread_id: "code", mode: "code" },
        { t3_thread_id: "work", mode: "work" },
      ]);
      yield* Ingestion.ingestNextPage({ environmentId: "env", limit: 10 });
      assert.equal((yield* sql`SELECT id FROM sh_sessions`).length, 2);
    }).pipe(
      Effect.provide(
        Layer.mergeAll(
          EventStore.layer.pipe(Layer.provide(T3Sqlite.layerMemory)),
          Storage.layerFromPath(":memory:"),
          Layer.succeed(ManagedProjectFolders.ManagedProjectFolders, folders),
        ),
      ),
    ),
);

it.effect(
  "ingests committed events from the native T3 store into a separate Semanggi database",
  () =>
    Effect.gen(function* () {
      const source = yield* EventStore.OrchestrationEventStore;
      const sql = yield* SqlClient.SqlClient;
      const threadId = ThreadId.make("native-thread");
      const at = DateTime.makeUnsafe("2026-01-01T00:00:00Z");
      const event = (id: string, text: string): OrchestrationV2DomainEvent => ({
        id: EventId.make(id),
        threadId,
        occurredAt: at,
        type: "message.updated",
        payload: {
          createdBy: "user",
          creationSource: "web",
          id: MessageId.make("message"),
          threadId,
          runId: null,
          nodeId: null,
          role: "user",
          text,
          attachments: [],
          streaming: false,
          createdAt: at,
          updatedAt: at,
        },
      });
      yield* source.appendAgentEvents({
        events: [event("event-1", "original"), event("event-2", "edited")],
      });
      const first = yield* Ingestion.ingestNextPage({ environmentId: "env", limit: 1 });
      assert.equal(first.state, "CATCHING_UP");
      const second = yield* Ingestion.ingestNextPage({ environmentId: "env", limit: 1 });
      assert.equal(second.state, "CATCHING_UP");
      const inbox = yield* sql<{
        event_id: string;
      }>`SELECT event_id FROM sh_inbox ORDER BY source_sequence`;
      assert.deepEqual(
        inbox.map(({ event_id }) => event_id),
        ["event-1", "event-2"],
      );
      const pending = yield* sql<{
        thread_id: string;
        source_sequence: number;
      }>`SELECT thread_id,source_sequence FROM sh_thread_refresh_queue`;
      assert.equal(pending[0]?.thread_id, "native-thread");
      assert.equal(pending[0]?.source_sequence, second.cursor);
    }).pipe(
      Effect.provide(
        Layer.mergeAll(
          EventStore.layer.pipe(Layer.provide(T3Sqlite.layerMemory)),
          Storage.layerFromPath(":memory:"),
          Layer.succeed(ManagedProjectFolders.ManagedProjectFolders, folders),
        ),
      ),
    ),
);
