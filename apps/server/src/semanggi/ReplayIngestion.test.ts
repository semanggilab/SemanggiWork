import { assert, it } from "@effect/vitest";
import {
  EventId,
  MessageId,
  ProjectId,
  ThreadId,
  type ApplicationStoredEvent,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/sql/SqlClient";
import * as EventStore from "../persistence/OrchestrationEventStore.ts";
import * as Storage from "./Storage.ts";
import * as Ingestion from "./ReplayIngestion.ts";
import * as ManagedProjectFolders from "../project/ManagedProjectFolders.ts";

const folders = {
  isScratchProject: () => Effect.succeed(false),
} as unknown as ManagedProjectFolders.ManagedProjectFolders["Service"];

it.effect(
  "ingests committed source pages atomically and retains a durable refresh on a gap",
  () => {
    const at = DateTime.makeUnsafe("2026-01-01T00:00:00Z");
    const threadId = ThreadId.make("thread");
    const projectId = ProjectId.make("project");
    const events: ReadonlyArray<ApplicationStoredEvent> = [
      {
        sequence: 1,
        eventId: EventId.make("p1"),
        aggregateKind: "project",
        aggregateId: projectId,
        occurredAt: "2026-01-01T00:00:00Z",
        commandId: null,
        causationEventId: null,
        correlationId: null,
        metadata: {},
        type: "project.deleted",
        payload: { projectId, deletedAt: "2026-01-01T00:00:00Z" },
      },
      {
        sequence: 2,
        commandId: null,
        event: {
          id: EventId.make("e2"),
          threadId,
          occurredAt: at,
          type: "message.updated",
          payload: {
            createdBy: "user",
            creationSource: "web",
            id: MessageId.make("m1"),
            threadId,
            runId: null,
            nodeId: null,
            role: "user",
            text: "private test message",
            attachments: [],
            streaming: false,
            createdAt: at,
            updatedAt: at,
          },
        },
      },
    ];
    let pruned = 0;
    let highWater = 2;
    const source = {
      latestApplicationSequence: Effect.sync(() => highWater),
      prunedThroughSequence: Effect.sync(() => pruned),
      readApplicationEvents: ({
        afterSequence,
        throughSequence,
      }: {
        afterSequence: number;
        throughSequence: number;
      }) =>
        Stream.fromIterable(
          events.filter(
            (event) => event.sequence > afterSequence && event.sequence <= throughSequence,
          ),
        ),
    } as unknown as EventStore.OrchestrationEventStore["Service"];
    return Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      assert.deepEqual(yield* Ingestion.ingestNextPage({ environmentId: "env", limit: 1 }), {
        state: "CATCHING_UP",
        cursor: 1,
        hasMore: true,
      });
      assert.deepEqual(yield* Ingestion.ingestNextPage({ environmentId: "env", limit: 1 }), {
        state: "CATCHING_UP",
        cursor: 2,
        hasMore: false,
      });
      const queue = yield* sql<{
        thread_id: string;
        source_sequence: number;
      }>`SELECT thread_id, source_sequence FROM sh_thread_refresh_queue`;
      assert.deepEqual(queue, [{ thread_id: "thread", source_sequence: 2 }]);
      const inbox = yield* sql<{
        event_id: string;
        event_hash: string;
      }>`SELECT event_id, event_hash FROM sh_inbox ORDER BY source_sequence`;
      assert.deepEqual(
        inbox.map(({ event_id }) => event_id),
        ["p1", "e2"],
      );
      assert.isTrue(inbox.every(({ event_hash }) => event_hash.length === 64));
      pruned = 3;
      highWater = 3;
      assert.deepEqual(yield* Ingestion.ingestNextPage({ environmentId: "env", limit: 1 }), {
        state: "RECONCILING",
        cursor: 2,
      });
      assert.equal((yield* sql`SELECT thread_id FROM sh_thread_refresh_queue`).length, 1);
    }).pipe(
      Effect.provide(
        Layer.mergeAll(
          Storage.layerFromPath(":memory:"),
          Layer.succeed(EventStore.OrchestrationEventStore, source),
          Layer.succeed(ManagedProjectFolders.ManagedProjectFolders, folders),
        ),
      ),
    );
  },
);
