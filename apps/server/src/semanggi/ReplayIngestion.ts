// @effect-diagnostics nodeBuiltinImport:off
import * as NodeCrypto from "node:crypto";
import {
  ApplicationProjectEvent,
  OrchestrationV2StoredEvent,
  type ApplicationStoredEvent,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as Replay from "./CommittedReplay.ts";
import * as Inbox from "./ReplayInbox.ts";
import * as ManagedProjectFolders from "../project/ManagedProjectFolders.ts";

const encodeThreadEvent = Schema.encodeSync(
  Schema.fromJsonString(Schema.toCodecJson(OrchestrationV2StoredEvent)),
);
const encodeProjectEvent = Schema.encodeSync(Schema.fromJsonString(ApplicationProjectEvent));

const normalize = (event: ApplicationStoredEvent): Inbox.IncomingEvent => {
  const isThread = "event" in event;
  const serialized = isThread ? encodeThreadEvent(event) : encodeProjectEvent(event);
  return {
    sequence: event.sequence,
    eventId: isThread ? event.event.id : event.eventId,
    hash: NodeCrypto.createHash("sha256").update(serialized).digest("hex"),
    threadId: isThread ? event.event.threadId : null,
  };
};

/** Commits invalidations with the cursor; queued threads are not yet fresh projections. */
export const ingestNextPage = Effect.fn("semanggi.replayIngestion.ingestNextPage")(
  function* (input: { readonly environmentId: string; readonly limit: number }) {
    const sql = yield* SqlClient.SqlClient;
    const folders = yield* ManagedProjectFolders.ManagedProjectFolders;
    const rows = yield* sql<{
      cursor: number;
      state: string;
    }>`SELECT cursor, state FROM sh_ingestion_checkpoints WHERE environment_id = ${input.environmentId}`;
    if (rows[0]?.state === "RECONCILING")
      return { state: "RECONCILING" as const, cursor: rows[0].cursor };
    const cursor = rows[0]?.cursor ?? 0;
    const page = yield* Replay.readCommittedPage({ afterSequence: cursor, limit: input.limit });
    if (page.state === "GAP") {
      yield* Inbox.markGap(input.environmentId, cursor, page.highWater);
      return { state: "RECONCILING" as const, cursor };
    }
    if (rows[0] && page.events.length === 0 && page.nextSequence === cursor) {
      return {
        state: rows[0].state === "LIVE" ? ("LIVE" as const) : ("CATCHING_UP" as const),
        cursor,
        hasMore: false,
      };
    }
    const events = yield* Effect.forEach(page.events, (event) => {
      const incoming = normalize(event);
      if ("event" in event && event.event.type === "thread.deleted")
        return Effect.succeed({ ...incoming, cancelDiscovery: true });
      return "event" in event &&
        event.event.type === "thread.created" &&
        event.event.payload.lineage.relationshipToParent !== "subagent" &&
        event.event.payload.lineage.sideChat !== true
        ? folders.isScratchProject(event.event.payload.projectId).pipe(
            Effect.map((scratch) => ({
              ...incoming,
              discovery: { mode: scratch ? ("work" as const) : ("code" as const) },
            })),
          )
        : Effect.succeed(incoming);
    });
    yield* Inbox.commitPage({
      environmentId: input.environmentId,
      expectedCursor: cursor,
      nextCursor: page.nextSequence,
      highWater: page.highWater,
      events,
    });
    return { state: "CATCHING_UP" as const, cursor: page.nextSequence, hasMore: page.hasMore };
  },
);
