import { assert, it } from "@effect/vitest";
import type { ApplicationStoredEvent } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Stream from "effect/Stream";
import * as EventStore from "../persistence/OrchestrationEventStore.ts";
import * as Replay from "./CommittedReplay.ts";

it.effect("accepts a cursor at the prune boundary when every event was removed", () => {
  const source = {
    latestApplicationSequence: Effect.succeed(0),
    prunedThroughSequence: Effect.succeed(9),
    readApplicationEvents: () => Stream.empty,
  } as unknown as EventStore.OrchestrationEventStore["Service"];
  return Effect.gen(function* () {
    const page = yield* Replay.readCommittedPage({ afterSequence: 9, limit: 10 });
    assert.deepEqual(page, {
      state: "OK",
      events: [],
      nextSequence: 9,
      highWater: 9,
      hasMore: false,
    });
  }).pipe(Effect.provideService(EventStore.OrchestrationEventStore, source));
});

it.effect("pages committed events and detects a prune that races the read", () => {
  let pruned = 0;
  let pruneDuringRead = false;
  const events = [1, 3, 5].map((sequence) => ({ sequence }) as ApplicationStoredEvent);
  const source = {
    latestApplicationSequence: Effect.succeed(8),
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
      ).pipe(
        Stream.map((event) => {
          if (pruneDuringRead) pruned = 4;
          return event;
        }),
      ),
  } as unknown as EventStore.OrchestrationEventStore["Service"];
  return Effect.gen(function* () {
    const first = yield* Replay.readCommittedPage({ afterSequence: 0, limit: 2 });
    assert.equal(first.state, "OK");
    if (first.state !== "OK") return;
    assert.deepEqual(
      first.events.map(({ sequence }) => sequence),
      [1, 3],
    );
    assert.equal(first.nextSequence, 3);
    assert.isTrue(first.hasMore);
    const second = yield* Replay.readCommittedPage({ afterSequence: first.nextSequence, limit: 2 });
    assert.equal(second.state, "OK");
    if (second.state !== "OK") return;
    assert.equal(second.nextSequence, 8);
    assert.isFalse(second.hasMore);
    pruneDuringRead = true;
    assert.equal((yield* Replay.readCommittedPage({ afterSequence: 0, limit: 2 })).state, "GAP");
    assert.equal((yield* Replay.readCommittedPage({ afterSequence: 0, limit: 2 })).state, "GAP");
    assert.equal((yield* Replay.readCommittedPage({ afterSequence: 9, limit: 2 })).state, "GAP");
    yield* Effect.flip(Replay.readCommittedPage({ afterSequence: 0, limit: 0 }));
  }).pipe(Effect.provideService(EventStore.OrchestrationEventStore, source));
});
