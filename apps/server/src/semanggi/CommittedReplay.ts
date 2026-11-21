import type { ApplicationStoredEvent } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as EventStore from "../persistence/OrchestrationEventStore.ts";

export class InvalidReplayCursor extends Schema.TaggedError<InvalidReplayCursor>()(
  "InvalidReplayCursor",
  { message: Schema.String },
) {}

export type ReplayPage =
  | { readonly state: "GAP"; readonly prunedThrough: number; readonly highWater: number }
  | {
      readonly state: "OK";
      readonly events: ReadonlyArray<ApplicationStoredEvent>;
      readonly nextSequence: number;
      readonly highWater: number;
      readonly hasMore: boolean;
    };

/** The caller commits the returned cursor only with its inbox and projection. */
export const readCommittedPage = Effect.fn("semanggi.committedReplay.readPage")(function* (input: {
  readonly afterSequence: number;
  readonly limit: number;
}): Effect.fn.Return<
  ReplayPage,
  InvalidReplayCursor | import("../persistence/Errors.ts").OrchestrationEventStoreError,
  EventStore.OrchestrationEventStore
> {
  if (
    !Number.isSafeInteger(input.afterSequence) ||
    input.afterSequence < 0 ||
    !Number.isInteger(input.limit) ||
    input.limit < 1 ||
    input.limit > 500
  ) {
    return yield* new InvalidReplayCursor({ message: "Invalid replay cursor or page size." });
  }
  const source = yield* EventStore.OrchestrationEventStore;
  const latest = yield* source.latestApplicationSequence;
  const prunedBefore = yield* source.prunedThroughSequence;
  const highWater = Math.max(latest, prunedBefore);
  if (input.afterSequence < prunedBefore || input.afterSequence > highWater) {
    return { state: "GAP", prunedThrough: prunedBefore, highWater };
  }
  const events = Array.from(
    yield* source
      .readApplicationEvents({ afterSequence: input.afterSequence, throughSequence: highWater })
      .pipe(Stream.take(input.limit + 1), Stream.runCollect),
  );
  const prunedAfter = yield* source.prunedThroughSequence;
  if (input.afterSequence < prunedAfter)
    return {
      state: "GAP",
      prunedThrough: prunedAfter,
      highWater: Math.max(highWater, prunedAfter),
    };
  const hasMore = events.length > input.limit;
  const page = hasMore ? events.slice(0, input.limit) : events;
  return {
    state: "OK",
    events: page,
    nextSequence: hasMore ? page[page.length - 1]!.sequence : highWater,
    highWater,
    hasMore,
  };
});
