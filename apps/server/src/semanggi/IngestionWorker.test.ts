import { assert, it } from "@effect/vitest";
import { EnvironmentId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import * as Integration from "./NativeIntegrationService.ts";
import * as Worker from "./IngestionWorker.ts";

it.effect("refreshes after a committed page but stops on a replay gap", () => {
  let gap = false;
  let refreshes = 0;
  const integration = {
    resolvePendingDiscovery: () => Effect.succeed(false),
    markLiveIfIdle: () => Effect.succeed(false),
    reconcilePage: () => Effect.succeed({ state: "SCANNING" as const, count: 1 }),
    ingestNextPage: (input: { environmentId: string; limit: number }) =>
      Effect.sync(() => {
        assert.equal(input.environmentId, "env");
        assert.equal(input.limit, 100);
        return gap
          ? { state: "RECONCILING" as const, cursor: 1 }
          : { state: "CATCHING_UP" as const, cursor: 1, hasMore: false };
      }),
    refreshOne: () =>
      Effect.sync(() => {
        refreshes++;
        return { state: "EMPTY" as const };
      }),
  } as unknown as Integration.NativeIntegrationService["Service"];
  const environment = {
    getEnvironmentId: Effect.succeed(EnvironmentId.make("env")),
  } as ServerEnvironment.ServerEnvironment["Service"];
  return Effect.gen(function* () {
    assert.deepEqual(yield* Worker.tick(), {
      state: "CATCHING_UP",
      hasMore: false,
      refresh: "EMPTY",
    });
    gap = true;
    assert.deepEqual(yield* Worker.tick(), { state: "RECONCILING", reconciliation: "SCANNING" });
    assert.equal(refreshes, 1);
  }).pipe(
    Effect.provide(
      Layer.mergeAll(
        Layer.succeed(Integration.NativeIntegrationService, integration),
        Layer.succeed(ServerEnvironment.ServerEnvironment, environment),
      ),
    ),
  );
});
