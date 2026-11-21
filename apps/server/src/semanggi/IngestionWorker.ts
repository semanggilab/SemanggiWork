import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schedule from "effect/Schedule";
import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import * as ServerActivation from "../serverActivation.ts";
import * as Integration from "./NativeIntegrationService.ts";

export const tick = Effect.fn("semanggi.ingestionWorker.tick")(function* () {
  const integration = yield* Integration.NativeIntegrationService;
  const environment = yield* ServerEnvironment.ServerEnvironment;
  const environmentId = yield* environment.getEnvironmentId;
  const page = yield* integration.ingestNextPage({ environmentId, limit: 100 });
  if (page.state === "RECONCILING") {
    const reconciliation = yield* integration.reconcilePage(environmentId, 100);
    return { state: "RECONCILING" as const, reconciliation: reconciliation.state };
  }
  if (!page.hasMore) yield* integration.resolvePendingDiscovery(environmentId);
  const refresh = yield* integration.refreshOne(environmentId);
  const live = page.hasMore ? false : yield* integration.markLiveIfIdle(environmentId);
  return {
    state: live ? ("LIVE" as const) : ("CATCHING_UP" as const),
    hasMore: page.hasMore,
    refresh: refresh.state,
  };
});

export const layer = Layer.effectDiscard(
  ServerActivation.forkParked(
    tick().pipe(
      Effect.catch(() => Effect.logWarning("Semanggi ingestion could not finish; it will retry.")),
      Effect.repeat(Schedule.spaced("5 seconds")),
      Effect.asVoid,
    ),
  ),
);
