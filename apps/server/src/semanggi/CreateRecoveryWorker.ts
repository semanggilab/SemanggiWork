import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schedule from "effect/Schedule";
import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import * as ServerActivation from "../serverActivation.ts";
import * as Integration from "./NativeIntegrationService.ts";

export const tick = Effect.fn("semanggi.createRecoveryWorker.tick")(function* () {
  const integration = yield* Integration.NativeIntegrationService;
  const environment = yield* ServerEnvironment.ServerEnvironment;
  return yield* integration.recoverNextCreate(yield* environment.getEnvironmentId);
});

export const layer = Layer.effectDiscard(
  ServerActivation.forkParked(
    tick().pipe(
      Effect.catch(() =>
        Effect.logWarning("Semanggi create recovery could not finish; it will retry."),
      ),
      Effect.repeat(Schedule.spaced("15 seconds")),
      Effect.asVoid,
    ),
  ),
);
