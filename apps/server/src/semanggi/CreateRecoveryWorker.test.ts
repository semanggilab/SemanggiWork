import { assert, it } from "@effect/vitest";
import { EnvironmentId } from "@t3tools/contracts";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import * as ServerActivation from "../serverActivation.ts";
import * as Integration from "./NativeIntegrationService.ts";
import * as Worker from "./CreateRecoveryWorker.ts";

it.effect("parks create recovery until the server activates", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const activated = yield* Deferred.make<void>();
      const recovered = yield* Deferred.make<string>();
      let calls = 0;
      const integration = {
        recoverNextCreate: (environmentId: string) =>
          Effect.sync(() => {
            calls++;
          }).pipe(
            Effect.andThen(Deferred.succeed(recovered, environmentId)),
            Effect.as({ state: "IDLE" as const }),
          ),
      } as unknown as Integration.NativeIntegrationService["Service"];
      const environment = {
        getEnvironmentId: Effect.succeed(EnvironmentId.make("local")),
      } as ServerEnvironment.ServerEnvironment["Service"];
      yield* Effect.gen(function* () {
        assert.equal(calls, 0);
        yield* Deferred.succeed(activated, undefined);
        assert.equal(yield* Deferred.await(recovered), "local");
        assert.equal(calls, 1);
      }).pipe(
        Effect.provide(
          Worker.layer.pipe(
            Layer.provide(
              Layer.mergeAll(
                Layer.succeed(Integration.NativeIntegrationService, integration),
                Layer.succeed(ServerEnvironment.ServerEnvironment, environment),
                Layer.succeed(ServerActivation.ServerActivation, Deferred.await(activated)),
              ),
            ),
          ),
        ),
      );
    }),
  ),
);
