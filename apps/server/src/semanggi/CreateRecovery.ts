import * as Effect from "effect/Effect";
import * as Saga from "./CreateSagaStore.ts";
import * as Coordinator from "./CreateCoordinator.ts";

export const recoverNext = Effect.fn("semanggi.createRecovery.recoverNext")(function* (
  environmentId: string,
) {
  const pending = yield* Saga.nextRecovery(environmentId);
  if (!pending) return { state: "IDLE" as const };
  const operation = yield* Coordinator.createOrRecover(pending);
  return { state: operation.state, sessionId: operation.session_id };
});
