import { CommandId, OrchestrationV2Command } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import * as NativeAdapter from "./NativeAdapter.ts";
import * as Saga from "./CreateSagaStore.ts";

const decodeCommand = Schema.decodeUnknownEffect(Schema.fromJsonString(OrchestrationV2Command), {
  onExcessProperty: "error",
});

/** Never resend a command after dispatch may have happened; resolve its durable receipt first. */
export const createOrRecover = Effect.fn("semanggi.createCoordinator.createOrRecover")(function* (
  input: Saga.PrepareCreate,
) {
  const native = yield* NativeAdapter.NativeAdapter;
  const command = yield* decodeCommand(input.requestPayload).pipe(
    Effect.mapError(
      () => new Saga.CreateSagaConflict({ message: "Invalid native create payload." }),
    ),
  );
  if (
    command.type !== "thread.create" ||
    command.commandId !== input.commandId ||
    command.threadId !== input.threadId
  ) {
    return yield* new Saga.CreateSagaConflict({
      message: "Native command identity does not match create intent.",
    });
  }
  let operation = yield* Saga.prepare(input);
  if (operation.state === "BOUND" || operation.state === "REJECTED") return operation;
  if (operation.state === "PREPARED") {
    const preflight = yield* native
      .lookupCommand(CommandId.make(operation.command_id))
      .pipe(Effect.result);
    if (Result.isFailure(preflight)) return operation;
    if (Option.isSome(preflight.success)) {
      return yield* Saga.transition({
        environmentId: input.environmentId,
        key: input.idempotencyKey,
        expected: "PREPARED",
        next: "REJECTED",
      });
    }
    operation = yield* Saga.transition({
      environmentId: input.environmentId,
      key: input.idempotencyKey,
      expected: "PREPARED",
      next: "DISPATCHED",
    });
    // A failure or interruption can follow a native commit, so the receipt decides the outcome.
    yield* native.dispatch(command).pipe(Effect.result);
  }
  if (operation.state === "DISPATCHED" || operation.state === "NEEDS_RECONCILIATION") {
    const receiptResult = yield* native
      .lookupCommand(CommandId.make(operation.command_id))
      .pipe(Effect.result);
    if (Result.isFailure(receiptResult) || Option.isNone(receiptResult.success)) {
      if (operation.state === "DISPATCHED") {
        operation = yield* Saga.transition({
          environmentId: input.environmentId,
          key: input.idempotencyKey,
          expected: "DISPATCHED",
          next: "NEEDS_RECONCILIATION",
        });
      }
      return operation;
    }
    const receipt = receiptResult.success.value;
    if (
      receipt.commandId !== operation.command_id ||
      receipt.threadId !== operation.thread_id ||
      receipt.commandType !== "thread.create"
    ) {
      if (operation.state === "DISPATCHED") {
        operation = yield* Saga.transition({
          environmentId: input.environmentId,
          key: input.idempotencyKey,
          expected: "DISPATCHED",
          next: "NEEDS_RECONCILIATION",
        });
      }
      return operation;
    }
    operation = yield* Saga.transition({
      environmentId: input.environmentId,
      key: input.idempotencyKey,
      expected: operation.state,
      next: receipt.status === "accepted" ? "NATIVE_CONFIRMED" : "REJECTED",
      ...(receipt.status === "accepted" ? { nativeSequence: receipt.resultSequence } : {}),
    });
  }
  return operation.state === "NATIVE_CONFIRMED"
    ? yield* Saga.bindConfirmed(input.environmentId, input.idempotencyKey)
    : operation;
});
