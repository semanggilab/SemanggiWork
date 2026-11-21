// @effect-diagnostics nodeBuiltinImport:off
import * as NodeCrypto from "node:crypto";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as Clock from "effect/Clock";
import * as Repository from "./Repository.ts";

export class CreateSagaConflict extends Schema.TaggedError<CreateSagaConflict>()(
  "CreateSagaConflict",
  { message: Schema.String },
) {}

export type CreateState =
  | "PREPARED"
  | "DISPATCHED"
  | "NATIVE_CONFIRMED"
  | "BOUND"
  | "REJECTED"
  | "NEEDS_RECONCILIATION";
export interface CreateOperation {
  readonly environment_id: string;
  readonly idempotency_key: string;
  readonly actor_id: string;
  readonly request_id: string;
  readonly request_hash: string;
  readonly request_payload: string;
  readonly session_id: string;
  readonly command_id: string;
  readonly thread_id: string;
  readonly state: CreateState;
  readonly native_sequence: number | null;
  readonly revision: number;
  readonly recovery_order: number;
  readonly created_at_ms: number;
}

export interface PrepareCreate {
  readonly environmentId: string;
  readonly idempotencyKey: string;
  readonly actorId: string;
  readonly requestId: string;
  readonly sessionId: string;
  readonly commandId: string;
  readonly threadId: string;
  readonly projectId: string;
  readonly sectionId: string | null;
  readonly mode: "work" | "code";
  readonly requestPayload: string;
}

const identity = (input: PrepareCreate) =>
  NodeCrypto.createHash("sha256")
    .update(
      [
        input.sessionId,
        input.commandId,
        input.threadId,
        input.projectId,
        input.sectionId,
        input.mode,
        input.requestPayload,
      ]
        .map((value) => (value === null ? "-1:" : `${value.length}:${value}`))
        .join(""),
    )
    .digest("hex");

const get = Effect.fn("semanggi.createSaga.get")(function* (
  environmentId: string,
  key: string,
) {
  const sql = yield* SqlClient.SqlClient;
  const rows = yield* sql<CreateOperation>`SELECT * FROM sh_thread_create_operations
    WHERE environment_id = ${environmentId} AND idempotency_key = ${key}`;
  return rows[0] ?? null;
});

export const prepare = Effect.fn("semanggi.createSaga.prepare")(function* (input: PrepareCreate) {
  const sql = yield* SqlClient.SqlClient;
  if (
    [
      input.environmentId,
      input.idempotencyKey,
      input.actorId,
      input.requestId,
      input.sessionId,
      input.commandId,
      input.threadId,
      input.projectId,
      input.requestPayload,
    ].some((value) => !value.trim())
  ) {
    return yield* new CreateSagaConflict({
      message: "Create operation requires non-empty identity and payload.",
    });
  }
  const hash = identity(input);
  const createdAt = yield* Clock.currentTimeMillis;
  return yield* sql.withTransaction(
    Effect.gen(function* () {
      const existing = yield* get(input.environmentId, input.idempotencyKey);
      if (existing) {
        if (existing.actor_id !== input.actorId || existing.request_hash !== hash) {
          return yield* new CreateSagaConflict({
            message: "Idempotency key belongs to another request.",
          });
        }
        return existing;
      }
      yield* Repository.createSession({
        id: input.sessionId,
        projectId: input.projectId,
        sectionId: input.sectionId,
        mode: input.mode,
      });
      yield* sql`INSERT INTO sh_thread_create_operations(environment_id,idempotency_key,actor_id,request_id,request_hash,request_payload,session_id,command_id,thread_id,state,created_at_ms)
      VALUES (${input.environmentId},${input.idempotencyKey},${input.actorId},${input.requestId},${hash},${input.requestPayload},${input.sessionId},${input.commandId},${input.threadId},'PREPARED',${createdAt})`;
      yield* sql`INSERT INTO sh_audit(actor_id,environment_id,request_id,event_type,entity_type,entity_id,revision)
      VALUES (${input.actorId},${input.environmentId},${input.requestId},'session.create.prepared','session',${input.sessionId},1)`;
      const operation = yield* get(input.environmentId, input.idempotencyKey);
      if (!operation)
        return yield* new CreateSagaConflict({ message: "Create operation was not stored." });
      return operation;
    }),
  );
});

export const transition = Effect.fn("semanggi.createSaga.transition")(function* (input: {
  readonly environmentId: string;
  readonly key: string;
  readonly expected: CreateState;
  readonly next: "DISPATCHED" | "NATIVE_CONFIRMED" | "REJECTED" | "NEEDS_RECONCILIATION";
  readonly nativeSequence?: number;
}) {
  const sql = yield* SqlClient.SqlClient;
  const legal =
    (input.expected === "PREPARED" && (input.next === "DISPATCHED" || input.next === "REJECTED")) ||
    (input.expected === "DISPATCHED" &&
      ["NATIVE_CONFIRMED", "REJECTED", "NEEDS_RECONCILIATION"].includes(input.next)) ||
    (input.expected === "NEEDS_RECONCILIATION" &&
      ["NATIVE_CONFIRMED", "REJECTED"].includes(input.next));
  if (
    !legal ||
    (input.next === "NATIVE_CONFIRMED" &&
      (!Number.isSafeInteger(input.nativeSequence) || input.nativeSequence! < 0))
  ) {
    return yield* new CreateSagaConflict({ message: "Invalid create operation transition." });
  }
  const rows =
    yield* sql<CreateOperation>`UPDATE sh_thread_create_operations SET state = ${input.next}, native_sequence = ${input.nativeSequence ?? null}, revision = revision + 1
    WHERE environment_id = ${input.environmentId} AND idempotency_key = ${input.key} AND state = ${input.expected} RETURNING *`;
  if (!rows[0])
    return yield* new CreateSagaConflict({ message: "Create operation state changed." });
  return rows[0];
});

export const bindConfirmed = Effect.fn("semanggi.createSaga.bindConfirmed")(function* (
  environmentId: string,
  key: string,
) {
  const sql = yield* SqlClient.SqlClient;
  return yield* sql.withTransaction(
    Effect.gen(function* () {
      const operation = yield* get(environmentId, key);
      if (!operation)
        return yield* new CreateSagaConflict({ message: "Create operation was not found." });
      if (operation.state === "BOUND") return operation;
      if (operation.state !== "NATIVE_CONFIRMED")
        return yield* new CreateSagaConflict({ message: "Native creation is not confirmed." });
      const created = yield* Repository.bindThread({
        sessionId: operation.session_id,
        environmentId,
        threadId: operation.thread_id,
      });
      if (created) {
        yield* sql`INSERT INTO sh_audit(actor_id,environment_id,request_id,event_type,entity_type,entity_id,revision)
      VALUES (${operation.actor_id},${environmentId},${operation.request_id},'binding.create','binding',${operation.session_id},1)`;
      }
      const rows =
        yield* sql<CreateOperation>`UPDATE sh_thread_create_operations SET state = 'BOUND', revision = revision + 1
      WHERE environment_id = ${environmentId} AND idempotency_key = ${key} AND state = 'NATIVE_CONFIRMED' RETURNING *`;
      if (!rows[0])
        return yield* new CreateSagaConflict({ message: "Create operation state changed." });
      return rows[0];
    }),
  );
});

/** Rotate durable pending operations so an unresolved receipt cannot starve later work. */
export const nextRecovery = Effect.fn("semanggi.createSaga.nextRecovery")(function* (
  environmentId: string,
) {
  const sql = yield* SqlClient.SqlClient;
  if (!environmentId.trim())
    return yield* new CreateSagaConflict({ message: "Environment is required." });
  const eligibleBefore = (yield* Clock.currentTimeMillis) - 30_000;
  return yield* sql.withTransaction(
    Effect.gen(function* () {
      const rows =
        yield* sql<CreateOperation>`UPDATE sh_thread_create_operations SET recovery_order = (
      SELECT COALESCE(MAX(recovery_order), 0) + 1 FROM sh_thread_create_operations WHERE environment_id = ${environmentId}
    ) WHERE rowid = (
      SELECT rowid FROM sh_thread_create_operations WHERE environment_id = ${environmentId}
        AND state IN ('PREPARED', 'DISPATCHED', 'NATIVE_CONFIRMED', 'NEEDS_RECONCILIATION')
        AND created_at_ms <= ${eligibleBefore}
      ORDER BY recovery_order, rowid LIMIT 1
    ) RETURNING *`;
      const operation = rows[0];
      if (!operation) return null;
      const sessions = yield* sql<{
        project_id: string;
        section_id: string | null;
        mode: "work" | "code";
      }>`
      SELECT project_id, section_id, mode FROM sh_sessions WHERE id = ${operation.session_id}`;
      const session = sessions[0];
      if (!session)
        return yield* new CreateSagaConflict({ message: "Pending session is missing." });
      return {
        environmentId,
        idempotencyKey: operation.idempotency_key,
        actorId: operation.actor_id,
        requestId: operation.request_id,
        sessionId: operation.session_id,
        commandId: operation.command_id,
        threadId: operation.thread_id,
        projectId: session.project_id,
        sectionId: session.section_id,
        mode: session.mode,
        requestPayload: operation.request_payload,
      } satisfies PrepareCreate;
    }),
  );
});
