import * as Schema from "effect/Schema";
import { PositiveInt } from "./baseSchemas.ts";

// Match the published JSON Schema: identifiers are non-empty and are not transformed.
const NonEmptyString = Schema.String.check(Schema.isMinLength(1));

export const SemanggiMode = Schema.Literals(["work", "code"]);
export type SemanggiMode = typeof SemanggiMode.Type;

export const SemanggiScope = Schema.Struct({
  environmentId: NonEmptyString,
  projectId: NonEmptyString,
});

export const SemanggiSessionMove = Schema.Struct({
  requestId: NonEmptyString,
  idempotencyKey: NonEmptyString,
  scope: SemanggiScope,
  expectedRevision: PositiveInt,
  operation: Schema.Struct({
    type: Schema.Literal("session.move"),
    sessionId: NonEmptyString,
    projectId: NonEmptyString,
    sectionId: Schema.NullOr(NonEmptyString),
  }),
});
export type SemanggiSessionMove = typeof SemanggiSessionMove.Type;

/** Reject unknown properties at every level, as required by mutation.schema.json. */
export const decodeSemanggiSessionMove = Schema.decodeUnknownSync(SemanggiSessionMove, {
  onExcessProperty: "error",
});

export const SemanggiCapabilities = Schema.Struct({
  protocolVersion: Schema.Literal(1),
  sessions: Schema.Boolean,
  ingestion: Schema.Boolean,
  search: Schema.Boolean,
});

export const SemanggiProject = Schema.Struct({
  id: NonEmptyString,
  name: Schema.String,
  revision: PositiveInt,
});
export const SemanggiSection = Schema.Struct({
  id: NonEmptyString,
  parentId: Schema.NullOr(NonEmptyString),
  name: Schema.String,
  depth: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 4 })),
  revision: PositiveInt,
});
export const SemanggiWorkspaceLink = Schema.Struct({
  projectId: NonEmptyString,
  t3ProjectId: NonEmptyString,
  isDefault: Schema.Boolean,
});
export const SemanggiCatalog = Schema.Struct({
  projects: Schema.Array(SemanggiProject),
  workspaceLinks: Schema.Array(SemanggiWorkspaceLink),
  sectionsByProject: Schema.Array(
    Schema.Struct({ projectId: NonEmptyString, sections: Schema.Array(SemanggiSection) }),
  ),
  sessions: Schema.Array(
    Schema.Struct({
      sessionId: NonEmptyString,
      projectId: NonEmptyString,
      sectionId: Schema.NullOr(NonEmptyString),
      mode: SemanggiMode,
      revision: PositiveInt,
      threadId: NonEmptyString,
    }),
  ),
});

export const SemanggiDomainMutation = Schema.Union([
  Schema.Struct({
    requestId: NonEmptyString,
    type: Schema.Literal("project.create"),
    id: NonEmptyString,
    name: NonEmptyString,
  }),
  Schema.Struct({
    requestId: NonEmptyString,
    type: Schema.Literal("project.rename"),
    id: NonEmptyString,
    name: NonEmptyString,
    expectedRevision: PositiveInt,
  }),
  Schema.Struct({
    requestId: NonEmptyString,
    type: Schema.Literal("project.archive"),
    id: NonEmptyString,
    archived: Schema.Boolean,
    expectedRevision: PositiveInt,
  }),
  Schema.Struct({
    requestId: NonEmptyString,
    type: Schema.Literal("section.create"),
    id: NonEmptyString,
    projectId: NonEmptyString,
    parentId: Schema.NullOr(NonEmptyString),
    name: NonEmptyString,
  }),
  Schema.Struct({
    requestId: NonEmptyString,
    type: Schema.Literal("section.rename"),
    id: NonEmptyString,
    name: NonEmptyString,
    expectedRevision: PositiveInt,
  }),
  Schema.Struct({
    requestId: NonEmptyString,
    type: Schema.Literal("section.move"),
    id: NonEmptyString,
    parentId: Schema.NullOr(NonEmptyString),
    expectedRevision: PositiveInt,
  }),
  Schema.Struct({
    requestId: NonEmptyString,
    type: Schema.Literal("section.delete"),
    id: NonEmptyString,
    expectedRevision: PositiveInt,
  }),
  Schema.Struct({
    requestId: NonEmptyString,
    type: Schema.Literal("section.reorder"),
    id: NonEmptyString,
    direction: Schema.Literals(["up", "down"]),
    expectedRevision: PositiveInt,
  }),
  Schema.Struct({
    requestId: NonEmptyString,
    type: Schema.Literal("workspace.link"),
    projectId: NonEmptyString,
    t3ProjectId: NonEmptyString,
    isDefault: Schema.Boolean,
  }),
  Schema.Struct({
    requestId: NonEmptyString,
    type: Schema.Literal("workspace.unlink"),
    projectId: NonEmptyString,
    t3ProjectId: NonEmptyString,
  }),
]);
export type SemanggiDomainMutation = typeof SemanggiDomainMutation.Type;
export const SemanggiDomainMutationResult = Schema.Struct({ revision: PositiveInt });

export class SemanggiRequestError extends Schema.TaggedError<SemanggiRequestError>()(
  "SemanggiRequestError",
  { reason: Schema.Literals(["invalid_request", "scope_mismatch", "conflict", "storage_error"]) },
) {}

export const SemanggiSessionMoveResult = Schema.Struct({ revision: PositiveInt });

export const SemanggiCreateThreadInput = Schema.Struct({
  requestId: NonEmptyString,
  idempotencyKey: NonEmptyString,
  scope: SemanggiScope,
  sessionId: NonEmptyString,
  commandId: NonEmptyString,
  threadId: NonEmptyString,
  sectionId: Schema.NullOr(NonEmptyString),
  mode: SemanggiMode,
  nativeCommandJson: NonEmptyString.check(Schema.isMaxLength(131072)),
});
export type SemanggiCreateThreadInput = typeof SemanggiCreateThreadInput.Type;

export const SemanggiCreateStatusInput = Schema.Struct({
  scope: SemanggiScope,
  idempotencyKey: NonEmptyString,
});

export const SemanggiCreateStatus = Schema.Struct({
  state: Schema.Literals([
    "PREPARED",
    "DISPATCHED",
    "NATIVE_CONFIRMED",
    "BOUND",
    "REJECTED",
    "NEEDS_RECONCILIATION",
  ]),
  sessionId: NonEmptyString,
  threadId: NonEmptyString,
  revision: PositiveInt,
});
export type SemanggiCreateStatus = typeof SemanggiCreateStatus.Type;

export const SemanggiPendingCreatesInput = Schema.Struct({
  scope: SemanggiScope,
  limit: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 100 })),
  afterKey: Schema.optionalKey(NonEmptyString),
});
export const SemanggiPendingCreatesResult = Schema.Struct({
  operations: Schema.Array(
    Schema.Struct({ idempotencyKey: NonEmptyString, status: SemanggiCreateStatus }),
  ),
  hasMore: Schema.Boolean,
});
export type SemanggiPendingCreatesResult = typeof SemanggiPendingCreatesResult.Type;

export const SemanggiSearchInput = Schema.Struct({
  environmentId: NonEmptyString,
  projectId: Schema.NullOr(NonEmptyString),
  query: NonEmptyString.check(Schema.isMaxLength(200)),
  limit: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 50 })),
});

export const SemanggiSearchResult = Schema.Struct({
  matches: Schema.Array(
    Schema.Struct({
      sessionId: NonEmptyString,
      threadId: NonEmptyString,
      projectId: NonEmptyString,
      mode: SemanggiMode,
      title: Schema.String,
      source: Schema.Literals(["title", "user", "assistant"]),
      messageId: Schema.optionalKey(NonEmptyString),
      freshness: Schema.Literals(["fresh", "pending", "unknown"]),
    }),
  ),
  coverage: Schema.Literal("partial"),
  syncState: Schema.Literals(["UNINITIALIZED", "CATCHING_UP", "LIVE", "RECONCILING"]),
});
export type SemanggiSearchResult = typeof SemanggiSearchResult.Type;

export const SemanggiOpenSourceInput = Schema.Struct({
  environmentId: NonEmptyString,
  projectId: Schema.NullOr(NonEmptyString),
  sessionId: NonEmptyString,
  messageId: Schema.optionalKey(NonEmptyString),
});
export const SemanggiOpenSourceResult = Schema.Struct({
  sessionId: NonEmptyString,
  threadId: NonEmptyString,
  projectId: NonEmptyString,
  mode: SemanggiMode,
  sourceSequence: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  messageId: Schema.optionalKey(NonEmptyString),
});
export type SemanggiOpenSourceResult = typeof SemanggiOpenSourceResult.Type;
