import { describe, expect, it } from "vite-plus/test";
import { decodeSemanggiSessionMove } from "./semanggi.ts";
import { Ajv2020 } from "ajv/dist/2020.js";
import mutationSchema from "./semanggi-mutation.schema.json" with { type: "json" };

const valid = {
  requestId: "request",
  idempotencyKey: "key",
  scope: { environmentId: "env", projectId: "project" },
  expectedRevision: 1,
  operation: { type: "session.move", sessionId: "session", projectId: "project", sectionId: null },
};

describe("Semanggi session move contract", () => {
  it("accepts a valid JSON contract and preserves identifiers", () => {
    expect(decodeSemanggiSessionMove(valid)).toEqual(valid);
    expect(decodeSemanggiSessionMove({ ...valid, requestId: " " }).requestId).toBe(" ");
  });

  it("rejects unknown properties, malformed fields and nonpositive revisions", () => {
    for (const input of [
      { ...valid, actor: "forged" },
      { ...valid, scope: { ...valid.scope, extra: true } },
      { ...valid, operation: { ...valid.operation, extra: true } },
      { ...valid, idempotencyKey: "" },
      { ...valid, expectedRevision: 0 },
      { ...valid, expectedRevision: 1.5 },
      { ...valid, operation: { ...valid.operation, sectionId: "" } },
      { ...valid, operation: { ...valid.operation, type: "session.delete" } },
    ])
      expect(() => decodeSemanggiSessionMove(input)).toThrow();
  });

  it("agrees with the published JSON Schema for valid and invalid messages", () => {
    const validate = new Ajv2020({ strict: false }).compile(mutationSchema);
    const cases: unknown[] = [
      valid,
      { ...valid, requestId: " " },
      { ...valid, operation: { ...valid.operation, sectionId: "section" } },
      null,
      {},
      { ...valid, actor: "forged" },
      { ...valid, scope: { ...valid.scope, extra: true } },
      { ...valid, operation: { ...valid.operation, extra: true } },
      { ...valid, requestId: "" },
      { ...valid, expectedRevision: 0 },
      { ...valid, expectedRevision: 1.5 },
      { ...valid, operation: { ...valid.operation, sectionId: "" } },
      { ...valid, operation: { ...valid.operation, type: "session.delete" } },
    ];
    for (const input of cases) {
      let accepted = true;
      try {
        decodeSemanggiSessionMove(input);
      } catch {
        accepted = false;
      }
      expect(accepted, JSON.stringify(input)).toBe(validate(input));
    }
  });
});
