// @effect-diagnostics nodeBuiltinImport:off
import * as NodeCrypto from "node:crypto";

export const sessionIdForThread = (environmentId: string, threadId: string) =>
  `native:${NodeCrypto.createHash("sha256")
    .update(`${environmentId.length}:${environmentId}${threadId.length}:${threadId}`)
    .digest("hex")}`;
