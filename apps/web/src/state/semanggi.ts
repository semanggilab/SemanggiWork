import { WS_METHODS } from "@t3tools/contracts";
import {
  createEnvironmentRpcCommand,
  createEnvironmentRpcQueryAtomFamily,
} from "@t3tools/client-runtime/state/runtime";
import { connectionAtomRuntime } from "../connection/runtime";

export const semanggiCatalog = createEnvironmentRpcQueryAtomFamily(connectionAtomRuntime, {
  label: "semanggi:catalog",
  tag: WS_METHODS.semanggiCatalog,
  staleTimeMs: 10_000,
  idleTtlMs: 5 * 60_000,
});

export const semanggiMutate = createEnvironmentRpcCommand(connectionAtomRuntime, {
  label: "semanggi:mutate",
  tag: WS_METHODS.semanggiMutate,
});
