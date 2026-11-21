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

export const semanggiSessionMove = createEnvironmentRpcCommand(connectionAtomRuntime, {
  label: "semanggi:session-move",
  tag: WS_METHODS.semanggiSessionMove,
});

// Search and open-source are client-guarded RPCs, so they run as commands rather than query atoms.
export const semanggiSearch = createEnvironmentRpcCommand(connectionAtomRuntime, {
  label: "semanggi:search",
  tag: WS_METHODS.semanggiSearch,
});

export const semanggiOpenSource = createEnvironmentRpcCommand(connectionAtomRuntime, {
  label: "semanggi:open-source",
  tag: WS_METHODS.semanggiOpenSource,
});
