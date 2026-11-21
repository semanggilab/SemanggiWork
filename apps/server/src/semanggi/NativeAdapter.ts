import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/sql/SqlClient";
import type * as SqlError from "effect/sql/SqlError";
import * as Orchestrator from "../orchestration-v2/Orchestrator.ts";
import * as CommandReceipts from "../orchestration-v2/CommandReceiptStore.ts";
import * as ThreadSearch from "../orchestration-v2/ThreadSearch.ts";

export class NativeAdapter extends Context.Service<
  NativeAdapter,
  {
    readonly dispatch: Orchestrator.OrchestratorV2["Service"]["dispatch"];
    readonly lookupCommand: CommandReceipts.CommandReceiptStoreV2["Service"]["getByCommandId"];
    readonly readThread: Orchestrator.OrchestratorV2["Service"]["getThreadSnapshot"];
    readonly readHistory: Orchestrator.OrchestratorV2["Service"]["getThreadHistoryPage"];
    readonly searchThreads: ThreadSearch.ThreadSearch["Service"]["searchThreads"];
    readonly listReconciliationPage: (
      after: string,
      limit: number,
    ) => Effect.Effect<
      ReadonlyArray<{
        readonly thread_id: string;
        readonly project_id: string;
        readonly deleted_at: string | null;
        readonly relationship: string | null;
        readonly side_chat: number | null;
      }>,
      SqlError.SqlError
    >;
    readonly capabilities: {
      readonly create: true;
      readonly commandReceiptLookup: true;
      readonly historyPagination: true;
      readonly lexicalSearch: true;
      readonly durableBridge: false;
    };
  }
>()("t3/semanggi/NativeAdapter") {}

const make = Effect.gen(function* () {
  const orchestrator = yield* Orchestrator.OrchestratorV2;
  const receipts = yield* CommandReceipts.CommandReceiptStoreV2;
  const search = yield* ThreadSearch.ThreadSearch;
  const sql = yield* SqlClient.SqlClient;
  return NativeAdapter.of({
    dispatch: orchestrator.dispatch,
    lookupCommand: receipts.getByCommandId,
    readThread: orchestrator.getThreadSnapshot,
    readHistory: orchestrator.getThreadHistoryPage,
    searchThreads: search.searchThreads,
    listReconciliationPage: (after, limit) => sql`
      SELECT thread_id, project_id, deleted_at,
        json_extract(payload_json, '$.lineage.relationshipToParent') AS relationship,
        json_extract(payload_json, '$.lineage.sideChat') AS side_chat
      FROM orchestration_v2_projection_threads WHERE thread_id > ${after}
      ORDER BY thread_id LIMIT ${limit}`,
    capabilities: {
      create: true,
      commandReceiptLookup: true,
      historyPagination: true,
      lexicalSearch: true,
      durableBridge: false,
    },
  });
});

export const layer = Layer.effect(NativeAdapter, make);
