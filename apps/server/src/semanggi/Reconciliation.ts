import { ProjectId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";
import * as Adapter from "./NativeAdapter.ts";
import * as Events from "../persistence/OrchestrationEventStore.ts";
import * as Folders from "../project/ManagedProjectFolders.ts";
import { sessionIdForThread } from "./DiscoveryIdentity.ts";
import { UNASSIGNED_PROJECT_ID } from "./Storage.ts";
import * as Inbox from "./ReplayInbox.ts";

export const reconcilePage = Effect.fn("semanggi.reconciliation.page")(function* (
  environmentId: string,
  limit: number,
) {
  const sql = yield* SqlClient.SqlClient;
  const native = yield* Adapter.NativeAdapter;
  const events = yield* Events.OrchestrationEventStore;
  const folders = yield* Folders.ManagedProjectFolders;
  if (!environmentId.trim() || !Number.isInteger(limit) || limit < 1 || limit > 500)
    return yield* new Inbox.ReplayConflict({ message: "Invalid reconciliation page." });
  const checkpoint = yield* sql<{
    state: string;
    cursor: number;
  }>`SELECT state,cursor FROM sh_ingestion_checkpoints WHERE environment_id = ${environmentId}`;
  if (checkpoint[0]?.state !== "RECONCILING") return { state: "IDLE" as const };
  const latest = yield* events.latestApplicationSequence;
  const pruned = yield* events.prunedThroughSequence;
  if (checkpoint[0].cursor > Math.max(latest, pruned)) return { state: "SOURCE_REWOUND" as const };
  let rows = yield* sql<{
    base_sequence: number;
    after_thread_id: string;
  }>`SELECT base_sequence,after_thread_id FROM sh_reconciliation WHERE environment_id = ${environmentId}`;
  if (!rows[0] || rows[0].base_sequence < pruned) {
    const base = Math.max(latest, pruned);
    yield* sql.withTransaction(
      Effect.gen(function* () {
        yield* sql`DELETE FROM sh_reconciliation_items WHERE environment_id=${environmentId}`;
        yield* sql`INSERT INTO sh_reconciliation(environment_id,base_sequence,after_thread_id) VALUES (${environmentId},${base},'')
        ON CONFLICT(environment_id) DO UPDATE SET base_sequence=excluded.base_sequence,after_thread_id=''`;
      }),
    );
    rows = [{ base_sequence: base, after_thread_id: "" }];
  }
  const scan = rows[0]!;
  const page = yield* native.listReconciliationPage(scan.after_thread_id, limit + 1);
  const prunedAfter = yield* events.prunedThroughSequence;
  if (prunedAfter > scan.base_sequence) {
    yield* sql`DELETE FROM sh_reconciliation WHERE environment_id = ${environmentId} AND base_sequence = ${scan.base_sequence} AND after_thread_id = ${scan.after_thread_id}`;
    return { state: "RESTART" as const };
  }
  const current = page.slice(0, limit);
  const discoveries = yield* Effect.forEach(current, (row) =>
    Effect.gen(function* () {
      let mode: "work" | "code" | null = null;
      if (row.deleted_at === null && row.relationship !== "subagent" && row.side_chat !== 1)
        mode = (yield* folders.isScratchProject(ProjectId.make(row.project_id))) ? "work" : "code";
      return { row, mode };
    }),
  );
  return yield* sql.withTransaction(
    Effect.gen(function* () {
      const active = yield* sql<{
        base_sequence: number;
        after_thread_id: string;
      }>`SELECT r.base_sequence,r.after_thread_id FROM sh_reconciliation r JOIN sh_ingestion_checkpoints c ON c.environment_id=r.environment_id WHERE r.environment_id=${environmentId} AND c.state='RECONCILING'`;
      if (
        active[0]?.base_sequence !== scan.base_sequence ||
        active[0]?.after_thread_id !== scan.after_thread_id
      )
        return yield* new Inbox.ReplayConflict({ message: "Reconciliation cursor changed." });
      for (const { row, mode } of discoveries) {
        yield* sql`INSERT INTO sh_reconciliation_items(environment_id,thread_id,mode,session_id)
          VALUES (${environmentId},${row.thread_id},${mode},${sessionIdForThread(environmentId, row.thread_id)})
          ON CONFLICT(environment_id,thread_id) DO UPDATE SET mode=excluded.mode,session_id=excluded.session_id`;
      }
      if (page.length > limit) {
        yield* sql`UPDATE sh_reconciliation SET after_thread_id=${current[current.length - 1]!.thread_id} WHERE environment_id=${environmentId}`;
        return { state: "SCANNING" as const, count: current.length };
      }
      const requestId = `reconcile:${scan.base_sequence}`;
      yield* sql`DELETE FROM sh_discovery_pending WHERE environment_id=${environmentId} AND EXISTS
        (SELECT 1 FROM sh_reconciliation_items i WHERE i.environment_id=sh_discovery_pending.environment_id AND i.thread_id=sh_discovery_pending.thread_id AND i.mode IS NULL)`;
      yield* sql`UPDATE sh_reconciliation_items AS i SET will_create=1 WHERE i.environment_id=${environmentId}
        AND i.mode IS NOT NULL
        AND NOT EXISTS (SELECT 1 FROM sh_thread_bindings b WHERE b.environment_id=i.environment_id AND b.t3_thread_id=i.thread_id)
        AND NOT EXISTS (SELECT 1 FROM sh_thread_create_operations op WHERE op.environment_id=i.environment_id AND op.thread_id=i.thread_id AND op.state != 'REJECTED')`;
      yield* sql`INSERT OR IGNORE INTO sh_discovery_pending(environment_id,thread_id,mode,request_id)
        SELECT i.environment_id,i.thread_id,i.mode,${requestId} FROM sh_reconciliation_items i
        WHERE i.environment_id=${environmentId} AND i.mode IS NOT NULL AND i.will_create=0
          AND NOT EXISTS (SELECT 1 FROM sh_thread_bindings b WHERE b.environment_id=i.environment_id AND b.t3_thread_id=i.thread_id)
          AND EXISTS (SELECT 1 FROM sh_thread_create_operations op WHERE op.environment_id=i.environment_id AND op.thread_id=i.thread_id AND op.state != 'REJECTED')`;
      yield* sql`INSERT INTO sh_sessions(id,project_id,section_id,mode)
        SELECT session_id,${UNASSIGNED_PROJECT_ID},NULL,mode FROM sh_reconciliation_items WHERE environment_id=${environmentId} AND will_create=1`;
      yield* sql`INSERT INTO sh_thread_bindings(session_id,environment_id,t3_thread_id,binding_state)
        SELECT session_id,environment_id,thread_id,'bound' FROM sh_reconciliation_items WHERE environment_id=${environmentId} AND will_create=1`;
      yield* sql`INSERT INTO sh_audit(actor_id,environment_id,request_id,event_type,entity_type,entity_id,revision)
        SELECT 'system:ingestion',environment_id,${requestId},'session.discover','session',session_id,1
        FROM sh_reconciliation_items WHERE environment_id=${environmentId} AND will_create=1`;
      yield* sql`INSERT INTO sh_audit(actor_id,environment_id,request_id,event_type,entity_type,entity_id,revision)
        SELECT 'system:ingestion',environment_id,${requestId},'binding.create','binding',session_id,1
        FROM sh_reconciliation_items WHERE environment_id=${environmentId} AND will_create=1`;
      yield* sql`DELETE FROM sh_discovery_pending WHERE environment_id=${environmentId} AND EXISTS
        (SELECT 1 FROM sh_thread_bindings b WHERE b.environment_id=sh_discovery_pending.environment_id AND b.t3_thread_id=sh_discovery_pending.thread_id)`;
      if (scan.base_sequence > 0)
        yield* sql`INSERT INTO sh_thread_refresh_queue(environment_id,thread_id,source_sequence,force_snapshot)
        SELECT environment_id,thread_id,${scan.base_sequence},1 FROM sh_reconciliation_items WHERE environment_id=${environmentId}
        ON CONFLICT(environment_id,thread_id) DO UPDATE SET
          force_snapshot=CASE WHEN excluded.source_sequence >= source_sequence THEN 1 ELSE force_snapshot END,
          source_sequence=MAX(source_sequence,excluded.source_sequence),attempts=0,retry_at_ms=0`;
      yield* sql`UPDATE sh_ingestion_checkpoints SET cursor=${scan.base_sequence},high_water=MAX(high_water,${scan.base_sequence}),state='CATCHING_UP' WHERE environment_id=${environmentId}`;
      yield* sql`DELETE FROM sh_reconciliation_items WHERE environment_id=${environmentId}`;
      yield* sql`DELETE FROM sh_reconciliation WHERE environment_id=${environmentId}`;
      return { state: "COMPLETE" as const, cursor: scan.base_sequence, count: current.length };
    }),
  );
});
