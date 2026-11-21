// @effect-diagnostics nodeBuiltinImport:off
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/sql/SqlClient";
import * as Schema from "effect/Schema";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import { normalizeName } from "./casefold.ts";
import * as NodeCrypto from "node:crypto";
import { sessionIdForThread } from "./DiscoveryIdentity.ts";

export const UNASSIGNED_PROJECT_ID = "00000000-0000-0000-0000-000000000001";

export class SemanggiMigrationError extends Schema.TaggedError<SemanggiMigrationError>()(
  "SemanggiMigrationError",
  { message: Schema.String },
) {}

const setup = Layer.effectDiscard(
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql`PRAGMA busy_timeout = 5000`;
    yield* sql`PRAGMA foreign_keys = ON`;
    yield* sql`PRAGMA journal_mode = WAL`;
    yield* sql.withTransaction(
      Effect.gen(function* () {
        yield* sql`CREATE TABLE IF NOT EXISTS sh_migrations (version INTEGER PRIMARY KEY)`;
        const versions = yield* sql<{
          version: number;
        }>`SELECT version FROM sh_migrations ORDER BY version`;
        if (versions.some(({ version }, index) => version !== index + 1 || version > 16)) {
          return yield* new SemanggiMigrationError({
            message: "Unsupported Semanggi database migration version.",
          });
        }
        yield* sql`CREATE TABLE IF NOT EXISTS sh_projects (
          id TEXT PRIMARY KEY, name TEXT NOT NULL, normalized_name TEXT NOT NULL UNIQUE,
          revision INTEGER NOT NULL DEFAULT 1 CHECK (revision > 0), archived_at TEXT
        )`;
        yield* sql`CREATE TABLE IF NOT EXISTS sh_sections (
          id TEXT PRIMARY KEY, project_id TEXT NOT NULL REFERENCES sh_projects(id),
          parent_id TEXT REFERENCES sh_sections(id), name TEXT NOT NULL,
          normalized_name TEXT NOT NULL, depth INTEGER NOT NULL CHECK (depth BETWEEN 1 AND 4),
          revision INTEGER NOT NULL DEFAULT 1 CHECK (revision > 0),
          UNIQUE(project_id, parent_id, normalized_name)
        )`;
        yield* sql`CREATE UNIQUE INDEX IF NOT EXISTS sh_root_section_names
          ON sh_sections(project_id, normalized_name) WHERE parent_id IS NULL`;
        yield* sql`CREATE TRIGGER IF NOT EXISTS sh_section_parent_insert
          BEFORE INSERT ON sh_sections WHEN
          (NEW.parent_id IS NULL AND NEW.depth != 1) OR
          (NEW.parent_id IS NOT NULL AND NOT EXISTS (
            SELECT 1 FROM sh_sections p WHERE p.id = NEW.parent_id
            AND p.project_id = NEW.project_id AND p.depth + 1 = NEW.depth
          )) BEGIN SELECT RAISE(ABORT, 'invalid section parent'); END`;
        yield* sql`CREATE TRIGGER IF NOT EXISTS sh_section_parent_update
          BEFORE UPDATE OF parent_id, project_id, depth ON sh_sections WHEN
          (NEW.parent_id IS NULL AND NEW.depth != 1) OR
          (NEW.parent_id IS NOT NULL AND NOT EXISTS (
            SELECT 1 FROM sh_sections p WHERE p.id = NEW.parent_id
            AND p.project_id = NEW.project_id AND p.depth + 1 = NEW.depth
          )) OR EXISTS (
            WITH RECURSIVE ancestors(id, parent_id) AS (
              SELECT id, parent_id FROM sh_sections WHERE id = NEW.parent_id
              UNION SELECT s.id, s.parent_id FROM sh_sections s JOIN ancestors a ON s.id = a.parent_id
            ) SELECT 1 FROM ancestors WHERE id = NEW.id
          ) BEGIN SELECT RAISE(ABORT, 'invalid section parent'); END`;
        yield* sql`CREATE TABLE IF NOT EXISTS sh_sessions (
          id TEXT PRIMARY KEY, project_id TEXT NOT NULL REFERENCES sh_projects(id),
          section_id TEXT REFERENCES sh_sections(id), mode TEXT NOT NULL CHECK (mode IN ('work','code')),
          revision INTEGER NOT NULL DEFAULT 1 CHECK (revision > 0)
        )`;
        yield* sql`CREATE TABLE IF NOT EXISTS sh_thread_bindings (
          session_id TEXT PRIMARY KEY REFERENCES sh_sessions(id), environment_id TEXT NOT NULL,
          t3_thread_id TEXT NOT NULL, binding_state TEXT NOT NULL,
          revision INTEGER NOT NULL DEFAULT 1 CHECK (revision > 0),
          UNIQUE(environment_id, t3_thread_id)
        )`;
        yield* sql`CREATE TABLE IF NOT EXISTS sh_commands (
          environment_id TEXT NOT NULL, project_id TEXT NOT NULL, idempotency_key TEXT NOT NULL,
          actor_id TEXT NOT NULL, request_identity TEXT NOT NULL, result_revision INTEGER NOT NULL,
          PRIMARY KEY(environment_id, project_id, idempotency_key)
        )`;
        yield* sql`CREATE TABLE IF NOT EXISTS sh_audit (
          sequence INTEGER PRIMARY KEY AUTOINCREMENT, actor_id TEXT NOT NULL,
          environment_id TEXT NOT NULL, request_id TEXT NOT NULL, event_type TEXT NOT NULL,
          session_id TEXT NOT NULL, revision INTEGER NOT NULL, created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
        )`;
        yield* sql`CREATE INDEX IF NOT EXISTS sh_sections_by_project ON sh_sections(project_id, parent_id)`;
        yield* sql`CREATE INDEX IF NOT EXISTS sh_sessions_by_project ON sh_sessions(project_id, section_id)`;
        yield* sql`CREATE TRIGGER IF NOT EXISTS sh_session_section_insert
          BEFORE INSERT ON sh_sessions WHEN NEW.section_id IS NOT NULL AND NOT EXISTS (
            SELECT 1 FROM sh_sections s WHERE s.id = NEW.section_id AND s.project_id = NEW.project_id
          ) BEGIN SELECT RAISE(ABORT, 'section belongs to another project'); END`;
        yield* sql`CREATE TRIGGER IF NOT EXISTS sh_session_section_update
          BEFORE UPDATE OF section_id, project_id ON sh_sessions WHEN NEW.section_id IS NOT NULL AND NOT EXISTS (
            SELECT 1 FROM sh_sections s WHERE s.id = NEW.section_id AND s.project_id = NEW.project_id
          ) BEGIN SELECT RAISE(ABORT, 'section belongs to another project'); END`;
        yield* sql`INSERT INTO sh_projects(id,name,normalized_name)
          VALUES (${UNASSIGNED_PROJECT_ID}, 'Unassigned', 'unassigned')
          ON CONFLICT(id) DO NOTHING`;
        const unassigned = yield* sql<{
          name: string;
          normalized_name: string;
          archived_at: string | null;
        }>`
          SELECT name, normalized_name, archived_at FROM sh_projects WHERE id = ${UNASSIGNED_PROJECT_ID}`;
        if (
          unassigned[0]?.name !== "Unassigned" ||
          unassigned[0].normalized_name !== "unassigned" ||
          unassigned[0].archived_at !== null
        ) {
          return yield* new SemanggiMigrationError({ message: "Invalid Unassigned project." });
        }
        yield* sql`CREATE TRIGGER IF NOT EXISTS sh_unassigned_no_delete
          BEFORE DELETE ON sh_projects WHEN OLD.id = '00000000-0000-0000-0000-000000000001'
          BEGIN SELECT RAISE(ABORT, 'cannot delete Unassigned'); END`;
        yield* sql`CREATE TRIGGER IF NOT EXISTS sh_unassigned_no_change
          BEFORE UPDATE OF id, name, normalized_name, archived_at ON sh_projects
          WHEN OLD.id = '00000000-0000-0000-0000-000000000001'
          AND (NEW.id != OLD.id OR NEW.name != OLD.name OR NEW.normalized_name != OLD.normalized_name OR NEW.archived_at IS NOT NULL)
          BEGIN SELECT RAISE(ABORT, 'cannot modify Unassigned'); END`;
        yield* sql`CREATE TRIGGER IF NOT EXISTS sh_unassigned_no_id_change
          BEFORE UPDATE OF id ON sh_projects
          WHEN OLD.id = '00000000-0000-0000-0000-000000000001' AND NEW.id != OLD.id
          BEGIN SELECT RAISE(ABORT, 'cannot modify Unassigned'); END`;
        yield* sql`CREATE TRIGGER IF NOT EXISTS sh_section_children_consistent
          BEFORE UPDATE OF project_id, depth ON sh_sections
          WHEN EXISTS (SELECT 1 FROM sh_sections c WHERE c.parent_id = OLD.id
            AND (c.project_id != NEW.project_id OR c.depth != NEW.depth + 1))
          OR EXISTS (SELECT 1 FROM sh_sessions s WHERE s.section_id = OLD.id AND s.project_id != NEW.project_id)
          BEGIN SELECT RAISE(ABORT, 'section has dependent records'); END`;
        yield* sql`INSERT OR IGNORE INTO sh_migrations(version) VALUES (1)`;
        yield* sql`INSERT OR IGNORE INTO sh_migrations(version) VALUES (2)`;
        if (!versions.some(({ version }) => version === 3)) {
          const projects = yield* sql<{
            id: string;
            name: string;
          }>`SELECT id, name FROM sh_projects`;
          for (const project of projects) {
            yield* sql`UPDATE sh_projects SET normalized_name = ${normalizeName(project.name)} WHERE id = ${project.id}`;
          }
          const sections = yield* sql<{
            id: string;
            name: string;
          }>`SELECT id, name FROM sh_sections`;
          for (const section of sections) {
            yield* sql`UPDATE sh_sections SET normalized_name = ${normalizeName(section.name)} WHERE id = ${section.id}`;
          }
          yield* sql`INSERT INTO sh_migrations(version) VALUES (3)`;
        }
        yield* sql`INSERT OR IGNORE INTO sh_migrations(version) VALUES (4)`;
        if (!versions.some(({ version }) => version === 5)) {
          const commands = yield* sql<{
            environment_id: string;
            project_id: string;
            idempotency_key: string;
            request_identity: string;
          }>`
            SELECT environment_id, project_id, idempotency_key, request_identity FROM sh_commands`;
          for (const command of commands) {
            const hash = NodeCrypto.createHash("sha256")
              .update(command.request_identity)
              .digest("hex");
            yield* sql`UPDATE sh_commands SET request_identity = ${hash} WHERE environment_id = ${command.environment_id}
              AND project_id = ${command.project_id} AND idempotency_key = ${command.idempotency_key}`;
          }
          yield* sql`INSERT INTO sh_migrations(version) VALUES (5)`;
        }
        if (!versions.some(({ version }) => version === 6)) {
          yield* sql`ALTER TABLE sh_audit RENAME COLUMN session_id TO entity_id`;
          yield* sql`ALTER TABLE sh_audit ADD COLUMN entity_type TEXT NOT NULL DEFAULT 'session'`;
          yield* sql`INSERT INTO sh_migrations(version) VALUES (6)`;
        }
        if (!versions.some(({ version }) => version === 7)) {
          yield* sql`CREATE TABLE sh_thread_create_operations (
            environment_id TEXT NOT NULL, idempotency_key TEXT NOT NULL, actor_id TEXT NOT NULL,
            request_id TEXT NOT NULL, request_hash TEXT NOT NULL, request_payload TEXT NOT NULL,
            session_id TEXT NOT NULL UNIQUE REFERENCES sh_sessions(id), command_id TEXT NOT NULL UNIQUE,
            thread_id TEXT NOT NULL, state TEXT NOT NULL CHECK (state IN
              ('PREPARED', 'DISPATCHED', 'NATIVE_CONFIRMED', 'BOUND', 'REJECTED', 'NEEDS_RECONCILIATION')),
            native_sequence INTEGER, revision INTEGER NOT NULL DEFAULT 1 CHECK (revision > 0),
            PRIMARY KEY(environment_id, idempotency_key), UNIQUE(environment_id, thread_id)
          )`;
          yield* sql`INSERT INTO sh_migrations(version) VALUES (7)`;
        }
        if (!versions.some(({ version }) => version === 8)) {
          yield* sql`CREATE TABLE sh_ingestion_checkpoints (
            environment_id TEXT PRIMARY KEY, cursor INTEGER NOT NULL CHECK (cursor >= 0),
            high_water INTEGER NOT NULL CHECK (high_water >= 0),
            state TEXT NOT NULL CHECK (state IN ('CATCHING_UP', 'RECONCILING'))
          )`;
          yield* sql`CREATE TABLE sh_inbox (
            environment_id TEXT NOT NULL, source_sequence INTEGER NOT NULL CHECK (source_sequence > 0),
            event_id TEXT NOT NULL, event_hash TEXT NOT NULL,
            PRIMARY KEY(environment_id, source_sequence), UNIQUE(environment_id, event_id)
          )`;
          yield* sql`CREATE TABLE sh_thread_refresh_queue (
            environment_id TEXT NOT NULL, thread_id TEXT NOT NULL,
            source_sequence INTEGER NOT NULL CHECK (source_sequence > 0),
            PRIMARY KEY(environment_id, thread_id)
          )`;
          yield* sql`INSERT INTO sh_migrations(version) VALUES (8)`;
        }
        if (!versions.some(({ version }) => version === 9)) {
          yield* sql`CREATE TABLE sh_native_projection (
            environment_id TEXT NOT NULL, thread_id TEXT NOT NULL, source_sequence INTEGER NOT NULL,
            title TEXT NOT NULL, deleted INTEGER NOT NULL CHECK(deleted IN (0,1)),
            PRIMARY KEY(environment_id, thread_id)
          )`;
          yield* sql`CREATE TABLE sh_message_projection (
            environment_id TEXT NOT NULL, thread_id TEXT NOT NULL, message_id TEXT NOT NULL,
            source_sequence INTEGER NOT NULL, role TEXT NOT NULL, content_hash TEXT NOT NULL,
            deleted INTEGER NOT NULL CHECK(deleted IN (0,1)),
            PRIMARY KEY(environment_id, thread_id, message_id)
          )`;
          yield* sql`INSERT INTO sh_migrations(version) VALUES (9)`;
        }
        if (!versions.some(({ version }) => version === 10)) {
          yield* sql`ALTER TABLE sh_thread_create_operations ADD COLUMN recovery_order INTEGER NOT NULL DEFAULT 0`;
          yield* sql`ALTER TABLE sh_thread_create_operations ADD COLUMN created_at_ms INTEGER NOT NULL DEFAULT 0`;
          yield* sql`CREATE TRIGGER sh_pending_session_no_move BEFORE UPDATE OF project_id, section_id, mode ON sh_sessions
            WHEN EXISTS (SELECT 1 FROM sh_thread_create_operations op WHERE op.session_id = OLD.id AND op.state != 'BOUND')
            BEGIN SELECT RAISE(ABORT, 'pending create session cannot be moved'); END`;
          yield* sql`INSERT INTO sh_migrations(version) VALUES (10)`;
        }
        if (!versions.some(({ version }) => version === 11)) {
          yield* sql`ALTER TABLE sh_thread_refresh_queue ADD COLUMN attempts INTEGER NOT NULL DEFAULT 0`;
          yield* sql`ALTER TABLE sh_thread_refresh_queue ADD COLUMN retry_at_ms INTEGER NOT NULL DEFAULT 0`;
          yield* sql`INSERT INTO sh_migrations(version) VALUES (11)`;
        }
        if (!versions.some(({ version }) => version === 12)) {
          yield* sql`ALTER TABLE sh_thread_refresh_queue ADD COLUMN force_snapshot INTEGER NOT NULL DEFAULT 0 CHECK(force_snapshot IN (0,1))`;
          yield* sql`CREATE TABLE sh_reconciliation (
            environment_id TEXT PRIMARY KEY, base_sequence INTEGER NOT NULL CHECK(base_sequence >= 0),
            after_thread_id TEXT NOT NULL DEFAULT ''
          )`;
          yield* sql`CREATE TABLE sh_discovery_pending (
            environment_id TEXT NOT NULL, thread_id TEXT NOT NULL, mode TEXT NOT NULL CHECK(mode IN ('work','code')),
            request_id TEXT NOT NULL, PRIMARY KEY(environment_id,thread_id)
          )`;
          yield* sql`INSERT INTO sh_migrations(version) VALUES (12)`;
        }
        if (!versions.some(({ version }) => version === 13)) {
          yield* sql`CREATE TABLE sh_reconciliation_items (
            environment_id TEXT NOT NULL, thread_id TEXT NOT NULL, mode TEXT CHECK(mode IS NULL OR mode IN ('work','code')),
            PRIMARY KEY(environment_id,thread_id)
          )`;
          yield* sql`INSERT INTO sh_migrations(version) VALUES (13)`;
        }
        if (!versions.some(({ version }) => version === 14)) {
          yield* sql`ALTER TABLE sh_reconciliation_items ADD COLUMN session_id TEXT`;
          yield* sql`ALTER TABLE sh_reconciliation_items ADD COLUMN will_create INTEGER NOT NULL DEFAULT 0 CHECK(will_create IN (0,1))`;
          const staged = yield* sql<{
            environment_id: string;
            thread_id: string;
          }>`SELECT environment_id,thread_id FROM sh_reconciliation_items`;
          for (const row of staged) {
            yield* sql`UPDATE sh_reconciliation_items SET session_id=${sessionIdForThread(row.environment_id, row.thread_id)}
              WHERE environment_id=${row.environment_id} AND thread_id=${row.thread_id}`;
          }
          yield* sql`INSERT INTO sh_migrations(version) VALUES (14)`;
        }
        if (!versions.some(({ version }) => version === 15)) {
          yield* sql`CREATE TABLE sh_ingestion_checkpoints_v15 (
            environment_id TEXT PRIMARY KEY, cursor INTEGER NOT NULL CHECK (cursor >= 0),
            high_water INTEGER NOT NULL CHECK (high_water >= 0),
            state TEXT NOT NULL CHECK (state IN ('CATCHING_UP', 'LIVE', 'RECONCILING'))
          )`;
          yield* sql`INSERT INTO sh_ingestion_checkpoints_v15(environment_id,cursor,high_water,state)
            SELECT environment_id,cursor,high_water,state FROM sh_ingestion_checkpoints`;
          yield* sql`DROP TABLE sh_ingestion_checkpoints`;
          yield* sql`ALTER TABLE sh_ingestion_checkpoints_v15 RENAME TO sh_ingestion_checkpoints`;
          yield* sql`INSERT INTO sh_migrations(version) VALUES (15)`;
        }
        if (!versions.some(({ version }) => version === 16)) {
          const sectionColumns = yield* sql<{ name: string }>`PRAGMA table_info(sh_sections)`;
          if (!sectionColumns.some(({ name }) => name === "position")) {
            yield* sql`ALTER TABLE sh_sections ADD COLUMN position INTEGER NOT NULL DEFAULT 0`;
          }
          yield* sql`UPDATE sh_sections SET position = rowid WHERE position = 0`;
          // A subtree move updates every descendant in one transaction. Parent/depth integrity is
          // enforced by the domain operation rather than a row trigger that cannot observe the batch.
          yield* sql`DROP TRIGGER IF EXISTS sh_section_parent_update`;
          yield* sql`CREATE TRIGGER sh_section_parent_update
            BEFORE UPDATE OF parent_id, project_id ON sh_sections WHEN
            (NEW.parent_id IS NOT NULL AND NOT EXISTS (
              SELECT 1 FROM sh_sections p WHERE p.id = NEW.parent_id
              AND p.project_id = NEW.project_id
            )) OR EXISTS (
              WITH RECURSIVE ancestors(id, parent_id) AS (
                SELECT id, parent_id FROM sh_sections WHERE id = NEW.parent_id
                UNION SELECT s.id, s.parent_id FROM sh_sections s JOIN ancestors a ON s.id = a.parent_id
              ) SELECT 1 FROM ancestors WHERE id = NEW.id
            ) BEGIN SELECT RAISE(ABORT, 'invalid section parent'); END`;
          yield* sql`DROP TRIGGER IF EXISTS sh_section_children_consistent`;
          yield* sql`CREATE TRIGGER sh_section_children_consistent
            BEFORE UPDATE OF project_id ON sh_sections
            WHEN EXISTS (SELECT 1 FROM sh_sections c WHERE c.parent_id = OLD.id
              AND c.project_id != NEW.project_id)
            OR EXISTS (SELECT 1 FROM sh_sessions s WHERE s.section_id = OLD.id AND s.project_id != NEW.project_id)
            BEGIN SELECT RAISE(ABORT, 'section has dependent records'); END`;
          yield* sql`CREATE INDEX IF NOT EXISTS sh_sections_ordered ON sh_sections(project_id, parent_id, position)`;
          yield* sql`INSERT INTO sh_migrations(version) VALUES (16)`;
        }
        const integrity = yield* sql<{ integrity_check: string }>`PRAGMA integrity_check`;
        const foreignKeys = yield* sql`PRAGMA foreign_key_check`;
        if (
          integrity.length !== 1 ||
          integrity[0]?.integrity_check !== "ok" ||
          foreignKeys.length
        ) {
          return yield* new SemanggiMigrationError({
            message: "Semanggi database integrity check failed.",
          });
        }
      }),
    );
  }),
);

export const layerFromPath = (filename: string) =>
  Layer.provideMerge(setup, NodeSqliteClient.layer({ filename }));
