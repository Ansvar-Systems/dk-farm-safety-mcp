import BetterSqlite3 from 'better-sqlite3';
import { join, dirname } from 'path';
import { fileURLToPath } from 'url';

export interface Database {
  get<T>(sql: string, params?: unknown[]): T | undefined;
  all<T>(sql: string, params?: unknown[]): T[];
  run(sql: string, params?: unknown[]): void;
  close(): void;
  readonly instance: BetterSqlite3.Database;
}

export function createDatabase(dbPath?: string): Database {
  const resolvedPath =
    dbPath ??
    join(dirname(fileURLToPath(import.meta.url)), '..', 'data', 'database.db');
  const db = new BetterSqlite3(resolvedPath);

  db.pragma('journal_mode = DELETE');
  db.pragma('foreign_keys = ON');

  initSchema(db);

  return {
    get<T>(sql: string, params: unknown[] = []): T | undefined {
      return db.prepare(sql).get(...params) as T | undefined;
    },
    all<T>(sql: string, params: unknown[] = []): T[] {
      return db.prepare(sql).all(...params) as T[];
    },
    run(sql: string, params: unknown[] = []): void {
      db.prepare(sql).run(...params);
    },
    close(): void {
      db.close();
    },
    get instance() {
      return db;
    },
  };
}

function initSchema(db: BetterSqlite3.Database): void {
  db.exec(`
    CREATE TABLE IF NOT EXISTS safety_guidance (
      id INTEGER PRIMARY KEY,
      topic TEXT NOT NULL,
      machine_type TEXT,
      species TEXT,
      hazards TEXT,
      control_measures TEXT,
      legal_requirements TEXT,
      ppe_required TEXT,
      regulation_ref TEXT,
      jurisdiction TEXT DEFAULT 'DK'
    );

    CREATE TABLE IF NOT EXISTS children_rules (
      id INTEGER PRIMARY KEY,
      age_group TEXT NOT NULL,
      activity TEXT NOT NULL,
      permitted INTEGER NOT NULL,
      conditions TEXT,
      regulation_ref TEXT,
      jurisdiction TEXT DEFAULT 'DK'
    );

    CREATE TABLE IF NOT EXISTS reporting_requirements (
      id INTEGER PRIMARY KEY,
      incident_type TEXT NOT NULL,
      reportable INTEGER NOT NULL,
      deadline TEXT,
      notify TEXT,
      method TEXT,
      record_retention_years INTEGER,
      regulation_ref TEXT,
      jurisdiction TEXT DEFAULT 'DK'
    );

    CREATE TABLE IF NOT EXISTS coshh_guidance (
      id INTEGER PRIMARY KEY,
      substance_type TEXT,
      activity TEXT,
      assessment_required INTEGER,
      ppe TEXT,
      storage_requirements TEXT,
      disposal_requirements TEXT,
      regulation_ref TEXT,
      jurisdiction TEXT DEFAULT 'DK'
    );

    CREATE TABLE IF NOT EXISTS risk_assessment_templates (
      id INTEGER PRIMARY KEY,
      activity TEXT NOT NULL,
      hazards TEXT,
      controls TEXT,
      residual_risk TEXT,
      review_frequency TEXT,
      jurisdiction TEXT DEFAULT 'DK'
    );

    CREATE VIRTUAL TABLE IF NOT EXISTS search_index USING fts5(
      title, body, topic, jurisdiction
    );

    CREATE TABLE IF NOT EXISTS db_metadata (
      key TEXT PRIMARY KEY,
      value TEXT
    );

    INSERT OR IGNORE INTO db_metadata (key, value) VALUES ('schema_version', '1.0');
    INSERT OR IGNORE INTO db_metadata (key, value) VALUES ('mcp_name', 'Denmark Farm Safety MCP');
    INSERT OR IGNORE INTO db_metadata (key, value) VALUES ('jurisdiction', 'DK');
  `);
}

export function ftsSearch(
  db: Database,
  query: string,
  limit: number = 10
): { title: string; body: string; topic: string; jurisdiction: string; rank: number }[] {
  return db.all(
    `SELECT title, body, topic, jurisdiction, rank
     FROM search_index
     WHERE search_index MATCH ?
     ORDER BY rank
     LIMIT ?`,
    [query, limit]
  );
}
