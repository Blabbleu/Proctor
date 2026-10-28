"""SQLite job store. One file, WAL mode, shared by supervisor (user swe) and broker (root)."""
from __future__ import annotations

import json
import os
import sqlite3
from pathlib import Path

from .common import STATE, now

SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
  id TEXT PRIMARY KEY,
  project TEXT NOT NULL,
  title TEXT NOT NULL,
  goal TEXT NOT NULL,
  acceptance TEXT NOT NULL,          -- JSON list
  harness TEXT NOT NULL,
  model TEXT,
  state TEXT NOT NULL,
  step TEXT NOT NULL DEFAULT '',
  reason TEXT NOT NULL DEFAULT '',
  idem_key TEXT,
  created_at REAL NOT NULL,
  started_at REAL,
  deadline REAL,
  finished_at REAL,
  base_branch TEXT,
  base_commit TEXT,
  candidate_commit TEXT,
  round INTEGER NOT NULL DEFAULT 0,        -- 0 = first implementation, n = repair round n
  infra_retries INTEGER NOT NULL DEFAULT 0,
  attempt_seq INTEGER NOT NULL DEFAULT 0,
  current_attempt TEXT,
  session_id TEXT,
  phase_container TEXT,
  phase_started_at REAL,
  phase_deadline REAL,
  kill_reason TEXT,
  cancel_requested INTEGER NOT NULL DEFAULT 0,
  notify INTEGER NOT NULL DEFAULT 1,
  retry_of TEXT,
  image TEXT,
  failure_sig TEXT,
  publication TEXT,                  -- JSON
  meta TEXT NOT NULL DEFAULT '{}'    -- JSON
);
CREATE INDEX IF NOT EXISTS jobs_state ON jobs(state);
CREATE INDEX IF NOT EXISTS jobs_idem ON jobs(idem_key);
CREATE TABLE IF NOT EXISTS attempts (
  id TEXT PRIMARY KEY,               -- <job>-a<seq>
  job_id TEXT NOT NULL,
  seq INTEGER NOT NULL,
  round INTEGER NOT NULL,
  kind TEXT NOT NULL,                -- initial | repair | infra-retry
  container TEXT,
  started_at REAL NOT NULL,
  ended_at REAL,
  status TEXT NOT NULL DEFAULT 'running',
  exit_code INTEGER,
  note TEXT NOT NULL DEFAULT ''
);
CREATE TABLE IF NOT EXISTS events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  job_id TEXT,
  ts REAL NOT NULL,
  kind TEXT NOT NULL,
  msg TEXT NOT NULL,
  data TEXT
);
CREATE INDEX IF NOT EXISTS events_job ON events(job_id, id);
CREATE TABLE IF NOT EXISTS checks (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  job_id TEXT NOT NULL,
  stage TEXT NOT NULL,               -- baseline | validate-r<n> | prepare
  name TEXT NOT NULL,
  command TEXT NOT NULL,
  required INTEGER NOT NULL,
  commit_sha TEXT,
  exit_code INTEGER,
  duration REAL,
  status TEXT NOT NULL,              -- pass | fail | timeout | oom | error
  log_path TEXT,
  ts REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS tokens (
  hash TEXT PRIMARY KEY,
  job_id TEXT NOT NULL,
  attempt_id TEXT NOT NULL,
  created_at REAL NOT NULL,
  revoked INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS usage (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  job_id TEXT NOT NULL,
  attempt_id TEXT NOT NULL,
  ts REAL NOT NULL,
  day TEXT NOT NULL,
  model TEXT,
  path TEXT,
  http_status INTEGER,
  input INTEGER NOT NULL DEFAULT 0,
  cache_write INTEGER NOT NULL DEFAULT 0,
  cache_read INTEGER NOT NULL DEFAULT 0,
  output INTEGER NOT NULL DEFAULT 0,
  cost_usd REAL NOT NULL DEFAULT 0,
  reserved_usd REAL NOT NULL DEFAULT 0,
  util_5h REAL,
  util_7d REAL,
  overage_status TEXT,
  note TEXT
);
CREATE INDEX IF NOT EXISTS usage_job ON usage(job_id);
CREATE INDEX IF NOT EXISTS usage_day ON usage(day);
CREATE TABLE IF NOT EXISTS gateway_blocks (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  job_id TEXT,
  attempt_id TEXT,
  ts REAL NOT NULL,
  kind TEXT NOT NULL,                -- budget | quota | stale | model | auth
  reason TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS quota (
  id INTEGER PRIMARY KEY,            -- 1 = anthropic (Claude), 2 = openai-codex (ChatGPT)
  ts REAL NOT NULL,
  util_5h REAL,
  util_7d REAL,
  status TEXT,
  overage_status TEXT,
  source TEXT
);
CREATE TABLE IF NOT EXISTS outbox (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  job_id TEXT,
  created_at REAL NOT NULL,
  body TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'pending',
  attempts INTEGER NOT NULL DEFAULT 0,
  sent_at REAL,
  error TEXT
);
CREATE TABLE IF NOT EXISTS requests (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  kind TEXT NOT NULL,                -- sync | publish
  job_id TEXT,
  payload TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'pending',
  created_at REAL NOT NULL,
  done_at REAL,
  result TEXT
);
"""


def connect(path: Path | None = None) -> sqlite3.Connection:
    path = path or (STATE / "db" / "swe.db")
    conn = sqlite3.connect(str(path), timeout=30, isolation_level=None, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA foreign_keys=ON")
    if os.geteuid() == 0:
        # root (broker/CLI) must never leave root-owned db files the supervisor cannot write
        import grp
        import pwd
        try:
            uid, gid = pwd.getpwnam("swe").pw_uid, grp.getgrnam("swe").gr_gid
            for suf in ("", "-wal", "-shm"):
                f = Path(str(path) + suf)
                if f.exists():
                    if f.stat().st_uid != uid:
                        os.chown(f, uid, gid)
                    os.chmod(f, 0o660)
        except KeyError:
            pass
    return conn


def init(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA)
    # v2: quota held one row (CHECK id = 1); now one row per provider.
    sql = (conn.execute("SELECT sql FROM sqlite_master WHERE name='quota'").fetchone() or [""])[0] or ""
    if "CHECK (id = 1)" in sql:
        conn.executescript("""
BEGIN;
ALTER TABLE quota RENAME TO quota_v1;
CREATE TABLE quota (id INTEGER PRIMARY KEY, ts REAL NOT NULL, util_5h REAL, util_7d REAL, status TEXT,
                    overage_status TEXT, source TEXT);
INSERT INTO quota SELECT * FROM quota_v1;
DROP TABLE quota_v1;
COMMIT;
""")


def event(conn, job_id, kind, msg, **data):
    conn.execute("INSERT INTO events(job_id, ts, kind, msg, data) VALUES (?,?,?,?,?)",
                 (job_id, now(), kind, msg, json.dumps(data, default=str) if data else None))


def job(conn, job_id):
    return conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()


def meta(row) -> dict:
    try:
        return json.loads(row["meta"] or "{}")
    except Exception:
        return {}
