# app/schemas/degrees_schema.py
"""
Degrees schema and migration for curriculum flags.
Keeps audit + indexes. Safe to run multiple times.
"""

from __future__ import annotations
from sqlalchemy import text as sa_text

# Helper to run DDL idempotently
def _exec(conn, sql: str):
    conn.execute(sa_text(sql))

def _has_column(conn, table: str, col: str) -> bool:
    """Helper to check if a column exists in a SQLite table."""
    # Note: PRAGMA table_info is specific to SQLite, but is used here
    # as the original file implies SQLite usage (AUTOINCREMENT, sqlite_master).
    rows = conn.execute(sa_text(f"PRAGMA table_info({table})")).fetchall()
    return any(r[1].lower() == col.lower() for r in rows)


def ensure_degrees_schema(engine):
    """
    Initial schema creation, equivalent to the original degrees_schema.py.
    This is kept for reference but the migration function will be the entry point.
    """
    with engine.begin() as conn:
        # degrees table: no years/terms_per_year
        _exec(conn, """
        CREATE TABLE IF NOT EXISTS degrees (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code TEXT NOT NULL UNIQUE,
            title TEXT NOT NULL,
            cohort_splitting_mode TEXT NOT NULL DEFAULT 'both',
            roll_number_scope TEXT NOT NULL DEFAULT 'degree',
            logo_file_name TEXT NULL,
            active INTEGER NOT NULL DEFAULT 1,
            sort_order INTEGER NOT NULL DEFAULT 100,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        """)

        # Useful indexes
        _exec(conn, "CREATE UNIQUE INDEX IF NOT EXISTS uq_degrees_code ON degrees(code)")
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_degrees_active ON degrees(active)")
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_degrees_sort ON degrees(sort_order)")

        # Audit table
        _exec(conn, """
        CREATE TABLE IF NOT EXISTS degrees_audit (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            degree_code TEXT NOT NULL,
            action TEXT NOT NULL,
            note TEXT NULL,
            changed_fields TEXT NULL,    -- JSON blob
            actor TEXT NULL,
            at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        """)
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_deg_audit_code ON degrees_audit(degree_code)")
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_deg_audit_at ON degrees_audit(at)")


def migrate_degrees(engine):
    """
    Ensures the degrees table exists and adds new curriculum-governance (cg) flags.
    This function combines schema creation/update and migration logic.
    """
    with engine.begin() as conn:
        # ensure base table (using the structure from the code you provided, which
        # uses code as PRIMARY KEY and adds created_at)
        # NOTE: This DDL is slightly different from the original 'ensure_degrees_schema'
        # (PRIMARY KEY on code vs id, added created_at). Using your provided DDL:
        _exec(conn, """
            CREATE TABLE IF NOT EXISTS degrees(
                code TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                cohort_splitting_mode TEXT NOT NULL DEFAULT 'both',
                roll_number_scope TEXT NOT NULL DEFAULT 'degree',
                logo_file_name TEXT,
                active INTEGER NOT NULL DEFAULT 1,
                sort_order INTEGER NOT NULL DEFAULT 100,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Add the curriculum flags (all default off)
        if not _has_column(conn, "degrees", "cg_degree"):
            _exec(conn, "ALTER TABLE degrees ADD COLUMN cg_degree INTEGER NOT NULL DEFAULT 0")
        if not _has_column(conn, "degrees", "cg_program"):
            _exec(conn, "ALTER TABLE degrees ADD COLUMN cg_program INTEGER NOT NULL DEFAULT 0")
        if not _has_column(conn, "degrees", "cg_branch"):
            _exec(conn, "ALTER TABLE degrees ADD COLUMN cg_branch INTEGER NOT NULL DEFAULT 0")

        # audit table (reusing the original definition but matching your provided DDL's 'at' type)
        _exec(conn, """
            CREATE TABLE IF NOT EXISTS degrees_audit(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                degree_code TEXT NOT NULL,
                action TEXT NOT NULL,
                note TEXT,
                changed_fields TEXT,
                actor TEXT,
                at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        """)
        # Ensure indexes are present for degrees_audit
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_deg_audit_code ON degrees_audit(degree_code)")
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_deg_audit_at ON degrees_audit(at)")
        
        # Ensure indexes for degrees table (needed if the initial CREATE TABLE IF NOT EXISTS
        # was skipped because the table already existed from the original schema)
        _exec(conn, "CREATE UNIQUE INDEX IF NOT EXISTS uq_degrees_code ON degrees(code)")
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_degrees_active ON degrees(active)")
        _exec(conn, "CREATE INDEX IF NOT EXISTS ix_degrees_sort ON degrees(sort_order)")


def run(engine):
    """Entry point for schema registry auto-discovery."""
    migrate_degrees(engine)

# Clean up unused helper/entry point from original if full replacement is desired
# if 'ensure_degrees_schema' is no longer the intended entry point.
# I'll keep it as 'migrate_degrees' is more comprehensive for this change.
