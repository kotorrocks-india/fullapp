# schemas/approvals_schema.py
from __future__ import annotations
from sqlalchemy import text as sa_text

# Assuming your decorator is in core
try:
    from core.schema_registry import register
except ImportError:
    # Dummy decorator for safety
    def register(func): return func

@register
def ensure_approvals_schema(engine):
    """
    Ensures the 'approvals' and 'approvals_votes' tables are correct.
    This replaces the old migration script with a safe, idempotent version.
    """
    with engine.begin() as conn:
        
        # --- 1. Check for old schema (object_id=INTEGER) and migrate ---
        table_exists = conn.execute(sa_text("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name='approvals'
        """)).fetchone()
        
        if table_exists:
            object_id_info = next((row for row in conn.execute(sa_text("PRAGMA table_info(approvals)")).fetchall() if row[1] == 'object_id'), None)
            if object_id_info and object_id_info[2].upper() == 'INTEGER':
                # We have the old schema. We must migrate it.
                _migrate_approvals_to_text_object_id(conn)
        
        # --- 2. Create/Update 'approvals' table ---
        conn.execute(sa_text("""
        CREATE TABLE IF NOT EXISTS approvals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            object_type TEXT NOT NULL,
            object_id   TEXT NOT NULL,
            action      TEXT NOT NULL,
            status      TEXT NOT NULL DEFAULT 'pending',
            requester   TEXT,
            requester_email TEXT,
            approver    TEXT,
            payload     TEXT,
            reason_note TEXT,
            decision_note TEXT,
            rule        TEXT,
            created_at  DATETIME DEFAULT CURRENT_TIMESTAMP,
            decided_at  DATETIME,
            updated_at  DATETIME DEFAULT CURRENT_TIMESTAMP
        )"""))

        # --- 3. Create/Update 'approvals_votes' table ---
        conn.execute(sa_text("""
        CREATE TABLE IF NOT EXISTS approvals_votes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            approval_id INTEGER NOT NULL,
            voter_email TEXT NOT NULL,
            decision TEXT NOT NULL,
            note TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (approval_id) REFERENCES approvals(id) ON DELETE CASCADE,
            UNIQUE(approval_id, voter_email)
        )
        """))

        # --- 4. Ensure Indexes ---
        conn.execute(sa_text("CREATE INDEX IF NOT EXISTS idx_approvals_status ON approvals(status)"))
        conn.execute(sa_text("CREATE INDEX IF NOT EXISTS idx_approvals_type_action ON approvals(object_type, action)"))
        conn.execute(sa_text("CREATE INDEX IF NOT EXISTS idx_approvals_requester ON approvals(requester_email)"))
        conn.execute(sa_text("CREATE INDEX IF NOT EXISTS idx_votes_approval ON approvals_votes(approval_id)"))
        
        # --- 5. Add any missing columns (idempotent) ---
        _safe_add_column(conn, 'approvals', 'decision_note', 'TEXT')


def _safe_add_column(conn, table_name, col_name, col_type):
    """Safely adds a column to a table if it doesn't exist."""
    try:
        # Check if column exists
        cols = {row[1] for row in conn.execute(sa_text(f"PRAGMA table_info({table_name})")).fetchall()}
        if col_name not in cols:
            conn.execute(sa_text(f'ALTER TABLE {table_name} ADD COLUMN {col_name} {col_type}'))
    except Exception:
        pass # Ignore errors, like if table is in a transaction

def _migrate_approvals_to_text_object_id(conn):
    """Migrate existing approvals table (with object_id=INTEGER) to new schema"""
    try:
        # 1. Rename old table
        conn.execute(sa_text("ALTER TABLE approvals RENAME TO approvals_old"))
        
        # 2. Create new table with correct schema
        conn.execute(sa_text("""
        CREATE TABLE approvals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            object_type TEXT NOT NULL,
            object_id   TEXT NOT NULL,
            action      TEXT NOT NULL,
            status      TEXT NOT NULL DEFAULT 'pending',
            requester   TEXT,
            requester_email TEXT,
            approver    TEXT,
            payload     TEXT,
            reason_note TEXT,
            decision_note TEXT,
            rule        TEXT,
            created_at  DATETIME DEFAULT CURRENT_TIMESTAMP,
            decided_at  DATETIME,
            updated_at  DATETIME DEFAULT CURRENT_TIMESTAMP
        )"""))
        
        # 3. Copy data, casting object_id to TEXT
        conn.execute(sa_text("""
            INSERT INTO approvals (
                id, object_type, object_id, action, status, requester, 
                requester_email, approver, payload, reason_note, decision_note, 
                rule, created_at, decided_at, updated_at
            )
            SELECT 
                id, object_type, CAST(object_id AS TEXT), action, status, requester, 
                requester_email, approver, payload, reason_note, note, -- 'note' becomes 'decision_note'
                rule, created_at, decided_at, CURRENT_TIMESTAMP
            FROM approvals_old
        """))
        
        # 4. Drop old table
        conn.execute(sa_text("DROP TABLE approvals_old"))
        
    except Exception as e:
        # If migration fails, try to restore
        conn.execute(sa_text("DROP TABLE IF EXISTS approvals"))
        conn.execute(sa_text("ALTER TABLE approvals_old RENAME TO approvals"))
        raise e
