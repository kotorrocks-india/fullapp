from __future__ import annotations
from sqlalchemy import text as sa_text
from core.schema_registry import register

@register
def ensure_approvals_schema(engine):
    with engine.begin() as conn:
        # First, check if the table exists and what columns it has
        table_exists = conn.execute(sa_text("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name='approvals'
        """)).fetchone()
        
        if table_exists:
            # Table exists, check if we need to migrate schema
            existing_columns = {row[1] for row in conn.execute(sa_text("PRAGMA table_info(approvals)")).fetchall()}
            
            # Add missing columns if needed (without complex defaults)
            if 'payload' not in existing_columns:
                conn.execute(sa_text("ALTER TABLE approvals ADD COLUMN payload TEXT"))
            if 'requester_email' not in existing_columns:
                conn.execute(sa_text("ALTER TABLE approvals ADD COLUMN requester_email TEXT"))
            if 'reason_note' not in existing_columns:
                conn.execute(sa_text("ALTER TABLE approvals ADD COLUMN reason_note TEXT"))
            if 'updated_at' not in existing_columns:
                conn.execute(sa_text("ALTER TABLE approvals ADD COLUMN updated_at DATETIME"))
                # Set default value for existing rows
                conn.execute(sa_text("UPDATE approvals SET updated_at = CURRENT_TIMESTAMP WHERE updated_at IS NULL"))
            
            # Check if object_id is INTEGER and needs migration
            object_id_info = next((row for row in conn.execute(sa_text("PRAGMA table_info(approvals)")).fetchall() if row[1] == 'object_id'), None)
            if object_id_info and object_id_info[2].upper() == 'INTEGER':
                # We'll handle this with a full table migration
                _migrate_approvals_table(conn)
        else:
            # Create new table with correct schema
            conn.execute(sa_text("""
            CREATE TABLE approvals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                object_type TEXT NOT NULL,
                object_id   TEXT NOT NULL,
                action      TEXT NOT NULL,
                requester   TEXT,
                requester_email TEXT,
                approver    TEXT,
                rule        TEXT,
                status      TEXT NOT NULL DEFAULT 'pending',
                reason_note TEXT,
                note        TEXT,
                payload     TEXT,
                created_at  DATETIME DEFAULT CURRENT_TIMESTAMP,
                decided_at  DATETIME,
                updated_at  DATETIME DEFAULT CURRENT_TIMESTAMP
            )"""))

        # Create indexes (they will be created only if they don't exist)
        conn.execute(sa_text("""
            CREATE INDEX IF NOT EXISTS idx_approvals_object 
            ON approvals(object_type, object_id)
        """))
        conn.execute(sa_text("""
            CREATE INDEX IF NOT EXISTS idx_approvals_status 
            ON approvals(status)
        """))
        conn.execute(sa_text("""
            CREATE INDEX IF NOT EXISTS idx_approvals_requester 
            ON approvals(requester)
        """))
        
        # Only create requester_email index if the column exists
        existing_columns_after = {row[1] for row in conn.execute(sa_text("PRAGMA table_info(approvals)")).fetchall()}
        if 'requester_email' in existing_columns_after:
            conn.execute(sa_text("""
                CREATE INDEX IF NOT EXISTS idx_approvals_requester_email 
                ON approvals(requester_email)
            """))
        
        # Create payload index if column exists
        if 'payload' in existing_columns_after:
            conn.execute(sa_text("""
                CREATE INDEX IF NOT EXISTS idx_approvals_payload 
                ON approvals(payload)
            """))

def _migrate_approvals_table(conn):
    """Migrate existing approvals table to new schema - only if object_id is INTEGER"""
    print("Migrating approvals table schema...")
    
    # Create temporary table with new schema
    conn.execute(sa_text("""
        CREATE TABLE approvals_new (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            object_type TEXT NOT NULL,
            object_id   TEXT NOT NULL,
            action      TEXT NOT NULL,
            requester   TEXT,
            requester_email TEXT,
            approver    TEXT,
            rule        TEXT,
            status      TEXT NOT NULL DEFAULT 'pending',
            reason_note TEXT,
            note        TEXT,
            payload     TEXT,
            created_at  DATETIME DEFAULT CURRENT_TIMESTAMP,
            decided_at  DATETIME,
            updated_at  DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """))
    
    # Copy data from old table to new table
    conn.execute(sa_text("""
        INSERT INTO approvals_new (
            id, object_type, object_id, action, requester, requester_email, 
            approver, rule, status, reason_note, note, payload, created_at, decided_at, updated_at
        )
        SELECT 
            id, object_type, CAST(object_id AS TEXT), action, requester, requester,
            approver, rule, status, 
            CASE WHEN note IS NOT NULL THEN note ELSE '' END,  -- Use existing note as reason_note if needed
            note, 
            '' as payload,  -- Initialize empty payload for existing records
            created_at, decided_at, CURRENT_TIMESTAMP
        FROM approvals
    """))
    
    # Drop old table and rename new one
    conn.execute(sa_text("DROP TABLE approvals"))
    conn.execute(sa_text("ALTER TABLE approvals_new RENAME TO approvals"))
    
    print("Approvals table migration completed.")
