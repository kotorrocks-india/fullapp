# faculty_initial_credentials_schema.py
from sqlalchemy import text as sa_text
from core.schema_registry import register

# Import helper
try:
    from core.schema_helpers import _ensure_column
except ImportError:
    def _ensure_column(conn, table, col, col_def):
        """Add column if it doesn't exist"""
        cols = conn.execute(sa_text(f"PRAGMA table_info({table});")).fetchall()
        names = {str(c[1]).lower() for c in cols}
        if col.lower() in names:
            return
        conn.execute(sa_text(f"ALTER TABLE {table} ADD COLUMN {col} {col_def};"))


@register
def ensure_faculty_initial_credentials_schema(engine):
    """Create faculty_initial_credentials table for storing temporary login credentials"""
    print("🔧 [SCHEMA] Installing faculty_initial_credentials table...")
    
    with engine.begin() as conn:
        conn.execute(sa_text("""
        CREATE TABLE IF NOT EXISTS faculty_initial_credentials (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            faculty_profile_id INTEGER NOT NULL UNIQUE,
            username TEXT NOT NULL UNIQUE COLLATE NOCASE,
            plaintext TEXT NOT NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            consumed INTEGER NOT NULL DEFAULT 0,
            FOREIGN KEY(faculty_profile_id) REFERENCES faculty_profiles(id) ON DELETE CASCADE
        )
        """))
        
        print("✅ [SCHEMA] faculty_initial_credentials table created/verified")
        
        # Migrations (ensure columns exist if table was created differently before)
        _ensure_column(conn, "faculty_initial_credentials", "consumed", "INTEGER NOT NULL DEFAULT 0")
        
        # Indexes
        conn.execute(sa_text("CREATE UNIQUE INDEX IF NOT EXISTS uq_faculty_initcred_profileid ON faculty_initial_credentials(faculty_profile_id)"))
        conn.execute(sa_text("CREATE UNIQUE INDEX IF NOT EXISTS uq_faculty_initcred_username ON faculty_initial_credentials(username)"))
        conn.execute(sa_text("CREATE INDEX IF NOT EXISTS ix_faculty_initcred_consumed ON faculty_initial_credentials(consumed)"))
        
        print("✅ [SCHEMA] faculty_initial_credentials indexes created")
        
        # Verify
        cols = conn.execute(sa_text("PRAGMA table_info(faculty_initial_credentials);")).fetchall()
        col_names = [col[1] for col in cols]
        print(f"📊 [SCHEMA] faculty_initial_credentials columns: {', '.join(col_names)}")


__all__ = ['ensure_faculty_initial_credentials_schema']
