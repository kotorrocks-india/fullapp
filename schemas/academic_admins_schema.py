# app/schemas/academic_admins_schema.py
from sqlalchemy import text as sa_text
from core.schema_registry import register

@register
def ensure_academic_admins_schema(engine):
    with engine.begin() as conn:
        conn.execute(sa_text("""
        CREATE TABLE IF NOT EXISTS academic_admins (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          user_id INTEGER NOT NULL,
          fixed_role TEXT NOT NULL CHECK (fixed_role IN ('director','principal','management_representative')),
          username TEXT NOT NULL UNIQUE,
          password_hash TEXT,
          faculty_id INTEGER,
          first_login_pending INTEGER NOT NULL DEFAULT 1,
          password_export_available INTEGER NOT NULL DEFAULT 1,
          compliance_json TEXT,
          created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
          UNIQUE(user_id),
          FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
        );
        """))

        # NEW: ensure optional display-only designation column exists (safe on older DBs)
        _ensure_column(conn, "designation", "TEXT")

        # Existing indexes (kept as-is)
        conn.execute(sa_text("CREATE INDEX IF NOT EXISTS ix_aa_user ON academic_admins(user_id);"))
        conn.execute(sa_text("CREATE INDEX IF NOT EXISTS ix_aa_fixed_role ON academic_admins(fixed_role);"))

def _ensure_column(conn, col: str, col_def: str):
    """SQLite-safe helper to add a missing column on academic_admins."""
    info = conn.execute(sa_text("PRAGMA table_info(academic_admins)")).fetchall()
    have = {r[1] for r in info}
    if col not in have:
        conn.execute(sa_text(f"ALTER TABLE academic_admins ADD COLUMN {col} {col_def}"))
