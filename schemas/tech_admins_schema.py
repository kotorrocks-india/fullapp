# app/schemas/tech_admins_schema.py
from sqlalchemy import text as sa_text
from core.schema_registry import register

@register
def ensure_tech_admins_schema(engine):
    with engine.begin() as conn:
        conn.execute(sa_text("""
        CREATE TABLE IF NOT EXISTS tech_admins (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          user_id INTEGER NOT NULL UNIQUE,
          username TEXT NOT NULL UNIQUE,
          password_hash TEXT,
          first_login_pending INTEGER NOT NULL DEFAULT 1,
          password_export_available INTEGER NOT NULL DEFAULT 1,
          created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
          FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
        );
        """))
        conn.execute(sa_text("CREATE INDEX IF NOT EXISTS ix_ta_user ON tech_admins(user_id)"))
