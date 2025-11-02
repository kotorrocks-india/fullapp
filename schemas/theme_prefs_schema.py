# app/schemas/theme_prefs_schema.py
from __future__ import annotations
from sqlalchemy import text as sa_text
from core.schema_registry import register

@register
def ensure_user_prefs_schema(engine):
    with engine.begin() as conn:
        # Base table (keeps your original columns)
        conn.execute(sa_text("""
        CREATE TABLE IF NOT EXISTS user_prefs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT NOT NULL UNIQUE,
            theme_mode TEXT CHECK(theme_mode IN ('light','dark')) DEFAULT 'light',
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
        );
        """))

        # --- Additive, idempotent migrations (only if missing) ---

        # Whether the user explicitly chose the mode (vs. system/auto)
        _ensure_column(conn, "user_prefs", "mode_source",
                       "TEXT CHECK(mode_source IN ('user','system')) DEFAULT 'user'")

        # Per-user high-contrast toggle
        _ensure_column(conn, "user_prefs", "high_contrast",
                       "INTEGER NOT NULL DEFAULT 0")  # 0=false, 1=true

        # Optional per-user text scaling (Slide 6 has accessibility.text_scale)
        _ensure_column(conn, "user_prefs", "text_scale_percent",
                       "INTEGER DEFAULT 100")  # 80–130 typically

        # Persist “remember my choice” (Slide 1/6 remember_choice)
        _ensure_column(conn, "user_prefs", "remember_choice_post_login",
                       "INTEGER NOT NULL DEFAULT 1")  # 0/1
        _ensure_column(conn, "user_prefs", "remember_choice_pre_login",
                       "INTEGER NOT NULL DEFAULT 1")  # 0/1

        # Last time we saw this user (useful for cleanup/analytics)
        _ensure_column(conn, "user_prefs", "last_seen_at",
                       "DATETIME")

        # Helpful indexes (idempotent)
        conn.execute(sa_text("CREATE INDEX IF NOT EXISTS ix_user_prefs_email ON user_prefs(email)"))
        conn.execute(sa_text("CREATE INDEX IF NOT EXISTS ix_user_prefs_updated_at ON user_prefs(updated_at)"))

def _ensure_column(conn, table: str, col: str, col_def: str):
    info = conn.execute(sa_text(f"PRAGMA table_info({table})")).fetchall()
    have = {r[1] for r in info}
    if col not in have:
        conn.execute(sa_text(f"ALTER TABLE {table} ADD COLUMN {col} {col_def}"))
