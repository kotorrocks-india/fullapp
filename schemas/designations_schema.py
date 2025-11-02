from sqlalchemy import text as T
from core.schema_registry import register

def _ensure_column(conn, table: str, col: str, col_def: str):
    """SQLite-safe helper to add a missing column."""
    info = conn.execute(T(f"PRAGMA table_info({table})")).fetchall()
    have = {r[1] for r in info}
    if col not in have:
        conn.execute(T(f"ALTER TABLE {table} ADD COLUMN {col} {col_def}"))

@register
def install_designations(engine):
    with engine.begin() as c:
        # Create the table with the full, final schema.
        # This works correctly for new databases.
        c.execute(T("""
        CREATE TABLE IF NOT EXISTS designations(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          designation TEXT NOT NULL UNIQUE COLLATE NOCASE,
          is_active INTEGER NOT NULL DEFAULT 1,
          created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
          updated_at DATETIME
        )"""))

        
