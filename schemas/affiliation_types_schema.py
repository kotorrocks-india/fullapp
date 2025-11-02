# schemas/affiliation_types_schema.py
from __future__ import annotations

from sqlalchemy import text as sa_text
from sqlalchemy.engine import Engine

# Try to import the registry; if it's not available or not callable, we fall back gracefully.
try:
    from core.schema_registry import register as _registry_register  # type: ignore
except Exception:
    _registry_register = None


def _auto_register(name: str):
    """
    Safe local decorator:
    - If a callable `register` exists, tries @register(name) then @register.
    - Otherwise it's a no-op and just returns the function unchanged.
    """
    def _decorator(fn):
        reg = _registry_register
        if callable(reg):
            try:
                return reg(name)(fn)  # supports @register("name")
            except TypeError:
                try:
                    return reg(fn)     # supports @register
                except Exception:
                    pass
        return fn
    return _decorator


def _ensure_column(conn, table: str, column: str, decl: str) -> None:
    """
    Add a column to an existing table if it doesn't already exist.
    'decl' must be a full SQLite column declaration (e.g., "INTEGER NOT NULL DEFAULT 0").
    """
    cols = conn.execute(sa_text(f"PRAGMA table_info({table});")).fetchall()
    have = {str(c[1]).lower() for c in cols}
    if column.lower() in have:
        return
    conn.execute(sa_text(f"ALTER TABLE {table} ADD COLUMN {column} {decl};"))


# Commented out to prevent auto-registration (handled manually in schema_registry.py)
@_auto_register("affiliation_types")
def ensure_affiliation_types_schema(engine: Engine) -> None:
    """
    Creates/updates the affiliation_types table.

    - type_code is UNIQUE with COLLATE NOCASE (case-insensitive uniqueness).
    - Ensures 'updated_at' and 'label' columns exist (older DBs were missing them).
    - Adds helpful indexes.
    - Inserts 'core' and 'visiting' as system types.
    - Removes old deprecated types (adjunct, emeritus, research).

    Safe to run repeatedly (idempotent).
    """
    with engine.begin() as conn:
        # Base table (case-insensitive unique on type_code)
        conn.execute(sa_text("""
            CREATE TABLE IF NOT EXISTS affiliation_types(
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                type_code   TEXT NOT NULL UNIQUE COLLATE NOCASE,
                label       TEXT NOT NULL,
                description TEXT NOT NULL,
                is_system   INTEGER NOT NULL DEFAULT 0,
                is_active   INTEGER NOT NULL DEFAULT 1,
                created_at  DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at  DATETIME
            );
        """))

        # Additive migrations (idempotent) - ensure columns exist
        _ensure_column(conn, "affiliation_types", "label", "TEXT NOT NULL DEFAULT ''")
        _ensure_column(conn, "affiliation_types", "is_system", "INTEGER NOT NULL DEFAULT 0")
        _ensure_column(conn, "affiliation_types", "is_active", "INTEGER NOT NULL DEFAULT 1")
        _ensure_column(conn, "affiliation_types", "created_at", "DATETIME")
        _ensure_column(conn, "affiliation_types", "updated_at", "DATETIME")

        # --- Remove old deprecated types ---
        conn.execute(sa_text("""
            DELETE FROM affiliation_types 
            WHERE lower(type_code) IN ('adjunct', 'emeritus', 'research')
        """))

        # --- Add default system types (with label column) ---
        # Use INSERT...ON CONFLICT DO UPDATE to ensure they exist with correct values
        conn.execute(sa_text("""
            INSERT INTO affiliation_types (type_code, label, description, is_system, is_active)
            VALUES ('core', 'Core', 'Core Faculty', 1, 1)
            ON CONFLICT(type_code) DO UPDATE SET
                label = excluded.label,
                description = excluded.description,
                is_system = 1,
                is_active = 1;
        """))
        conn.execute(sa_text("""
            INSERT INTO affiliation_types (type_code, label, description, is_system, is_active)
            VALUES ('visiting', 'Visiting', 'Visiting Faculty', 1, 1)
            ON CONFLICT(type_code) DO UPDATE SET
                label = excluded.label,
                description = excluded.description,
                is_system = 1,
                is_active = 1;
        """))

        # Helpful indexes
        conn.execute(sa_text("""
            CREATE INDEX IF NOT EXISTS ix_aff_types_active
            ON affiliation_types(is_active, is_system);
        """))
        conn.execute(sa_text("""
            CREATE INDEX IF NOT EXISTS ix_aff_types_lower_code
            ON affiliation_types(lower(type_code));
        """))


# Backward-compatible alias: some code may call this directly
def install_affiliation_types(engine: Engine) -> None:
    ensure_affiliation_types_schema(engine)
