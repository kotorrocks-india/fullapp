# app/schemas/user_roles_migration.py
from __future__ import annotations

from sqlalchemy import text as sa_text
from core.schema_registry import register

@register
def ensure_roles_and_user_roles_by_id(engine):
    """
    Normalize RBAC tables to canonical schema:

      roles(id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT UNIQUE NOT NULL)
      user_roles(user_id INTEGER NOT NULL, role_id INTEGER NOT NULL, UNIQUE(user_id, role_id))

    If a legacy table 'user_roles' with (user_id, role_name TEXT) exists,
    migrate its data into the new structure and replace the table.
    Safe to run multiple times (idempotent).
    """
    with engine.begin() as conn:
        # Ensure roles table
        conn.execute(sa_text("""
            CREATE TABLE IF NOT EXISTS roles(
                id   INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE NOT NULL
            )
        """))

        # Detect user_roles current shape
        info = conn.execute(sa_text("SELECT name FROM sqlite_master WHERE type='table'")).fetchall()
        tables = {r[0] for r in info}

        if "user_roles" not in tables:
            # Fresh create canonical user_roles
            conn.execute(sa_text("""
                CREATE TABLE user_roles(
                    user_id INTEGER NOT NULL,
                    role_id INTEGER NOT NULL,
                    UNIQUE(user_id, role_id)
                )
            """))
            conn.execute(sa_text("CREATE INDEX IF NOT EXISTS ix_user_roles_uid ON user_roles(user_id)"))
            conn.execute(sa_text("CREATE INDEX IF NOT EXISTS ix_user_roles_rid ON user_roles(role_id)"))
            return

        # Introspect columns of existing user_roles
        cols = conn.execute(sa_text("PRAGMA table_info(user_roles)")).fetchall()
        colnames = {c[1] for c in cols}

        if "role_name" in colnames:
            # ─────────────── Migration path: by-name → by-id ───────────────
            # 1) Seed roles table with all distinct role names
            distinct_roles = conn.execute(sa_text("SELECT DISTINCT role_name FROM user_roles")).fetchall()
            for (rname,) in distinct_roles:
                if rname:
                    conn.execute(sa_text("INSERT OR IGNORE INTO roles(name) VALUES(:n)"), {"n": rname})

            # 2) Create new canonical table
            conn.execute(sa_text("""
                CREATE TABLE IF NOT EXISTS user_roles_new(
                    user_id INTEGER NOT NULL,
                    role_id INTEGER NOT NULL,
                    UNIQUE(user_id, role_id)
                )
            """))

            # 3) Migrate data
            # Map role_name → role_id
            conn.execute(sa_text("""
                INSERT OR IGNORE INTO user_roles_new(user_id, role_id)
                SELECT ur.user_id, r.id
                FROM user_roles ur
                JOIN roles r ON r.name = ur.role_name
            """))

            # 4) Replace old table
            conn.execute(sa_text("DROP TABLE user_roles"))
            conn.execute(sa_text("ALTER TABLE user_roles_new RENAME TO user_roles"))

            # 5) Indexes
            conn.execute(sa_text("CREATE INDEX IF NOT EXISTS ix_user_roles_uid ON user_roles(user_id)"))
            conn.execute(sa_text("CREATE INDEX IF NOT EXISTS ix_user_roles_rid ON user_roles(role_id)"))

        else:
            # Already canonical (user_id, role_id). Just ensure indexes exist.
            conn.execute(sa_text("CREATE INDEX IF NOT EXISTS ix_user_roles_uid ON user_roles(user_id)"))
            conn.execute(sa_text("CREATE INDEX IF NOT EXISTS ix_user_roles_rid ON user_roles(role_id)"))

        # Optional: enable FK pragmas for the session (SQLite)
        # (Harmless if off; we haven't declared FKs to avoid accidental breakage.)
        conn.execute(sa_text("PRAGMA foreign_keys=ON"))
