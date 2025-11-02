# schemas/academic_years_schema.py
from sqlalchemy import text as sa_text
from sqlalchemy.engine import Engine

def _ensure_column(conn, table, col, col_def):
    try:
        conn.execute(sa_text(f"ALTER TABLE {table} ADD COLUMN {col} {col_def}"))
    except Exception:
        pass  # already exists / sqlite limitation etc.

def install_academic_years(engine: Engine):
    with engine.begin() as conn:
        conn.execute(sa_text("""
            CREATE TABLE IF NOT EXISTS academic_years (
                ay_code   TEXT PRIMARY KEY COLLATE NOCASE,
                start_date TEXT NOT NULL,
                end_date   TEXT NOT NULL,
                status     TEXT NOT NULL DEFAULT 'planned'
                           CHECK(status IN ('planned','open','closed')),
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME
            )
        """))
        conn.execute(sa_text(
            "CREATE UNIQUE INDEX IF NOT EXISTS uq_ay_code ON academic_years(ay_code)"
        ))
        conn.execute(sa_text(
            "CREATE INDEX IF NOT EXISTS ix_ay_status ON academic_years(status)"
        ))
        conn.execute(sa_text(
            "CREATE INDEX IF NOT EXISTS ix_ay_start_date ON academic_years(start_date)"
        ))
        _ensure_column(conn, "academic_years", "created_at", "DATETIME DEFAULT CURRENT_TIMESTAMP")
        _ensure_column(conn, "academic_years", "updated_at", "DATETIME")

def install_academic_years_audit(engine: Engine):
    with engine.begin() as conn:
        conn.execute(sa_text("""
            CREATE TABLE IF NOT EXISTS academic_years_audit (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ay_code TEXT NOT NULL,
                action  TEXT NOT NULL,   -- create, edit, open, close, reopen, import, export, auto_rollover, delete
                note    TEXT,
                changed_fields TEXT,
                actor   TEXT,
                at      DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """))
        conn.execute(sa_text(
            "CREATE INDEX IF NOT EXISTS ix_ay_audit_code ON academic_years_audit(ay_code)"
        ))
        conn.execute(sa_text(
            "CREATE INDEX IF NOT EXISTS ix_ay_audit_at ON academic_years_audit(at)"
        ))
        conn.execute(sa_text(
            "CREATE INDEX IF NOT EXISTS ix_ay_audit_actor ON academic_years_audit(actor)"
        ))

def install_all(engine: Engine):
    install_academic_years(engine)
    install_academic_years_audit(engine)
