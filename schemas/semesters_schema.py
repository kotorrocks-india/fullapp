# app/schemas/semesters_schema.py
from __future__ import annotations
from sqlalchemy import text as sa_text
from sqlalchemy.engine import Engine
from core.schema_registry import register

def _col_exists(conn, table, col):
    rows = conn.execute(sa_text(f"PRAGMA table_info({table})")).fetchall()
    return any((r[1] or "").lower() == col.lower() for r in rows)

@register
def ensure_semesters_schema(engine: Engine):
    """Ensures all semester-related tables exist."""
    with engine.begin() as conn:
        # Binding per Degree
        conn.execute(sa_text("""
            CREATE TABLE IF NOT EXISTS semester_binding (
                degree_code     TEXT PRIMARY KEY,
                binding_mode    TEXT NOT NULL CHECK (binding_mode IN ('degree','program','branch')),
                label_mode      TEXT NOT NULL CHECK (label_mode IN ('year_term','semester_n')) DEFAULT 'year_term',
                created_at      DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at      DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(degree_code) REFERENCES degrees(code) ON DELETE CASCADE
            )
        """))

        # Independent structures per target (degree / program / branch)
        conn.execute(sa_text("""
            CREATE TABLE IF NOT EXISTS degree_semester_struct (
                degree_code     TEXT PRIMARY KEY,
                years           INTEGER NOT NULL CHECK (years BETWEEN 1 AND 10),
                terms_per_year  INTEGER NOT NULL CHECK (terms_per_year BETWEEN 1 AND 5),
                active          INTEGER NOT NULL DEFAULT 1,
                updated_at      DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(degree_code) REFERENCES degrees(code) ON DELETE CASCADE
            )
        """))

        conn.execute(sa_text("""
            CREATE TABLE IF NOT EXISTS program_semester_struct (
                program_id      INTEGER PRIMARY KEY,
                years           INTEGER NOT NULL CHECK (years BETWEEN 1 AND 10),
                terms_per_year  INTEGER NOT NULL CHECK (terms_per_year BETWEEN 1 AND 5),
                active          INTEGER NOT NULL DEFAULT 1,
                updated_at      DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(program_id) REFERENCES programs(id) ON DELETE CASCADE
            )
        """))

        conn.execute(sa_text("""
            CREATE TABLE IF NOT EXISTS branch_semester_struct (
                branch_id       INTEGER PRIMARY KEY,
                years           INTEGER NOT NULL CHECK (years BETWEEN 1 AND 10),
                terms_per_year  INTEGER NOT NULL CHECK (terms_per_year BETWEEN 1 AND 5),
                active          INTEGER NOT NULL DEFAULT 1,
                updated_at      DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(branch_id) REFERENCES branches(id) ON DELETE CASCADE
            )
        """))

        # Flat materialized "semesters"
        conn.execute(sa_text("""
            CREATE TABLE IF NOT EXISTS semesters (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                degree_code     TEXT NOT NULL,
                program_id      INTEGER,
                branch_id       INTEGER,
                year_index      INTEGER NOT NULL,  -- 1..Years
                term_index      INTEGER NOT NULL,  -- 1..Terms/Year
                semester_number INTEGER NOT NULL,  -- 1..Years*Terms
                label           TEXT NOT NULL,
                active          INTEGER NOT NULL DEFAULT 1,
                created_at      DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at      DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(degree_code) REFERENCES degrees(code) ON DELETE CASCADE,
                FOREIGN KEY(program_id) REFERENCES programs(id) ON DELETE CASCADE,
                FOREIGN KEY(branch_id) REFERENCES branches(id) ON DELETE CASCADE
            )
        """))

        # Simple audits
        conn.execute(sa_text("""
            CREATE TABLE IF NOT EXISTS semesters_audit (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                action      TEXT NOT NULL,      -- create|edit|rebuild|delete|binding_change
                actor       TEXT NOT NULL,      -- email
                degree_code TEXT NOT NULL,
                payload     TEXT,               -- JSON
                at          DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """))