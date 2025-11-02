# schemas/affiliations_schema.py
from __future__ import annotations
from sqlalchemy import text as sa_text
from sqlalchemy.engine import Engine
from core.schema_registry import register

@register
def install_faculty_affiliations(engine: Engine) -> None:
    """Creates faculty_affiliations table."""
    with engine.begin() as conn:
        conn.execute(sa_text("""
            CREATE TABLE IF NOT EXISTS faculty_affiliations(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                email TEXT NOT NULL,
                degree_code TEXT NOT NULL,
                program_code TEXT,
                branch_code TEXT,
                group_code TEXT,
                designation TEXT NOT NULL,
                type TEXT NOT NULL DEFAULT 'core',
                allowed_credit_override INTEGER DEFAULT 0,
                active INTEGER NOT NULL DEFAULT 1,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME
            )
        """))

        # --- ENSURE THIS INDEX IS PRESENT ---
        conn.execute(sa_text("""
            CREATE UNIQUE INDEX IF NOT EXISTS uq_faculty_affiliation_key
            ON faculty_affiliations (
                lower(email),
                lower(degree_code),
                COALESCE(program_code, ''),
                COALESCE(branch_code, ''),
                COALESCE(group_code, '')
            );
        """))
        # --- END OF INDEX ---

        conn.execute(sa_text("""
            CREATE INDEX IF NOT EXISTS ix_faculty_aff_email
            ON faculty_affiliations(lower(email), active)
        """))

        conn.execute(sa_text("""
            CREATE INDEX IF NOT EXISTS ix_faculty_aff_degree
            ON faculty_affiliations(lower(degree_code), active)
        """))
