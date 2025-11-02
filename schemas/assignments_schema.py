from __future__ import annotations
from sqlalchemy import text as sa_text
from core.schema_registry import register

@register
def ensure_assignments_schema(engine):
    with engine.begin() as conn:
        conn.execute(sa_text("""
        CREATE TABLE IF NOT EXISTS assignments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            degree_code TEXT NOT NULL,
            assignment_code TEXT NOT NULL,
            title TEXT NOT NULL,
            max_marks INTEGER NOT NULL DEFAULT 100,
            require_approval INTEGER NOT NULL DEFAULT 0,
            status TEXT NOT NULL DEFAULT 'draft',
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(degree_code, assignment_code)
        )"""))
        # If you added new columns later, ensure here similarly via PRAGMA + ALTER
