from __future__ import annotations
from sqlalchemy import text as sa_text
from core.schema_registry import register

@register
def ensure_marks_schema(engine):
    with engine.begin() as conn:
        conn.execute(sa_text("""
        CREATE TABLE IF NOT EXISTS marks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            degree_code TEXT NOT NULL,
            assignment_code TEXT NOT NULL,
            student_id TEXT NOT NULL,
            marks_obtained REAL NOT NULL DEFAULT 0,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(degree_code, assignment_code, student_id)
        )"""))
