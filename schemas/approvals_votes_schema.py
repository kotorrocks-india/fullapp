# app/schemas/approvals_votes_schema.py
from __future__ import annotations
from sqlalchemy import text as sa_text
from core.schema_registry import register

@register
def ensure_approvals_votes_schema(engine):
    with engine.begin() as conn:
        conn.execute(sa_text("""
        CREATE TABLE IF NOT EXISTS approvals_votes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            approval_id INTEGER NOT NULL,
            voter_email TEXT NOT NULL,
            decision TEXT NOT NULL CHECK(decision IN ('approve','reject')),
            note TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(approval_id, voter_email),
            FOREIGN KEY(approval_id) REFERENCES approvals(id) ON DELETE CASCADE
        )"""))
