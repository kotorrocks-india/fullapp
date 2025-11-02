from sqlalchemy import text as T
from core.schema_registry import register

@register
def install_credits_policy(engine):
    """Create faculty credits policy table for managing credit requirements per degree and designation."""
    # ... code
    with engine.begin() as c:
        c.execute(T("""
        CREATE TABLE IF NOT EXISTS faculty_credits_policy(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          degree_code TEXT NOT NULL COLLATE NOCASE,
          designation TEXT NOT NULL COLLATE NOCASE,
          required_credits INTEGER NOT NULL DEFAULT 0,
          allowed_credit_override INTEGER NOT NULL DEFAULT 0,
          UNIQUE(degree_code, designation)
        )"""))
