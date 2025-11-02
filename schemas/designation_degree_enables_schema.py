from sqlalchemy import text as T
from core.schema_registry import register

@register
def install_designation_degree_enables(engine):
    """Create table for managing which designations are enabled for which degrees."""
    # ... code
    with engine.begin() as c:
        c.execute(T("""
        CREATE TABLE IF NOT EXISTS designation_degree_enables(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          designation TEXT NOT NULL COLLATE NOCASE,
          degree_code TEXT NOT NULL COLLATE NOCASE,
          enabled INTEGER NOT NULL DEFAULT 1,
          UNIQUE(designation, degree_code)
        )"""))

