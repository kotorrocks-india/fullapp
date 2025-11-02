from sqlalchemy import text as T
from core.schema_registry import register

@register
def install_faculty_audit(engine):
    """Create faculty audit log table for tracking all faculty-related changes."""
    # ... code
    with engine.begin() as c:
        c.execute(T("""
        CREATE TABLE IF NOT EXISTS faculty_audit(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          who TEXT,
          action TEXT,
          entity TEXT,
          entity_id TEXT,
          payload TEXT,
          at DATETIME DEFAULT CURRENT_TIMESTAMP
        )"""))
