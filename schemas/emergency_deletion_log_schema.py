from sqlalchemy import text as T
from core.schema_registry import register

@register
def install_emergency_deletion_log(engine):
    """Create emergency deletion log table for tracking emergency profile deletions."""
    # ... code
    with engine.begin() as c:
        c.execute(T("""
        CREATE TABLE IF NOT EXISTS emergency_deletion_log(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          admin_email TEXT NOT NULL,
          deleted_profile_email TEXT NOT NULL,
          reason TEXT NOT NULL,
          deleted_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )"""))
