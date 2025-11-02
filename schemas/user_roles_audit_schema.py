from sqlalchemy import text as sa_text
from core.schema_registry import register

@register
def ensure_user_roles_audit_schema(engine):
    with engine.begin() as conn:
        conn.execute(sa_text("""
        CREATE TABLE IF NOT EXISTS user_roles_audit (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            target_email TEXT NOT NULL,
            role_name TEXT NOT NULL,
            action TEXT NOT NULL CHECK(action IN ('grant','revoke')),
            actor_email TEXT NOT NULL,
            note TEXT,
            at DATETIME DEFAULT CURRENT_TIMESTAMP
        )"""))
