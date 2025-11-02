from sqlalchemy import text as sa_text
from core.schema_registry import register

@register
def ensure_initial_credentials_schema(engine):
    with engine.begin() as conn:
        conn.execute(sa_text("""
        CREATE TABLE IF NOT EXISTS initial_credentials (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            username TEXT NOT NULL,
            plaintext TEXT NOT NULL,           -- keep very short-lived
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            consumed INTEGER NOT NULL DEFAULT 0,  -- 1 after export or first_login
            FOREIGN KEY(user_id) REFERENCES users(id)
        )
        """))
        conn.execute(sa_text("CREATE INDEX IF NOT EXISTS ix_initcred_userid ON initial_credentials(user_id)"))
        conn.execute(sa_text("CREATE INDEX IF NOT EXISTS ix_initcred_consumed ON initial_credentials(consumed)"))
