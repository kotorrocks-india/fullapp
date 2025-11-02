# app/core/db.py
from __future__ import annotations
from pathlib import Path
from sqlalchemy import create_engine, text as sa_text
from sqlalchemy.orm import sessionmaker

from core.schema_registry import auto_discover, run_all

def get_engine(db_url: str):
    if db_url.startswith("sqlite:///"):
        db_file = db_url.replace("sqlite:///", "")
        Path(db_file).parent.mkdir(parents=True, exist_ok=True)
    engine = create_engine(db_url, future=True)
    return engine

def init_db(engine):
    # 0) ultra-core tables that other modules may rely on
    with engine.begin() as conn:
        conn.execute(sa_text("""
        CREATE TABLE IF NOT EXISTS configs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            degree TEXT NOT NULL DEFAULT 'default',
            namespace TEXT NOT NULL,
            config_json TEXT NOT NULL,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(degree, namespace)
        )"""))
        # keep your original UNIQUE on (degree, namespace)

    # 1) auto-discover schema modules (app/schemas/*.py)
    auto_discover("schemas")

    # 2) run all registered ensure_*_schema(engine) functions
    run_all(engine)

SessionLocal = sessionmaker(autocommit=False, autoflush=False)
