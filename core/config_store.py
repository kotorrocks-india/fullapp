from __future__ import annotations
import json
from typing import Optional, Tuple, List
from sqlalchemy import text as sql_text

MAX_VERSIONS = 50  # cap history

def ensure_schema(engine):
    with engine.begin() as conn:
        conn.execute(sql_text("""
        CREATE TABLE IF NOT EXISTS configs_versions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            degree TEXT NOT NULL,
            namespace TEXT NOT NULL,
            version INTEGER NOT NULL,
            config_json TEXT NOT NULL,
            saved_by TEXT,
            reason TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(degree, namespace, version)
        );
        """))

def get(engine, degree: str, namespace: str) -> dict:
    with engine.begin() as conn:
        row = conn.execute(sql_text(
            "SELECT config_json FROM configs WHERE degree=:d AND namespace=:ns"
        ), dict(d=degree, ns=namespace)).fetchone()
    if not row:
        return {}
    try:
        return json.loads(row[0]) or {}
    except Exception:
        return {}

def save(engine, degree: str, namespace: str, new_cfg: dict, saved_by: Optional[str] = None, reason: str = "") -> Tuple[int, dict]:
    ensure_schema(engine)
    current = get(engine, degree, namespace)
    with engine.begin() as conn:
        vrow = conn.execute(sql_text(
            "SELECT COALESCE(MAX(version), 0) FROM configs_versions WHERE degree=:d AND namespace=:ns"
        ), dict(d=degree, ns=namespace)).fetchone()
        next_ver = (vrow[0] or 0) + 1
        if current:
            conn.execute(sql_text("""
                INSERT OR IGNORE INTO configs_versions (degree, namespace, version, config_json, saved_by, reason)
                VALUES (:d, :ns, :v, :cfg, :by, :why)
            """), dict(d=degree, ns=namespace, v=next_ver, cfg=json.dumps(current, ensure_ascii=False), by=saved_by, why=reason or "auto-version"))
        conn.execute(sql_text("""
            INSERT INTO configs (degree, namespace, config_json)
            VALUES (:d, :ns, :cfg)
            ON CONFLICT(degree, namespace) DO UPDATE
            SET config_json=excluded.config_json, updated_at=CURRENT_TIMESTAMP
        """), dict(d=degree, ns=namespace, cfg=json.dumps(new_cfg, ensure_ascii=False)))
        rows = conn.execute(sql_text("""
            SELECT id FROM configs_versions WHERE degree=:d AND namespace=:ns ORDER BY version DESC
        """), dict(d=degree, ns=namespace)).fetchall()
        if len(rows) > MAX_VERSIONS:
            to_delete = [r[0] for r in rows[MAX_VERSIONS:]]
            if to_delete:
                conn.execute(sql_text(
                    "DELETE FROM configs_versions WHERE id IN (%s)" % ",".join([str(i) for i in to_delete])
                ))
    return next_ver, current

def history(engine, degree: str, namespace: str) -> List[dict]:
    ensure_schema(engine)
    with engine.begin() as conn:
        rows = conn.execute(sql_text("""
            SELECT version, saved_by, reason, created_at, config_json
            FROM configs_versions
            WHERE degree=:d AND namespace=:ns
            ORDER BY version DESC
        """), dict(d=degree, ns=namespace)).fetchall()
    return [
        {"version": r[0], "saved_by": r[1], "reason": r[2], "created_at": r[3], "config": json.loads(r[4]) if r[4] else {}}
        for r in rows
    ]

def rollback(engine, degree: str, namespace: str, version: int, saved_by: Optional[str] = None, reason: str = "rollback") -> bool:
    ensure_schema(engine)
    with engine.begin() as conn:
        row = conn.execute(sql_text("""
            SELECT config_json FROM configs_versions
            WHERE degree=:d AND namespace=:ns AND version=:v
        """), dict(d=degree, ns=namespace, v=version)).fetchone()
        if not row:
            return False
        conn.execute(sql_text("""
            INSERT INTO configs (degree, namespace, config_json)
            VALUES (:d, :ns, :cfg)
            ON CONFLICT(degree, namespace) DO UPDATE SET config_json=excluded.config_json, updated_at=CURRENT_TIMESTAMP
        """), dict(d=degree, ns=namespace, cfg=row[0]))
        conn.execute(sql_text("""
            INSERT OR IGNORE INTO configs_versions (degree, namespace, version, config_json, saved_by, reason)
            VALUES (:d, :ns, (SELECT COALESCE(MAX(version),0)+1 FROM configs_versions WHERE degree=:d AND namespace=:ns), :cfg, :by, :why)
        """), dict(d=degree, ns=namespace, cfg=row[0], by=saved_by, why=reason))
    return True
