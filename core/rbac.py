# app/core/rbac.py
from __future__ import annotations
from typing import Optional, Set, Union
import streamlit as st
from sqlalchemy import text as sa_text
from sqlalchemy.engine import Engine, Connection
from core.settings import load_settings
from core.db import get_engine

__all__ = ["user_roles", "upsert_user", "get_user_id", "grant_role", "revoke_role"]

def _ensure_engine(engine: Optional[Engine] = None) -> Engine:
    if engine: return engine
    if "engine" in st.session_state: return st.session_state.engine
    settings = load_settings()
    eng = get_engine(settings.db.url)
    st.session_state.engine = eng
    return eng

def _table_has_column(conn: Connection, table: str, column: str) -> bool:
    info = conn.execute(sa_text(f"PRAGMA table_info({table})")).fetchall()
    return any(row[1] == column for row in info)

def _user_roles_schema_mode(conn: Connection) -> str:
    return "by_name" if _table_has_column(conn, "user_roles", "role_name") else "by_id"

def _ensure_roles_table(conn: Connection) -> None:
    conn.execute(sa_text("CREATE TABLE IF NOT EXISTS roles(id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT UNIQUE NOT NULL)"))

def _ensure_role_row(conn: Connection, role_name: str) -> Optional[int]:
    _ensure_roles_table(conn)
    conn.execute(sa_text("INSERT OR IGNORE INTO roles(name) VALUES(:n)"), {"n": role_name})
    row = conn.execute(sa_text("SELECT id FROM roles WHERE name=:n"), {"n": role_name}).fetchone()
    return int(row[0]) if row else None

def user_roles(engine: Optional[Engine], email: Optional[str]) -> Set[str]:
    if not email:
        return {"public"}
    engine = _ensure_engine(engine)
    with engine.begin() as conn:
        u = conn.execute(sa_text("SELECT id FROM users WHERE LOWER(email)=LOWER(:e) AND active=1"), {"e": email}).fetchone()
        if not u: return set()
        uid = int(u[0])
        mode = _user_roles_schema_mode(conn)
        if mode == "by_name":
            rows = conn.execute(sa_text("SELECT role_name FROM user_roles WHERE user_id=:uid"), {"uid": uid}).fetchall()
        else:
            rows = conn.execute(sa_text("SELECT r.name FROM user_roles ur JOIN roles r ON r.id = ur.role_id WHERE ur.user_id=:uid"), {"uid": uid}).fetchall()
        return {r[0] for r in rows} if rows else set()

def upsert_user(email: str, full_name: str = "", active: bool = True, employee_id: str = "", engine: Optional[Engine] = None) -> int:
    engine = _ensure_engine(engine)
    with engine.begin() as conn:
        conn.execute(sa_text("CREATE TABLE IF NOT EXISTS users(id INTEGER PRIMARY KEY AUTOINCREMENT, email TEXT NOT NULL UNIQUE, full_name TEXT, active INTEGER NOT NULL DEFAULT 1, created_at DATETIME DEFAULT CURRENT_TIMESTAMP, employee_id TEXT UNIQUE)"))
        
        # Use employee_id.strip() or None to handle empty strings gracefully with the UNIQUE constraint
        emp_id = employee_id.strip() or None
        
        conn.execute(
            sa_text("INSERT OR IGNORE INTO users(email, full_name, active, employee_id) VALUES(:e, :n, :a, :eid)"),
            {"e": email.lower(), "n": full_name, "a": 1 if active else 0, "eid": emp_id}
        )
        conn.execute(
            sa_text("UPDATE users SET full_name=:n, active=:a, employee_id=:eid WHERE LOWER(email)=LOWER(:e)"),
            {"n": full_name, "a": 1 if active else 0, "eid": emp_id, "e": email.lower()}
        )
        row = conn.execute(sa_text("SELECT id FROM users WHERE LOWER(email)=LOWER(:e)"), {"e": email.lower()}).fetchone()
        return int(row[0])

def get_user_id(engine_or_conn: Union[Engine, Connection], email: str) -> int:
    if isinstance(engine_or_conn, Engine):
        with engine_or_conn.begin() as conn:
            row = conn.execute(sa_text("SELECT id FROM users WHERE LOWER(email)=LOWER(:e)"), {"e": email}).fetchone()
            if not row: raise ValueError(f"User not found: {email}")
            return int(row[0])
    conn = engine_or_conn
    row = conn.execute(sa_text("SELECT id FROM users WHERE LOWER(email)=LOWER(:e)"), {"e": email}).fetchone()
    if not row: raise ValueError(f"User not found: {email}")
    return int(row[0])

def _count_superadmins(conn: Connection) -> int:
    mode = _user_roles_schema_mode(conn)
    if mode == "by_name":
        row = conn.execute(sa_text("SELECT COUNT(*) FROM user_roles WHERE role_name='superadmin'")).fetchone()
    else:
        row = conn.execute(sa_text("SELECT COUNT(*) FROM user_roles ur JOIN roles r ON r.id = ur.role_id WHERE r.name='superadmin'")).fetchone()
    return int(row[0]) if row else 0

def _user_has_role(conn: Connection, user_id: int, role_name: str) -> bool:
    mode = _user_roles_schema_mode(conn)
    if mode == "by_name":
        row = conn.execute(sa_text("SELECT 1 FROM user_roles WHERE user_id=:u AND role_name=:r LIMIT 1"), {"u": user_id, "r": role_name}).fetchone()
    else:
        row = conn.execute(sa_text("SELECT 1 FROM user_roles ur JOIN roles r ON r.id = ur.role_id WHERE ur.user_id=:u AND r.name=:r LIMIT 1"), {"u": user_id, "r": role_name}).fetchone()
    return bool(row)

def grant_role(email: str, role_name: str, engine: Optional[Engine] = None) -> None:
    engine = _ensure_engine(engine)
    with engine.begin() as conn:
        conn.execute(sa_text("CREATE TABLE IF NOT EXISTS user_roles(user_id INTEGER NOT NULL, role_id INTEGER, role_name TEXT, UNIQUE(user_id, role_id))"))
        uid = get_user_id(conn, email)
        mode = _user_roles_schema_mode(conn)
        if mode == "by_name":
            conn.execute(sa_text("INSERT OR IGNORE INTO user_roles(user_id, role_name) VALUES (:u, :r)"), {"u": uid, "r": role_name})
        else:
            rid = _ensure_role_row(conn, role_name)
            if rid is not None:
                conn.execute(sa_text("INSERT OR IGNORE INTO user_roles(user_id, role_id) VALUES (:u, :rid)"), {"u": uid, "rid": rid})

def revoke_role(email: str, role_name: str, engine: Optional[Engine] = None) -> None:
    engine = _ensure_engine(engine)
    with engine.begin() as conn:
        uid = get_user_id(conn, email)
        if role_name == "superadmin" and _user_has_role(conn, uid, "superadmin") and _count_superadmins(conn) <= 1:
            raise RuntimeError("Cannot revoke the only remaining superadmin.")
        mode = _user_roles_schema_mode(conn)
        if mode == "by_name":
            conn.execute(sa_text("DELETE FROM user_roles WHERE user_id=:u AND role_name=:r"), {"u": uid, "r": role_name})
        else:
            rid = _ensure_role_row(conn, role_name)
            if rid is not None:
                conn.execute(sa_text("DELETE FROM user_roles WHERE user_id=:u AND role_id=:rid"), {"u": uid, "rid": rid})
