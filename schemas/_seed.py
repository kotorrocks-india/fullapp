# app/schemas/_seed.py
from __future__ import annotations

import os
from sqlalchemy import text as sa_text
from core.schema_registry import register

# Optional bcrypt seeding for superadmin password
try:
    import bcrypt  # type: ignore
    _HAS_BCRYPT = True
except Exception:
    _HAS_BCRYPT = False

# ──────────────────────────────────────────────────────────────────────────────
# RBAC seeds (roles + a few demo users)
# ──────────────────────────────────────────────────────────────────────────────

ROLE_NAMES = [
    "superadmin",
    "tech_admin",
    "principal",
    "director",
    "academic_admin",
    "management_representative",
]

DEFAULT_USERS = {
    os.getenv("SEED_SUPERADMIN_EMAIL", "admin@example.com").lower(): (
        os.getenv("SEED_SUPERADMIN_NAME", "Super Admin"),
        ["superadmin"],
    ),
    os.getenv("SEED_TECHADMIN_EMAIL", "tech@example.com").lower(): (
        os.getenv("SEED_TECHADMIN_NAME", "Tech Admin"),
        ["tech_admin"],
    ),
    os.getenv("SEED_PRINCIPAL_EMAIL", "principal@example.com").lower(): (
        os.getenv("SEED_PRINCIPAL_NAME", "Principal"),
        ["principal"],
    ),
    os.getenv("SEED_DIRECTOR_EMAIL", "director@example.com").lower(): (
        os.getenv("SEED_DIRECTOR_NAME", "Director"),
        ["director"],
    ),
}

SEED_SHOULD_RUN = os.getenv("SEED_RUN", "1").lower() not in ("0", "false")

def _ensure_rbac_tables(conn):
    """Idempotent: create core RBAC tables if missing."""
    conn.execute(sa_text("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT NOT NULL UNIQUE,
            full_name TEXT,
            active INTEGER NOT NULL DEFAULT 1,
            employee_id TEXT UNIQUE,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """))
    conn.execute(sa_text("CREATE UNIQUE INDEX IF NOT EXISTS ix_users_employee_id ON users(employee_id);"))
    conn.execute(sa_text("""
        CREATE TABLE IF NOT EXISTS roles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE
        )
    """))
    conn.execute(sa_text("""
        CREATE TABLE IF NOT EXISTS user_roles (
            user_id INTEGER NOT NULL,
            role_id INTEGER NOT NULL,
            PRIMARY KEY (user_id, role_id),
            FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE,
            FOREIGN KEY(role_id) REFERENCES roles(id) ON DELETE CASCADE
        )
    """))
    conn.execute(sa_text("CREATE INDEX IF NOT EXISTS ix_user_roles_user ON user_roles(user_id)"))
    conn.execute(sa_text("CREATE INDEX IF NOT EXISTS ix_user_roles_role ON user_roles(role_id)"))

def _get_role_id(conn, role_name: str):
    row = conn.execute(sa_text("SELECT id FROM roles WHERE name=:n"), {"n": role_name}).fetchone()
    return row[0] if row else None

def _ensure_role(conn, role_name: str) -> int:
    conn.execute(sa_text("INSERT OR IGNORE INTO roles(name) VALUES(:n)"), {"n": role_name})
    return _get_role_id(conn, role_name)

def _get_user_id(conn, email: str):
    row = conn.execute(sa_text("SELECT id FROM users WHERE email=:e"), {"e": email}).fetchone()
    return row[0] if row else None

def _ensure_user(conn, email: str, full_name: str) -> int:
    conn.execute(
        sa_text("INSERT OR IGNORE INTO users(email, full_name, active) VALUES(:e, :n, 1)"),
        {"e": email.lower(), "n": full_name},
    )
    return _get_user_id(conn, email.lower())

def _grant_role(conn, user_id: int, role_id: int):
    conn.execute(
        sa_text("INSERT OR IGNORE INTO user_roles(user_id, role_id) VALUES(:u, :r)"),
        {"u": user_id, "r": role_id},
    )

@register
def seed_rbac(engine):
    """
    Seed base RBAC: roles and a few default users with their roles.
    Controlled via env var SEED_RUN (set to 0/false to skip).
    """
    if not SEED_SHOULD_RUN:
        return
    with engine.begin() as conn:
        _ensure_rbac_tables(conn)
        # roles
        for rn in ROLE_NAMES:
            _ensure_role(conn, rn)
        # users + grants
        for email, (full_name, roles) in DEFAULT_USERS.items():
            uid = _ensure_user(conn, email, full_name)
            for rn in roles:
                rid = _get_role_id(conn, rn)
                if rid:
                    _grant_role(conn, uid, rid)

# ──────────────────────────────────────────────────────────────────────────────
# Superadmin with username+password (for login.py bcrypt auth path)
# ──────────────────────────────────────────────────────────────────────────────

def _ensure_tech_admins_table(conn):
    """
    Ensure a credentials table (tech_admins) exists for bcrypt-backed login.
    This matches the auth path used by login.py for both tech_admin and superadmin.
    """
    conn.execute(sa_text("""
        CREATE TABLE IF NOT EXISTS tech_admins (
            user_id INTEGER PRIMARY KEY,
            username TEXT UNIQUE,
            password_hash TEXT,
            first_login_pending INTEGER NOT NULL DEFAULT 1,
            password_export_available INTEGER NOT NULL DEFAULT 0,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
        )
    """))
    conn.execute(sa_text("CREATE UNIQUE INDEX IF NOT EXISTS uq_tech_admins_username ON tech_admins(username)"))

@register
def ensure_superadmin_with_password(engine):
    """
    Idempotently ensure a Superadmin exists with a username+password hash
    stored in tech_admins (so the shared bcrypt login path works).
    Change the DEFAULT_* envs in production, run once, then rotate the password.
    """
    if not _HAS_BCRYPT:
        # If bcrypt isn't installed, skip silently; login will not support password auth.
        return

    DEFAULT_EMAIL = os.getenv("SA_EMAIL", "superadmin@demo.edu").lower()
    DEFAULT_USERNAME = os.getenv("SA_USERNAME", "superadmin")
    DEFAULT_PASSWORD = os.getenv("SA_PASSWORD", "Admin@1234")

    pw_hash = bcrypt.hashpw(DEFAULT_PASSWORD.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")

    with engine.begin() as conn:
        _ensure_rbac_tables(conn)       # users/roles/user_roles
        _ensure_tech_admins_table(conn) # credentials table used by login

        # Ensure superadmin role row
        rid = _ensure_role(conn, "superadmin")

        # Ensure user row
        uid = _ensure_user(conn, DEFAULT_EMAIL, "Super Admin")

        # Grant superadmin role
        if rid is not None:
            _grant_role(conn, uid, rid)

        # Upsert into tech_admins with bcrypt hash
        conn.execute(sa_text("""
            INSERT INTO tech_admins(user_id, username, password_hash, first_login_pending, password_export_available)
            VALUES(:u, :un, :ph, 1, 0)
            ON CONFLICT(user_id) DO UPDATE SET
                username=excluded.username,
                password_hash=excluded.password_hash
        """), {"u": uid, "un": DEFAULT_USERNAME, "ph": pw_hash})

# Faculty

@register
def seed_faculty_data(engine):
    """Seed initial faculty data: designations, affiliation types, and credits policy."""
    with engine.begin() as conn:
        # Seed designations
        designations = ["Lecturer", "Assistant Professor", "Associate Professor", "Professor", "Visiting Faculty"]
        for designation in designations:
            conn.execute(sa_text("INSERT OR IGNORE INTO designations(designation) VALUES(:d)"), {"d": designation})
        
        # Seed affiliation types (custom types registry)
        affiliation_types = [
            ("adjunct", "Adjunct Faculty"),
            ("research", "Research Faculty"),
            ("emeritus", "Emeritus Professor")
        ]
        for type_code, label in affiliation_types:
            conn.execute(sa_text("INSERT OR IGNORE INTO affiliation_types(type_code, label) VALUES(:code, :label)"), 
                        {"code": type_code, "label": label})
        
        # Seed default credits policy for principal & director (0/0 as per YAML defaults)
        default_credits = [
            ("principal", 0, 0),
            ("director", 0, 0)
        ]
        for designation, required, override in default_credits:
            # Insert for all degrees (*) or specific degrees as needed
            conn.execute(sa_text("""
                INSERT OR IGNORE INTO faculty_credits_policy(degree_code, designation, required_credits, allowed_credit_override)
                VALUES('*', :desg, :req, :ovr)
            """), {"desg": designation, "req": required, "ovr": override})
