# schemas/office_admin_schema.py
"""
Schema for Office Admin accounts and their management.
Office Admins are staff members who can manage students, run reports,
but cannot configure system settings or manage faculty/academic structure.

Office admins are scoped to organizational units:
- Global scope: Can access all degrees/programs/branches
- Degree scope: Can access all programs/branches within a degree
- Program scope: Can access all branches within a program
- Branch scope: Can access only that specific branch
"""
from __future__ import annotations
from sqlalchemy import text as sa_text
from core.schema_registry import register

@register("office_admin")
def install_office_admin_schema(engine) -> None:
    """Create tables for office admin account management with organizational scoping."""
    ddl = """
    PRAGMA foreign_keys = ON;

    -- Office Admin Accounts (staff who manage students/reports)
    CREATE TABLE IF NOT EXISTS office_admin_accounts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        email TEXT NOT NULL UNIQUE,
        username TEXT UNIQUE,
        full_name TEXT NOT NULL,
        designation TEXT,          -- e.g., Jr. Office Admin, Sr. Office Admin
        status TEXT NOT NULL DEFAULT 'active',  -- active | disabled
        home_degree_code TEXT,     -- primary affiliation (optional)
        home_program_id INTEGER,
        home_branch_id INTEGER,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        created_by TEXT,
        last_login DATETIME,
        password_hash TEXT,
        totp_secret TEXT,
        must_change_password INTEGER DEFAULT 0,
        password_changed_at DATETIME,
        disabled_reason TEXT,
        disabled_at DATETIME,
        disabled_by TEXT
    );

    -- Office Admin Scope Assignments (hierarchical access control)
    CREATE TABLE IF NOT EXISTS office_admin_scopes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        admin_email TEXT NOT NULL,
        scope_type TEXT NOT NULL,  -- global | degree | program | branch
        scope_value TEXT,          -- NULL for global, degree_code for degree, program_id for program, branch_id for branch
        degree_code TEXT,          -- For easy filtering
        program_id INTEGER,
        branch_id INTEGER,
        active INTEGER DEFAULT 1,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        created_by TEXT,
        notes TEXT,
        FOREIGN KEY (admin_email) REFERENCES office_admin_accounts(email) ON DELETE CASCADE,
        UNIQUE(admin_email, scope_type, scope_value)
    );

    -- Office Admin Permissions/Capabilities (per admin, optional overrides)
    CREATE TABLE IF NOT EXISTS office_admin_permissions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        admin_email TEXT NOT NULL,
        can_manage_students INTEGER DEFAULT 1,
        can_run_reports INTEGER DEFAULT 1,
        can_export_data INTEGER DEFAULT 0,
        can_unlock_pii INTEGER DEFAULT 0,
        can_bulk_operations INTEGER DEFAULT 0,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(admin_email),
        FOREIGN KEY (admin_email) REFERENCES office_admin_accounts(email) ON DELETE CASCADE
    );

    -- PII Access Audit (tracks when office admins unmask PII)
    CREATE TABLE IF NOT EXISTS office_admin_pii_access (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        admin_email TEXT NOT NULL,
        student_id INTEGER,
        student_name TEXT,
        degree_code TEXT,
        program_id INTEGER,
        branch_id INTEGER,
        reason TEXT NOT NULL,
        approved_by TEXT,
        approval_type TEXT,  -- principal | director
        accessed_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        step_up_session_id TEXT,
        ip_address TEXT,
        user_agent TEXT
    );

    -- Export Requests (students roster, attendance, marks, credentials)
    CREATE TABLE IF NOT EXISTS office_admin_export_requests (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        request_code TEXT NOT NULL UNIQUE,
        admin_email TEXT NOT NULL,
        entity_type TEXT NOT NULL,  -- students_roster | attendance_summary | marks_summary | initial_credentials
        scope_type TEXT,            -- degree | program | branch
        scope_value TEXT,           -- degree_code, program_id, or branch_id
        reason TEXT NOT NULL,
        status TEXT DEFAULT 'pending',  -- pending | approved | rejected | completed
        requested_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        approved_by TEXT,
        approved_at DATETIME,
        rejection_reason TEXT,
        completed_at DATETIME,
        file_path TEXT
    );

    -- Bulk Operations Log
    CREATE TABLE IF NOT EXISTS office_admin_bulk_operations (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        operation_code TEXT NOT NULL UNIQUE,
        admin_email TEXT NOT NULL,
        operation_type TEXT NOT NULL,  -- disable_accounts | enable_accounts | reset_totp | bulk_move
        scope_type TEXT,
        scope_value TEXT,
        target_count INTEGER DEFAULT 0,
        reason TEXT,
        status TEXT DEFAULT 'pending',
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        completed_at DATETIME,
        error_log TEXT
    );

    -- Student Delete/Archive Requests
    CREATE TABLE IF NOT EXISTS office_admin_delete_requests (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        request_code TEXT NOT NULL UNIQUE,
        admin_email TEXT NOT NULL,
        student_id INTEGER NOT NULL,
        student_name TEXT,
        degree_code TEXT,
        program_id INTEGER,
        branch_id INTEGER,
        reason TEXT NOT NULL,
        has_child_data INTEGER DEFAULT 0,  -- 1 if student has linked data
        action_type TEXT,  -- hard_delete | archive_disable
        status TEXT DEFAULT 'pending',
        requested_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        approved_by TEXT,
        approved_at DATETIME,
        completed_at DATETIME
    );

    -- Audit Trail for Office Admin Actions
    CREATE TABLE IF NOT EXISTS office_admin_audit (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        actor_email TEXT NOT NULL,
        actor_role TEXT,
        action TEXT NOT NULL,
        target_type TEXT,
        target_id INTEGER,
        scope_type TEXT,
        scope_value TEXT,
        reason TEXT,
        diff_before TEXT,  -- JSON
        diff_after TEXT,   -- JSON
        ip_address TEXT,
        user_agent TEXT,
        step_up_session_id TEXT,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    );

    -- Step-up Authentication Sessions
    CREATE TABLE IF NOT EXISTS office_admin_step_up_sessions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        session_id TEXT NOT NULL UNIQUE,
        admin_email TEXT NOT NULL,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        expires_at DATETIME NOT NULL,
        reason TEXT,
        is_active INTEGER DEFAULT 1
    );

    CREATE INDEX IF NOT EXISTS idx_oa_accounts_email ON office_admin_accounts(email);
    CREATE INDEX IF NOT EXISTS idx_oa_accounts_status ON office_admin_accounts(status);
    CREATE INDEX IF NOT EXISTS idx_oa_scopes_email ON office_admin_scopes(admin_email, active);
    CREATE INDEX IF NOT EXISTS idx_oa_scopes_degree ON office_admin_scopes(degree_code, active);
    CREATE INDEX IF NOT EXISTS idx_oa_scopes_program ON office_admin_scopes(program_id, active);
    CREATE INDEX IF NOT EXISTS idx_oa_scopes_branch ON office_admin_scopes(branch_id, active);
    CREATE INDEX IF NOT EXISTS idx_oa_pii_admin ON office_admin_pii_access(admin_email);
    CREATE INDEX IF NOT EXISTS idx_oa_exports_admin ON office_admin_export_requests(admin_email, status);
    CREATE INDEX IF NOT EXISTS idx_oa_audit_actor ON office_admin_audit(actor_email, created_at);
    CREATE INDEX IF NOT EXISTS idx_oa_step_up_active ON office_admin_step_up_sessions(admin_email, is_active, expires_at);
    """

    with engine.begin() as conn:
        for stmt in filter(None, ddl.split(";")):
            s = stmt.strip()
            if s:
                conn.execute(sa_text(s + ";"))
