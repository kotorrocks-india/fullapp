# screens/office_admin/db.py
from __future__ import annotations
from typing import List, Dict, Any, Optional
from sqlalchemy import text as sa_text
from datetime import datetime, timedelta
import secrets

# ------- OFFICE ADMIN ACCOUNTS -------
def list_office_admins(conn, status: Optional[str] = None) -> List[Dict[str, Any]]:
    """List all office admin accounts with their scopes."""
    if status:
        sql = sa_text("""
            SELECT a.*, 
                   GROUP_CONCAT(s.scope_type || ':' || COALESCE(s.scope_value, 'global'), ', ') as scopes
            FROM office_admin_accounts a
            LEFT JOIN office_admin_scopes s ON a.email = s.admin_email AND s.active = 1
            WHERE a.status=:st
            GROUP BY a.id
            ORDER BY a.full_name
        """)
        rows = conn.execute(sql, {"st": status}).fetchall()
    else:
        sql = sa_text("""
            SELECT a.*, 
                   GROUP_CONCAT(s.scope_type || ':' || COALESCE(s.scope_value, 'global'), ', ') as scopes
            FROM office_admin_accounts a
            LEFT JOIN office_admin_scopes s ON a.email = s.admin_email AND s.active = 1
            GROUP BY a.id
            ORDER BY a.full_name
        """)
        rows = conn.execute(sql).fetchall()
    return [dict(r) for r in rows]

def get_office_admin(conn, email: str) -> Optional[Dict[str, Any]]:
    """Get a single office admin by email."""
    sql = sa_text("SELECT * FROM office_admin_accounts WHERE email=:email")
    row = conn.execute(sql, {"email": email}).fetchone()
    return dict(row) if row else None

def get_admin_scopes(conn, email: str) -> List[Dict[str, Any]]:
    """Get all scope assignments for an admin."""
    sql = sa_text("""
        SELECT * FROM office_admin_scopes 
        WHERE admin_email=:email AND active=1
        ORDER BY 
            CASE scope_type
                WHEN 'global' THEN 1
                WHEN 'degree' THEN 2
                WHEN 'program' THEN 3
                WHEN 'branch' THEN 4
            END
    """)
    rows = conn.execute(sql, {"email": email}).fetchall()
    return [dict(r) for r in rows]

def create_office_admin(conn, payload: Dict[str, Any], created_by: str) -> int:
    """Create a new office admin account."""
    sql = sa_text("""
        INSERT INTO office_admin_accounts(email, username, full_name, status, created_by, password_hash, must_change_password)
        VALUES (:email, :username, :name, :status, :created_by, :pw_hash, 1)
    """)
    res = conn.execute(sql, {
        "email": payload["email"],
        "username": payload.get("username"),
        "name": payload["full_name"],
        "status": "active",
        "created_by": created_by,
        "pw_hash": payload.get("password_hash")
    })
    return res.lastrowid

def update_office_admin(conn, email: str, payload: Dict[str, Any]) -> None:
    """Update office admin details."""
    sql = sa_text("""
        UPDATE office_admin_accounts
        SET full_name=:name, username=:username, updated_at=CURRENT_TIMESTAMP
        WHERE email=:email
    """)
    conn.execute(sql, {
        "email": email,
        "name": payload.get("full_name"),
        "username": payload.get("username")
    })

def disable_office_admin(conn, email: str, reason: str, disabled_by: str) -> None:
    """Disable an office admin account."""
    sql = sa_text("""
        UPDATE office_admin_accounts
        SET status='disabled', disabled_reason=:reason, disabled_at=CURRENT_TIMESTAMP, disabled_by=:by, updated_at=CURRENT_TIMESTAMP
        WHERE email=:email
    """)
    conn.execute(sql, {"email": email, "reason": reason, "by": disabled_by})

def enable_office_admin(conn, email: str) -> None:
    """Enable a disabled office admin account."""
    sql = sa_text("""
        UPDATE office_admin_accounts
        SET status='active', disabled_reason=NULL, disabled_at=NULL, disabled_by=NULL, updated_at=CURRENT_TIMESTAMP
        WHERE email=:email
    """)
    conn.execute(sql, {"email": email})

# ------- SCOPE MANAGEMENT -------
def assign_scope(conn, payload: Dict[str, Any], created_by: str) -> int:
    """
    Assign a scope to an office admin.
    
    scope_type: 'global' | 'degree' | 'program' | 'branch'
    scope_value: NULL for global, degree_code, program_id, or branch_id
    """
    sql = sa_text("""
        INSERT INTO office_admin_scopes(admin_email, scope_type, scope_value, degree_code, program_id, branch_id, created_by, notes)
        VALUES (:email, :type, :value, :degree, :program, :branch, :by, :notes)
    """)
    res = conn.execute(sql, {
        "email": payload["admin_email"],
        "type": payload["scope_type"],
        "value": payload.get("scope_value"),
        "degree": payload.get("degree_code"),
        "program": payload.get("program_id"),
        "branch": payload.get("branch_id"),
        "by": created_by,
        "notes": payload.get("notes")
    })
    return res.lastrowid

def revoke_scope(conn, scope_id: int) -> None:
    """Revoke a scope assignment."""
    sql = sa_text("UPDATE office_admin_scopes SET active=0 WHERE id=:id")
    conn.execute(sql, {"id": scope_id})

def get_admins_for_scope(conn, scope_type: str, scope_value: Optional[str] = None) -> List[Dict[str, Any]]:
    """Get all admins who have access to a specific scope (including inherited access)."""
    # Admins can access a scope if:
    # 1. They have global scope
    # 2. They have the exact scope
    # 3. They have a parent scope (e.g., degree admin can access programs/branches in that degree)
    
    if scope_type == "global":
        sql = sa_text("""
            SELECT DISTINCT a.* FROM office_admin_accounts a
            JOIN office_admin_scopes s ON a.email = s.admin_email
            WHERE s.active = 1 AND s.scope_type = 'global' AND a.status = 'active'
        """)
        rows = conn.execute(sql).fetchall()
    elif scope_type == "degree":
        sql = sa_text("""
            SELECT DISTINCT a.* FROM office_admin_accounts a
            JOIN office_admin_scopes s ON a.email = s.admin_email
            WHERE s.active = 1 AND a.status = 'active'
              AND (s.scope_type = 'global' OR (s.scope_type = 'degree' AND s.degree_code = :value))
        """)
        rows = conn.execute(sql, {"value": scope_value}).fetchall()
    elif scope_type == "program":
        sql = sa_text("""
            SELECT DISTINCT a.* FROM office_admin_accounts a
            JOIN office_admin_scopes s ON a.email = s.admin_email
            LEFT JOIN programs p ON s.program_id = p.id
            WHERE s.active = 1 AND a.status = 'active'
              AND (s.scope_type = 'global' 
                   OR (s.scope_type = 'degree' AND s.degree_code = (SELECT degree_code FROM programs WHERE id = :value))
                   OR (s.scope_type = 'program' AND s.program_id = :value))
        """)
        rows = conn.execute(sql, {"value": scope_value}).fetchall()
    elif scope_type == "branch":
        sql = sa_text("""
            SELECT DISTINCT a.* FROM office_admin_accounts a
            JOIN office_admin_scopes s ON a.email = s.admin_email
            LEFT JOIN branches b ON s.branch_id = b.id
            LEFT JOIN programs p ON b.program_id = p.id
            WHERE s.active = 1 AND a.status = 'active'
              AND (s.scope_type = 'global'
                   OR (s.scope_type = 'degree' AND s.degree_code = (SELECT p2.degree_code FROM branches b2 JOIN programs p2 ON b2.program_id = p2.id WHERE b2.id = :value))
                   OR (s.scope_type = 'program' AND s.program_id = (SELECT program_id FROM branches WHERE id = :value))
                   OR (s.scope_type = 'branch' AND s.branch_id = :value))
        """)
        rows = conn.execute(sql, {"value": scope_value}).fetchall()
    else:
        return []
    
    return [dict(r) for r in rows]

def check_admin_access(conn, admin_email: str, scope_type: str, scope_value: Optional[str] = None) -> bool:
    """Check if an admin has access to a specific scope."""
    admins = get_admins_for_scope(conn, scope_type, scope_value)
    return any(a["email"] == admin_email for a in admins)

# ------- PII ACCESS -------
def log_pii_access(conn, payload: Dict[str, Any]) -> int:
    """Log PII access event."""
    sql = sa_text("""
        INSERT INTO office_admin_pii_access(admin_email, student_id, student_name, degree_code, program_id, branch_id, reason, approved_by, approval_type, step_up_session_id, ip_address)
        VALUES (:admin, :student_id, :student_name, :degree, :program, :branch, :reason, :approved_by, :approval_type, :session_id, :ip)
    """)
    res = conn.execute(sql, {
        "admin": payload["admin_email"],
        "student_id": payload.get("student_id"),
        "student_name": payload.get("student_name"),
        "degree": payload.get("degree_code"),
        "program": payload.get("program_id"),
        "branch": payload.get("branch_id"),
        "reason": payload["reason"],
        "approved_by": payload.get("approved_by"),
        "approval_type": payload.get("approval_type"),
        "session_id": payload.get("step_up_session_id"),
        "ip": payload.get("ip_address")
    })
    return res.lastrowid

def list_pii_access_log(conn, admin_email: Optional[str] = None, degree_code: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
    """List PII access log entries."""
    conditions = []
    params = {"limit": limit}
    
    if admin_email:
        conditions.append("admin_email=:admin")
        params["admin"] = admin_email
    if degree_code:
        conditions.append("degree_code=:degree")
        params["degree"] = degree_code
    
    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    sql = sa_text(f"SELECT * FROM office_admin_pii_access {where} ORDER BY accessed_at DESC LIMIT :limit")
    rows = conn.execute(sql, params).fetchall()
    return [dict(r) for r in rows]

# ------- EXPORT REQUESTS -------
def create_export_request(conn, payload: Dict[str, Any]) -> str:
    """Create a new export request."""
    request_code = f"EXP-{datetime.now().strftime('%Y%m%d')}-{secrets.token_hex(4).upper()}"
    sql = sa_text("""
        INSERT INTO office_admin_export_requests(request_code, admin_email, entity_type, scope_type, scope_value, reason, status)
        VALUES (:code, :admin, :entity, :scope_type, :scope_value, :reason, 'pending')
    """)
    conn.execute(sql, {
        "code": request_code,
        "admin": payload["admin_email"],
        "entity": payload["entity_type"],
        "scope_type": payload.get("scope_type"),
        "scope_value": payload.get("scope_value"),
        "reason": payload["reason"]
    })
    return request_code

def list_export_requests(conn, status: Optional[str] = None, admin_email: Optional[str] = None) -> List[Dict[str, Any]]:
    """List export requests."""
    conditions = []
    params = {}
    
    if status:
        conditions.append("status=:status")
        params["status"] = status
    if admin_email:
        conditions.append("admin_email=:admin")
        params["admin"] = admin_email
    
    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    sql = sa_text(f"SELECT * FROM office_admin_export_requests {where} ORDER BY requested_at DESC")
    rows = conn.execute(sql, params).fetchall()
    return [dict(r) for r in rows]

def approve_export_request(conn, request_code: str, approved_by: str) -> None:
    """Approve an export request."""
    sql = sa_text("""
        UPDATE office_admin_export_requests
        SET status='approved', approved_by=:by, approved_at=CURRENT_TIMESTAMP
        WHERE request_code=:code
    """)
    conn.execute(sql, {"code": request_code, "by": approved_by})

def reject_export_request(conn, request_code: str, reason: str) -> None:
    """Reject an export request."""
    sql = sa_text("""
        UPDATE office_admin_export_requests
        SET status='rejected', rejection_reason=:reason
        WHERE request_code=:code
    """)
    conn.execute(sql, {"code": request_code, "reason": reason})

# ------- STEP-UP SESSIONS -------
def create_step_up_session(conn, admin_email: str, reason: str, ttl_minutes: int = 15) -> str:
    """Create a step-up authentication session."""
    session_id = secrets.token_urlsafe(32)
    expires_at = datetime.now() + timedelta(minutes=ttl_minutes)
    
    sql = sa_text("""
        INSERT INTO office_admin_step_up_sessions(session_id, admin_email, reason, expires_at, is_active)
        VALUES (:sid, :email, :reason, :expires, 1)
    """)
    conn.execute(sql, {
        "sid": session_id,
        "email": admin_email,
        "reason": reason,
        "expires": expires_at.isoformat()
    })
    return session_id

def validate_step_up_session(conn, session_id: str, admin_email: str) -> bool:
    """Check if a step-up session is valid."""
    sql = sa_text("""
        SELECT COUNT(*) FROM office_admin_step_up_sessions
        WHERE session_id=:sid AND admin_email=:email AND is_active=1 AND expires_at > datetime('now')
    """)
    count = conn.execute(sql, {"sid": session_id, "email": admin_email}).scalar()
    return count > 0

def invalidate_step_up_session(conn, session_id: str) -> None:
    """Invalidate a step-up session."""
    sql = sa_text("UPDATE office_admin_step_up_sessions SET is_active=0 WHERE session_id=:sid")
    conn.execute(sql, {"sid": session_id})

# ------- AUDIT -------
def log_audit(conn, payload: Dict[str, Any]) -> int:
    """Log an audit event."""
    sql = sa_text("""
        INSERT INTO office_admin_audit(actor_email, actor_role, action, target_type, target_id, scope_type, scope_value, reason, diff_before, diff_after, ip_address, step_up_session_id)
        VALUES (:actor, :role, :action, :target_type, :target_id, :scope_type, :scope_value, :reason, :before, :after, :ip, :session_id)
    """)
    res = conn.execute(sql, {
        "actor": payload["actor_email"],
        "role": payload.get("actor_role"),
        "action": payload["action"],
        "target_type": payload.get("target_type"),
        "target_id": payload.get("target_id"),
        "scope_type": payload.get("scope_type"),
        "scope_value": payload.get("scope_value"),
        "reason": payload.get("reason"),
        "before": payload.get("diff_before"),
        "after": payload.get("diff_after"),
        "ip": payload.get("ip_address"),
        "session_id": payload.get("step_up_session_id")
    })
    return res.lastrowid

def list_audit_log(conn, actor_email: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
    """List audit log entries."""
    if actor_email:
        sql = sa_text("SELECT * FROM office_admin_audit WHERE actor_email=:actor ORDER BY created_at DESC LIMIT :limit")
        rows = conn.execute(sql, {"actor": actor_email, "limit": limit}).fetchall()
    else:
        sql = sa_text("SELECT * FROM office_admin_audit ORDER BY created_at DESC LIMIT :limit")
        rows = conn.execute(sql, {"limit": limit}).fetchall()
    return [dict(r) for r in rows]
