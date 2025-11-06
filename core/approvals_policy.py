# core/approvals_policy_enhanced.py
"""
Enhanced Approvals Policy System

Supports both:
1. Dynamic user-based approver assignments (managed by superadmin)
2. Traditional role-based approver rules (fallback)

Superadmins can assign specific users as approvers for specific actions
through the Approval Management page.
"""
from __future__ import annotations
import json
from typing import Set, Optional, Dict, Any
from sqlalchemy import text as sa_text
from sqlalchemy.engine import Engine

NAMESPACE = "approvals_policy"

# Default role-based policies (used as fallback)
DEFAULT_ROLE_POLICIES = {
    "degree.delete": {
        "approver_roles": ["superadmin", "principal", "director"],
        "rule": "either_one",
        "requires_reason": True
    },
    "program.delete": {
        "approver_roles": ["superadmin", "principal", "director"],
        "rule": "either_one",
        "requires_reason": True
    },
    "branch.delete": {
        "approver_roles": ["superadmin", "principal", "director"],
        "rule": "either_one",
        "requires_reason": True
    },
    "faculty.delete": {
        "approver_roles": ["superadmin", "principal", "director"],
        "rule": "either_one",
        "requires_reason": True
    },
    "semester.delete": {
        "approver_roles": ["superadmin", "principal", "director"],
        "rule": "either_one",
        "requires_reason": True
    },
    "affiliation.edit_in_use": {
        "approver_roles": ["principal", "director"],
        "rule": "either_one",
        "requires_reason": True
    },
    "semesters.binding_change": {
        "approver_roles": ["superadmin", "principal", "director"],
        "rule": "either_one",
        "requires_reason": False
    },
    "semesters.edit_structure": {
        "approver_roles": ["superadmin", "principal", "director"],
        "rule": "either_one",
        "requires_reason": True
    },
}


# ============================================================================
# DYNAMIC APPROVER ASSIGNMENT FUNCTIONS
# ============================================================================

def get_assigned_approvers(
    engine: Engine,
    object_type: str,
    action: str,
    degree_code: Optional[str] = None,
    program_code: Optional[str] = None,
    branch_code: Optional[str] = None,
) -> Set[str]:
    """
    Get the set of users explicitly assigned as approvers for this action.
    
    Args:
        engine: Database engine
        object_type: Type of object (degree, program, faculty, etc.)
        action: Action being performed (delete, edit, create, etc.)
        degree_code: Optional degree scope filter
        program_code: Optional program scope filter
        branch_code: Optional branch scope filter
    
    Returns:
        Set of approver email addresses
    
    Example:
        approvers = get_assigned_approvers(engine, "degree", "delete")
        # Returns: {'john@univ.edu', 'mary@univ.edu'}
    """
    with engine.begin() as conn:
        # Check if approver_assignments table exists
        table_exists = conn.execute(sa_text(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='approver_assignments'"
        )).fetchone()
        
        if not table_exists:
            return set()
        
        # Build query with scope filters
        query = """
            SELECT DISTINCT approver_email
            FROM approver_assignments
            WHERE object_type = :obj_type
              AND action = :action
              AND is_active = 1
        """
        params = {"obj_type": object_type, "action": action}
        
        # Add scope filters
        if degree_code:
            query += " AND (degree_code IS NULL OR degree_code = :degree)"
            params["degree"] = degree_code
        
        if program_code:
            query += " AND (program_code IS NULL OR program_code = :program)"
            params["program"] = program_code
        
        if branch_code:
            query += " AND (branch_code IS NULL OR branch_code = :branch)"
            params["branch"] = branch_code
        
        rows = conn.execute(sa_text(query), params).fetchall()
        return {row[0].lower().strip() for row in rows}


def get_approval_config(
    engine: Engine,
    object_type: str,
    action: str,
) -> Dict[str, Any]:
    """
    Get the approval configuration for an object type and action.
    
    Returns configuration like:
    {
        'require_user_assignment': True/False,
        'fallback_to_roles': True/False,
        'requires_reason': True/False,
        'min_approvers': 1,
        'approval_rule': 'either_one' | 'all' | 'majority'
    }
    """
    with engine.begin() as conn:
        # Check if config table exists
        table_exists = conn.execute(sa_text(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='approval_rules_config'"
        )).fetchone()
        
        if not table_exists:
            # Return defaults
            return {
                'require_user_assignment': True,
                'fallback_to_roles': True,
                'requires_reason': True,
                'min_approvers': 1,
                'approval_rule': 'either_one'
            }
        
        row = conn.execute(sa_text("""
            SELECT 
                require_user_assignment,
                fallback_to_roles,
                requires_reason,
                min_approvers,
                approval_rule
            FROM approval_rules_config
            WHERE object_type = :obj_type AND action = :action
        """), {"obj_type": object_type, "action": action}).fetchone()
        
        if not row:
            # Return defaults
            return {
                'require_user_assignment': True,
                'fallback_to_roles': True,
                'requires_reason': True,
                'min_approvers': 1,
                'approval_rule': 'either_one'
            }
        
        return {
            'require_user_assignment': bool(row[0]),
            'fallback_to_roles': bool(row[1]),
            'requires_reason': bool(row[2]),
            'min_approvers': int(row[3]),
            'approval_rule': row[4]
        }


def get_role_based_approvers(
    engine: Engine,
    object_type: str,
    action: str,
) -> Set[str]:
    """
    Get the set of roles that can approve this action (traditional approach).
    
    This is used as a fallback when no specific users are assigned.
    """
    key = f"{object_type}.{action}"
    
    # Try to get from configs table first
    with engine.begin() as conn:
        table_exists = conn.execute(sa_text(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='configs'"
        )).fetchone()
        
        if table_exists:
            row = conn.execute(sa_text("""
                SELECT config_json FROM configs
                WHERE degree = '*' AND namespace = :ns
                ORDER BY updated_at DESC LIMIT 1
            """), {"ns": NAMESPACE}).fetchone()
            
            if row and row[0]:
                try:
                    doc = json.loads(row[0]) or {}
                    policies = doc.get("policies", {})
                    policy = policies.get(key, {})
                    if policy:
                        return set(policy.get("approver_roles", []))
                except json.JSONDecodeError:
                    pass
    
    # Fall back to defaults
    if key in DEFAULT_ROLE_POLICIES:
        return set(DEFAULT_ROLE_POLICIES[key].get("approver_roles", []))
    
    return {"superadmin"}  # Ultimate fallback


# ============================================================================
# MAIN POLICY FUNCTIONS (Enhanced)
# ============================================================================

def approver_roles(
    engine: Engine,
    object_type: str,
    action: str,
    degree: Optional[str] = None,
    program: Optional[str] = None,
    branch: Optional[str] = None,
) -> Set[str]:
    """
    Get who can approve this action.
    
    Returns either:
    1. Set of specific user emails (if users are assigned)
    2. Set of role names (if falling back to role-based)
    
    The caller should check both:
    - If user's email is in the returned set (user-based)
    - If user's role is in the returned set (role-based)
    """
    config = get_approval_config(engine, object_type, action)
    
    # Try to get assigned users first
    if config['require_user_assignment']:
        assigned_users = get_assigned_approvers(
            engine, object_type, action,
            degree_code=degree,
            program_code=program,
            branch_code=branch
        )
        
        if assigned_users:
            # Return user emails (prefixed to distinguish from roles)
            return {f"user:{email}" for email in assigned_users}
        
        # If no users assigned and fallback is disabled, return empty
        if not config['fallback_to_roles']:
            return set()
    
    # Fall back to role-based
    return get_role_based_approvers(engine, object_type, action)


def rule(
    engine: Engine,
    object_type: str,
    action: str,
    degree: Optional[str] = None,
) -> str:
    """
    Get the approval rule type.
    
    Returns: 'either_one', 'all', or 'majority'
    """
    config = get_approval_config(engine, object_type, action)
    return config.get('approval_rule', 'either_one')


def requires_reason(
    engine: Engine,
    object_type: str,
    action: str,
    degree: Optional[str] = None,
) -> bool:
    """
    Check if reason is required for this action.
    """
    config = get_approval_config(engine, object_type, action)
    return config.get('requires_reason', True)


def min_approvers(
    engine: Engine,
    object_type: str,
    action: str,
) -> int:
    """
    Get minimum number of approvers required.
    """
    config = get_approval_config(engine, object_type, action)
    return config.get('min_approvers', 1)


def can_user_approve(
    engine: Engine,
    user_email: str,
    user_roles: Set[str],
    object_type: str,
    action: str,
    degree: Optional[str] = None,
    program: Optional[str] = None,
    branch: Optional[str] = None,
) -> bool:
    """
    Check if a specific user can approve this action.
    
    Checks both:
    1. If user is explicitly assigned as an approver
    2. If user has a role that can approve (when falling back to roles)
    
    Args:
        engine: Database engine
        user_email: User's email address
        user_roles: Set of user's role names
        object_type: Type of object
        action: Action being performed
        degree: Optional degree scope
        program: Optional program scope
        branch: Optional branch scope
    
    Returns:
        True if user can approve, False otherwise
    """
    approvers = approver_roles(engine, object_type, action, degree, program, branch)
    
    # Check if user email is in approvers (user-based)
    user_email_normalized = user_email.lower().strip()
    if f"user:{user_email_normalized}" in approvers:
        return True
    
    # Check if any of user's roles can approve (role-based)
    if user_roles & approvers:
        return True
    
    return False


# ============================================================================
# MANAGEMENT FUNCTIONS (for Superadmin UI)
# ============================================================================

def assign_approver(
    engine: Engine,
    object_type: str,
    action: str,
    approver_email: str,
    assigned_by: str,
    degree_code: Optional[str] = None,
    program_code: Optional[str] = None,
    branch_code: Optional[str] = None,
    notes: Optional[str] = None,
) -> int:
    """
    Assign a user as an approver for a specific object type and action.
    
    Returns: ID of the assignment record
    """
    with engine.begin() as conn:
        # Get approver name from users table
        user_row = conn.execute(sa_text("""
            SELECT full_name FROM users 
            WHERE LOWER(email) = LOWER(:email)
        """), {"email": approver_email}).fetchone()
        
        approver_name = user_row[0] if user_row else approver_email
        
        # Insert or reactivate assignment
        conn.execute(sa_text("""
            INSERT INTO approver_assignments (
                object_type, action, approver_email, approver_name,
                degree_code, program_code, branch_code,
                assigned_by, notes, is_active
            )
            VALUES (
                :obj_type, :action, :email, :name,
                :degree, :program, :branch,
                :assigned_by, :notes, 1
            )
            ON CONFLICT(object_type, action, approver_email, degree_code, program_code, branch_code)
            DO UPDATE SET
                is_active = 1,
                assigned_by = :assigned_by,
                assigned_at = CURRENT_TIMESTAMP,
                notes = :notes,
                deactivated_by = NULL,
                deactivated_at = NULL
        """), {
            "obj_type": object_type,
            "action": action,
            "email": approver_email.lower().strip(),
            "name": approver_name,
            "degree": degree_code,
            "program": program_code,
            "branch": branch_code,
            "assigned_by": assigned_by,
            "notes": notes
        })
        
        # Get the ID
        result = conn.execute(sa_text("""
            SELECT id FROM approver_assignments
            WHERE object_type = :obj_type
              AND action = :action
              AND LOWER(approver_email) = LOWER(:email)
              AND COALESCE(degree_code, '') = COALESCE(:degree, '')
              AND COALESCE(program_code, '') = COALESCE(:program, '')
              AND COALESCE(branch_code, '') = COALESCE(:branch, '')
        """), {
            "obj_type": object_type,
            "action": action,
            "email": approver_email,
            "degree": degree_code or '',
            "program": program_code or '',
            "branch": branch_code or ''
        }).fetchone()
        
        return result[0] if result else 0


def revoke_approver(
    engine: Engine,
    assignment_id: int,
    revoked_by: str,
) -> None:
    """
    Revoke an approver assignment (soft delete).
    """
    with engine.begin() as conn:
        conn.execute(sa_text("""
            UPDATE approver_assignments
            SET is_active = 0,
                deactivated_by = :revoked_by,
                deactivated_at = CURRENT_TIMESTAMP
            WHERE id = :id
        """), {"id": assignment_id, "revoked_by": revoked_by})


def list_all_approver_assignments(
    engine: Engine,
    active_only: bool = True,
) -> list[Dict[str, Any]]:
    """
    Get all approver assignments for management UI.
    """
    with engine.begin() as conn:
        query = """
            SELECT 
                id, object_type, action, approver_email, approver_name,
                degree_code, program_code, branch_code,
                is_active, assigned_by, assigned_at,
                deactivated_by, deactivated_at, notes
            FROM approver_assignments
        """
        if active_only:
            query += " WHERE is_active = 1"
        
        query += " ORDER BY object_type, action, approver_email"
        
        rows = conn.execute(sa_text(query)).fetchall()
        return [dict(r._mapping) for r in rows]


def get_approver_stats(engine: Engine, approver_email: str) -> Dict[str, Any]:
    """
    Get statistics for a specific approver.
    
    Returns:
    {
        'assigned_count': int,  # Number of action types they can approve
        'pending_count': int,   # Number of approvals waiting for them
        'approved_count': int,  # Total they've approved
        'rejected_count': int,  # Total they've rejected
    }
    """
    with engine.begin() as conn:
        # Count assignments
        assigned = conn.execute(sa_text("""
            SELECT COUNT(*) FROM approver_assignments
            WHERE LOWER(approver_email) = LOWER(:email) AND is_active = 1
        """), {"email": approver_email}).fetchone()
        
        # Count pending approvals they can act on
        pending = conn.execute(sa_text("""
            SELECT COUNT(DISTINCT a.id)
            FROM approvals a
            JOIN approver_assignments aa 
                ON aa.object_type = a.object_type 
                AND aa.action = a.action
            WHERE LOWER(aa.approver_email) = LOWER(:email)
              AND aa.is_active = 1
              AND a.status IN ('pending', 'under_review')
        """), {"email": approver_email}).fetchone()
        
        # Count approved
        approved = conn.execute(sa_text("""
            SELECT COUNT(*) FROM approvals
            WHERE LOWER(approver) = LOWER(:email) AND status = 'approved'
        """), {"email": approver_email}).fetchone()
        
        # Count rejected
        rejected = conn.execute(sa_text("""
            SELECT COUNT(*) FROM approvals
            WHERE LOWER(approver) = LOWER(:email) AND status = 'rejected'
        """), {"email": approver_email}).fetchone()
        
        return {
            'assigned_count': assigned[0] if assigned else 0,
            'pending_count': pending[0] if pending else 0,
            'approved_count': approved[0] if approved else 0,
            'rejected_count': rejected[0] if rejected else 0,
        }
