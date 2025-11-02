# app/core/approvals_policy.py
from __future__ import annotations
import json
from sqlalchemy import text as sa_text

NAMESPACE = "approvals_policy"

# Default policies for critical operations
DEFAULT_POLICIES = {
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
    "user.role_change": {
        "approver_roles": ["superadmin"],
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
    # add inside DEFAULT_POLICIES
    "faculty.delete": {
        "approver_roles": ["superadmin", "principal", "director"],
        "rule": "either_one",
        "requires_reason": True
    },

    
    # --- POLICY ADDED AS PER todo.txt ---
    "affiliation.edit_in_use": {
        "approver_roles": ["principal", "director"],
        "rule": "either_one",
        "requires_reason": True
    }
    # --- END OF ADDED POLICY ---
}

# ─────────────────────────── Schema Helpers ───────────────────────────

def _table_exists(conn, table_name: str) -> bool:
    """Check if a table exists in the database."""
    try:
        row = conn.execute(sa_text(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=:t"
        ), {"t": table_name}).fetchone()
        return bool(row)
    except Exception:
        return False

def _table_has_column(conn, table_name: str, column: str) -> bool:
    """Check if a table has a specific column."""
    try:
        info = conn.execute(sa_text(f"PRAGMA table_info({table_name})")).fetchall()
        return any(row[1] == column for row in info)
    except Exception:
        return False

def _validate_configs_schema(conn) -> bool:
    """Validate that the configs table has the required schema."""
    if not _table_exists(conn, "configs"):
        return False

    required_cols = {"degree", "namespace", "config_json"}
    return all(_table_has_column(conn, "configs", col) for col in required_cols)

# ─────────────────────────── Policy Loading ───────────────────────────

def _load_policy_doc(conn, degree: str | None = None) -> dict:
    """
    Load policy document from configs table.
    Prefer degree-specific, else global (*), else empty dict.
    """
    try:
        if not _validate_configs_schema(conn): return {}
        if degree:
            row = conn.execute(sa_text("SELECT config_json FROM configs WHERE degree=:d AND namespace=:ns ORDER BY updated_at DESC LIMIT 1"), {"d": degree, "ns": NAMESPACE}).fetchone()
            if row and row[0]:
                try: return json.loads(row[0]) or {}
                except json.JSONDecodeError: pass

        row = conn.execute(sa_text("SELECT config_json FROM configs WHERE degree='*' AND namespace=:ns ORDER BY updated_at DESC LIMIT 1"), {"ns": NAMESPACE}).fetchone()
        if row and row[0]:
            try: return json.loads(row[0]) or {}
            except json.JSONDecodeError: pass
        return {}
    except Exception:
        return {}

def get_policy(engine, object_type: str, action: str, degree: str | None = None) -> dict:
    """
    Returns a policy dict. Falls back to DEFAULT_POLICIES if no policy is configured.
    """
    try:
        with engine.begin() as conn:
            doc = _load_policy_doc(conn, degree)
        key = f"{object_type}.{action}"
        policy = (doc.get("policies") or {}).get(key, {})
        if not policy and key in DEFAULT_POLICIES:
            return DEFAULT_POLICIES[key].copy()
        return policy or {}
    except Exception:
        key = f"{object_type}.{action}"
        return DEFAULT_POLICIES.get(key, {}).copy()

def approver_roles(engine, object_type: str, action: str, degree: str | None = None) -> set[str]:
    """Get set of approver roles for a specific object type and action."""
    policy = get_policy(engine, object_type, action, degree)
    return set(policy.get("approver_roles", []))

def rule(engine, object_type: str, action: str, degree: str | None = None) -> str:
    """Get approval rule type - 'either_one' or 'all'."""
    policy = get_policy(engine, object_type, action, degree)
    return policy.get("rule", "either_one")

def requires_reason(engine, object_type: str, action: str, degree: str | None = None) -> bool:
    """Check if reason note is required for the action."""
    policy = get_policy(engine, object_type, action, degree)
    return bool(policy.get("requires_reason", True))

# ─────────────────────────── Utility Functions ───────────────────────────

def list_configured_policies(engine, degree: str | None = None) -> dict:
    with engine.begin() as conn:
        doc = _load_policy_doc(conn, degree)
    return (doc.get("policies") or {}).copy()

def get_policy_summary(engine, object_type: str, action: str, degree: str | None = None) -> dict:
    return {
        "object_type": object_type, "action": action, "degree": degree,
        "approver_roles": list(approver_roles(engine, object_type, action, degree)),
        "rule": rule(engine, object_type, action, degree),
        "requires_reason": requires_reason(engine, object_type, action, degree),
        "is_default": not bool(list_configured_policies(engine, degree).get(f"{object_type}.{action}"))
    }

