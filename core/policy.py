# app/core/policy.py
from __future__ import annotations
from typing import Iterable, Optional, Set, Dict, Any, Callable
import functools
import streamlit as st

try:
    from core.approvals_policy import approver_roles as _approver_roles_for, rule as _rule_for, requires_reason as _requires_reason
except Exception:
    # Updated fallback functions to match the expected signature
    def _approver_roles_for(engine, object_type: str, action: str, degree: str | None = None) -> Set[str]: return {"superadmin"}
    def _rule_for(engine, object_type: str, action: str, degree: str | None = None) -> Optional[str]: return None
    def _requires_reason(engine, object_type: str, action: str, degree: str | None = None) -> bool: return False

PAGE_ACCESS = {
    "Login":  {"view": {"public"}},
    "Logout": {"view": {"public"}},
    "Profile": {"view": {"superadmin", "tech_admin", "academic_admin", "principal", "director", "management_representative"}, "edit": set()},
    "Users & Roles":     {"view": {"superadmin","tech_admin"}, "edit": {"superadmin","tech_admin"}},
    "Branding (Login)":  {"view": {"superadmin"}, "edit": {"superadmin"}},
    "Footer":            {"view": {"superadmin"}, "edit": {"superadmin"}},
    "Appearance / Theme":{"view": {"superadmin","tech_admin"}, "edit": {"superadmin","tech_admin"}},
    "Degrees":           {"view": {"superadmin","principal","director"}, "edit": {"superadmin","principal","director"}},
    "Programs / Branches": {"view": {"superadmin","principal","director"}, "edit": {"superadmin","principal","director"}},
    "Semesters":         {"view": {"superadmin","principal","director"}, "edit": {"superadmin","principal","director"}},
    "Subjects & Syllabus": {"view": {"superadmin","tech_admin","principal","director","academic_admin"}, "edit": {"superadmin","tech_admin","academic_admin"}},
    "Assignments":       {"view": {"superadmin","principal","director","academic_admin"}, "edit": {"superadmin","academic_admin"}},
    "Marks":             {"view": {"superadmin","principal","director","academic_admin"}, "edit": {"superadmin","academic_admin"}},
    "Approvals":         {"view": {"superadmin","principal","director","management_representative"}, "edit": {"superadmin","principal","director"}},
    # Fixed Faculty page access:
    "Faculty": {"view": {"superadmin", "tech_admin", "principal", "director", "academic_admin"}, "edit": {"superadmin", "tech_admin", "principal", "director"}},
    "Office Admins": {"view": {"superadmin", "tech_admin", "principal", "director"}, "edit": {"superadmin", "tech_admin"}}
}

def current_user() -> Dict[str, Any]:
    return st.session_state.get("user") or {}

def user_roles() -> Set[str]:
    user_data = current_user()
    if user_data:
        return set(user_data.get("roles") or [])
    return {"public"}

def can_view_page(page_name: str, roles: Set[str]) -> bool:
    rules = PAGE_ACCESS.get(page_name) or {}
    allowed = set(rules.get("view") or [])
    return True if not allowed else bool(roles & allowed)

def can_edit_page(page_name: str, roles: Set[str]) -> bool:
    rules = PAGE_ACCESS.get(page_name) or {}
    allowed = set(rules.get("edit") or [])
    return bool(roles & allowed)

def require_page(page_name: str):
    def _wrap(fn: Callable):
        @functools.wraps(fn)
        def _inner(*args, **kwargs):
            roles = user_roles()
            if not can_view_page(page_name, roles):
                st.error("Access Denied. You don't have permission to view this page.")
                st.stop()
            return fn(*args, **kwargs)
        return _inner
    return _wrap

def visible_pages_for(roles: Set[str]) -> list[str]:
    return [p for p in PAGE_ACCESS.keys() if can_view_page(p, roles)]

# Fixed functions - extract degree from kwargs and pass correctly
def approver_roles_for(object_type: str, action: str, **kwargs) -> Set[str]:
    engine = kwargs.get('engine') or st.session_state.get("engine")
    degree = kwargs.get('degree')
    # Call with proper positional arguments only
    return set(_approver_roles_for(engine, object_type, action, degree))

def rule_for(object_type: str, action: str, **kwargs) -> Optional[str]:
    engine = kwargs.get('engine') or st.session_state.get("engine")
    degree = kwargs.get('degree')
    # Call with proper positional arguments only
    return _rule_for(engine, object_type, action, degree)

def requires_reason(engine, object_type: str, action: str, **kwargs) -> bool:
    degree = kwargs.get('degree')
    return bool(_requires_reason(engine, object_type, action, degree))

def can_approve(object_type: str, action: str, roles: Optional[Iterable[str]] = None, **kwargs) -> bool:
    rset = set(roles) if roles is not None else user_roles()
    return bool(rset & approver_roles_for(object_type, action, **kwargs))

def can_request(object_type: str, action: str, roles: Optional[Iterable[str]] = None) -> bool:
    rset = set(roles) if roles is not None else user_roles()
    if object_type.lower() == "degree" and action.lower() == "delete":
        return bool(rset & {"superadmin", "tech_admin", "principal", "director"})
    return bool(rset & approver_roles_for(object_type, action))
