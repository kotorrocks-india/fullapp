# app/core/policy.py
# --- REFACTORED AND DYNAMIC (FINAL) ---

from __future__ import annotations
from typing import Iterable, Optional, Set, Dict, Any, Callable
import functools
import streamlit as st
from sqlalchemy.engine import Engine
from sqlalchemy import text as sa_text

# --- FIX 1: Import from rbac and the NEW enhanced policy ---
try:
    # Import from the NEW enhanced policy (from Batch 1)
    from core.approvals_policy import (
        approver_roles as _approver_roles_for,
        rule as _rule_for,
        requires_reason as _requires_reason,
        can_user_approve as _can_user_approve,
    )
except ImportError:
    # Fallback if enhanced policy is missing
    def _approver_roles_for(engine, object_type: str, action: str, degree: str | None = None, program: str | None = None, branch: str | None = None) -> Set[str]: return {"superadmin"}
    def _rule_for(engine, object_type: str, action: str, degree: str | None = None) -> Optional[str]: return None
    def _requires_reason(engine, object_type: str, action: str, degree: str | None = None) -> bool: return False
    def _can_user_approve(engine, user_email: str, user_roles: Set[str], object_type: str, action: str, **kwargs) -> bool: return "superadmin" in user_roles

# Import the REAL user_roles function from rbac
from core.rbac import user_roles as _db_user_roles
from core.db import get_engine
# --- END FIX 1 ---

# ============================================================================
# DYNAMIC PAGE ACCESS (Replaces hardcoded PAGE_ACCESS dictionary)
# ============================================================================

@st.cache_data(ttl=300)  # Cache rules for 5 minutes
def _load_page_access_rules(_engine: Engine) -> Dict[str, Set[str]]:
    """
    Fetches all page access rules from the database and returns a
    lookup dictionary.
    
    Returns:
        Dict[str, Set[str]]: A lookup mapping like
        {'view_Degrees': {'superadmin', 'principal'}, 'edit_Degrees': {'superadmin'}}
    """
    lookup = {}
    try:
        with _engine.begin() as conn:
            rules = conn.execute(sa_text(
                "SELECT page_name, permission_type, role_name FROM page_access_rules"
            )).fetchall()
        
        for page, perm_type, role in rules:
            key = f"{perm_type}_{page}"
            if key not in lookup:
                lookup[key] = set()
            lookup[key].add(role)
            
    except Exception as e:
        # Failsafe if table doesn't exist yet
        st.error(f"Error loading page access rules: {e}")
        lookup['view_Login'] = {'public'}  # Allow login
        
    return lookup

def current_user() -> Dict[str, Any]:
    return st.session_state.get("user") or {}

# --- FIX 2: Rewire user_roles to use core.rbac ---
def user_roles(engine: Optional[Engine] = None, email: Optional[str] = None) -> Set[str]:
    """
    Get user roles from the database.
    If email is not provided, uses the logged-in user's email.
    """
    if not email:
        user_data = current_user()
        email = user_data.get("email")

    if not email:
        return {"public"}
    
    # Ensure we have an engine to pass to rbac
    if not engine:
        engine = st.session_state.get("engine") or get_engine()
        
    return _db_user_roles(engine, email)
# --- END FIX 2 ---

def can_view_page(page_name: str, roles: Set[str]) -> bool:
    """Checks if any of the user's roles can view the page."""
    engine = st.session_state.get("engine") or get_engine()
    rules_lookup = _load_page_access_rules(engine)
    
    allowed_roles = rules_lookup.get(f"view_{page_name}", set())
    
    # 'public' can view any page that has 'public' as a view role
    if "public" in allowed_roles:
        return True
        
    return bool(roles & allowed_roles)

def can_edit_page(page_name: str, roles: Set[str]) -> bool:
    """Checks if any of the user's roles can edit the page."""
    engine = st.session_state.get("engine") or get_engine()
    rules_lookup = _load_page_access_rules(engine)
    
    allowed_roles = rules_lookup.get(f"edit_{page_name}", set())
    return bool(roles & allowed_roles)

def require_page(page_name: str):
    def _wrap(fn: Callable):
        @functools.wraps(fn)
        def _inner(*args, **kwargs):
            # Pass engine to user_roles
            engine = st.session_state.get("engine") or get_engine()
            roles = user_roles(engine=engine)
            if not can_view_page(page_name, roles):
                st.error("Access Denied. You don't have permission to view this page.")
                st.stop()
            return fn(*args, **kwargs)
        return _inner
    return _wrap

def visible_pages_for(roles: Set[str]) -> list[str]:
    """Gets all pages the user's roles have view access to."""
    engine = st.session_state.get("engine") or get_engine()
    rules_lookup = _load_page_access_rules(engine)
    
    visible = set()
    
    # Get all unique page names from the rules
    all_pages = set(key.split('_', 1)[1] for key in rules_lookup.keys())

    for page in all_pages:
        if can_view_page(page, roles):
            visible.add(page)
            
    return sorted(list(visible))

# ============================================================================
# APPROVALS POLICY (No changes, just wiring)
# ============================================================================

# --- FIX 3: Update all approval functions to use new policy ---

def approver_roles_for(object_type: str, action: str, **kwargs) -> Set[str]:
    """Gets all approvers (user emails OR roles) for an action."""
    engine = kwargs.get('engine') or st.session_state.get("engine")
    return set(_approver_roles_for(
        engine, object_type, action,
        degree=kwargs.get('degree'),
        program=kwargs.get('program'),
        branch=kwargs.get('branch')
    ))

def rule_for(object_type: str, action: str, **kwargs) -> Optional[str]:
    """Gets the approval rule (e.g., 'either_one')."""
    engine = kwargs.get('engine') or st.session_state.get("engine")
    return _rule_for(
        engine, object_type, action,
        degree=kwargs.get('degree')
    )

def requires_reason(engine: Engine, object_type: str, action: str, **kwargs) -> bool:
    """Checks if the action requires a reason."""
    return bool(_requires_reason(
        engine, object_type, action,
        degree=kwargs.get('degree')
    ))

def can_approve(
    object_type: str, 
    action: str, 
    roles: Optional[Iterable[str]] = None, 
    email: Optional[str] = None, 
    **kwargs
) -> bool:
    """
    Checks if a user can approve an action.
    This now checks for specific email assignment OR role.
    """
    engine = kwargs.get('engine') or st.session_state.get("engine")
    
    # Get user email
    user_email = email
    if not user_email:
        user_email = (current_user().get("email") or "").strip().lower()
    if not user_email:
        return False  # Must have an email to be an approver

    # Get user roles
    rset = set(roles) if roles is not None else user_roles(engine=engine, email=user_email)

    return _can_user_approve(
        engine,
        user_email,
        rset,
        object_type,
        action,
        degree=kwargs.get('degree'),
        program=kwargs.get('program'),
        branch=kwargs.get('branch')
    )

# ============================================================================
# DYNAMIC can_request function
# ============================================================================

@st.cache_data(ttl=300)
def _get_request_permission_map(_engine: Engine) -> Dict[str, str]:
    """
    Fetches the map of (object_type.action) -> page_name from the config.
    e.g., {"degree.delete": "Degrees", "program.delete": "Programs / Branches"}
    """
    try:
        with _engine.begin() as conn:
            rows = conn.execute(sa_text("""
                SELECT object_type, action, linked_page_permission
                FROM approval_rules_config
                WHERE linked_page_permission IS NOT NULL
            """)).fetchall()
        
        return {f"{r[0]}.{r[1]}": r[2] for r in rows}
    except Exception:
        return {}  # Failsafe

def can_request(object_type: str, action: str, roles: Optional[Iterable[str]] = None) -> bool:
    """
    Checks if a user can *initiate* a request.
    This is now dynamic and reads from the approval_rules_config table.
    """
    engine = st.session_state.get("engine") or get_engine()
    rset = set(roles) if roles is not None else user_roles(engine=engine)
    
    # Load the dynamic map
    permission_map = _get_request_permission_map(engine)
    
    key = f"{object_type}.{action}"
    page_name = permission_map.get(key)
    
    if page_name:
        # We found a rule: "To request a 'degree.delete', you must
        # be able to 'edit' the 'Degrees' page."
        return can_edit_page(page_name, rset)
    
    # Fallback for unmapped types (or if linked_page_permission is NULL)
    # You can decide if this should be True or False by default.
    # We'll keep the original fallback logic.
    return bool(rset & {"superadmin", "tech_admin", "principal", "director", "academic_admin"})
# --- END FIX 3 ---

# Add this new function to the end of app/core/policy.py

def render_policy_aware_tabs(
    all_tabs: list[tuple[str, Callable]],
    engine: Engine,
    roles: Set[str],
    degree: str = None,  # ← Changed from **kwargs to explicit params
    key_prefix: str = "",
    **extra_kwargs  # Keep this for any additional args
):
    """
    A universal helper to render Streamlit tabs based on user permissions.
    
    This function:
    1. Filters a list of tabs based on 'can_view_page(tab_title, roles)'.
    2. Renders the st.tabs() widget with only the visible tabs.
    3. Renders each tab's content using the OLD calling convention.
    
    Args:
        all_tabs: A list of (tab_title, render_function) tuples.
        engine: The database engine.
        roles: The user's roles set.
        degree: The selected degree code (matches old signature).
        key_prefix: Key prefix for Streamlit widgets.
        **extra_kwargs: Any additional arguments for future compatibility.
    """
    
    # 1. Filter tabs based on user's VIEW permissions
    visible_tabs_to_render = []
    for title, render_func in all_tabs:
        if can_view_page(title, roles):
            visible_tabs_to_render.append((title, render_func))

    # 2. Check if any tabs are visible
    if not visible_tabs_to_render:
        st.info("You do not have permission to view any content in this section.")
        return

    # 3. Create the st.tabs() widget ONLY with visible titles
    visible_titles = [title for title, func in visible_tabs_to_render]
    created_tabs = st.tabs(visible_titles)
    
    # 4. Render each visible tab
    for i, tab_widget in enumerate(created_tabs):
        with tab_widget:
            # Get the title and function for this tab
            title, render_func = visible_tabs_to_render[i]
            
            # 5. Check EDIT permissions for this specific tab
            can_edit_this_tab = can_edit_page(title, roles)
            
            # 6. Generate key_prefix for this specific tab
            tab_key_prefix = f"{key_prefix}_{title.lower().replace(' ', '_')}"
            
            try:
                # 7. Call using the OLD signature that your tabs expect:
                # func(engine, degree, roles, can_edit, key_prefix)
                render_func(
                    engine,
                    degree,
                    roles,
                    can_edit_this_tab,
                    tab_key_prefix
                )
            except TypeError as e:
                # If the function doesn't match the old signature, try the new one
                st.error(f"❌ {title} tab has incompatible signature.")
                st.error(f"Expected: func(engine, degree, roles, can_edit, key_prefix)")
                st.exception(e)
            except Exception as e:
                st.error(f"❌ {title} tab failed to render.")
                st.exception(e)
