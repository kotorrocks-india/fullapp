# app.py
from __future__ import annotations
import importlib
from pathlib import Path
import sys
import os
import streamlit as st
from sqlalchemy import text as sa_text
from core.settings import load_settings
from core.db import get_engine, init_db
from core.rbac import user_roles as fetch_roles_for
from core.policy import can_view_page, visible_pages_for
from core.theme_apply import apply_theme_for_degree
from core.theme_toggle import render_theme_toggle
from core.ui import render_footer_global
from core.public_config import (
    load_public_branding_config,
    load_public_footer_config,
    resolve_public_asset_path
)
from core.sidebar_logo import render_logo_for_navigation

# ── NEW: bring in schema installers (no functionality removed; just ensures tables exist)
from core.schema_registry import auto_discover, run_all as run_all_installers
#from core.schema_registry import run_all as run_all_installers
from screens.faculty.schema import install_all as install_faculty_module_schema
from schemas.administrative_positions_schema import install_administrative_positions

APP_FILE = Path(__file__).resolve()
APP_DIR  = APP_FILE.parent
CWD      = Path.cwd()
SCREEN_CANDIDATES = [APP_DIR / "screens", CWD / "app" / "screens", CWD / "screens"]
SCREENS_DIR = next((p for p in SCREEN_CANDIDATES if p.exists()), APP_DIR / "screens")

def _screen_path(stem: str) -> Path:
    """Find screen file in screens directory"""
    p = SCREENS_DIR / f"{stem}.py"
    if p.exists():
        return p
    root = APP_DIR.parent
    try:
        for fp in root.rglob(f"{stem}.py"):
            return fp.resolve()
    except Exception:
        pass
    return p

# init_db() has been removed from this function.
# This function's only job is to create or retrieve the engine
# and cache it in session_state.
def _ensure_engine():
    if "engine" not in st.session_state:
        settings = load_settings()
        engine = get_engine(settings.db.url)
        st.session_state["engine"] = engine
    return st.session_state["engine"]

def _session_user():
    u = st.session_state.get("user") or {}
    email = (u.get("email") or "").strip().lower()
    engine = _ensure_engine()
    if email and ("roles" not in u or not u.get("roles")):
        roles = fetch_roles_for(engine, email)
        u["roles"] = roles
        st.session_state["user"] = u
    roles = u.get("roles", set())
    return u, email, roles

def _add_page_if(policy_name: str, route_stem: str, title: str, roles: set[str], pages_out: list, missing_out: list):
    if not can_view_page(policy_name, roles):
        return

    page_path = None

    # Define all possible paths first
    screens_file_path = SCREENS_DIR / f"{route_stem}.py"
    screens_dir_page_path = SCREENS_DIR / route_stem / "page.py" # Check page.py
    screens_dir_main_path = SCREENS_DIR / route_stem / "main.py" # Check main.py
    screens_dir_init_path = SCREENS_DIR / route_stem / "__init__.py" # Check __init__.py last

    pages_file_path = APP_DIR / "pages" / f"{route_stem}.py"
    pages_dir_page_path = APP_DIR / "pages" / route_stem / "page.py" # Define page.py path
    pages_dir_main_path = APP_DIR / "pages" / route_stem / "main.py" # Define main.py path
    pages_dir_init_path = APP_DIR / "pages" / route_stem / "__init__.py"# Define __init__.py path

    # Check screens directory first (prioritize page.py/main.py over __init__.py)
    if screens_file_path.exists():
        page_path = screens_file_path
    elif screens_dir_page_path.exists(): # <-- Prefer page.py
        page_path = screens_dir_page_path
    elif screens_dir_main_path.exists(): # <-- Prefer main.py
        page_path = screens_dir_main_path
    elif screens_dir_init_path.exists(): # <-- Use __init__.py as fallback
        page_path = screens_dir_init_path
    # Fallback to pages directory
    elif pages_file_path.exists():
        page_path = pages_file_path
    elif pages_dir_page_path.exists(): # <-- Prefer page.py
         page_path = pages_dir_page_path
    elif pages_dir_main_path.exists(): # <-- Prefer main.py
        page_path = pages_dir_main_path
    elif pages_dir_init_path.exists(): # <-- Use __init__.py as fallback
        page_path = pages_dir_init_path

    if page_path is None or not page_path.exists():
        missing_out.append((route_stem, f"Not found"))
        return

    if hasattr(st, "Page"):
        is_default = (route_stem == "profile")
        try:
            # Assume all pages are found relative to APP_DIR
            relative_path_str = str(page_path.relative_to(APP_DIR)).replace(os.path.sep, '/')
            pages_out.append(st.Page(
                relative_path_str,
                title=title,
                default=is_default,
                url_path=route_stem  # <-- Explicitly set url_path
            ))
        except ValueError as e: # Catch specific error
            missing_out.append((route_stem, f"Path Error: Could not make path relative to APP_DIR. {e}"))
        except Exception as e: # Catch other potential errors from st.Page
             missing_out.append((route_stem, f"st.Page Error: {e}"))

def _build_flat_pages(roles: set[str]):
    pages, missing = [], []

    # Core configuration pages
    _add_page_if("Profile", "profile", "👤 Profile", roles, pages, missing)
    _add_page_if("Users & Roles", "users_roles", "👥 Users & Roles", roles, pages, missing)
    _add_page_if("Branding (Login)", "branding", "🎨 Branding (Login)", roles, pages, missing)
    _add_page_if("Appearance / Theme", "appearance_theme", "🎛️ Appearance / Theme", roles, pages, missing)
    _add_page_if("Footer", "footer", "🦶 Footer", roles, pages, missing)
    _add_page_if("Office Admins", "office_admin", "📋 Office Admin", roles, pages, missing)

    # Academic structure pages
    _add_page_if("Degrees", "degrees", "🎓 Degrees", roles, pages, missing)
    _add_page_if("Programs / Branches", "programs_branches", "📚 Programs / Branches", roles, pages, missing)
    _add_page_if("Semesters", "semesters", "📅 Semesters", roles, pages, missing)

    # Faculty and academic operations
    _add_page_if("Faculty", "faculty", "👨‍🏫 Faculty", roles, pages, missing)
    _add_page_if("Academic Years", "academic_years", "🗓️ Academic Years", roles, pages, missing)
    _add_page_if("Students", "students", "🎓 Students", roles, pages, missing)
    _add_page_if("Subjects Offerings", "subjects_syllabus", "📘 Subjects Offerings", roles, pages, missing)
    _add_page_if("Electives & College Projects", "electives_topics", "🎯 Electives & College Projects", roles, pages, missing)
    #_add_page_if("Subjects AY COs Rubrics", "subjects_syllabus", "📘 Subjects", roles, pages, missing)
    # _add_page_if("Assignments", "assignments", "📝 Assignments", roles, pages, missing)
    # _add_page_if("Marks", "marks", "✅ Marks", roles, pages, missing)
    _add_page_if("Approvals", "approvals", "📬 Approvals", roles, pages, missing)
    _add_page_if("Approval Management", "approval_management", "⚙️ Approval Management", roles, pages, missing)

    if missing:
        st.sidebar.warning(f"Missing pages: {[m[0] for m in missing]}")

    return pages, missing

def main():
    # 1. Get or create the engine.
    engine = _ensure_engine()

    # 2. Run database initialization ONCE per session.
    if "db_initialized" not in st.session_state:
        # Base engine/PRAGMA setup (idempotent)
        init_db(engine)

        # ── NEW: guarantee tables exist before any page queries
        try:
            run_all_installers(engine)                 # all global/baseline installers
            install_faculty_module_schema(engine)      # module-scoped faculty tables
            install_administrative_positions(engine)   # positions tables
        except Exception as e:
            st.error("Database schema initialization failed. See details below.")
            with st.expander("Diagnostics"):
                st.exception(e)
            st.stop()

        st.session_state["db_initialized"] = True

    branding_cfg = load_public_branding_config(engine)

    favicon_path = branding_cfg.get("favicon", {}).get("url", "🎉")
    is_valid_favicon, resolved_favicon = resolve_public_asset_path(favicon_path)
    try:
        st.set_page_config(page_title="LPEP", layout="wide", initial_sidebar_state="auto", page_icon=resolved_favicon if is_valid_favicon else "🎉")
    except Exception:
        st.set_page_config(page_title="LPEP", layout="wide")

    logo_path = branding_cfg.get("logo", {}).get("url")
    is_valid_logo, resolved_logo = (False, "")
    if logo_path:
        is_valid_logo, resolved_logo = resolve_public_asset_path(logo_path)

    # Handle logo display for authenticated users (not login/logout pages)
    if not st.session_state.get("show_login") and not st.session_state.get("show_logout"):
        active_degree = st.session_state.get("active_degree")

        degree_logo_shown = False
        if active_degree:
            degree_logo_result = render_logo_for_navigation(engine, active_degree, width=300)
            degree_logo_shown = degree_logo_result is not None

        if not degree_logo_shown and is_valid_logo:
            st.logo(resolved_logo, size="large")

    if st.session_state.get("show_login"):
        st.markdown("""
            <style>
                section[data-testid="stSidebar"] {
                    display: none;
                }
            </style>
        """, unsafe_allow_html=True)

        theme_cfg = branding_cfg.get("theme", {})
        if theme_cfg:
            _, toggle_col = st.columns([0.85, 0.15])
            with toggle_col:
                render_theme_toggle(engine, theme_cfg, key="login_theme_toggle", location="inline", label="Dark Mode")

        if is_valid_logo:
            max_width = branding_cfg.get("logo", {}).get("max_width_px", 240)
            st.image(resolved_logo, width=int(max_width))

        st.title("Login")
        with st.form("login_form"):
            email = st.text_input("Email", value="admin@example.com")
            password = st.text_input("Password", type="password", value="password")
            submitted = st.form_submit_button("Login")

        if submitted:
            # Simple mock login - this does not verify the password
            st.session_state["user"] = {
                "user_id": 1,
                "email": email,
                "username": "user",
                "full_name": "Test User",
                "roles": set(),  # Empty roles will force a fetch from the database
            }
            if "show_login" in st.session_state: del st.session_state["show_login"]
            st.success(f"Logged in as {email}! Redirecting...")
            st.rerun()

        render_footer_global()
        return

    if st.session_state.get("show_logout"):
        st.markdown("""
            <style>
                section[data-testid="stSidebar"] {
                    display: none;
                }
            </style>
        """, unsafe_allow_html=True)

        st.title("Logout")
        if "user" in st.session_state:
            user_email = st.session_state.user.get("email", "Unknown")
            st.success(f"Successfully logged out {user_email}")
            keys_to_keep = ["engine"] # Keep engine across logout
            for key in list(st.session_state.keys()):
                if key not in keys_to_keep: del st.session_state[key]
        else: st.info("You are already logged out")
        st.markdown("---")
        col1, _ = st.columns([1, 1])
        with col1:
            if st.button("🔄 Return to Login", type="primary", use_container_width=True):
                st.session_state["show_login"] = True
                st.rerun()

        render_footer_global()
        return

    # --- AUTHENTICATED APP FLOW ---
    user, email, roles = _session_user()

    if not email:
        st.markdown("""
            <style>
                section[data-testid="stSidebar"] {
                    display: none;
                }
            </style>
        """, unsafe_allow_html=True)

        st.warning("No user logged in. Please log in to continue.")
        if st.button("Go to Login Page"):
            st.session_state["show_login"] = True
            st.rerun()
        return

    active_degree = st.session_state.get("active_degree")
    theme_cfg = apply_theme_for_degree(engine, active_degree, email)
    render_theme_toggle(engine, theme_cfg, key="root_theme_toggle", location="inline", label="Dark mode")

    user_obj = user or {}
    display_name = (user_obj.get("full_name") or user_obj.get("email") or "").strip() or "User"
    roles_str = ", ".join(r for r in sorted(roles) if r != 'public')

    left, right = st.columns([0.75, 0.25])
    with left: st.caption(f"Signed in as **{display_name}** · _{roles_str}_")
    with right:
        if st.button("Logout", key="logout_top"):
            st.session_state["show_logout"] = True
            st.rerun()

    pages, missing = _build_flat_pages(roles)

    if hasattr(st, "navigation") and hasattr(st, "Page"):
        if not pages:
            st.error("No pages available for your current roles.")
        else:
            nav = st.navigation(pages, position="sidebar")
            nav.run()
    else:
        st.title("Welcome")
        st.info("Your Streamlit version is too old for this app's navigation.")
        accessible_pages = [page.title for page in pages]
        st.write(f"With your roles ({roles}), you can access: {accessible_pages}")

    render_footer_global()

if __name__ == "__main__":
    main()
