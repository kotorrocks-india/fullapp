# screens/academic_years/main.py - FIXED VERSION
from __future__ import annotations

import traceback
import streamlit as st
from sqlalchemy import text as sa_text

# --- Core imports (fail visibly; no blank screens) ---
try:
    from core.settings import load_settings
    from core.db import get_engine
    from core.rbac import user_roles
except Exception as e:
    st.error(f"Startup import failed: {e}")
    st.code(traceback.format_exc())
    st.stop()

# --- Schema import to ensure tables exist ---
try:
    from screens.academic_years.schema import install_all as install_academic_years_schema
except Exception as e:
    st.error(f"Schema import failed: {e}")
    st.code(traceback.format_exc())
    st.stop()

# --- UI imports (fail visibly) ---
try:
    from screens.academic_years.ui import (
        render_ay_list,
        render_ay_editor,
        render_ay_status_changer,
        render_calendar_profiles,
        render_calendar_assignment_editor,
        render_calendar_assignments,
    )
except Exception as e:
    st.error(f"UI import failed: {e}")
    st.code(traceback.format_exc())
    st.stop()

PAGE_TITLE = "🎓 Academic Years & Calendars"


def _get_engine_roles_email():
    """Create engine and derive roles/email for this session."""
    settings = load_settings()
    engine = get_engine(settings.db.url)
    user = st.session_state.get("user") or {}
    email = user.get("email") or "anonymous"
    roles = user_roles(engine, email) if email != "anonymous" else set()
    return engine, roles, email


def _degrees_exist(engine) -> bool:
    """Return True if there is at least one active degree; False if table missing or empty."""
    try:
        with engine.connect() as conn:
            row = conn.execute(sa_text("SELECT 1 FROM degrees WHERE active=1 LIMIT 1")).fetchone()
            return row is not None
    except Exception:
        return False


def render():
    """Main render function - called once per page load."""
    st.title(PAGE_TITLE)

    # Init engine/roles/email
    try:
        engine, roles, email = _get_engine_roles_email()
    except Exception as e:
        st.error(f"Initialization failed: {e}")
        st.code(traceback.format_exc())
        st.stop()

    # Ensure database tables are installed
    try:
        install_academic_years_schema(engine)
    except Exception as e:
        st.error(f"Database schema installation failed: {e}")
        st.code(traceback.format_exc())
        st.stop()

    # Create tabs - IMPORTANT: Tab creation should happen only once
    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
        "AY List",
        "AY Editor",
        "AY Status",
        "Calendar Profiles",
        "Assignment Editor",
        "Assignment Preview",
    ])

    # Render each tab with proper error handling
    with tab1:
        try:
            render_ay_list(engine)
        except Exception as e:
            st.error(f"AY List failed: {e}")
            st.code(traceback.format_exc())

    with tab2:
        try:
            render_ay_editor(engine, roles, email)
        except Exception as e:
            st.error(f"AY Editor failed: {e}")
            st.code(traceback.format_exc())

    with tab3:
        try:
            render_ay_status_changer(engine, roles, email)
        except Exception as e:
            st.error(f"AY Status failed: {e}")
            st.code(traceback.format_exc())

    with tab4:
        try:
            render_calendar_profiles(engine, roles, email)
        except Exception as e:
            st.error(f"Calendar Profiles failed: {e}")
            st.code(traceback.format_exc())

    with tab5:
        try:
            render_calendar_assignment_editor(engine, roles, email)
        except Exception as e:
            st.error(f"Assignment Editor failed: {e}")
            st.code(traceback.format_exc())

    with tab6:
        try:
            if not _degrees_exist(engine):
                st.warning("No Degrees found. This section is disabled until at least one active Degree exists.")
            else:
                render_calendar_assignments(engine, roles, email)
        except Exception as e:
            st.error(f"Assignments/Term Preview failed: {e}")
            st.code(traceback.format_exc())


# Call render only if this is the main module
if __name__ == "__main__":
    render()
else:
    # If imported as a module, still render (Streamlit pattern)
    render()
