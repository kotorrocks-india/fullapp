# screens/faculty/page.py
from __future__ import annotations

import sys
import traceback
from pathlib import Path
from typing import Set

import streamlit as st
from sqlalchemy import text as sa_text  # retained (harmless if unused)

# Ensure project root (adjust the number of parents if your layout differs)
project_root = Path(__file__).resolve().parents[2]
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

# Core app imports
from core.settings import load_settings
from core.db import get_engine
from core.policy import require_page, can_edit_page

# Faculty module tabs (unchanged)
from screens.faculty.ui_tabs import (
    _tab_designations,
    _tab_designation_removal,
    _tab_custom_types,
    _tab_profiles,
    _tab_affiliations,
    _tab_export_credentials,
    _tab_bulk_ops,  # Added import in your original; kept
)

# Import the credits policy tab
from screens.faculty.tabs.credits_policy import render as _tab_credits_policy

# Import the positions tab
from screens.faculty.tabs.positions import render as _tab_positions

from screens.faculty.db import (
    _active_degrees,
    _add_fixed_role_admins_to_degree,
)

PAGE_KEY = "Faculty"


@require_page(PAGE_KEY)
def render():
    """
    Main Faculty page renderer (without bootstrapping).
    - Assumes schema is already initialized by app.py or external migration.
    - Namescopes keys to avoid cross-page collisions.
    - Reads active degrees; if none, informs the user and exits gracefully.
    """
    try:
        st.title("Faculty")

        settings = load_settings()
        engine = get_engine(settings.db.url)

        # --- Read active degrees (relies on schema being present) ---
        degrees = []
        try:
            with engine.begin() as conn:
                degrees = _active_degrees(conn)
        except Exception as e:
            # Helpful error for column mismatches on degrees table
            msg = str(e).lower()
            if "no such column: title" in msg or "no such column: name" in msg:
                st.error("**Schema Mismatch:** `degrees` table structure is incompatible.")
                st.info("💡 Ensure `degrees` has expected columns (e.g., code, title, active, sort_order).")
                st.code(f"Details: {e}")
                return
            else:
                st.error(f"❌ Error accessing core tables: {e}")
                return

        # Session/user permissions
        user = st.session_state.get("user") or {}
        roles: Set[str] = set(user.get("roles") or [])
        user_email = user.get("email") or "anonymous"
        can_edit_faculty = can_edit_page(PAGE_KEY, roles)

        # Handle case where no active degrees are found
        if not degrees:
            st.warning("⚠️ No **active** degrees found.")
            st.info("💡 Go to **Degrees & Programs** to create/activate a degree.")
            return  # Stop rendering tabs

        # Header controls (degree selector + user caption)
        col_left, col_right = st.columns([2, 1])
        with col_left:
            degree = st.selectbox("Degree", degrees, key=f"{PAGE_KEY.lower()}_degree_select")
        with col_right:
            st.caption(f"Signed in as **{user_email}**")

        if not degree:
            st.info("Please select a degree.")
            return

        # --- Sync admin affiliations for the SELECTED degree (runs each render; non-blocking on error) ---
        try:
            with engine.begin() as conn:
                _add_fixed_role_admins_to_degree(conn, degree)
        except Exception as e:
            st.warning(f"Note: Could not sync admin affiliations for {degree}: {e}")

        # --- Tabs ---
        tab_titles = [
            "Credits Policy",
            "Designation Catalog",
            "Designation Removal",
            "Custom Types",
            "Profiles",
            "Affiliations",
            "Manage Positions",
            "Bulk Operations",
            "Export Credentials",
        ]
        tabs = st.tabs(tab_titles)
        tab_map = {title: tab for title, tab in zip(tab_titles, tabs)}

        render_funcs = {
            "Credits Policy": _tab_credits_policy,
            "Designation Catalog": _tab_designations,
            "Designation Removal": _tab_designation_removal,
            "Custom Types": _tab_custom_types,
            "Profiles": _tab_profiles,
            "Affiliations": _tab_affiliations,
            "Manage Positions": _tab_positions,
            "Bulk Operations": _tab_bulk_ops,
            "Export Credentials": _tab_export_credentials,
        }

        # Render each tab safely
        for title, func in render_funcs.items():
            if title in tab_map:
                with tab_map[title]:
                    try:
                        func(
                            engine,
                            degree,
                            roles,
                            can_edit_faculty,
                            f"{PAGE_KEY.lower()}_{title.lower().replace(' ', '_')}",
                        )
                    except Exception as e:
                        st.error(f"❌ {title} tab failed.")
                        st.exception(e)

    except Exception as e:
        # Catch-all for unexpected errors during main page setup
        st.error("An unexpected error occurred while rendering the Faculty page.")
        st.exception(e)
        st.code("".join(traceback.format_exc()))


# Keep this call if your navigation expects pages to render on import
render()
