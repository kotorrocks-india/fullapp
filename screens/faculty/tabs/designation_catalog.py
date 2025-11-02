from __future__ import annotations
from typing import Set
import streamlit as st
import pandas as pd
from sqlalchemy import text as sa_text
from sqlalchemy.engine import Engine
from screens.faculty.utils import _handle_error
from screens.faculty.db import _designation_catalog, _degree_enabled_map

def render(engine: Engine, degree: str, roles: Set[str], can_edit: bool, key_prefix: str):
    st.subheader("Designation Catalog")

    try:
        with engine.begin() as conn:
            catalog = _designation_catalog(conn)  # MR/Principal/Director + user_roles derived EXCLUDED
            enabled_map = _degree_enabled_map(conn, degree)
        display_df = pd.DataFrame({
            "Designation": catalog,
            f"Enabled in {degree}": [enabled_map.get(d.lower(), False) for d in catalog],
        })
        st.dataframe(display_df, use_container_width=True)
    except Exception as e:
        _handle_error(e, "Could not load designations.")
        return

    if not can_edit:
        st.caption("View-only.")
        return

    st.divider()

    # --- NEW SECTION TO ADD A DESIGNATION ---
    st.markdown("**Add to global catalog**")
    with st.form(key=f"{key_prefix}_add_form"):
        new_designation = st.text_input("New designation name")
        add_submitted = st.form_submit_button("Add designation")

    if add_submitted:
        if not new_designaction:
            st.warning("Please enter a designation name.")
        else:
            try:
                with engine.begin() as conn:
                    conn.execute(sa_text(
                        "INSERT INTO designations (designation) VALUES (:name)"
                    ), {"name": new_designation})
                st.success(f"Successfully added '{new_designation}' to the catalog.")
                st.rerun()  # Rerun the script to refresh the lists
            except Exception as ex:
                # Specific check for unique constraint
                if "UNIQUE constraint failed" in str(ex):
                    st.error(f"Designation '{new_designation}' already exists.")
                else:
                    _handle_error(ex, "Failed to add new designation.")
    # --- END NEW SECTION ---


    st.divider()
    st.markdown("**Enable/Disable for this degree**")
    with st.form(key=f"{key_prefix}_enable_form"):
        for d in catalog:
            st.checkbox(d, value=enabled_map.get(d.lower(), False), key=f"{key_prefix}_en_{d}")
        submitted = st.form_submit_button("Save Enablement")

    if submitted:
        try:
            with engine.begin() as conn:
                for d in catalog:
                    is_enabled = st.session_state.get(f"{key_prefix}_en_{d}", False)
                    conn.execute(sa_text("""
                        INSERT INTO designation_degree_enables(designation, degree_code, enabled)
                        VALUES(:g, :d, :e)
                        ON CONFLICT(designation, degree_code) DO UPDATE SET enabled = excluded.enabled
                    """), {"g": d, "d": degree, "e": 1 if is_enabled else 0})
            st.success("Saved enablement state."); st.rerun()
        except Exception as ex:
            _handle_error(ex, "Failed to save enablement.")
