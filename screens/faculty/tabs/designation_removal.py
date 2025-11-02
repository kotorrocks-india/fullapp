# app/screens/faculty/tabs/designation_removal.py
from __future__ import annotations
from typing import Set

import streamlit as st
from sqlalchemy import text as sa_text
from sqlalchemy.engine import Engine

from screens.faculty.utils import _handle_error

def render(engine: Engine, degree: str, roles: Set[str], can_edit: bool, key_prefix: str):
    if not can_edit:
        st.info("Edit permission required")
        return

    st.subheader("Remove Designations")

    try:
        with engine.begin() as conn:
            active_desg = [r[0] for r in conn.execute(sa_text(
                """
                SELECT designation FROM designations
                WHERE is_active = 1
                  AND designation NOT IN (
                    SELECT DISTINCT designation FROM academic_admins WHERE designation IS NOT NULL
                  )
                  AND lower(designation) NOT LIKE '%visit%'
                  AND lower(designation) NOT LIKE '%core%'
                  AND lower(designation) NOT LIKE '%custom%'
                ORDER BY designation
                """
            )).fetchall()]
    except Exception as e:
        _handle_error(e, "Could not load designations.")
        return

    if not active_desg:
        st.info("No active designations found")
        return

    removal_desg = st.selectbox("Select designation to remove", options=active_desg, key=f"{key_prefix}_removal")
    if not removal_desg:
        return

    try:
        with engine.begin() as conn:
            affected = conn.execute(sa_text(
                """
                SELECT COUNT(*)
                FROM faculty_affiliations
                WHERE designation=:d AND active=1
                """
            ), {"d": removal_desg}).scalar_one()
    except Exception as e:
        _handle_error(e, "Could not count affected affiliations.")
        return

    st.warning(f"This will affect {affected} affiliations.")

    new_desg_options = [d for d in active_desg if d != removal_desg]
    new_desg = st.selectbox("Reassign to designation", options=new_desg_options, key=f"{key_prefix}_reassign")

    confirm = st.checkbox("I understand and wish to proceed", key=f"{key_prefix}_confirm")
    if st.button("Reassign & Remove", type="primary", key=f"{key_prefix}_exec", disabled=not confirm):
        try:
            with engine.begin() as conn:
                conn.execute(sa_text(
                    """
                    UPDATE faculty_affiliations SET designation=:new
                    WHERE designation=:old AND active=1
                    """
                ), {"new": new_desg, "old": removal_desg})
                conn.execute(sa_text(
                    "UPDATE designations SET is_active=0, updated_at=CURRENT_TIMESTAMP WHERE designation=:d"
                ), {"d": removal_desg})
            st.success("Designation removed and affiliations reassigned.")
            st.rerun()
        except Exception as e:
            _handle_error(e, "Reassignment failed.")
