# app/screens/faculty/tabs/export_credentials.py
from __future__ import annotations
from typing import Set

import streamlit as st
import pandas as pd
from sqlalchemy import text as sa_text
from sqlalchemy.engine import Engine

from screens.faculty.utils import _handle_error

def render(engine: Engine, degree: str, roles: Set[str], can_edit: bool, key_prefix: str):
    st.subheader("Export Initial Credentials")

    try:
        with engine.begin() as conn:
            rows = conn.execute(sa_text(
                """
                SELECT
                    fp.id AS faculty_profile_id,
                    fp.email,
                    fp.username,
                    ic.plaintext AS initial_password
                FROM faculty_profiles fp
                JOIN faculty_initial_credentials ic
                  ON ic.faculty_profile_id = fp.id AND ic.consumed = 0
                WHERE fp.status = 'active'
                  AND fp.first_login_pending = 1
                  AND fp.password_export_available = 1
                ORDER BY fp.email
                """
            )).fetchall()
        if rows:
            data = [dict(r._mapping) for r in rows]
            df = pd.DataFrame(data)
            st.dataframe(df, use_container_width=True, hide_index=True)
            csv = df.to_csv(index=False).encode("utf-8")

            if st.download_button(
                "⬇️ Download Faculty Credentials CSV",
                data=csv,
                file_name="faculty_initial_credentials.csv",
                mime="text/csv",
                key=f"{key_prefix}_dl_csv"
            ):
                ids = [int(d["faculty_profile_id"]) for d in data]
                with engine.begin() as conn:
                    for pid in ids:
                        conn.execute(sa_text("UPDATE faculty_initial_credentials SET consumed=1 WHERE consumed=0 AND faculty_profile_id=:pid"), {"pid": pid})
                        conn.execute(sa_text("UPDATE faculty_profiles SET password_export_available=0 WHERE first_login_pending=1 AND id=:pid"), {"pid": pid})
                st.success("Exported and invalidated plaintexts.")
                st.rerun()
        else:
            st.info("No faculty initial credentials available for export.")
    except Exception as e:
        _handle_error(e, "Failed to load or export credentials.")
