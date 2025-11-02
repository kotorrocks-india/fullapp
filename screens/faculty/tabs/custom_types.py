# screens/faculty/tabs/custom_types.py
from __future__ import annotations
from typing import Set

import streamlit as st
import pandas as pd
from sqlalchemy.engine import Engine
# Removed unused imports: sa_text, _handle_error

def render(engine: Engine, degree: str, roles: Set[str], can_edit: bool, key_prefix: str):
    st.subheader("Affiliation Types")

    # --- FIXED: Display only core and visiting ---
    fixed_types = [
        {"Type Code": "core", "Description": "Core faculty member", "Is System": 1},
        {"Type Code": "visiting", "Description": "Visiting faculty member", "Is System": 1},
    ]

    df = pd.DataFrame(fixed_types)

    st.dataframe(df, use_container_width=True, hide_index=True)

    st.caption("System-defined affiliation types. This list is view-only.")
    st.info("ℹ️ The only available affiliation types are 'core' and 'visiting'.")
