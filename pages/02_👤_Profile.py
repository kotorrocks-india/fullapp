# pages/02_👤_Profile.py
from __future__ import annotations
import sys
from pathlib import Path

# Add path setup
APP_DIR = Path(__file__).resolve().parents[1]
if str(APP_DIR) not in sys.path:
    sys.path.insert(0, str(APP_DIR))

import streamlit as st
from screens import profile as profile_screen

# Add authentication check
if not st.session_state.get("user"):
    st.warning("Please log in to view this page.")
    try:
        st.switch_page("pages/01_🔐_Login.py")
    except:
        st.stop()
else:
    profile_screen.render()
