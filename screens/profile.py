# screens/profile.py
import streamlit as st

# The security decorator and its import have been removed from this file.
# from core.policy import require_page  <-- REMOVED

from core.theme_toggle import render_theme_toggle
from core.theme_apply import apply_theme_for_degree

# The decorator is no longer needed here because app.py already
# verified the user has permission to see this page.
# @require_page("Profile")  <-- REMOVED
def render():
    st.title("👤 Profile")

    engine = st.session_state.get("engine")
    user = st.session_state.get("user") or {}
    email = (user.get("email") or "").strip().lower()
    roles = user.get("roles") or []

    if not user:
        st.error("User not found in session. Please log in again.")
        return

    # Light/Dark inline toggle on the page
    theme_cfg = apply_theme_for_degree(engine, st.session_state.get("active_degree"), email)
    render_theme_toggle(engine, theme_cfg, key="profile_theme_toggle", location="inline", label="Dark mode")

    st.markdown("### Account")
    st.json({
        "name": user.get("full_name") or email or "—",
        "email": email or "—",
        "roles": sorted(list(roles)) or ["—"],
    })

    st.info("This is a neutral landing page shown right after login.")
