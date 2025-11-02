# app/screens/superadmin.py
from __future__ import annotations

import streamlit as st
from core.settings import load_settings
from core.forms import tagline

def _is_superadmin() -> bool:
    u = st.session_state.get("user") or {}
    roles = u.get("roles") or set()
    # allow legacy 'role' string just in case
    legacy = u.get("role")
    return ("superadmin" in roles) or (legacy == "superadmin")

def render():
    settings = load_settings()

    st.title("🛠️ Superadmin Console")
    tagline()
    st.write("Manage platform-wide settings and access admin tools.")

    # If user is signed in and is superadmin, show console
    user = st.session_state.get("user")
    if user and _is_superadmin():
        roles = user.get("roles") or {user.get("role")} if user.get("role") else set()
        st.success(f"Logged in as **{user.get('email','?')}** · roles: {', '.join(sorted(roles)) or '—'}")

        st.markdown("### Quick links")
        cols = st.columns(3)
        with cols[0]:
            st.page_link("pages/10_🎨_Branding_Login.py", label="🎨 Branding (Login)")
            st.page_link("pages/11_🦶_Footer.py", label="🦶 Footer")
        with cols[1]:
            st.page_link("pages/12_🎛️_Appearance_Theme.py", label="🎛️ Appearance / Theme")
            st.page_link("pages/20_🎓_Degrees.py", label="🎓 Degrees")
        with cols[2]:
            st.page_link("pages/21_📝_Assignments.py", label="📝 Assignments")
            st.page_link("pages/23_📬_Approvals.py", label="📬 Approvals")

        st.markdown("---")
        st.info(
            "Tip: Use **Users & Roles** to add Tech Admins and Academic Admins, "
            "and export initial credentials when needed."
        )
        st.page_link("pages/03_👥_Users_Roles.py", label="👥 Users & Roles")

        return

    # Not signed in (or not superadmin) → nudge to the centralized Login page
    st.warning("You’re not signed in as superadmin.")
    st.page_link("pages/01_🔐_Login.py", label="Go to Login")

    # Optional: keep a DEV demo login (stores roles in the new shape).
    # Set ENABLE_DEMO_SUPERADMIN=True in your settings if you want this.
    if getattr(settings.auth, "enable_demo_superadmin", False):
        st.markdown("#### Demo sign-in (development only)")
        with st.form("demo_sa_login"):
            email = st.text_input("Email", value=getattr(settings.auth, "demo_superadmin_user", "admin@example.com"))
            pwd = st.text_input("Password", value=getattr(settings.auth, "demo_superadmin_pass", "admin"), type="password")
            submitted = st.form_submit_button("Login as Superadmin")
            if submitted:
                if (email == getattr(settings.auth, "demo_superadmin_user", "")) and (
                    pwd == getattr(settings.auth, "demo_superadmin_pass", "")
                ):
                    st.session_state["user"] = {
                        "email": email,
                        "full_name": "Super Admin",
                        "roles": {"superadmin"},   # ← NEW SHAPE
                        "role_scope": "superadmin"
                    }
                    st.success("Logged in! Open a page from the sidebar.")
                    st.rerun()
                else:
                    st.error("Invalid credentials")
