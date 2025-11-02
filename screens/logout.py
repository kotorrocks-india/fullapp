# app/screens/logout.py
from __future__ import annotations
import streamlit as st

def render():
    # --- FIX 1: Hide the sidebar for a clean logout screen ---
    st.markdown(
        """
        <style>
            [data-testid="stSidebar"] {
                display: none;
            }
        </style>
        """,
        unsafe_allow_html=True,
    )

    st.title("🚪 Logout")

    # --- FIX 2: Clear all relevant session state keys ---
    # This ensures a full and clean logout.
    keys_to_clear = ["user", "active_degree", "route", "roles"]
    for key in keys_to_clear:
        if key in st.session_state:
            del st.session_state[key]

    st.success("You have been logged out successfully.")
    
    # --- FIX 3: Provide a clear, primary button to return ---
    # This creates a better user experience than a simple link.
    if st.button("Go to Login Page", type="primary"):
        st.switch_page("pages/01_🔐_Login.py")

# This allows the script to be run directly as a Streamlit page
if __name__ == "__main__":
    render()
