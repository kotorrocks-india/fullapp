# test_login.py
import streamlit as st

st.set_page_config(page_title="Test", layout="wide")

st.title("Login Test")

# Display current session state
st.write("Current session state:", dict(st.session_state))

if st.button("Clear Session"):
    st.session_state.clear()
    st.rerun()

if st.button("Set Test User"):
    st.session_state["user"] = {
        "user_id": 1,
        "email": "test@example.com", 
        "username": "testuser",
        "full_name": "Test User",
        "roles": {"superadmin"},
        "role_scope": "superadmin",
        "first_login_pending": 0
    }
    st.rerun()
