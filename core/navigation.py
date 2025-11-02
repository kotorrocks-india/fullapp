# app/core/navigation.py
import streamlit as st

def navigate_to_login():
    """Navigate to login page"""
    st.session_state["show_login"] = True
    st.rerun()

def navigate_to_logout():
    """Navigate to logout page"""
    st.session_state["show_logout"] = True
    st.rerun()

def navigate_to_app():
    """Navigate to main app"""
    if "show_login" in st.session_state:
        del st.session_state["show_login"]
    if "show_logout" in st.session_state:
        del st.session_state["show_logout"]
    st.rerun()
