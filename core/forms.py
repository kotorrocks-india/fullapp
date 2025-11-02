from __future__ import annotations
import streamlit as st

def tagline():
    st.caption("Phase 1: Superadmin → Branding (Login) → Degrees → Assignments → Marks")

def success(msg: str): st.success(msg)
def warn(msg: str): st.warning(msg)
def info(msg: str): st.info(msg)
