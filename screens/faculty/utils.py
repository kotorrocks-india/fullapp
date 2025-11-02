# app/screens/faculty/utils.py
# -------------------------------------------------------------------
# Utility functions for the Faculty module
# -------------------------------------------------------------------
from __future__ import annotations
import pandas as pd
import streamlit as st
import logging

from core.settings import load_settings

# Set up a logger for server-side logging
logger = logging.getLogger(__name__)

def _safe_int_convert(value, default=0) -> int:
    """Safely convert pandas/CSV values (like NaN, '', or None) to int."""
    if pd.isna(value) or value == '':
        return default
    try:
        # Cast to float first to handle "123.0" then to int
        return int(float(value))
    except (ValueError, TypeError):
        return default

def _handle_error(e: Exception, user_message: str = "An error occurred."):
    """
    Log the full exception server-side and show a friendly or
    detailed error in Streamlit based on the debug setting.
    """
    settings = load_settings()
    logger.error(f"Faculty module error: {e}", exc_info=True)
    
    if getattr(settings, "debug", False):
        # In debug mode, show the full error
        st.error(f"{user_message}\n\n**Debug Info:**\n```\n{e}\n```")
    else:
        # In production, show a generic message
        st.error(user_message)
