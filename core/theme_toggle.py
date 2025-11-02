# app/core/theme_toggle.py
from __future__ import annotations
import streamlit as st
from typing import Optional
from sqlalchemy import text as sa_text
from sqlalchemy.exc import OperationalError


# --- internal: ensure table exists (in case schema wasn't run) ------------
def _ensure_theme_prefs_table(engine) -> None:
    if not engine:
        return
    with engine.begin() as conn:
        conn.execute(sa_text("""
            CREATE TABLE IF NOT EXISTS theme_prefs (
              user_email TEXT PRIMARY KEY,
              mode       TEXT NOT NULL CHECK (mode IN ('light','dark')),
              updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))


# --- internal: slide-6 remember-choice toggle ------------------------------
def _remember_choice_enabled(theme_cfg: dict) -> bool:
    # Support both slide-6 and legacy placement
    remember = (theme_cfg.get("remember_choice") or {}) \
        or (theme_cfg.get("theme") or {}).get("remember_choice") or {}
    # Default True if unspecified
    return bool(remember.get("post_login_user_prefs", True))


# --- internal: write preference --------------------------------------------
def _upsert_user_mode(engine, email: str, mode: str) -> None:
    if not (engine and email and mode in ("light", "dark")):
        return
    _ensure_theme_prefs_table(engine)
    with engine.begin() as conn:
        conn.execute(sa_text("""
            INSERT INTO theme_prefs(user_email, mode)
            VALUES(:e, :m)
            ON CONFLICT(user_email) DO UPDATE SET
                mode=excluded.mode,
                updated_at=CURRENT_TIMESTAMP
        """), {"e": email.lower(), "m": mode})


# --- public: render the toggle ---------------------------------------------
def render_theme_toggle(
    engine,
    theme_cfg: dict,
    key: str = "theme_toggle",
    location: str = "sidebar",   # "sidebar" or "inline"
    label: str = "Dark mode",
) -> Optional[str]:
    """
    Renders a Light/Dark toggle and persists the choice (if YAML allows).
    - location: "sidebar" or "inline" (inline = on the page)
    Returns the selected mode ("light"/"dark") or None if unchanged/hidden.
    """
    user = st.session_state.get("user") or {}
    email = (user.get("email") or "").strip().lower()
    current = st.session_state.get("theme_mode", "light")

    # Pick a proper container (context manager) for both locations
    if location == "sidebar":
        block = st.sidebar.container()
    else:
        block = st.container()

    with block:
        # Prefer toggle; fall back to checkbox if older Streamlit
        try:
            dark_on = current == "dark"
            dark_on = st.toggle(label, value=dark_on, key=key)
        except Exception:
            dark_on = st.checkbox(label, value=(current == "dark"), key=key)

    choice = "dark" if dark_on else "light"
    if choice != current:
        # Temporary override so CSS applies immediately (handled by theme_apply)
        st.session_state["theme_force_mode"] = choice

        # Persist to DB if allowed by YAML
        if email and _remember_choice_enabled(theme_cfg):
            try:
                _upsert_user_mode(engine, email, choice)
            except OperationalError:
                _ensure_theme_prefs_table(engine)
                _upsert_user_mode(engine, email, choice)

        st.rerun()

    return choice
