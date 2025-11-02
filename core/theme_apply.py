# app/core/theme_apply.py
from __future__ import annotations
from typing import Optional, Dict, Any
import streamlit as st

from core.theme_manager import get_app_theme
from core.theme import inject_css, decide_mode


def apply_theme_for_degree(
    engine,
    degree_code: Optional[str],
    user_email: Optional[str],
) -> Dict[str, Any]:
    """
    Apply the Slide-6 theme for a given degree (with fallbacks), decide light/dark,
    inject CSS once, and return the loaded theme config.
    
    --- CHANGED ---
    This function is now simpler as get_app_theme() guarantees a complete
    theme config with defaults.
    """
    # 1) Load full theme config for the degree (or global fallback)
    #    This is now guaranteed to be a complete, merged config.
    theme_cfg: Dict[str, Any] = get_app_theme(engine, degree_code)

    # 2) Resolve light/dark mode; allow temporary in-session override if present
    forced = st.session_state.get("theme_force_mode")
    if forced in ("light", "dark"):
        mode = forced
    else:
        mode = decide_mode(theme_cfg, engine=engine, logged_email=user_email)

    # 3) Collect tokens/colors safely
    # --- CHANGED ---
    # Simplified logic, as we now have a guaranteed structure.
    theme_node = theme_cfg.get("theme", {})
    tokens = theme_node.get("tokens", {})
    colors = tokens.get(mode, tokens.get("light", {})) # Fallback to light tokens
    
    background = theme_node.get("background", {})
    primitives = theme_node.get("ui_primitives", {})
    components = theme_node.get("components", {}) # --- BUG FIX ---
    fonts = theme_cfg.get("fonts", {}) # This is at the root level

    # 4) Inject the CSS once for this render
    # --- BUG FIX ---
    # Now correctly passes the `components` dictionary to inject_css,
    # so things like sidebar colors will be applied.
    inject_css(
        mode, 
        colors, 
        background, 
        fonts, 
        primitives, 
        components=components
    )

    # 5) Stash the chosen mode for other widgets
    st.session_state["theme_mode"] = mode

    return theme_cfg
