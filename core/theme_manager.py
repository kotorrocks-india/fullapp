# app/core/theme_manager.py
from __future__ import annotations
import json
from sqlalchemy import text as sa_text
import copy # --- ADDED ---

NAMESPACE = "app_theme"  # Slide 6

# --- ADDED ---
# This is now the single source of truth for all default theme values.
# It's based on the fallbacks that were previously scattered across
# theme.py and theme_apply.py.
DEFAULT_THEME_CONFIG = {
    "theme": {
        "tokens": {
            "light": {
                "primary": "#0a84ff",
                "accent": "#0a84ff",
                "surface": "#ffffff",
                "text": "#111111",
                "muted": "#6b7280"
            },
            "dark": {
                "primary": "#0a84ff",
                "accent": "#0a84ff",
                "surface": "#0f1116",
                "text": "#e6e6e6",
                "muted": "#9aa3b2"
            }
        },
        "background": {},
        "ui_primitives": {},
        "components": {},
        "remember_choice": {
            "post_login_user_prefs": True
        }
    },
    "fonts": {
        "family": "system"
    },
    "default_mode": "light"
}

# --- ADDED ---
def _deep_merge(base: dict, new: dict) -> dict:
    """Recursively merges `new` dict into `base` dict."""
    for k, v in new.items():
        if isinstance(v, dict) and k in base and isinstance(base[k], dict):
            base[k] = _deep_merge(base[k], v)
        else:
            base[k] = v
    return base

def _fetch(conn, degree: str):
    return conn.execute(
        sa_text("SELECT config_json FROM configs WHERE namespace=:ns AND degree=:d ORDER BY updated_at DESC LIMIT 1"),
        {"ns": NAMESPACE, "d": degree}
    ).fetchone()

def get_app_theme(engine, degree: str | None = None) -> dict:
    """
    Return theme for degree, else global ('*'), else latest any-degree.
    
    --- CHANGED ---
    Now returns a deep merge of the loaded config on top of the 
    DEFAULT_THEME_CONFIG, ensuring a complete theme object is always returned.
    """
    
    db_config = {}
    with engine.begin() as conn:
        row_to_load = None
        if degree:
            row_to_load = _fetch(conn, degree)
        
        if not row_to_load:
            row_to_load = _fetch(conn, "*")
            
        if not row_to_load:
            row_to_load = conn.execute(sa_text(
                "SELECT config_json FROM configs WHERE namespace=:ns ORDER BY updated_at DESC LIMIT 1"
            ), {"ns": NAMESPACE}).fetchone()

        if row_to_load:
            try: 
                db_config = json.loads(row_to_load[0]) or {}
            except Exception: 
                db_config = {}

    # --- CHANGED ---
    # Start with a fresh copy of the defaults and merge the loaded config onto it
    final_config = copy.deepcopy(DEFAULT_THEME_CONFIG)
    final_config = _deep_merge(final_config, db_config)
    
    return final_config
