# app/core/sidebar_logo.py
from __future__ import annotations
import streamlit as st
from typing import Optional, Dict, Any
from pathlib import Path
from urllib.parse import urlparse
from sqlalchemy import text as sa_text

# Optional: fallback to branding config if degree has no logo
try:
    from core.config_store import load_json_config
except Exception:  # pragma: no cover
    load_json_config = None


def _looks_like_url(s: str) -> bool:
    try:
        u = urlparse(s)
        return bool(u.scheme and u.netloc)
    except Exception:
        return False


def _resolve_logo_path(raw_value: Optional[str], degree_code: Optional[str]) -> Optional[str]:
    """
    Accepts:
      - Absolute/relative path (e.g., "assets/degrees/BE/logo.png")
      - Bare filename (e.g., "logo.png") -> try "assets/degrees/{DEGREE}/logo.png"
      - URL (http/https)
    Returns a string path/URL if it looks usable, else None.
    """
    if not raw_value:
        return None

    s = str(raw_value).strip()
    if _looks_like_url(s):
        return s  # Streamlit can display URLs directly

    p = Path(s)
    if p.exists():
        return str(p)

    # Bare filename → construct conventional path
    if degree_code and ("/" not in s and "\\" not in s):
        p2 = Path("assets") / "degrees" / degree_code / s
        if p2.exists():
            return str(p2)

    # As a final try, allow relative-from-app-root
    if p.is_absolute() and not p.exists():
        return None
    return str(p) if p.exists() else None


def _table_has_column(conn, table_name: str, column_name: str) -> bool:
    """Check if a table has a specific column"""
    try:
        result = conn.execute(sa_text(f"PRAGMA table_info({table_name})")).fetchall()
        return any(row[1] == column_name for row in result)
    except Exception:
        return False


def get_logo_config(engine, degree_code: Optional[str]) -> Dict[str, Any]:
    """
    Fetch logo configuration from database or fallback to branding config.
    Returns a dictionary with logo path and display settings.
    """
    config = {
        "logo_path": None,
        "max_height": "250px",
        "container_style": "text-align: center; margin: 20px 0 15px 0;",
        "image_style": "max-width: 100%; height: auto; border-radius: 8px;",
        "custom_css": "",
        "container_class": "degree-logo-container",
        "image_class": "degree-logo-image"
    }

    # 1) Try degree logo from DB
    if engine and degree_code:
        with engine.begin() as conn:
            has_display_config = _table_has_column(conn, "degrees", "logo_display_config")
            
            if has_display_config:
                row = conn.execute(
                    sa_text("SELECT logo_file_name, logo_display_config FROM degrees WHERE code=:c"),
                    {"c": degree_code},
                ).fetchone()
            else:
                row = conn.execute(
                    sa_text("SELECT logo_file_name FROM degrees WHERE code=:c"),
                    {"c": degree_code},
                ).fetchone()
            
        if row:
            config["logo_path"] = _resolve_logo_path(row[0], degree_code)
            if has_display_config and row[1]:
                try:
                    import json
                    db_config = json.loads(row[1])
                    config.update({k: v for k, v in db_config.items() if v is not None})
                except:
                    pass

    # 2) Fallback to branding logo (optional)
    if not config["logo_path"] and load_json_config is not None and engine:
        try:
            branding = load_json_config(engine, degree="default", namespace="branding") or {}
            fallback_path = branding.get("logo_path") or (branding.get("logo") or {}).get("path")
            config["logo_path"] = _resolve_logo_path(fallback_path, degree_code=None)
            
            brand_config = branding.get("logo_config") or branding.get("logo_display") or {}
            if brand_config:
                config.update({k: v for k, v in brand_config.items() if v is not None})
        except Exception:
            pass

    return config


def render_degree_sidebar_logo(engine, degree_code: Optional[str], custom_config: Dict[str, Any] = None) -> Optional[str]:
    """
    Renders the degree logo in the sidebar (top) with extensive customization options.
    Uses a DIV with background-image to bypass Streamlit's img tag styling.
    """
    
    config = get_logo_config(engine, degree_code)
    
    if custom_config:
        config.update({k: v for k, v in custom_config.items() if v is not None})
    
    logo_path = config["logo_path"]
    if not logo_path:
        return None

    degree_name = ""
    if config.get("show_degree_name") and degree_code and engine:
        try:
            with engine.begin() as conn:
                row = conn.execute(
                    sa_text("SELECT title FROM degrees WHERE code=:c"), {"c": degree_code}
                ).fetchone()
                if row:
                    degree_name = row[0]
        except Exception:
            degree_name = ""

    html_content = []
    
    # Use background-image on a div instead of img tag to avoid emotion cache styling
    override_style = f"""
    <style>
        div[data-testid="stSidebar"] .{config['container_class']} {{
            height: auto !important;
            min-height: {config['max_height']} !important;
        }}

        div[data-testid="stSidebar"] .{config['image_class']} {{
            height: {config['max_height']} !important;
            min-height: {config['max_height']} !important;
            width: 100% !important;
            background-size: contain !important;
            background-repeat: no-repeat !important;
            background-position: center !important;
            display: block !important;
            margin: 0 auto !important;
        }}
    </style>
    """
    html_content.append(override_style)
    
    if config["custom_css"]:
        html_content.append(f"<style>{config['custom_css']}</style>")

    container_style = "width: 100%; " + config["container_style"]
    container_class = config["container_class"]
    
    html_content.append(f'<div class="{container_class}" style="{container_style}">')
    
    # Use a DIV with background-image instead of IMG tag
    image_class = config["image_class"]
    div_style = f"background-image: url('{logo_path}'); height: {config['max_height']}; width: 100%; background-size: contain; background-repeat: no-repeat; background-position: center; display: block; margin: 0 auto;"
    html_content.append(f'<div class="{image_class}" style="{div_style}"></div>')
    
    if degree_name and config.get("show_degree_name"):
        name_style = config.get("degree_name_style", "margin-top: 8px; font-weight: 600; color: #333; font-size: 14px;")
        html_content.append(f'<div style="{name_style}">{degree_name}</div>')
    
    html_content.append('</div>')

    st.sidebar.markdown("\n".join(html_content), unsafe_allow_html=True)
    
    return logo_path


def render_degree_logo_with_preset(engine, degree_code: Optional[str], preset: str = "default") -> Optional[str]:
    presets = {
        "default": {"max_height": "250px"},
        "large": {"max_height": "300px"},
        "minimal": {"max_height": "180px"},
        "banner": {
            "max_height": "200px",
            "container_style": "text-align: center; margin: 20px 0 15px 0; padding: 15px; background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); border-radius: 12px;",
            "image_style": "max-width: 100%; height: auto; filter: brightness(0) invert(1);",
            "show_degree_name": True,
            "degree_name_style": "margin-top: 10px; font-weight: 700; color: white; font-size: 16px; text-shadow: 0 1px 2px rgba(0,0,0,0.3);",
        },
        "framed": {
            "max_height": "220px",
            "container_style": "text-align: center; margin: 20px 0 15px 0; padding: 15px; background: white; border: 2px solid #e0e0e0; border-radius: 12px; box-shadow: 0 2px 6px rgba(0,0,0,0.1);",
        },
        "compact": {"max_height": "120px"},
    }
    preset_config = presets.get(preset, presets["default"])
    return render_degree_sidebar_logo(engine, degree_code, preset_config)


def render_logo_for_navigation(engine, degree_code: Optional[str] = None, width: int = 250, show_when_sidebar_hidden: bool = False) -> Optional[str]:
    """
    Renders logo specifically for st.navigation pages.
    Uses a div with background-image to completely avoid img tag styling.
    
    Args:
        engine: Database engine
        degree_code: Degree code (defaults to active_degree from session_state)
        width: Logo width in pixels (default 250)
        show_when_sidebar_hidden: If True, logo will be visible even when sidebar is hidden (for login page)
    """
    if degree_code is None: 
        degree_code = st.session_state.get("active_degree")
    
    if not engine or not degree_code:
        return None
        
    try:
        with engine.begin() as conn:
            row = conn.execute(
                sa_text("SELECT logo_file_name FROM degrees WHERE code=:c"),
                {"c": degree_code},
            ).fetchone()
            
        if row and row[0]:
            logo_path = _resolve_logo_path(row[0], degree_code)
            if logo_path:
                # Use a div with background-image instead of img tag to avoid ALL img styling
                st.sidebar.markdown(f"""
                    <style>
                    .custom-sidebar-logo {{
                        width: 100%;
                        height: {width}px;
                        background-image: url('{logo_path}');
                        background-size: contain;
                        background-repeat: no-repeat;
                        background-position: center;
                        margin: 20px auto;
                        display: block;
                    }}
                    </style>
                    <div class="custom-sidebar-logo"></div>
                """, unsafe_allow_html=True)
                
                return logo_path
    except Exception:
        pass
    
    return None


def render_logo(engine, degree_code: Optional[str] = None, width: int = 250, show_when_sidebar_hidden: bool = False) -> Optional[str]:
    """
    Render logo - automatically detects if using st.navigation.
    
    Args:
        engine: Database engine
        degree_code: Degree code (defaults to active_degree from session_state)
        width: Logo width in pixels (default 250)
        show_when_sidebar_hidden: If True, logo will show even when sidebar is hidden (useful for login page)
    """
    if degree_code is None: 
        degree_code = st.session_state.get("active_degree")
    
    # Try using st.logo for navigation (Streamlit 1.30+)
    return render_logo_for_navigation(engine, degree_code, width, show_when_sidebar_hidden)


def render_logo_advanced(engine, degree_code: Optional[str] = None, **kwargs) -> Optional[str]:
    if degree_code is None: degree_code = st.session_state.get("active_degree")
    return render_degree_sidebar_logo(engine, degree_code, kwargs)
