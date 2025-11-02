# app/core/theme.py
from __future__ import annotations
import streamlit as st
from sqlalchemy import text as sa_text

# ---------- optional per-user preference ----------

# --- REMOVED ---
# The function ensure_user_prefs_schema() was removed.
# All CREATE TABLE logic should be in your main core/db.py init_db().

def load_user_theme_mode(engine, email: str) -> str|None:
    if not email:
        return None
    with engine.begin() as conn:
        row = conn.execute(
            sa_text("SELECT theme_mode FROM user_prefs WHERE email=:e"),
            {"e": email},
        ).fetchone()
    return (row[0] if row else None)

def save_user_theme_mode(engine, email: str, mode: str):
    if not email or mode not in ("light", "dark"):
        return
    with engine.begin() as conn:
        conn.execute(sa_text("""
            INSERT INTO user_prefs (email, theme_mode)
            VALUES (:e, :m)
            ON CONFLICT(email) DO UPDATE SET
                theme_mode=excluded.theme_mode,
                updated_at=CURRENT_TIMESTAMP
        """), {"e": email, "m": mode})

# --- ADDED ---
def _remember_choice_enabled(theme_cfg: dict) -> bool:
    """
    Checks for the theme persistence setting in both new and legacy paths.
    Logic copied from theme_toggle.py to avoid circular import.
    """
    remember = (theme_cfg.get("remember_choice") or {}) \
        or (theme_cfg.get("theme", {}).get("remember_choice") or {}) or {}
    # Default True if unspecified
    return bool(remember.get("post_login_user_prefs", True))

# ---------- dark / light decide + apply ----------
def decide_mode(theme_cfg: dict, engine=None, logged_email: str|None=None) -> str:
    # 1) session override
    m = st.session_state.get("theme_mode")
    if m in ("light", "dark"):
        return m
    
    # 2) user pref
    # --- CHANGED ---
    # Now uses the consistent _remember_choice_enabled helper
    if engine and logged_email and _remember_choice_enabled(theme_cfg):
        pref = load_user_theme_mode(engine, logged_email)
        if pref in ("light", "dark"):
            st.session_state["theme_mode"] = pref
            return pref
            
    # 3) default
    default = theme_cfg.get("default_mode", "light")
    if default not in ("light", "dark"):
        default = "light"
    st.session_state["theme_mode"] = default
    return default

def set_mode(mode: str, engine=None, logged_email: str|None=None, theme_cfg: dict|None=None):
    if mode not in ("light", "dark"):
        return
    st.session_state["theme_mode"] = mode
    
    # --- CHANGED ---
    # Now uses the consistent _remember_choice_enabled helper
    if engine and logged_email and _remember_choice_enabled(theme_cfg or {}):
        save_user_theme_mode(engine, logged_email, mode)

# ---------- CSS injector with Slide-6 primitives ----------
def _css_vars_from_primitives(primitives: dict) -> dict:
    """
    Map Slide-6 primitives to CSS variables. Returns a dict of var->value.
    (Function content is unchanged)
    """
    p = primitives or {}
    vars_map = {}

    # Radii
    rs = (p.get("radius_scale") or {})
    for k, v in rs.items():
        vars_map[f"--radius-{k}"] = f"{int(v)}px"
    dr = (p.get("default_radius") or {})
    for part, size in dr.items():
        vars_map[f"--radius-{part}"] = f"var(--radius-{size})"

    # Borders / focus ring
    bw = (p.get("border_width") or {})
    if "thin" in bw:
        vars_map["--border-thin"] = f"{int(bw['thin'])}px"
    if "thick" in bw:
        vars_map["--border-thick"] = f"{int(bw['thick'])}px"
    fr = (p.get("focus_ring") or {})
    vars_map["--focus-width"] = f"{int(fr.get('width_px', 2))}px"
    vars_map["--focus-offset"] = f"{int(fr.get('offset_px', 2))}px"
    vars_map["--focus-style"] = fr.get("style", "outline")
    vars_map["--focus-color-mode"] = fr.get("color_mode", "accent")  # 'accent' | 'primary'

    # Elevation (shadows)
    ev = (p.get("elevation") or {})
    if "sm" in ev:
        vars_map["--shadow-sm"] = ev["sm"]
    if "md" in ev:
        vars_map["--shadow-md"] = ev["md"]
    if "lg" in ev:
        vars_map["--shadow-lg"] = ev["lg"]

    # Sizing
    sz = (p.get("sizing") or {})
    ih = (sz.get("input_heights") or {})
    bh = (sz.get("button_heights") or {})
    ic = (sz.get("icon_sizes") or {})
    if ih:
        vars_map["--input-h-sm"], vars_map["--input-h-md"], vars_map["--input-h-lg"] = \
            f"{int(ih.get('sm', 32))}px", f"{int(ih.get('md', 40))}px", f"{int(ih.get('lg', 48))}px"
    if bh:
        vars_map["--btn-h-sm"], vars_map["--btn-h-md"], vars_map["--btn-h-lg"] = \
            f"{int(bh.get('sm', 32))}px", f"{int(bh.get('md', 40))}px", f"{int(bh.get('lg', 48))}px"
    if ic:
        vars_map["--icon-sm"], vars_map["--icon-md"], vars_map["--icon-lg"] = \
            f"{int(ic.get('sm', 16))}px", f"{int(ic.get('md', 20))}px", f"{int(ic.get('lg', 24))}px"
    if "container_max_width_px" in sz:
        vars_map["--container-max"] = f"{int(sz['container_max_width_px'])}px"
    if "grid_gutter_px" in sz:
        vars_map["--gutter"] = f"{int(sz['grid_gutter_px'])}px"

    # Spacing scale
    scale = p.get("spacing_scale_px") or []
    # expose first 10 as --space-1.. --space-10
    for i, val in enumerate(scale[:10], start=1):
        vars_map[f"--space-{i}"] = f"{int(val)}px"

    return vars_map


def inject_css(
    mode: str,
    colors: dict,
    background: dict,
    fonts: dict,
    primitives: dict | None = None,
    components: dict | None = None,
):
    # --- CHANGED ---
    # Removed all hardcoded fallbacks (e.g., "... or '#0a84ff'").
    # We now trust theme_manager to provide a complete 'colors' dict.
    
    # Tokens
    primary = colors.get("primary")
    accent  = colors.get("accent") or primary # Accent can still fall back to primary

    if mode == "dark":
        surface = colors.get("surface")
        text    = colors.get("text")
        muted   = colors.get("muted")
        panel_bg = "#151823" # This is a derived value, not a token
        panel_border = "#2a2f3f" # This is a derived value, not a token
    else:
        surface = colors.get("surface")
        text    = colors.get("text")
        muted   = colors.get("muted")
        panel_bg = "#ffffff" # This is a derived value, not a token
        panel_border = "#e6e6e6" # This is a derived value, not a token

    # Background override (light only) if provided via background
    page_bg_css = ""
    if mode == "light":
        if background.get("type") == "solid_color" and isinstance(background.get("color"), str) and background["color"].startswith("#"):
            page_bg_css = f"background: {background['color']} !important;"
        elif background.get("type") == "gradient":
            g1 = background.get("start", "#fff"); g2 = background.get("end", "#f0f0f0"); ang = int(background.get("angle", 90))
            page_bg_css = f"background: linear-gradient({ang}deg, {g1}, {g2}) !important;"
        else:
            page_bg_css = f"background: {surface} !important;"
    else:
        page_bg_css = f"background: {surface} !important;"

    # Fonts
    # --- CHANGED ---
    # Removed hardcoded fallback.
    font_family = fonts.get("family")
    font_css = "" if not font_family or font_family in ("system", "system_default") else f"font-family: {font_family};"

    # Slide-6 primitives as CSS variables
    var_map = _css_vars_from_primitives(primitives or {})

    # Build :root variables
    root_vars = {
        "--color-primary": primary,
        "--color-accent": accent,
        "--color-surface": surface,
        "--color-text": text,
        "--color-muted": muted,
        "--panel-bg": panel_bg,
        "--panel-border": panel_border,
    }
    root_vars.update(var_map)
    root_css = ";\n      ".join([f"{k}: {v}" for k, v in root_vars.items()])

    # Focus ring color based on mode
    focus_color = "var(--color-accent)" if var_map.get("--focus-color-mode", "accent") == "accent" else "var(--color-primary)"

    # Sidebar colors from components
    sb_bg = sb_txt = sb_acc = None
    if isinstance(components, dict):
        sb = (components.get("sidebar", {}) or {}).get("colors", {}) or {}
        # values may be {"mode":"auto","value":"#hex"}
        def _val(node, fallback=None):
            if isinstance(node, dict):
                return node.get("value") or fallback
            return node or fallback
        sb_bg  = _val(sb.get("background"))
        sb_txt = _val(sb.get("text"))
        sb_acc = _val(sb.get("accent"))

    # Build CSS
    css_sidebar = ""
    if sb_bg or sb_txt or sb_acc:
        css_sidebar = f"""
/* Stronger selectors for Streamlit sidebar */
:root {{
  --lpep-sidebar-bg: {sb_bg or "transparent"};
  --lpep-sidebar-tx: {sb_txt or "inherit"};
  --lpep-sidebar-ac: {sb_acc or primary};
}}
[data-testid="stSidebar"],
section[data-testid="stSidebar"],
[data-testid="stSidebar"] > div:first-child,
[data-testid="stSidebar"] [data-testid="stSidebarContent"],
section[data-testid="stSidebar"] div.stSidebarContent {{
  background: var(--lpep-sidebar-bg) !important;
  color: var(--lpep-sidebar-tx) !important;
}}
[data-testid="stSidebar"] a,
section[data-testid="stSidebar"] a {{
  color: var(--lpep-sidebar-ac) !important;
}}
[data-testid="stSidebar"] .stButton > button {{
  background: var(--lpep-sidebar-ac) !important;
  border-color: var(--lpep-sidebar-ac) !important;
  color: #fff !important;
}}
"""

    # (Rest of CSS injection is unchanged)
    st.markdown(f"""
    <style>
      :root {{
        {root_css};
      }}

      /* Page base */
      div.block-container {{
        {page_bg_css}
        {font_css}
        max-width: var(--container-max, 1280px);
      }}

      /* Buttons */
      .stButton>button {{
        height: var(--btn-h-md, 40px);
        background: var(--color-primary);
        border: var(--border-thin, 1px) solid var(--color-primary);
        color: #fff;
        border-radius: var(--radius-buttons, var(--radius-md, 6px));
        box-shadow: var(--shadow-sm, none);
      }}
      .stButton>button:focus {{
        outline: var(--focus-width, 2px) var(--focus-style, outline) {focus_color};
        outline-offset: var(--focus-offset, 2px);
      }}

      /* Inputs & selects (best effort across Streamlit DOM) */
      .stTextInput>div>div>input,
      .stSelectbox>div>div>div,
      .stNumberInput input {{
        height: var(--input-h-md, 40px);
        background: var(--panel-bg);
        color: var(--color-text);
        border: var(--border-thin, 1px) solid var(--panel-border);
        border-radius: var(--radius-inputs, var(--radius-md, 6px));
      }}

      /* Tables */
      .stDataFrame, .stTable {{
        background: var(--panel-bg);
        color: var(--color-text);
        border: var(--border-thin, 1px) solid var(--panel-border);
        border-radius: var(--radius-cards, var(--radius-md, 6px));
        box-shadow: var(--shadow-sm, none);
      }}

      /* Sidebar border & text defaults */
      section[data-testid="stSidebar"] {{
        border-right: var(--border-thin, 1px) solid var(--panel-border);
      }}

      /* Hide default accent block if present */
      div[data-testid="stDecoration"] {{ display: none; }}

      /* Typography */
      .stMarkdown, label, p, h1, h2, h3, h4, h5, h6 {{
        color: var(--color-text);
      }}
      a, .stMarkdown a {{
        color: var(--color-primary) !important;
      }}

      /* Spacing helpers */
      .app-gutter {{
        padding-left: var(--gutter, 16px);
        padding-right: var(--gutter, 16px);
      }}

      {css_sidebar}
    </style>
    """, unsafe_allow_html=True)
