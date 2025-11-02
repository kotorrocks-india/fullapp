# app/screens/appearance_theme.py
from __future__ import annotations

import json
import streamlit as st
from sqlalchemy import text as sa_text

from core.settings import load_settings
from core.db import get_engine, init_db
from core.policy import require_page, user_roles, can_edit_page
from core.theme_manager import get_app_theme
from core.theme import decide_mode, inject_css
from core.ui import render_footer_global
from core.theme_profiles import (
    list_profiles, load_profile, save_profile, delete_profile, apply_profile_to_draft
)

def _ensure_path(d: dict, path: list[str], default: dict | None = None) -> dict:
    cur = d
    for i, key in enumerate(path):
        if key not in cur or not isinstance(cur[key], dict):
            cur[key] = {} if i < len(path) - 1 else (default or {})
        cur = cur[key]
    return cur

def _K(name: str) -> str:
    """Helper to create unique session state keys"""
    return f"theme_cp_{name}"

def _set_path(d: dict, path: list[str], value):
    """
    Safely sets a value in a nested dictionary based on a list of keys.
    e.g., _set_path(d, ["a", "b", "c"], 10) -> d["a"]["b"]["c"] = 10
    """
    cur = d
    for i, key in enumerate(path):
        if i == len(path) - 1:
            cur[key] = value
        else:
            if key not in cur or not isinstance(cur[key], dict):
                cur[key] = {}
            cur = cur[key]

# This CONFIG_MAP defines the single source of truth for the theme structure.
# It maps the widget key (from _K()) to its path in the JSON config.
CONFIG_MAP = {
    # Design Tokens (Light)
    _K("l_primary"): ["theme", "tokens", "light", "primary"],
    _K("l_surface"): ["theme", "tokens", "light", "surface"],
    _K("l_text"):    ["theme", "tokens", "light", "text"],
    _K("l_muted"):   ["theme", "tokens", "light", "muted"],
    _K("l_accent"):  ["theme", "tokens", "light", "accent"],
    # Design Tokens (Dark)
    _K("d_primary"): ["theme", "tokens", "dark", "primary"],
    _K("d_surface"): ["theme", "tokens", "dark", "surface"],
    _K("d_text"):    ["theme", "tokens", "dark", "text"],
    _K("d_muted"):   ["theme", "tokens", "dark", "muted"],
    _K("d_accent"):  ["theme", "tokens", "dark", "accent"],
    # Components - Sidebar
    _K("sb_bg"):  ["theme", "components", "sidebar", "colors", "background", "value"],
    _K("sb_txt"): ["theme", "components", "sidebar", "colors", "text", "value"],
    _K("sb_acc"): ["theme", "components", "sidebar", "colors", "accent", "value"],
    # Components - Tables
    _K("tb_hbg"): ["theme", "components", "tables", "colors", "header_bg", "value"],
    _K("tb_htx"): ["theme", "components", "tables", "colors", "header_text", "value"],
    _K("tb_rbg"): ["theme", "components", "tables", "colors", "row_bg", "value"],
    _K("tb_rtx"): ["theme", "components", "tables", "colors", "row_text", "value"],
    _K("tb_brd"): ["theme", "components", "tables", "colors", "border", "value"],
    # Components - Dropdowns
    _K("dd_bg"):  ["theme", "components", "dropdowns", "colors", "bg", "value"],
    _K("dd_txt"): ["theme", "components", "dropdowns", "colors", "text", "value"],
    _K("dd_brd"): ["theme", "components", "dropdowns", "colors", "border", "value"],
    _K("dd_hbg"): ["theme", "components", "dropdowns", "colors", "hover_bg", "value"],
    # Components - Form Inputs
    _K("fi_bg"):  ["theme", "components", "forms", "inputs", "colors", "bg", "value"],
    _K("fi_txt"): ["theme", "components", "forms", "inputs", "colors", "text", "value"],
    _K("fi_brd"): ["theme", "components", "forms", "inputs", "colors", "border", "value"],
    _K("fi_ph"):  ["theme", "components", "forms", "inputs", "colors", "placeholder", "value"],
    # Components - Buttons (Submit)
    _K("fb_s_bg"):  ["theme", "components", "forms", "buttons", "submit", "colors", "bg", "value"],
    _K("fb_s_txt"): ["theme", "components", "forms", "buttons", "submit", "colors", "text", "value"],
    _K("fb_s_brd"): ["theme", "components", "forms", "buttons", "submit", "colors", "border", "value"],
    # Components - Buttons (Primary)
    _K("fb_p_bg"):  ["theme", "components", "forms", "buttons", "primary", "colors", "bg", "value"],
    _K("fb_p_txt"): ["theme", "components", "forms", "buttons", "primary", "colors", "text", "value"],
    _K("fb_p_brd"): ["theme", "components", "forms", "buttons", "primary", "colors", "border", "value"],
    # Components - Buttons (Secondary)
    _K("fb_s2_bg"):  ["theme", "components", "forms", "buttons", "secondary", "colors", "bg", "value"],
    _K("fb_s2_txt"): ["theme", "components", "forms", "buttons", "secondary", "colors", "text", "value"],
    _K("fb_s2_brd"): ["theme", "components", "forms", "buttons", "secondary", "colors", "border", "value"],
    # Components - Buttons (Danger)
    _K("fb_d_bg"):  ["theme", "components", "forms", "buttons", "danger", "colors", "bg", "value"],
    _K("fb_d_txt"): ["theme", "components", "forms", "buttons", "danger", "colors", "text", "value"],
    _K("fb_d_brd"): ["theme", "components", "forms", "buttons", "danger", "colors", "border", "value"],
    # Components - Headers
    _K("hd_txt"): ["theme", "components", "headers", "colors", "text", "value"],
    _K("hd_ulv"): ["theme", "components", "headers", "colors", "underline", "value"],
    # UI Primitives - Radius Scale
    _K("r_none"): ["theme", "ui_primitives", "shape", "radius_scale", "none"],
    _K("r_sm"):   ["theme", "ui_primitives", "shape", "radius_scale", "sm"],
    _K("r_md"):   ["theme", "ui_primitives", "shape", "radius_scale", "md"],
    _K("r_lg"):   ["theme", "ui_primitives", "shape", "radius_scale", "lg"],
    _K("r_xl"):   ["theme", "ui_primitives", "shape", "radius_scale", "xl"],
    _K("r_pill"): ["theme", "ui_primitives", "shape", "radius_scale", "pill"],
    # UI Primitives - Default Radius
    _K("dr_inputs"):  ["theme", "ui_primitives", "shape", "default_radius", "inputs"],
    _K("dr_buttons"): ["theme", "ui_primitives", "shape", "default_radius", "buttons"],
    _K("dr_cards"):   ["theme", "ui_primitives", "shape", "default_radius", "cards"],
    _K("dr_modals"):  ["theme", "ui_primitives", "shape", "default_radius", "modals"],
    _K("dr_sidebar"): ["theme", "ui_primitives", "shape", "default_radius", "sidebar"],
    # UI Primitives - Borders
    _K("bw_thin"):   ["theme", "ui_primitives", "borders", "width", "thin"],
    _K("bw_thick"):  ["theme", "ui_primitives", "borders", "width", "thick"],
    _K("fr_width"):  ["theme", "ui_primitives", "borders", "focus_ring", "width_px"],
    _K("fr_off"):    ["theme", "ui_primitives", "borders", "focus_ring", "offset_px"],
    _K("fr_style"):  ["theme", "ui_primitives", "borders", "focus_ring", "style"],
    _K("fr_color"):  ["theme", "ui_primitives", "borders", "focus_ring", "color_mode"],
    # UI Primitives - Elevation
    _K("el_none"): ["theme", "ui_primitives", "elevation", "none"],
    _K("el_sm"):   ["theme", "ui_primitives", "elevation", "sm"],
    _K("el_md"):   ["theme", "ui_primitives", "elevation", "md"],
    _K("el_lg"):   ["theme", "ui_primitives", "elevation", "lg"],
    # UI Primitives - Sizing
    _K("ih_sm"): ["theme", "ui_primitives", "sizing", "input_heights", "sm"],
    _K("ih_md"): ["theme", "ui_primitives", "sizing", "input_heights", "md"],
    _K("ih_lg"): ["theme", "ui_primitives", "sizing", "input_heights", "lg"],
    _K("bn_sm"): ["theme", "ui_primitives", "sizing", "button_heights", "sm"],
    _K("bn_md"): ["theme", "ui_primitives", "sizing", "button_heights", "md"],
    _K("bn_lg"): ["theme", "ui_primitives", "sizing", "button_heights", "lg"],
    _K("ic_sm"): ["theme", "ui_primitives", "sizing", "icon_sizes", "sm"],
    _K("ic_md"): ["theme", "ui_primitives", "sizing", "icon_sizes", "md"],
    _K("ic_lg"): ["theme", "ui_primitives", "sizing", "icon_sizes", "lg"],
    _K("container_max"): ["theme", "ui_primitives", "sizing", "container_max_width_px"],
    _K("grid_gutter"):   ["theme", "ui_primitives", "sizing", "grid_gutter_px"],
    # UI Primitives - Spacing
    _K("spacing"): ["theme", "ui_primitives", "spacing_scale_px"],
    # Fonts
    _K("fg_family"): ["fonts", "global_defaults", "family"],
    _K("fg_size"):   ["fonts", "global_defaults", "size_px"],
    _K("fg_weight"): ["fonts", "global_defaults", "weight"],
    _K("fg_style"):  ["fonts", "global_defaults", "style"],
    _K("h_inherit"): ["fonts", "headers_and_titles", "inherit_from_global"],
    _K("h_delta"):   ["fonts", "headers_and_titles", "size_delta_vs_content_px"],
    _K("h_weight"):  ["fonts", "headers_and_titles", "default_weight"],
}


@require_page("Appearance / Theme")
def render():
    settings = load_settings()
    engine = get_engine(settings.db.url)
    
    st.session_state["engine"] = engine

    roles = user_roles()
    CAN_EDIT = can_edit_page("Appearance / Theme", roles)
    CAN_PUBLISH = "superadmin" in roles

    cfg = get_app_theme(engine, degree=None) or {}
    cfg.setdefault("theme", {})
    cfg.setdefault("fonts", {})
    cfg.setdefault("workflow", {})
    cfg.setdefault("preview", {})
    cfg.setdefault("high_contrast", {"user_visible": True})

    # Pre-create nodes for easier access
    ui_primitives = _ensure_path(cfg, ["theme", "ui_primitives"], {})
    tokens_node  = _ensure_path(cfg, ["theme", "tokens"], {})
    components   = _ensure_path(cfg, ["theme", "components"], {})
    fonts_node   = _ensure_path(cfg, ["fonts"], {})

    # --- FIX ---
    # Moved _write_cfg here, before it is called by the "Save Profile" button.
    # It depends on 'ui_primitives', so it must come after that is defined.
    def _write_cfg(state: str, base_cfg: dict):
        """
        Populates the base_cfg dict with values from st.session_state
        based on the CONFIG_MAP.
        """
        def _mv(v): return {"mode": "auto", "value": v}
        
        # Get the original spacing values as a fallback
        default_spacing = ui_primitives.get("spacing_scale_px") or [2,4,6,8,12,16,20,24,32]

        for key, path in CONFIG_MAP.items():
            if key not in st.session_state:
                continue

            value = st.session_state[key]
            final_path = path
            final_value = value
            
            # --- Handle Special Cases ---

            # Case 1: Spacing string needs to be parsed into a list of ints
            if key == _K("spacing"):
                try:
                    parsed = [int(x.strip()) for x in (value or "").split(",") if x.strip().isdigit()]
                    final_value = parsed or default_spacing # Use default if parsing results in empty list
                except Exception:
                    final_value = default_spacing # Fallback on any error
            
            # Case 2: Component colors need to be wrapped in the {"mode": ..., "value": ...} dict
            elif "components" in path and path[-1] == "value":
                final_path = path[:-1] # Go up one level (e.g., to "background")
                final_value = _mv(value)
            
            # Set the value in the nested dictionary
            _set_path(base_cfg, final_path, final_value)

        # Finally, set the workflow state
        _set_path(base_cfg, ["workflow", "publish", "state"], state)
    # --- END MOVED FUNCTION ---

    st.title("🎛️ Appearance / Theme (Slide 6)")
    mode_cfg = {"default_mode": "light", "remember_choice": {"post_login_user_prefs": True}}
    user = st.session_state.get("user") or {}
    email = (user.get("email") or "").strip().lower()
    mode = decide_mode(mode_cfg, engine=engine, logged_email=email)

    READ_ONLY = False if CAN_EDIT else True
    if CAN_EDIT:
        READ_ONLY = st.toggle("Preview (read-only)", value=False, key=_K("readonly"))

    st.caption(
        "Login-page branding/fonts are on Slide 1; this slide controls in-app theme. "
        "High-contrast is per-user. WCAG AA guardrails apply."
    )

    # <editor-fold desc="Full UI Rendering Logic">
    with st.expander("Theme profiles (save / load)", expanded=False):
        cols = st.columns([1, 1, 1, 2])
        with cols[0]:
            prof_name = st.text_input("New profile name", key=_K("prof_name"))
            if st.button("Save current to profile", disabled=(not CAN_EDIT or not prof_name), key=_K("prof_save")):
                try:
                    # We need to build the cfg from session state *before* saving
                    _write_cfg("draft", cfg) # Use "draft" state, it doesn't matter for profile
                    save_profile(engine, prof_name, cfg)
                    st.success(f"Saved profile: {prof_name}")
                except Exception as ex:
                    st.error(str(ex))
        with cols[1]:
            existing = [""] + list_profiles(engine)
            pick = st.selectbox("Load profile", existing, index=0, key=_K("prof_pick"))
            if st.button("Apply to draft", disabled=(not CAN_EDIT or not pick), key=_K("prof_apply")):
                try:
                    apply_profile_to_draft(engine, pick)
                    st.success(f"Applied profile '{pick}' into draft. Reload page to reflect persisted config.")
                except Exception as ex:
                    st.error(str(ex))
        with cols[2]:
            del_pick = st.selectbox("Delete profile", existing, index=0, key=_K("prof_del_pick"))
            if st.button("Delete profile", disabled=(not CAN_EDIT or not del_pick), key=_K("prof_delete")):
                try:
                    delete_profile(engine, del_pick)
                    st.success(f"Deleted profile '{del_pick}'")
                except Exception as ex:
                    st.error(str(ex))
        with cols[3]:
            st.info("Profiles are stored in `configs` under the `theme_profiles` namespace.")

    st.markdown("---")

    st.header("Design Tokens")
    lt = tokens_node.get("light", {}) or {}
    dk = tokens_node.get("dark", {}) or {}

    col = st.columns(5)
    with col[0]: l_primary = st.color_picker("Primary", lt.get("primary") or "#3B82F6", key=_K("l_primary"), disabled=READ_ONLY)
    with col[1]: l_surface = st.color_picker("Surface", lt.get("surface") or "#FFFFFF", key=_K("l_surface"), disabled=READ_ONLY)
    with col[2]: l_text = st.color_picker("Text", lt.get("text") or "#111111", key=_K("l_text"), disabled=READ_ONLY)
    with col[3]: l_muted = st.color_picker("Muted", lt.get("muted") or "#6B7280", key=_K("l_muted"), disabled=READ_ONLY)
    with col[4]: l_accent = st.color_picker("Accent", lt.get("accent") or "#10B981", key=_K("l_accent"), disabled=READ_ONLY)

    col = st.columns(5)
    with col[0]: d_primary = st.color_picker("Primary (dark)", dk.get("primary") or "#60A5FA", key=_K("d_primary"), disabled=READ_ONLY)
    with col[1]: d_surface = st.color_picker("Surface (dark)", dk.get("surface") or "#0B1020", key=_K("d_surface"), disabled=READ_ONLY)
    with col[2]: d_text = st.color_picker("Text (dark)", dk.get("text") or "#E5E7EB", key=_K("d_text"), disabled=READ_ONLY)
    with col[3]: d_muted = st.color_picker("Muted (dark)", dk.get("muted") or "#9CA3AF", key=_K("d_muted"), disabled=READ_ONLY)
    with col[4]: d_accent = st.color_picker("Accent (dark)", dk.get("accent") or "#34D399", key=_K("d_accent"), disabled=READ_ONLY)

    st.header("Component Colors")
    st.subheader("Sidebar")
    sb = _ensure_path(components, ["sidebar", "colors"], {})
    sb_bg   = st.color_picker("Sidebar background", (sb.get("background") or {}).get("value") or "#FFFFFF", key=_K("sb_bg"), disabled=READ_ONLY)
    sb_txt  = st.color_picker("Sidebar text",       (sb.get("text") or {}).get("value") or "#111111", key=_K("sb_txt"), disabled=READ_ONLY)
    sb_acc  = st.color_picker("Sidebar accent",     (sb.get("accent") or {}).get("value") or l_primary, key=_K("sb_acc"), disabled=READ_ONLY)

    st.subheader("Tables")
    tb = _ensure_path(components, ["tables", "colors"], {})
    tb_hbg = st.color_picker("Header background", (tb.get("header_bg") or {}).get("value") or "#F5F6F8", key=_K("tb_hbg"), disabled=READ_ONLY)
    tb_htx = st.color_picker("Header text",       (tb.get("header_text") or {}).get("value") or "#111111", key=_K("tb_htx"), disabled=READ_ONLY)
    tb_rbg = st.color_picker("Row background",    (tb.get("row_bg") or {}).get("value") or "#FFFFFF", key=_K("tb_rbg"), disabled=READ_ONLY)
    tb_rtx = st.color_picker("Row text",          (tb.get("row_text") or {}).get("value") or "#111111", key=_K("tb_rtx"), disabled=READ_ONLY)
    tb_br  = st.color_picker("Border",            (tb.get("border") or {}).get("value") or "#E5E7EB", key=_K("tb_brd"), disabled=READ_ONLY)

    st.subheader("Dropdowns")
    dd = _ensure_path(components, ["dropdowns", "colors"], {})
    dd_bg  = st.color_picker("Dropdown bg",       (dd.get("bg") or {}).get("value") or "#FFFFFF", key=_K("dd_bg"), disabled=READ_ONLY)
    dd_txt = st.color_picker("Dropdown text",     (dd.get("text") or {}).get("value") or "#111111", key=_K("dd_txt"), disabled=READ_ONLY)
    dd_brd = st.color_picker("Dropdown border",   (dd.get("border") or {}).get("value") or "#E5E7EB", key=_K("dd_brd"), disabled=READ_ONLY)
    dd_hbg = st.color_picker("Dropdown hover bg", (dd.get("hover_bg") or {}).get("value") or "#F3F4F6", key=_K("dd_hbg"), disabled=READ_ONLY)

    st.subheader("Form Inputs")
    fi = _ensure_path(components, ["forms", "inputs", "colors"], {})
    fi_bg  = st.color_picker("Input bg",          (fi.get("bg") or {}).get("value") or "#FFFFFF", key=_K("fi_bg"), disabled=READ_ONLY)
    fi_txt = st.color_picker("Input text",        (fi.get("text") or {}).get("value") or "#111111", key=_K("fi_txt"), disabled=READ_ONLY)
    fi_brd = st.color_picker("Input border",      (fi.get("border") or {}).get("value") or "#E5E7EB", key=_K("fi_brd"), disabled=READ_ONLY)
    fi_ph  = st.color_picker("Input placeholder", (fi.get("placeholder") or {}).get("value") or "#6B7280", key=_K("fi_ph"), disabled=READ_ONLY)

    st.subheader("Buttons — Submit")
    fb_s = _ensure_path(components, ["forms", "buttons", "submit", "colors"], {})
    fb_s_bg  = st.color_picker("Submit bg",   (fb_s.get("bg") or {}).get("value") or l_primary, key=_K("fb_s_bg"), disabled=READ_ONLY)
    fb_s_txt = st.color_picker("Submit text", (fb_s.get("text") or {}).get("value") or "#FFFFFF", key=_K("fb_s_txt"), disabled=READ_ONLY)
    fb_s_brd = st.color_picker("Submit border", (fb_s.get("border") or {}).get("value") or l_primary, key=_K("fb_s_brd"), disabled=READ_ONLY)

    st.subheader("Buttons — Primary")
    fb_p = _ensure_path(components, ["forms", "buttons", "primary", "colors"], {})
    fb_p_bg  = st.color_picker("Primary bg",   (fb_p.get("bg") or {}).get("value") or l_primary, key=_K("fb_p_bg"), disabled=READ_ONLY)
    fb_p_txt = st.color_picker("Primary text", (fb_p.get("text") or {}).get("value") or "#FFFFFF", key=_K("fb_p_txt"), disabled=READ_ONLY)
    fb_p_brd = st.color_picker("Primary border", (fb_p.get("border") or {}).get("value") or l_primary, key=_K("fb_p_brd"), disabled=READ_ONLY)

    st.subheader("Buttons — Secondary")
    fb_s2 = _ensure_path(components, ["forms", "buttons", "secondary", "colors"], {})
    fb_s2_bg  = st.color_picker("Secondary bg",   (fb_s2.get("bg") or {}).get("value") or "#F3F4F6", key=_K("fb_s2_bg"), disabled=READ_ONLY)
    fb_s2_txt = st.color_picker("Secondary text", (fb_s2.get("text") or {}).get("value") or "#111111", key=_K("fb_s2_txt"), disabled=READ_ONLY)
    fb_s2_brd = st.color_picker("Secondary border", (fb_s2.get("border") or {}).get("value") or "#E5E7EB", key=_K("fb_s2_brd"), disabled=READ_ONLY)

    st.subheader("Buttons — Danger")
    fb_d = _ensure_path(components, ["forms", "buttons", "danger", "colors"], {})
    fb_d_bg  = st.color_picker("Danger bg",   (fb_d.get("bg") or {}).get("value") or "#EF4444", key=_K("fb_d_bg"), disabled=READ_ONLY)
    fb_d_txt = st.color_picker("Danger text", (fb_d.get("text") or {}).get("value") or "#FFFFFF", key=_K("fb_d_txt"), disabled=READ_ONLY)
    fb_d_brd = st.color_picker("Danger border", (fb_d.get("border") or {}).get("value") or "#B91C1C", key=_K("fb_d_brd"), disabled=READ_ONLY)

    st.subheader("Headers")
    hd = _ensure_path(components, ["headers", "colors"], {})
    hd_txt = st.color_picker("Header text",      (hd.get("text") or {}).get("value") or "#111111", key=_K("hd_txt"), disabled=READ_ONLY)
    hd_ulv = st.color_picker("Header underline", (hd.get("underline") or {}).get("value") or "#000000", key=_K("hd_ulv"), disabled=READ_ONLY)

    
    # --- THIS IS THE CORRECTED, USER-FRIENDLY SECTION ---
    # The old, duplicate block has been removed.
    st.header("UI Primitives")
    
    st.subheader("1. Define Corner Roundness (Radius Scale)")
    st.caption("First, set the pixel (px) size for each 'roundness' name. These names will be used in the next step.")
    
    radius = _ensure_path(ui_primitives, ["shape", "radius_scale"], {})
    default_radius = _ensure_path(ui_primitives, ["shape", "default_radius"], {})
    borders = _ensure_path(ui_primitives, ["borders"], {})
    elevation = _ensure_path(ui_primitives, ["elevation"], {})
    sizing = _ensure_path(ui_primitives, ["sizing"], {})
    spacing_vals = ui_primitives.get("spacing_scale_px") or [2,4,6,8,12,16,20,24,32]

    c = st.columns(6)
    
    with c[0]: r_none = st.number_input("None (0px)", 0, 64, int(radius.get("none", 0)), disabled=READ_ONLY, key=_K("r_none"))
    with c[1]: r_sm   = st.number_input("Small (sm)",   0, 64, int(radius.get("sm", 2)), disabled=READ_ONLY, key=_K("r_sm"))
    with c[2]: r_md   = st.number_input("Medium (md)",   0, 64, int(radius.get("md", 6)), disabled=READ_ONLY, key=_K("r_md"))
    with c[3]: r_lg   = st.number_input("Large (lg)",   0, 64, int(radius.get("lg", 12)), disabled=READ_ONLY, key=_K("r_lg"))
    with c[4]: r_xl   = st.number_input("Extra-Large (xl)",   0, 64, int(radius.get("xl", 20)), disabled=READ_ONLY, key=_K("r_xl"))
    with c[5]: r_pill = st.number_input("Pill (Full)", 0, 9999, int(radius.get("pill", 9999)), disabled=READ_ONLY, key=_K("r_pill"))

    st.subheader("2. Apply Roundness to Components")
    st.caption("Now, choose which 'roundness' name (from Step 1) to apply as the default for each component type.")

    radius_options = ["none", "sm", "md", "lg", "xl", "pill"]
    
    def _get_idx(options, value, default_key="md"):
        """Helper to safely find the index of the saved value."""
        try: return options.index(value)
        except ValueError:
            try: return options.index(default_key)
            except ValueError: return 0

    c = st.columns(5)
    with c[0]: dr_inputs  = st.selectbox("Inputs",  radius_options, index=_get_idx(radius_options, default_radius.get("inputs","md")), disabled=READ_ONLY, key=_K("dr_inputs"))
    with c[1]: dr_buttons = st.selectbox("Buttons", radius_options, index=_get_idx(radius_options, default_radius.get("buttons","md")), disabled=READ_ONLY, key=_K("dr_buttons"))
    with c[2]: dr_cards   = st.selectbox("Cards & Tables",   radius_options, index=_get_idx(radius_options, default_radius.get("cards","md")), disabled=READ_ONLY, key=_K("dr_cards"))
    with c[3]: dr_modals  = st.selectbox("Modals & Popups",  radius_options, index=_get_idx(radius_options, default_radius.get("modals","lg")), disabled=READ_ONLY, key=_K("dr_modals"))
    with c[4]: dr_sidebar = st.selectbox("Sidebar", radius_options, index=_get_idx(radius_options, default_radius.get("sidebar","lg")), disabled=READ_ONLY, key=_K("dr_sidebar"))
    # --- END OF CORRECTED SECTION ---


    c = st.columns(3)
    with c[0]: bw_thin  = st.number_input("Border thin", 0, 6, int(_ensure_path(borders, ["width"]).get("thin", 1)), disabled=READ_ONLY, key=_K("bw_thin"))
    with c[1]: bw_thick = st.number_input("Border thick", 0, 6, int(_ensure_path(borders, ["width"]).get("thick", 2)), disabled=READ_ONLY, key=_K("bw_thick"))
    with c[2]: fr_width = st.number_input("Focus ring width px", 0, 12, int(_ensure_path(borders, ["focus_ring"]).get("width_px", 2)), disabled=READ_ONLY, key=_K("fr_width"))
    fr_offset = st.number_input("Focus ring offset px", 0, 12, int(_ensure_path(borders, ["focus_ring"]).get("offset_px", 2)), disabled=READ_ONLY, key=_K("fr_off"))
    fr_style  = st.selectbox("Focus ring style", ["outline","inset"], index=["outline","inset"].index(_ensure_path(borders, ["focus_ring"]).get("style","outline")), disabled=READ_ONLY, key=_K("fr_style"))
    fr_color  = st.selectbox("Focus ring color mode", ["accent","neutral"], index=["accent","neutral"].index(_ensure_path(borders, ["focus_ring"]).get("color_mode","accent")), disabled=READ_ONLY, key=_K("fr_color"))

    st.subheader("Elevation")
    el_none = st.text_input("none", elevation.get("none", "none"), disabled=READ_ONLY, key=_K("el_none"))
    el_sm   = st.text_input("sm",   elevation.get("sm",   "0 1px 2px rgba(0,0,0,.08)"), disabled=READ_ONLY, key=_K("el_sm"))
    el_md   = st.text_input("md",   elevation.get("md",   "0 4px 10px rgba(0,0,0,.10)"), disabled=READ_ONLY, key=_K("el_md"))
    el_lg   = st.text_input("lg",   elevation.get("lg",   "0 10px 20px rgba(0,0,0,.12)"), disabled=READ_ONLY, key=_K("el_lg"))

    st.subheader("Sizing")
    sz_in_h = _ensure_path(sizing, ["input_heights"], {})
    sz_bn_h = _ensure_path(sizing, ["button_heights"], {})
    sz_icn  = _ensure_path(sizing, ["icon_sizes"], {})
    c = st.columns(3)
    with c[0]:
        ih_sm = st.number_input("Input h sm", 20, 80, int(sz_in_h.get("sm", 32)), disabled=READ_ONLY, key=_K("ih_sm"))
        bn_sm = st.number_input("Button h sm", 20, 80, int(sz_bn_h.get("sm", 32)), disabled=READ_ONLY, key=_K("bn_sm"))
        icn_sm= st.number_input("Icon sz sm",  8,  64, int(sz_icn.get("sm",  16)), disabled=READ_ONLY, key=_K("ic_sm"))
    with c[1]:
        ih_md = st.number_input("Input h md", 20, 80, int(sz_in_h.get("md", 40)), disabled=READ_ONLY, key=_K("ih_md"))
        bn_md = st.number_input("Button h md", 20, 80, int(sz_bn_h.get("md", 40)), disabled=READ_ONLY, key=_K("bn_md"))
        icn_md= st.number_input("Icon sz md",  8,  64, int(sz_icn.get("md",  20)), disabled=READ_ONLY, key=_K("ic_md"))
    with c[2]:
        ih_lg = st.number_input("Input h lg", 20, 80, int(sz_in_h.get("lg", 48)), disabled=READ_ONLY, key=_K("ih_lg"))
        bn_lg = st.number_input("Button h lg", 20, 80, int(sz_bn_h.get("lg", 48)), disabled=READ_ONLY, key=_K("bn_lg"))
        icn_lg= st.number_input("Icon sz lg",  8,  64, int(sz_icn.get("lg",  24)), disabled=READ_ONLY, key=_K("ic_lg"))

    container_max = st.number_input("Container max width (px)", 640, 2400, int(sizing.get("container_max_width_px", 1280)), disabled=READ_ONLY, key=_K("container_max"))
    grid_gutter   = st.number_input("Grid gutter (px)", 0, 64, int(sizing.get("grid_gutter_px", 16)), disabled=READ_ONLY, key=_K("grid_gutter"))

    st.subheader("Spacing scale (px)")
    spacing_str = st.text_input("Comma-separated values", ", ".join(str(v) for v in spacing_vals), disabled=READ_ONLY, key=_K("spacing"))

    st.header("Fonts")
    fonts_global = _ensure_path(fonts_node, ["global_defaults"], {})
    fg_family = st.selectbox("Global family", ["system","Arial","Helvetica","Inter","custom"],
                             index=["system","Arial","Helvetica","Inter","custom"].index(fonts_global.get("family","system")),
                             disabled=READ_ONLY, key=_K("fg_family"))
    fg_size   = st.number_input("Global size (px)", 10, 22, int(fonts_global.get("size_px", 14)), disabled=READ_ONLY, key=_K("fg_size"))
    fg_weight = st.selectbox("Weight", ["normal","medium","bold"],
                             index=["normal","medium","bold"].index(fonts_global.get("weight","normal")),
                             disabled=READ_ONLY, key=_K("fg_weight"))
    fg_style  = st.selectbox("Style", ["normal","italic"],
                             index=["normal","italic"].index(fonts_global.get("style","normal")),
                             disabled=READ_ONLY, key=_K("fg_style"))

    headers = _ensure_path(fonts_node, ["headers_and_titles"], {})
    h_inherit = st.checkbox("Headers inherit from global", value=bool(headers.get("inherit_from_global", True)), disabled=READ_ONLY, key=_K("h_inherit"))
    h_delta   = st.number_input("Header size delta vs content (px)", 0, 12, int(headers.get("size_delta_vs_content_px", 2)), disabled=READ_ONLY, key=_K("h_delta"))
    h_default_weight = st.selectbox("Header default weight", ["bold","normal"],
                                    index=["bold","normal"].index(headers.get("default_weight","bold")),
                                    disabled=READ_ONLY, key=_K("h_weight"))
    # </editor-fold>
    
    st.markdown("---")

    st.header("Live Preview")
    theme_tokens = {
        "light": {"primary": l_primary, "surface": l_surface, "text": l_text, "muted": l_muted, "accent": l_accent},
        "dark":  {"primary": d_primary, "surface": d_surface, "text": d_text, "muted": d_muted, "accent": d_accent},
    }

    primitives_dict = {
        "radius_scale": {"none": r_none, "sm": r_sm, "md": r_md, "lg": r_lg, "xl": r_xl, "pill": r_pill},
        "default_radius": {"inputs": dr_inputs, "buttons": dr_buttons, "cards": dr_cards, "modals": dr_modals, "sidebar": dr_sidebar},
        "border_width": {"thin": bw_thin, "thick": bw_thick},
        "focus_ring": {"width_px": fr_width, "offset_px": fr_offset, "style": fr_style, "color_mode": fr_color},
        "elevation": {"none": el_none, "sm": el_sm, "md": el_md, "lg": el_lg},
        "sizing": {
            "input_heights": {"sm": ih_sm, "md": ih_md, "lg": ih_lg},
            "button_heights": {"sm": bn_sm, "md": bn_md, "lg": bn_lg},
            "icon_sizes": {"sm": icn_sm, "md": icn_md, "lg": icn_md},
            "container_max_width_px": container_max,
            "grid_gutter_px": grid_gutter,
        },
        "spacing_scale_px": ([int(x.strip()) for x in (spacing_str or "").split(",") if x.strip().isdigit()] or spacing_vals),
    }

    inject_css(
        mode,
        colors=theme_tokens[mode],
        background={},
        fonts={"family": fg_family},
        primitives=primitives_dict,
        components=components
    )

    def _k(s: str) -> str:
        return f"theme_prev_{s}"

    col_prev = st.columns([1, 1])
    with col_prev[0]:
        st.markdown("**Buttons**"); st.button("Primary", key=_k("btn_primary")); st.button("Secondary", key=_k("btn_secondary"))
        st.markdown("**Inputs**"); st.text_input("Example input", "Hello", key=_K("ti_example"))
        st.markdown("**Select**"); st.selectbox("Example select", ["One", "Two", "Three"], key=_K("sb_example"))
    with col_prev[1]:
        st.markdown("**Headers**"); st.markdown("### Section header"); st.markdown("Body text…")
        st.info("Sidebar styling updates because `components=components` was passed to inject_css. 🎨")
        st.markdown("**Table**"); st.dataframe({"A": [1, 2, 3], "B": [4, 5, 6]}, use_container_width=True)

    st.header("Save / Publish")

    # The _write_cfg function definition has been moved to the top of render()
    # to fix the NameError.

    col_s1, col_s2 = st.columns(2)
    with col_s1:
        if st.button("💾 Save Draft", disabled=(READ_ONLY or not CAN_EDIT), key=_K("save")):
            try:
                _write_cfg("draft", cfg) # Pass the loaded cfg to be modified
                with engine.begin() as conn:
                    conn.execute(sa_text("INSERT INTO configs(degree, namespace, config_json) VALUES ('default','app_theme', :j) ON CONFLICT(degree, namespace) DO UPDATE SET config_json=excluded.config_json"), {"j": json.dumps(cfg)})
                st.success("Saved draft.")
            except Exception as ex: st.error(str(ex))
    with col_s2:
        if st.button("🚀 Publish", disabled=(not CAN_PUBLISH), key=_K("publish")):
            try:
                _write_cfg("published", cfg) # Pass the loaded cfg to be modified
                with engine.begin() as conn:
                    conn.execute(sa_text("INSERT INTO configs(degree, namespace, config_json) VALUES ('default','app_theme', :j) ON CONFLICT(degree, namespace) DO UPDATE SET config_json=excluded.config_json"), {"j": json.dumps(cfg)})
                st.success("Published theme.")
            except Exception as ex: st.error(str(ex))

    st.markdown("---")
render()
