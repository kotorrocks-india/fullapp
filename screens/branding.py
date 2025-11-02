# app/screens/branding.py
from __future__ import annotations
import os, json, yaml
import streamlit as st
from sqlalchemy import text as sa_text

# --- IMPORTS HAVE BEEN CORRECTED ---
from core.settings import load_settings
from core.db import get_engine, init_db, SessionLocal
from core.forms import tagline, success, warn
from core.config_store import save, history
# We now use the modern security system from policy.py
from core.policy import require_page, can_edit_page, user_roles
from core.a11y import contrast_ratio, meets_wcag_aa

# --------------------------------------------------------------------
# Constants / locations
# --------------------------------------------------------------------
SLIDE_PATH = "app/core/slide1_branding_login.yaml"  # Using a relative path for portability
DEFAULT_NAMESPACE = "branding"
APP_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))  # .../app

#<editor-fold desc="Helper Functions (Unchanged)">
def _assets_dir(degree: str) -> str:
    """Directory where branding assets are stored for a degree."""
    return os.path.join(APP_ROOT, "assets", "branding", degree)

def _save_upload(file, degree: str) -> str:
    """
    Save an uploaded file into app/assets/branding/<degree>/ and
    return a RELATIVE path like 'assets/branding/<degree>/<filename>'.
    """
    folder = _assets_dir(degree)
    os.makedirs(folder, exist_ok=True)
    dest = os.path.join(folder, file.name)
    with open(dest, "wb") as f:
        f.write(file.read())
    rel = os.path.relpath(dest, APP_ROOT).replace("\\", "/")
    return rel

def _resolve_local_or_url(path_or_url: str) -> tuple[bool, str]:
    """
    Returns (is_displayable, resolved_path_or_url).
    If URL -> pass through. If local -> resolve against app/ and ensure it exists.
    """
    if not path_or_url:
        return (False, "")
    p = str(path_or_url).strip()
    if p.startswith(("http://", "https://")):
        return (True, p)
    abs_path = os.path.join(APP_ROOT, p.replace("\\", "/"))
    return (os.path.isfile(abs_path), abs_path)

def _load_slide_yaml() -> dict:
    try:
        if os.path.exists(SLIDE_PATH):
            with open(SLIDE_PATH, "r", encoding="utf-8") as f:
                return yaml.safe_load(f) or {}
    except Exception:
        pass
    return {}

def _load_existing_cfg(engine, degree: str, namespace: str) -> dict:
    with engine.begin() as conn:
        row = conn.execute(
            sa_text("SELECT config_json FROM configs WHERE degree=:d AND namespace=:ns"),
            dict(d=degree, ns=namespace)
        ).fetchone()
    if not row: return {}
    try: return json.loads(row[0]) or {}
    except Exception: return {}

def _lint_and_persist(engine, degree: str, namespace: str, cfg: dict, lint_rules: dict):
    # Enforce YAML lint rules (if any)
    forbidden_patterns = set((lint_rules or {}).get("branding_edit_scope", {}).get("fields", []))
    keys = set(cfg.keys()) | {f"branding.{k}" for k in cfg.keys()}
    for pat in forbidden_patterns:
        if pat.endswith(".*"):
            prefix = pat[:-2]
            if any(k.startswith(prefix) for k in keys):
                raise ValueError(f"Fields under '{prefix}*' are restricted to their own slide.")
        elif pat in keys:
            raise ValueError(f"Field '{pat}' is restricted to its own slide.")

    save(engine, degree, namespace, cfg, saved_by=(st.session_state.get("user", {}) or {}).get("email"), reason="update via branding page")
    payload = json.dumps(cfg, ensure_ascii=False)
    with engine.begin() as conn:
        conn.execute(sa_text("INSERT INTO configs (degree, namespace, config_json) VALUES (:d, :ns, :cfg) ON CONFLICT(degree, namespace) DO UPDATE SET config_json=excluded.config_json, updated_at=CURRENT_TIMESTAMP"), dict(d=degree, ns=namespace, cfg=payload))
#</editor-fold>

def get_login_branding(engine, degree: str = "default", namespace: str = DEFAULT_NAMESPACE) -> dict:
    cfg = _load_existing_cfg(engine, degree, namespace)
    logo = cfg.get("logo", {})
    placement = logo.get("placement", "left_of_form")
    max_w = int(logo.get("max_width_px", 240))
    logo_url = logo.get("url", "")
    show = placement in ("left_of_form", "above_form") and bool(logo_url)
    ok, resolved = _resolve_local_or_url(logo_url)
    theme, login_header = cfg.get("theme", {}) or {}, cfg.get("login_header", {}) or {}

    return {
        "logo_show": bool(show), "logo_ok": bool(ok), "resolved_logo": resolved if ok else "",
        "logo_max_width_px": max_w, "theme_default_mode": theme.get("default_mode", "light"),
        "show_theme_toggle": bool(login_header.get("show_theme_toggle", True)), "favicon": cfg.get("favicon", {}) or {},
        "colors": cfg.get("colors", {}) or {}, "background": cfg.get("background", {}) or {}, "fonts": cfg.get("fonts", {}) or {},
    }

def _section(title: str):
    st.markdown(f"### {title}")

# --- DECORATOR HAS BEEN CORRECTED ---
@require_page("Branding (Login)")
def render():
    st.title("Branding (Login) — Full YAML")
    tagline()

    settings = load_settings()
    engine = get_engine(settings.db.url)
    init_db(engine)
    SessionLocal.configure(bind=engine)

    slide = _load_slide_yaml()
    namespace = slide.get("branding", {}).get("namespace", DEFAULT_NAMESPACE)
    editable_in = slide.get("branding", {}).get("editable_in", "this_slide_only")
    scope = slide.get("scope", "login_only")
    lint_rules = slide.get("lint_rules", {})
    schema_settings = slide.get("settings", {}) or {}
    
    current_roles = user_roles()
    can_edit = can_edit_page("Branding (Login)", current_roles)

    st.info(f"Scope: **{scope}** — affects login experience only. Namespace: **{namespace}**")
    degree = st.text_input("Degree code (use 'default' for global)", value="default")

    existing = _load_existing_cfg(engine, degree, namespace)
    cfg = {}
    for k, v in (schema_settings or {}).items(): cfg[k] = v
    for k, v in (existing or {}).items(): cfg[k] = v

    #<editor-fold desc="Full UI Rendering Logic (Unchanged)">
    theme = cfg.get("theme", {})
    _section("Theme")
    c1, c2 = st.columns(2)
    with c1: default_mode = st.selectbox("Default mode", ["light","dark"], index=0 if theme.get("default_mode","light") == "light" else 1)
    with c2: pre_login_cookie = st.checkbox("Remember via pre-login cookie", value=theme.get("remember_choice",{}).get("pre_login_cookie", True))
    post_login_prefs = st.checkbox("Persist to user prefs after login", value=theme.get("remember_choice",{}).get("post_login_user_prefs", True))
    theme_unknown = {k:v for k,v in theme.items() if k not in ("default_mode","remember_choice")}
    theme_unknown_json = st.text_area("Advanced theme JSON", value=json.dumps(theme_unknown, indent=2) if theme_unknown else "{}", height=150)
    try: theme_unknown_parsed = json.loads(theme_unknown_json) if theme_unknown_json.strip() else {}
    except Exception: theme_unknown_parsed = theme_unknown

    _section("Logo")
    logo = cfg.get("logo", {})
    c3, c4, c5 = st.columns(3)
    with c3: placement = st.selectbox("Placement", ["left_of_form","above_form","hidden"], index=["left_of_form","above_form","hidden"].index(logo.get("placement","left_of_form")))
    with c4: max_width_px = st.number_input("Max width (px)", 64, 512, int(logo.get("max_width_px",240)), step=8)
    with c5: customizable = st.checkbox("Customizable by degree", value=bool(logo.get("customizable", True)))
    up_logo = st.file_uploader("Upload logo", type=["png","jpg","jpeg","svg"])
    logo_url = st.text_input("Logo path/URL", value=logo.get("url",""))
    if up_logo: logo_url = _save_upload(up_logo, degree); st.success(f"Logo saved to {logo_url}")
    logo_unknown = {k:v for k,v in logo.items() if k not in ("placement","max_width_px","customizable","url")}
    logo_unknown_json = st.text_area("Advanced logo JSON", value=json.dumps(logo_unknown, indent=2) if logo_unknown else "{}", height=120)
    try: logo_unknown_parsed = json.loads(logo_unknown_json) if logo_unknown_json.strip() else {}
    except Exception: logo_unknown_parsed = logo_unknown

    _section("Favicon")
    favicon = cfg.get("favicon", {})
    files = favicon.get("files", {}) or {}
    
    strategy = st.selectbox("Strategy", ["autogenerated_initials","upload","url_only"], index=["autogenerated_initials","upload","url_only"].index(favicon.get("strategy","autogenerated_initials")))
    initials = st.text_input("Initials (if autogenerated)", value=favicon.get("initials",""))
    colf1, colf2 = st.columns(2)
    with colf1:
        up_ico = st.file_uploader("Upload .ico (32x32)", type=["ico"], key="fav_ico")
        ico_32 = st.text_input("ICO 32 path/URL", value=files.get("ico_32",""))
        if up_ico: ico_32 = _save_upload(up_ico, degree); st.success(f"Favicon .ico saved to {ico_32}")
    with colf2:
        up_png = st.file_uploader("Upload PNG 180x180", type=["png"], key="fav_png")
        png_180 = st.text_input("PNG 180 path/URL", value=files.get("png_180",""))
        if up_png: png_180 = _save_upload(up_png, degree); st.success(f"Favicon PNG saved to {png_180}")
    favicon_url = st.text_input("Favicon URL (optional)", value=favicon.get("url",""))
    st.caption("Favicon preview")
    pcol1, pcol2 = st.columns(2)
    ok_ico, ico_abs = _resolve_local_or_url(ico_32)
    ok_png, png_abs = _resolve_local_or_url(png_180)
    with pcol1:
        if ok_ico: st.image(ico_abs, width=32, caption="ICO 32")
        elif ico_32 or files.get("ico_32"): st.warning("ICO not found.")
    with pcol2:
        if ok_png: st.image(png_abs, width=36, caption="PNG 180")
        elif png_180 or files.get("png_180"): st.warning("PNG not found.")
    fav_unknown = {k:v for k,v in favicon.items() if k not in ("strategy","initials","files","url")}
    fav_unknown_json = st.text_area("Advanced favicon JSON", value=json.dumps(fav_unknown, indent=2) if fav_unknown else "{}", height=120)
    try: fav_unknown_parsed = json.loads(fav_unknown_json) if fav_unknown_json.strip() else {}
    except Exception: fav_unknown_parsed = fav_unknown
    
    _section("Background")
    bg = cfg.get("background", {})
    bg_type = st.selectbox("Type", ["solid_color","gradient","image"], index=["solid_color","gradient","image"].index(bg.get("type","solid_color")))
    blur, editable_variants, fit_options = st.checkbox("Blur behind form", value=bg.get("blur_behind_form", False)), bg.get("editable_variants", ["solid_color","gradient","image"]), bg.get("image_fit_options", ["cover","contain"])
    bg_payload = {"type": bg_type, "blur_behind_form": bool(blur), "editable_variants": editable_variants, "image_fit_options": fit_options}
    if bg_type == "solid_color": bg_payload["color"] = st.color_picker("Solid color", value=bg.get("color", "#ffffff"))
    elif bg_type == "gradient":
        g1, g2, ang = st.color_picker("Gradient start", value=bg.get("start","#ffffff")), st.color_picker("Gradient end", value=bg.get("end","#f0f0f0")), st.number_input("Angle", 0, 360, int(bg.get("angle", 90)))
        bg_payload.update({"start": g1, "end": g2, "angle": int(ang)})
    else:
        fit, up_bg, url = st.selectbox("Image fit", ["cover","contain"], index=["cover","contain"].index(bg.get("fit","cover"))), st.file_uploader("Upload background (JPG/PNG)", type=["jpg","jpeg","png"], key="bg_up"), st.text_input("Background path/URL", value=bg.get("url",""))
        if up_bg: url = _save_upload(up_bg, degree); st.success(f"Background saved to {url}")
        bg_payload.update({"fit": fit, "url": url})

    _section("Fonts")
    fonts, common_fonts = cfg.get("fonts", {}), ["system_default", "Inter", "Roboto", "Lato", "Open Sans", "Montserrat", "Poppins", "Nunito", "Source Sans 3", "PT Sans", "Merriweather"]
    family_sel, family_choices = fonts.get("family","system_default"), common_fonts + ["Custom..."]
    family_index = family_choices.index(family_sel) if family_sel in family_choices else len(common_fonts)
    family = st.selectbox("Font family", family_choices, index=family_index)
    if family == "Custom...": family = st.text_input("Custom CSS font-family", value=fonts.get("family","")) or "system_default"
    fonts_payload = {"family": family, "editable_dropdown": st.checkbox("Allow dropdown edit", value=fonts.get("editable_dropdown", True))}

    _section("Colors & Contrast Guard")
    colors = cfg.get("colors", {})
    primary_default = colors.get("primary", "#0a84ff")

    if isinstance(primary_default, str) and primary_default.startswith("#"): primary = st.color_picker("Primary color", value=primary_default)
    else:
        col_pc, col_tok = st.columns([2,1])
        with col_pc: primary = st.color_picker("Primary color", value="#0a84ff")
        with col_tok: primary_token = st.text_input("or design token", value=colors.get("primary","")); primary = primary_token.strip() or primary
    cg = colors.get("contrast_guard", {})
    wcag_required, allow_override, override_reason = st.checkbox("WCAG AA required", value=cg.get("wcag_aa_required", True)), st.checkbox("Allow override with reason", value=cg.get("allow_override_with_reason", True)), st.text_input("Override reason (optional)", value=cg.get("override_reason",""))
    colors_payload = {"primary": primary, "contrast_guard": {"wcag_aa_required": bool(wcag_required), "allow_override_with_reason": bool(allow_override), **({"override_reason": override_reason} if override_reason else {})}}
    _section("Login header")
    login_header = cfg.get("login_header", {})
    login_header_payload = {"show_theme_toggle": st.checkbox("Show theme toggle", value=login_header.get("show_theme_toggle", True))}
    
    _section("Fallbacks")
    fallbacks = cfg.get("fallbacks", {})
    fallbacks_payload = {"use_safe_fallbacks_on_asset_failure": st.checkbox("Use safe fallbacks on asset failure", value=fallbacks.get("use_safe_fallbacks_on_asset_failure", True))}
    
    st.markdown("---"); st.caption("Login preview (mock)")
    logo_ok, resolved_logo = _resolve_local_or_url(logo_url)
    if placement in ("left_of_form", "above_form"):
        if logo_ok: st.image(resolved_logo, width=int(max_width_px))
        elif logo_url: st.warning(f"Logo not found at '{logo_url}'.")
    st.text_input("Email", value=""); st.text_input("Password", value="", type="password"); st.button("Sign in")

    files_payload = {}
    if 'ico_32' in locals() and locals()['ico_32']: files_payload["ico_32"] = locals()['ico_32']
    elif files.get("ico_32"): files_payload["ico_32"] = files.get("ico_32")
    if 'png_180' in locals() and locals()['png_180']: files_payload["png_180"] = locals()['png_180']
    elif files.get("png_180"): files_payload["png_180"] = files.get("png_180")
    new_cfg = {
        "theme": {"default_mode": default_mode, "remember_choice": {"pre_login_cookie": bool(pre_login_cookie), "post_login_user_prefs": bool(post_login_prefs)}, **(theme_unknown_parsed or {})},
        "logo": {"placement": placement, "max_width_px": int(max_width_px), "customizable": bool(customizable), "url": logo_url, **(logo_unknown_parsed or {})},
        "favicon": {"strategy": strategy, "initials": initials, "files": files_payload, "url": favicon_url, **(fav_unknown_parsed or {})},
        "background": bg_payload, "fonts": fonts_payload, "colors": colors_payload,
        "login_header": login_header_payload, "fallbacks": fallbacks_payload,
    }
    #</editor-fold>

    if editable_in != "this_slide_only":
        warn("YAML says editable_in != this_slide_only — proceed carefully.")

    if st.button("Save Branding (Login)", disabled=not can_edit):
        try:
            if isinstance(new_cfg["colors"]["primary"], str) and new_cfg["colors"]["primary"].startswith("#"):
                bg_hex = new_cfg["background"].get("color", "#ffffff") if new_cfg["background"].get("type") == "solid_color" else "#ffffff"
                ratio = contrast_ratio(new_cfg["colors"]["primary"], bg_hex)
                require_aa, allow_ovr, reason = new_cfg["colors"]["contrast_guard"].get("wcag_aa_required", True), new_cfg["colors"]["contrast_guard"].get("allow_override_with_reason", True), new_cfg["colors"]["contrast_guard"].get("override_reason", "")
                if require_aa and not meets_wcag_aa(ratio) and not (allow_ovr and reason):
                    st.error(f"Contrast ratio {ratio:.2f} fails WCAG AA. Provide override reason or adjust."); st.stop()
        except Exception: pass
        try:
            _lint_and_persist(engine, degree, namespace, new_cfg, lint_rules)
            success(f"Saved branding for '{degree}'.")
        except Exception as e: st.error(str(e))

    st.subheader("Version history (last 50)")
    hist = history(engine, degree, namespace)
    if hist:
        import pandas as pd
        df = pd.DataFrame([{"version":h["version"], "by":h["saved_by"], "reason":h["reason"], "at":h["created_at"]} for h in hist])
        st.dataframe(df)

render()
