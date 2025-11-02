# app/core/ui.py
from __future__ import annotations
import json, datetime
import streamlit as st
from sqlalchemy import text as sa_text
from core.settings import load_settings
from core.db import get_engine, init_db

def _fetch_one(conn, degree: str):
    return conn.execute(
        sa_text("SELECT config_json FROM configs WHERE namespace='footer' AND degree=:d ORDER BY updated_at DESC LIMIT 1"),
        dict(d=degree)
    ).fetchone()

def _get_footer_cfg_global() -> dict:
    """Return a single footer config to apply everywhere, preferring degree='*'."""
    settings = load_settings()
    engine = get_engine(settings.db.url)
    init_db(engine)

    with engine.begin() as conn:
        # 1) global '*' wins
        row = _fetch_one(conn, "*")
        if not row:
            # 2) fallback to 'default'
            row = _fetch_one(conn, "default")
        if not row:
            # 3) fallback to most recent footer of any degree
            row = conn.execute(sa_text(
                "SELECT config_json FROM configs WHERE namespace='footer' ORDER BY updated_at DESC LIMIT 1"
            )).fetchone()

    if not row:
        return {}
    try:
        return json.loads(row[0]) or {}
    except Exception:
        return {}

def _expand(text: str, cfg: dict) -> str:
    year = str(datetime.datetime.now().year)
    designer = (cfg.get("designer_name") or "").strip()
    return (text or "").replace("{year}", year).replace("{designer_name}", designer)

def render_footer_global():
    """Render one global footer regardless of degree; call this on every page."""
    cfg = _get_footer_cfg_global()
    if not cfg or not cfg.get("enabled", True):
        return

    # Base text (template or explicit footer_text)
    footer_text = (cfg.get("footer_text") or "").strip()
    if not footer_text and cfg.get("template"):
        footer_text = cfg.get("template")
    if not footer_text and cfg.get("designer_name"):
        footer_text = "© {year} • Designed by {designer_name}"
    footer_text = _expand(footer_text, cfg)

    # Links
    links_html = []
    for ln in (cfg.get("links") or []):
        label = (ln.get("label") or "").strip()
        url = (ln.get("url") or "#").strip()
        if label:
            links_html.append(f'<a href="{url}" target="_blank" rel="noopener">{label}</a>')

    # Designer badge (if not already in text)
    designer_html = ""
    dn = (cfg.get("designer_name") or "").strip()
    du = (cfg.get("designer_url") or "").strip()
    if dn and ("Designed by" not in footer_text):
        if du:
            designer_html = f'• Designed by <a href="{du}" target="_blank" rel="noopener">{dn}</a>'
        else:
            designer_html = f'• Designed by {dn}'

    parts = []
    if footer_text:
        parts.append(f"<span>{footer_text}</span>")
    if links_html:
        parts.append(" | ".join(links_html))
    if designer_html:
        parts.append(designer_html)
    if not parts:
        return

    html = f"""
    <div style="
        margin-top: 2rem;
        padding: 0.75rem 0;
        font-size: 0.9rem;
        color: inherit;
        border-top: 1px solid rgba(0,0,0,0.15);
        opacity: 0.9;
        display:flex; gap:0.75rem; flex-wrap:wrap;
    ">
      {' &nbsp; '.join(parts)}
    </div>
    """
    st.markdown(html, unsafe_allow_html=True)

# Back-compat alias
def render_footer():
    return render_footer_global()


