# app/screens/footer.py
from __future__ import annotations
import json
import streamlit as st
from sqlalchemy import text as sa_text

# --- IMPORTS HAVE BEEN CORRECTED ---
from core.settings import load_settings
from core.db import get_engine, init_db
from core.forms import tagline, success
from core.config_store import save, history
# We now import and use the modern security system from policy.py
from core.policy import require_page, can_edit_page, user_roles

NAMESPACE = "footer"

def _load_existing(engine, degree: str) -> dict:
    with engine.begin() as conn:
        row = conn.execute(
            sa_text("SELECT config_json FROM configs WHERE degree=:d AND namespace=:ns"),
            dict(d=degree, ns=NAMESPACE),
        ).fetchone()
    if row:
        try:
            return json.loads(row[0]) or {}
        except Exception:
            return {}
    return {}


def _save(engine, degree: str, cfg: dict) -> None:
    payload = json.dumps(cfg, ensure_ascii=False)
    with engine.begin() as conn:
        conn.execute(
            sa_text(
                """
                INSERT INTO configs (degree, namespace, config_json)
                VALUES (:d, :ns, :cfg)
                ON CONFLICT(degree, namespace) DO UPDATE
                SET config_json=excluded.config_json, updated_at=CURRENT_TIMESTAMP
                """
            ),
            dict(d=degree, ns=NAMESPACE, cfg=payload),
        )

# --- DECORATOR HAS BEEN CORRECTED ---
@require_page("Footer")
def render():
    st.title("🦶 Footer (Global)")
    tagline()
    st.info("This footer applies to ALL pages and ALL degrees. Stored under degree='*'.")

    settings = load_settings()
    engine = get_engine(settings.db.url)
    init_db(engine)

    # Use the can_edit_page helper for granular control
    current_roles = user_roles()
    can_edit = can_edit_page("Footer", current_roles)

    DEGREE = "*"  # global slot only

    existing = _load_existing(engine, DEGREE)
    enabled = st.checkbox("Enable footer", value=bool(existing.get("enabled", True)))
    footer_text = st.text_area(
        "Footer text",
        value=existing.get("footer_text", "© {year} • IESCOA • All rights reserved"),
    )
    designer_name = st.text_input("Designer/Owner name (optional)", value=existing.get("designer_name", ""))
    designer_url = st.text_input("Designer URL (optional)", value=existing.get("designer_url", ""))

    st.subheader("Links")
    links = existing.get("links", [{"label": "Privacy", "url": "#"}])
    count = st.number_input("Number of links", min_value=0, max_value=20, value=len(links), step=1)
    new_links = []
    for i in range(int(count)):
        col1, col2 = st.columns([1, 2])
        default_label = links[i]["label"] if i < len(links) and "label" in links[i] else ""
        default_url = links[i]["url"] if i < len(links) and "url" in links[i] else ""
        with col1:
            lbl = st.text_input(f"Link {i+1} label", value=default_label, key=f"ln_label_{i}")
        with col2:
            url = st.text_input(f"Link {i+1} URL", value=default_url, key=f"ln_url_{i}")
        if lbl or url:
            new_links.append({"label": lbl, "url": url})

    cfg = {
        "enabled": bool(enabled),
        "footer_text": footer_text,
        "designer_name": designer_name,
        "designer_url": designer_url,
        "links": new_links,
    }

    # The "Save" button is now disabled if the user lacks 'edit' permission
    if st.button("Save Footer", disabled=not can_edit):
        try:
            save(
                engine,
                DEGREE,
                NAMESPACE,
                cfg,
                saved_by=(st.session_state.get("user", {}) or {}).get("email"),
                reason="update via footer",
            )
            _save(engine, DEGREE, cfg)
            success("Saved global footer (degree='*').")
        except Exception as e:
            st.error(str(e))

    st.subheader("Stored config (read-only)")
    st.json(_load_existing(engine, DEGREE))

    st.subheader("Version history (last 50)")
    hist = history(engine, DEGREE, NAMESPACE)
    if hist:
        import pandas as pd
        df = pd.DataFrame(
            [
                {"version": h["version"], "by": h["saved_by"], "reason": h["reason"], "at": h["created_at"]}
                for h in hist
            ]
        )
        st.dataframe(df)

render()
