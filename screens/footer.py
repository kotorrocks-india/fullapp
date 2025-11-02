# app/screens/footer.py
from __future__ import annotations
import json
import streamlit as st
from sqlalchemy import text as sa_text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.engine import Connection  # <-- FIX 1: Added this import

# --- IMPORTS HAVE BEEN CORRECTED ---
from core.settings import load_settings
from core.db import get_engine
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
        except json.JSONDecodeError as e:
            st.error(f"Failed to load config: Corrupt JSON data in database. {e}")
            return {}
    return {}


def _save(engine_or_conn, degree: str, cfg: dict) -> None:
    """Saves the config, using either a passed-in connection or a new transaction."""
    payload = json.dumps(cfg, ensure_ascii=False)
    sql = sa_text(
        """
        INSERT INTO configs (degree, namespace, config_json)
        VALUES (:d, :ns, :cfg)
        ON CONFLICT(degree, namespace) DO UPDATE
        SET config_json=excluded.config_json, updated_at=CURRENT_TIMESTAMP
        """
    )
    params = dict(d=degree, ns=NAMESPACE, cfg=payload)
    
    # <-- FIX 2: Changed logic to correctly detect a Connection
    if isinstance(engine_or_conn, Connection):
        # It's a connection, use it directly
        engine_or_conn.execute(sql, params)
    else:
        # It's an engine, create a new transaction
        with engine_or_conn.begin() as conn:
            conn.execute(sql, params)


# --- DECORATOR HAS BEEN CORRECTED ---
@require_page("Footer")
def render():
    st.title("🦶 Footer (Global)")
    tagline()
    st.info("This footer applies to ALL pages and ALL degrees. Stored under degree='*'.")

    settings = load_settings()
    engine = get_engine(settings.db.url)
    
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
    num_links = int(count or 0) # Safer typecast
    
    for i in range(num_links):
        col1, col2 = st.columns([1, 2])
        
        default_label = ""
        default_url = ""
        
        if i < len(links) and isinstance(links[i], dict):
            default_label = links[i].get("label", "")
            default_url = links[i].get("url", "")
            
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
            # <-- FIX 3: Removed the 'with engine.begin() as conn:' wrapper.
            # We must pass the 'engine' to both functions and let them
            # manage their own transactions, as the imported 'save'
            # function does not support participating in an existing transaction.
            save(
                engine,  # Pass the engine
                DEGREE,
                NAMESPACE,
                cfg,
                saved_by=(st.session_state.get("user", {}) or {}).get("email"),
                reason="update via footer",
            )
            _save(engine, DEGREE, cfg) # Pass the engine
            
            success("Saved global footer (degree='*').")
            st.rerun()

        except SQLAlchemyError as e:
            st.error(f"Database error: {e}")
        except Exception as e:
            st.error(f"An unexpected error occurred: {e}")

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
