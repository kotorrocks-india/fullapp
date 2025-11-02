# app/core/theme_profiles.py
from __future__ import annotations
from typing import List, Optional
import json
from sqlalchemy import text as sa_text

# We store named theme profiles in the existing `configs` table.
# - namespace = "theme_profiles"
# - degree column is reused to hold the *profile name*
# The Slide-6 working theme continues to live at:
# - namespace = "app_theme", degree = "default"

PROFILE_NS = "theme_profiles"
THEME_NS   = "app_theme"   # Slide 6 draft/published row (degree='default')


def _ensure_configs_table(engine) -> None:
    """
    Safety guard in case the configs table wasn't created yet.
    Your core/db.init_db() should already create it; this is a no-op if it exists.
    """
    if not engine:
        return
    with engine.begin() as conn:
        conn.execute(sa_text("""
            CREATE TABLE IF NOT EXISTS configs (
              id          INTEGER PRIMARY KEY AUTOINCREMENT,
              degree      TEXT NOT NULL,
              namespace   TEXT NOT NULL,
              config_json TEXT,
              updated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
              UNIQUE(degree, namespace)
            )
        """))


def list_profiles(engine) -> List[str]:
    """Return all saved profile names."""
    _ensure_configs_table(engine)
    with engine.begin() as conn:
        rows = conn.execute(
            sa_text("SELECT degree FROM configs WHERE namespace=:ns ORDER BY degree"),
            {"ns": PROFILE_NS},
        ).fetchall()
    return [r[0] for r in rows]


def load_profile(engine, name: str) -> Optional[dict]:
    """Load a profile JSON by name (or None)."""
    if not name:
        return None
    _ensure_configs_table(engine)
    with engine.begin() as conn:
        row = conn.execute(sa_text("""
            SELECT config_json
              FROM configs
             WHERE namespace=:ns AND degree=:name
             ORDER BY updated_at DESC
             LIMIT 1
        """), {"ns": PROFILE_NS, "name": name}).fetchone()
    if not row:
        return None
    raw = row[0]
    if raw is None:
        return None
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode("utf-8", "ignore")
    try:
        return json.loads(raw)
    except Exception:
        return None


def save_profile(engine, name: str, theme_cfg: dict) -> None:
    """Upsert the given theme JSON under the profile name."""
    if not name:
        raise ValueError("Profile name is required.")
    _ensure_configs_table(engine)
    payload = json.dumps(theme_cfg or {})
    with engine.begin() as conn:
        conn.execute(sa_text("""
            INSERT INTO configs(degree, namespace, config_json)
            VALUES (:name, :ns, :j)
            ON CONFLICT(degree, namespace) DO UPDATE SET
              config_json=excluded.config_json,
              updated_at=CURRENT_TIMESTAMP
        """), {"name": name, "ns": PROFILE_NS, "j": payload})


def delete_profile(engine, name: str) -> None:
    """Delete a profile by name."""
    if not name:
        return
    _ensure_configs_table(engine)
    with engine.begin() as conn:
        conn.execute(sa_text("""
            DELETE FROM configs
             WHERE degree=:name AND namespace=:ns
        """), {"name": name, "ns": PROFILE_NS})


def apply_profile_to_draft(engine, name: str) -> None:
    """
    Copy a stored profile into the Slide-6 draft row:
      degree='default', namespace='app_theme'
    """
    prof = load_profile(engine, name)
    if not prof:
        raise RuntimeError(f"Profile '{name}' not found.")
    _ensure_configs_table(engine)
    with engine.begin() as conn:
        conn.execute(sa_text("""
            INSERT INTO configs(degree, namespace, config_json)
            VALUES ('default', :ns, :j)
            ON CONFLICT(degree, namespace) DO UPDATE SET
              config_json=excluded.config_json,
              updated_at=CURRENT_TIMESTAMP
        """), {"ns": THEME_NS, "j": json.dumps(prof)})
