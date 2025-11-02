# app/core/public_config.py
from __future__ import annotations
import json
from pathlib import Path
from sqlalchemy import text as sa_text
from sqlalchemy.engine import Engine
from core.config_store import get

# Constants for clarity
BRANDING_NAMESPACE = "branding"
FOOTER_NAMESPACE = "footer"
APP_ROOT = Path(__file__).resolve().parents[1] # Assumes core is in app/, gets .../app/

def resolve_public_asset_path(path_or_url: str) -> tuple[bool, str]:
    """
    Returns (is_displayable, resolved_path_or_url).
    If it's a URL, it's passed through. If it's a local path, it's
    resolved relative to the app's root directory and checked for existence.
    """
    if not path_or_url:
        return (False, "")
    
    p = str(path_or_url).strip()
    if p.startswith(("http://", "https://")):
        return (True, p) # It's a URL, assume it's valid

    # It's a local path, resolve it and check if the file exists
    # Converts 'assets/branding/logo.png' into 'E:/.../app/assets/branding/logo.png'
    abs_path = APP_ROOT / p.replace("\\", "/")
    return (abs_path.is_file(), str(abs_path))

def load_public_branding_config(engine: Engine) -> dict:
    """
    Publicly fetches the branding config using the core get() function.
    Reads from degree='default' and namespace='branding'.
    """
    return get(engine, degree='default', namespace=BRANDING_NAMESPACE)

def load_public_footer_config(engine: Engine) -> dict:
    """
    Publicly fetches the footer config using the core get() function.
    Reads from degree='*' and namespace='footer'.
    """
    return get(engine, degree='*', namespace=FOOTER_NAMESPACE)
