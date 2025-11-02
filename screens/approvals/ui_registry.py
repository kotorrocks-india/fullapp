from typing import Dict, Callable
import streamlit as st

_detail_renderers: Dict[str, Callable] = {}

def register_detail_renderer(object_type: str, action: str):
    """Decorator to register custom detail renderers."""
    def decorator(func: Callable):
        key = f"{object_type}.{action}"
        _detail_renderers[key] = func
        return func
    return decorator

def get_detail_renderer(object_type: str, action: str) -> Callable:
    """Get custom detail renderer for an approval."""
    key = f"{object_type}.{action}"
    return _detail_renderers.get(key, _default_detail_renderer)

def _default_detail_renderer(row, engine):
    """Default detail renderer."""
    st.write(
        f"**Object:** `{row['object_type']}` • **ID:** `{row['object_id']}` • "
        f"**Action:** `{row['action']}` • **Requested by:** `{row['requester']}`"
    )
