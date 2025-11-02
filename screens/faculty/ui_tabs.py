# screens/faculty/ui_tabs.py
# Compatibility wrapper so older imports keep working with the modular tab layout.
# If you haven't split into tabs/, this still works as long as these imports resolve.

from __future__ import annotations
from typing import Set
from sqlalchemy.engine import Engine

# Try to import from the modular tabs/ layout first...
try:
    from screens.faculty.tabs.credits_policy import render as _tab_credits_policy  # type: ignore
    from screens.faculty.tabs.designation_catalog import render as _tab_designations  # type: ignore
    from screens.faculty.tabs.designation_removal import render as _tab_designation_removal  # type: ignore
    from screens.faculty.tabs.custom_types import render as _tab_custom_types  # type: ignore
    from screens.faculty.tabs.profiles import render as _tab_profiles  # type: ignore
    from screens.faculty.tabs.affiliations import render as _tab_affiliations  # type: ignore
    from screens.faculty.tabs.export_credentials import render as _tab_export_credentials  # type: ignore
    # --- ADD THIS LINE ---
    from screens.faculty.tabs.bulk_ops import render as _tab_bulk_ops # type: ignore

except ModuleNotFoundError:
    # ...otherwise fall back to the original single-file implementation
    # (import your existing implementations if you still have them in one file).
    # Replace 'screens.faculty.ui_tabs_legacy' with the module that defines the original functions.
    # Make sure your legacy file also defines _tab_bulk_ops if falling back
    from screens.faculty.ui_tabs_legacy import (  # type: ignore
        _tab_credits_policy,
        _tab_designations,
        _tab_designation_removal,
        _tab_custom_types,
        _tab_profiles,
        _tab_affiliations,
        _tab_export_credentials,
        _tab_bulk_ops, # Ensure legacy file has this too
    )

# Re-export the names so existing code keeps working.
# --- ADD _tab_bulk_ops TO THIS LIST ---
__all__ = [
    "_tab_credits_policy",
    "_tab_designations",
    "_tab_designation_removal",
    "_tab_custom_types",
    "_tab_profiles",
    "_tab_affiliations",
    "_tab_export_credentials",
    "_tab_bulk_ops", # Added
]
