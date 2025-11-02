# app/screens/faculty/tabs/__init__.py
from __future__ import annotations

from .credits_policy import render as tab_credits_policy
from .designation_catalog import render as tab_designations
from .designation_removal import render as tab_designation_removal
from .custom_types import render as tab_custom_types
from .profiles import render as tab_profiles
from .affiliations import render as tab_affiliations
from .bulk_ops import render as tab_bulk_ops
from .export_credentials import render as tab_export_credentials

TAB_REGISTRY = [
    ("Credits Policy",      tab_credits_policy,      "credits"),
    ("Designation Catalog", tab_designations,        "desg"),
    ("Designation Removal", tab_designation_removal, "desgrem"),
    ("Custom Types",        tab_custom_types,        "custom"),
    ("Profiles",            tab_profiles,            "profiles"),
    ("Affiliations",        tab_affiliations,        "aff"),
    ("Bulk Operations",     tab_bulk_ops,            "bulk"),
    ("Export Credentials",  tab_export_credentials,  "export"),
]
