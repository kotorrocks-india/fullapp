# core/nav_registry.py
from __future__ import annotations
from dataclasses import dataclass
from typing import Callable, Optional, List, Dict

# Page renderer signature: (engine) -> None. If your pages need extra,
# you can curry with lambdas.
PageFn = Callable[[object], None]

@dataclass(frozen=True)
class Route:
    key: str                  # stable id, also used in ?page=<key>
    label: str                # UI label
    icon: str                 # emoji or short string
    policy_page_key: str      # must match policy.can_view_page(...) name
    render: PageFn            # callable that renders the page

@dataclass
class Section:
    title: str
    routes: List[Route]

# Import your existing page renderers. Prefer "screens" modules to avoid
# accidental page auto-run code.
from screens.degrees import render as degrees_render
from screens.assignments import render as assignments_render
from screens.marks import render as marks_render
from screens.programs_branches import render as programs_branches_render
from screens.semesters import render as semesters_render
from screens.approvals import render as approvals_render
from screens.branding import render as branding_render
from screens.footer import render as footer_render
from screens.appearance_theme import render as appearance_theme_render  # your 12_* page
from screens.users_roles import render as users_roles_render
from screens.superadmin import render as superadmin_render
from screens.login import render as login_render
from screens.logout import render as logout_render
from screens.faculty import render as faculty_render

SECTIONS: List[Section] = [
    Section("Setup", [
        Route("login",        "Login",            "🔐", "Login",        login_render),
        Route("superadmin",   "Superadmin",       "🛠️", "Superadmin",   superadmin_render),
        Route("users_roles",  "Users & Roles",    "👥", "UsersRoles",    users_roles_render),
    ]),
    Section("Branding & Theme", [
        Route("branding",     "Branding (Login)", "🎨", "Branding",      branding_render),
        Route("footer",       "Footer",           "🦶", "Footer",        footer_render),
        Route("appearance",   "Appearance/Theme", "🎛️", "AppearanceTheme", appearance_theme_render),
    ]),
    Section("Academics", [
        Route("degrees",      "Degrees",          "🎓", "Degrees",       degrees_render),
        Route("assignments",  "Assignments",      "📝", "Assignments",   assignments_render),
        Route("marks",        "Marks",            "✅", "Marks",         marks_render),
        Route("programs",     "Programs/Branches","📚", "ProgramsBranches", programs_branches_render),
        Route("semesters",    "Semesters",        "📅", "Semesters",     semesters_render),
        Route("faculty",      "Faculty",          "👨‍🏫", "Faculty",       faculty_render),  # Add Faculty page
    ]),
    Section("Governance", [
        Route("approvals",    "Approvals",        "📬", "Approvals",     approvals_render),
        Route("logout",       "Logout",           "🚪", "Logout",        logout_render),
    ]),
]

# Index for quick lookup (used by router)
ROUTE_INDEX: Dict[str, Route] = {r.key: r for s in SECTIONS for r in s.routes}
DEFAULT_ROUTE_KEY = "degrees"  # after login, where to land
