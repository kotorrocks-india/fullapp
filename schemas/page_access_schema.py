# schemas/page_access_schema.py
from __future__ import annotations
from sqlalchemy import text as sa_text
from sqlalchemy.engine import Engine

try:
    from core.schema_registry import register
except ImportError:
    def register(func): return func

# The pages from your core/policy.py file
# We will use this to populate the table
DEFAULT_PAGE_ACCESS = {
    # --- Core Pages ---
    "Login":  {"view": {"public"}},
    "Logout": {"view": {"public"}},
    "Profile": {"view": {"superadmin", "tech_admin", "academic_admin", "principal", "director", "management_representative"}, "edit": set()},
    "Users & Roles":     {"view": {"superadmin","tech_admin"}, "edit": {"superadmin","tech_admin"}},
    "Branding (Login)":  {"view": {"superadmin"}, "edit": {"superadmin"}},
    "Footer":            {"view": {"superadmin"}, "edit": {"superadmin"}},
    "Appearance / Theme":{"view": {"superadmin","tech_admin"}, "edit": {"superadmin","tech_admin"}},
    "Degrees":           {"view": {"superadmin","principal","director"}, "edit": {"superadmin","principal","director"}},
    "Programs / Branches": {"view": {"superadmin","principal","director"}, "edit": {"superadmin","principal","director"}},
    "Semesters":         {"view": {"superadmin","principal","director"}, "edit": {"superadmin","principal","director"}},
    "Subjects & Syllabus": {"view": {"superadmin","tech_admin","principal","director","academic_admin"}, "edit": {"superadmin","tech_admin","academic_admin"}},
    "Assignments":       {"view": {"superadmin","principal","director","academic_admin"}, "edit": {"superadmin","academic_admin"}},
    "Marks":             {"view": {"superadmin","principal","director","academic_admin"}, "edit": {"superadmin","academic_admin"}},
    "Approvals":         {"view": {"superadmin","principal","director","management_representative"}, "edit": {"superadmin","principal","director"}},
    "Approval Management": {"view": {"superadmin"}, "edit": {"superadmin"}},
    "Faculty": {"view": {"superadmin", "tech_admin", "principal", "director", "academic_admin"}, "edit": {"superadmin", "tech_admin", "principal", "director"}},
    "Office Admins": {"view": {"superadmin", "tech_admin", "principal", "director"}, "edit": {"superadmin", "tech_admin"}},
    "Academic Years":    {"view": {"superadmin", "principal", "director", "academic_admin"}, "edit": {"superadmin", "academic_admin"}},
    "Students":          {"view": {"superadmin", "principal", "director", "academic_admin"}, "edit": {"superadmin", "academic_admin"}},
    "Subjects Offerings": {"view": {"superadmin", "principal", "director", "academic_admin"}, "edit": {"superadmin", "academic_admin"}},
    "Electives & College Projects": {"view": {"superadmin", "principal", "director", "academic_admin"}, "edit": {"superadmin", "academic_admin"}},


    # --- NEW: Faculty Tabs ---
    # These will now appear in your management UI
    "Credits Policy": {"view": {"superadmin", "principal", "director"}, "edit": {"superadmin"}},
    "Designation Catalog": {"view": {"superadmin", "tech_admin", "principal"}, "edit": {"superadmin", "tech_admin"}},
    "Designation Removal": {"view": {"superadmin", "tech_admin"}, "edit": {"superadmin", "tech_admin"}},
    "Custom Types": {"view": {"superadmin", "tech_admin"}, "edit": {"superadmin", "tech_admin"}},
    "Profiles": {"view": {"superadmin", "tech_admin", "principal", "director", "academic_admin"}, "edit": {"superadmin", "tech_admin"}},
    "Affiliations": {"view": {"superadmin", "tech_admin", "principal", "director", "academic_admin"}, "edit": {"superadmin", "tech_admin"}},
    "Manage Positions": {"view": {"superadmin", "tech_admin", "principal", "director"}, "edit": {"superadmin", "tech_admin"}},
    "Bulk Operations": {"view": {"superadmin", "tech_admin"}, "edit": {"superadmin", "tech_admin"}},
    "Export Credentials": {"view": {"superadmin", "tech_admin"}, "edit": {"superadmin", "tech_admin"}},
}


@register
def ensure_page_access_schema(engine: Engine):
    """
    Creates a table 'page_access_rules' to store View/Edit permissions,
    migrating them from the hardcoded core/policy.py file.
    """
    with engine.begin() as conn:
        
        # 1. Create the main table
        conn.execute(sa_text("""
        CREATE TABLE IF NOT EXISTS page_access_rules (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            page_name TEXT NOT NULL,
            permission_type TEXT NOT NULL, -- 'view' or 'edit'
            role_name TEXT NOT NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            created_by TEXT,
            UNIQUE(page_name, permission_type, role_name)
        )
        """))
        
        # 2. Check if it's a fresh table
        count = conn.execute(sa_text("SELECT COUNT(*) FROM page_access_rules")).fetchone()[0]
        
        # 3. If fresh, populate it from the defaults
        if count == 0:
            admin_email = "system_migration"
            
            for page, permissions in DEFAULT_PAGE_ACCESS.items():
                for perm_type, roles in permissions.items():
                    if not roles: # Handle empty sets like {"edit": set()}
                        continue
                    for role in roles:
                        conn.execute(sa_text("""
                            INSERT OR IGNORE INTO page_access_rules 
                                (page_name, permission_type, role_name, created_by)
                            VALUES (:page, :perm, :role, :admin)
                        """), {
                            "page": page,
                            "perm": perm_type,
                            "role": role,
                            "admin": admin_email
                        })
