# Replace the entire rbac_policy.py with this content
import json
from sqlalchemy import text as sa_text

NAMESPACE = "rbac_policy"

def load_assignment_policy(engine) -> dict:
    with engine.begin() as conn:
        row = conn.execute(sa_text("""
          SELECT config_json FROM configs
          WHERE degree='*' AND namespace=:ns
          ORDER BY updated_at DESC LIMIT 1
        """), {"ns": NAMESPACE}).fetchone()
    if not row: return {}
    try: return json.loads(row[0]) or {}
    except: return {}

def assignable_roles_for(engine, actor_roles: set[str]) -> set[str]:
    doc = load_assignment_policy(engine)
    mapping = (doc.get("role_assignment") or {})
    allowed = set()
    for r in actor_roles:
        allowed |= set(mapping.get(r, []))
    return allowed

# Add faculty resource rules as per todo.txt
def can_edit_faculty_resource(engine, resource: str, roles: set[str]) -> bool:
    """Check if user can edit specific faculty resources."""
    faculty_edit_roles = {
        "faculty.designations": {"superadmin", "tech_admin", "principal", "director"},
        "faculty.credits_policy": {"superadmin", "tech_admin", "principal", "director"},
        "faculty.profiles": {"superadmin", "tech_admin", "principal", "director"},
        "faculty.affiliations": {"superadmin", "tech_admin", "principal", "director"}
    }
    
    allowed_roles = faculty_edit_roles.get(resource, set())
    return bool(roles & allowed_roles)

# For backward compatibility with faculty.py
def can_edit(resource: str, roles: set[str]) -> bool:
    """Simple can_edit function for faculty.py - checks faculty resource permissions."""
    faculty_resources = {
        "faculty.designations", 
        "faculty.credits_policy", 
        "faculty.profiles", 
        "faculty.affiliations"
    }
    
    if resource in faculty_resources:
        engine = st.session_state.get("engine")
        if engine:
            return can_edit_faculty_resource(engine, resource, roles)
    
    # Default fallback for non-faculty resources
    return bool(roles & {"superadmin", "tech_admin"})
