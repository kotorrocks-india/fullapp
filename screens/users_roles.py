# app/screens/users_roles.py
from __future__ import annotations

#<editor-fold desc="Bootstrap Imports">
import sys
from pathlib import Path
APP_DIR = Path(__file__).resolve().parents[1]
if str(APP_DIR) not in sys.path:
    sys.path.insert(0, str(APP_DIR))
#</editor-fold>

import random
import string
import bcrypt
import pandas as pd
import streamlit as st
from sqlalchemy import text as sa_text, Engine
from datetime import datetime

from core.settings import load_settings
from core.db import get_engine, init_db
# from core.ui import render_footer_global # Removed if not used
from core.policy import require_page
from core.rbac import upsert_user, grant_role, revoke_role, get_user_id

#<editor-fold desc="Helper Functions">
FIXED_ROLES = ["director", "principal", "management_representative"]
TECH_ADMIN_CAP = 10

def _username_from_name(full_name: str) -> tuple[str, str, str]:
    tokens = [t for t in (full_name or "").strip().split() if t]
    given, surname = (tokens[0] if tokens else ""), (tokens[-1] if len(tokens) > 1 else "")
    base5 = (given[:5] or (surname[:5] if surname else "xxxxx")).ljust(5, "x")
    last_initial = (surname[:1] or "x")
    digits = "".join(random.choices(string.digits, k=4))
    return base5, last_initial, digits

def _generate_username(conn, full_name: str, table: str, retries: int = 6) -> str:
    base5, last_initial, digits = _username_from_name(full_name)
    for _ in range(retries):
        candidate = f"{base5}{last_initial}{digits}"
        exists = conn.execute(sa_text(f"SELECT 1 FROM {table} WHERE username=:u"), {"u": candidate}).fetchone()
        if not exists: return candidate
        digits = "".join(random.choices(string.digits, k=4))
    # Fallback for too many conflicts
    fallback_suffix = f"{random.choice(string.ascii_lowercase)}{''.join(random.choices(string.digits, k=3))}"
    return f"{base5}{last_initial}{digits}{fallback_suffix}".lower()

def _initial_password_from_name(full_name: str, digits: str) -> str:
    base5, last_initial, _ = _username_from_name(full_name)
    return f"{base5.lower()}{(last_initial or 'x').lower()}@{digits}"

def _log_revocation_event(engine: Engine, revoked_user_id: int, revoked_email: str, revoked_role: str, revoked_by_email: str):
    """
    Inserts an audit trail record for a role revocation event.
    Creates the audit_log table if it doesn't exist.
    """
    try:
        with engine.begin() as conn:
            # Check/Create audit_log table
            conn.execute(sa_text("""
                CREATE TABLE IF NOT EXISTS audit_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    action TEXT NOT NULL,
                    target_user_id INTEGER,
                    target_email TEXT,
                    actor_email TEXT,
                    details TEXT
                )
            """))

            # Log the event
            action_desc = f"ROLE_REVOKED: {revoked_role}"
            details_json = {
                "revoked_role": revoked_role,
                "revoked_user_id": revoked_user_id
            }

            conn.execute(sa_text("""
                INSERT INTO audit_log (timestamp, action, target_user_id, target_email, actor_email, details)
                VALUES (:ts, :action, :target_uid, :target_email, :actor_email, :details)
            """), {
                "ts": datetime.now().isoformat(),
                "action": action_desc,
                "target_uid": revoked_user_id,
                "target_email": revoked_email,
                "actor_email": revoked_by_email,
                "details": str(details_json) # Simplistic JSON representation for SQLite TEXT
            })

    except Exception as e:
        # Log the audit failure, but don't crash the main operation
        print(f"AUDIT LOG FAILED: {e}")


def _force_password_reset(engine, user_id: int, full_name: str, admin_type: str):
    """Resets a user's password and flags them for a forced change on next login."""
    if admin_type not in ["tech_admins", "academic_admins"]:
        st.error("Invalid admin type for password reset.")
        return

    try:
        with engine.begin() as conn:
            digits = "".join(random.choices(string.digits, k=4))
            new_password = _initial_password_from_name(full_name, digits)
            pw_hash = bcrypt.hashpw(new_password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")

            admin_check = conn.execute(sa_text(f"SELECT 1 FROM {admin_type} WHERE user_id = :uid"), {"uid": user_id}).fetchone()
            if not admin_check:
                st.error(f"User ID {user_id} not found in {admin_type} table.")
                return

            conn.execute(sa_text(f"""
                UPDATE {admin_type}
                SET password_hash = :ph, first_login_pending = 1, password_export_available = 1
                WHERE user_id = :uid
            """), {"ph": pw_hash, "uid": user_id})

            username_row = conn.execute(sa_text(f"SELECT username FROM {admin_type} WHERE user_id = :uid"), {"uid": user_id}).fetchone()
            if not username_row or not username_row._mapping['username']:
                st.error(f"Could not retrieve username for user ID {user_id} to store credentials.")
            else:
                username = username_row._mapping['username']
                conn.execute(sa_text("DELETE FROM initial_credentials WHERE user_id = :uid"), {"uid": user_id})
                # --- FIX: Removed ON CONFLICT clause ---
                conn.execute(sa_text("""
                    INSERT INTO initial_credentials(user_id, username, plaintext)
                    VALUES(:uid, :un, :pt)
                """), {"uid": user_id, "un": username, "pt": new_password})

        st.success(f"🔑 Password for {full_name} has been reset.")
        st.info(f"Their new temporary password is: **{new_password}**")
        # st.rerun()

    except Exception as e:
        st.error(f"Failed to reset password: {e}")
        import traceback
        st.code(traceback.format_exc())

def _is_email_already_admin(engine, email: str, type_to_check: str) -> bool:
    """Checks if an email is already an admin of the OTHER type."""
    table_to_check = ""
    if type_to_check == "academic_admins": table_to_check = "tech_admins"
    elif type_to_check == "tech_admins": table_to_check = "academic_admins"
    else: return False

    try:
        with engine.begin() as conn:
            user_id = get_user_id(conn, email)
            if not user_id: return False
            row = conn.execute(sa_text(f"SELECT 1 FROM {table_to_check} WHERE user_id = :uid"), {"uid": user_id}).fetchone()
        return bool(row)
    except ValueError: return False
    except Exception as e: print(f"Error checking admin status for {email}: {e}"); return False

def _table_has_column(conn, table: str, column: str) -> bool:
    info = conn.execute(sa_text(f"PRAGMA table_info({table})")).fetchall()
    return any(row[1] == column for row in info)

def _user_roles_mode(conn) -> str:
    try:
        has_user_roles = conn.execute(sa_text("SELECT 1 FROM sqlite_master WHERE type='table' AND name='user_roles'")).fetchone()
        if has_user_roles and _table_has_column(conn, "user_roles", "role_name"): return "by_name"
        elif has_user_roles: return "by_id"
        else: return "none"
    except: return "none"

def _roles_csv_expr(conn, user_alias: str = "u") -> str:
    mode = _user_roles_mode(conn)
    if mode == "by_name": return f"(SELECT GROUP_CONCAT(role_name, ', ') FROM user_roles ur WHERE ur.user_id={user_alias}.id)"
    elif mode == "by_id": return f"(SELECT GROUP_CONCAT(r.name, ', ') FROM user_roles ur JOIN roles r ON r.id = ur.role_id WHERE ur.user_id = {user_alias}.id)"
    else: return "''"

def _count_active_tech_admins(engine) -> int:
    with engine.begin() as conn:
        mode = _user_roles_mode(conn)
        if mode == "by_name": row = conn.execute(sa_text("SELECT COUNT(DISTINCT u.id) FROM users u JOIN user_roles ur ON ur.user_id=u.id WHERE u.active=1 AND ur.role_name='tech_admin'")).fetchone()
        elif mode == "by_id": row = conn.execute(sa_text("SELECT COUNT(DISTINCT u.id) FROM users u JOIN user_roles ur ON ur.user_id=u.id JOIN roles r ON r.id=ur.role_id WHERE u.active=1 AND r.name='tech_admin'")).fetchone()
        else: row = [0]
    return int(row[0]) if row else 0

def _list_tech_admins(engine):
    with engine.begin() as conn:
        roles_csv = _roles_csv_expr(conn, "u")
        has_ta_table = conn.execute(sa_text("SELECT 1 FROM sqlite_master WHERE type='table' AND name='tech_admins'")).fetchone()
        if not has_ta_table: return []
        rows = conn.execute(sa_text(f"""
            SELECT u.id AS user_id, u.email, u.full_name, u.employee_id, u.active, ta.username, ta.first_login_pending, ta.password_export_available, {roles_csv} AS roles
            FROM users u JOIN tech_admins ta ON ta.user_id=u.id ORDER BY u.email
        """)).fetchall()
        return [dict(r._mapping) for r in rows]

def _list_academic_admins(engine):
    with engine.begin() as conn:
        roles_csv = _roles_csv_expr(conn, "u")
        has_aa_table = conn.execute(sa_text("SELECT 1 FROM sqlite_master WHERE type='table' AND name='academic_admins'")).fetchone()
        if not has_aa_table: return []
        rows = conn.execute(sa_text(f"""
            SELECT u.id AS user_id, u.email, u.full_name, u.employee_id, u.active, aa.username, aa.fixed_role, aa.designation, aa.first_login_pending, aa.password_export_available, {roles_csv} AS roles
            FROM users u JOIN academic_admins aa ON aa.user_id=u.id ORDER BY u.email
        """)).fetchall()
        return [dict(r._mapping) for r in rows]

def _check_employee_id_exists(conn, employee_id: str, current_user_id: int | None = None) -> tuple[bool, str | None]:
    """Checks if employee_id exists, optionally excluding the current user. Returns (exists, owner_email)."""
    if not employee_id: return False, None
    query = "SELECT email FROM users WHERE employee_id = :eid"
    params = {"eid": employee_id}
    if current_user_id: query += " AND id != :uid"; params["uid"] = current_user_id
    row = conn.execute(sa_text(query), params).fetchone()
    return bool(row), row._mapping['email'] if row else None

def _list_audit_log(engine):
    """Lists recent audit log entries."""
    with engine.begin() as conn:
        has_audit_table = conn.execute(sa_text("SELECT 1 FROM sqlite_master WHERE type='table' AND name='audit_log'")).fetchone()
        if not has_audit_table: return []
        rows = conn.execute(sa_text("SELECT timestamp, action, target_email, actor_email, details FROM audit_log ORDER BY timestamp DESC LIMIT 50")).fetchall()
        return [dict(r._mapping) for r in rows]
#</editor-fold>

@require_page("Users & Roles")
def render():
    settings = load_settings()
    engine = get_engine(settings.db.url)
    init_db(engine)

    user = st.session_state.get("user") or {}
    current_user_roles = set(user.get("roles") or [])
    current_user_email = user.get("email") # Get current user's email for logging
    
    can_manage_tech = "superadmin" in current_user_roles
    can_manage_academic = current_user_roles.intersection({"superadmin", "tech_admin"})
    can_export = can_manage_academic
    can_view_audit = can_manage_academic # Tech Admins and Superadmins can view the log

    st.set_page_config(layout="wide")
    st.title("👥 Users & Roles")
    st.caption("Create and manage Tech Admins (System) and Academic Admins (Faculty/Staff).")

    tab_ta, tab_aa, tab_export, tab_audit = st.tabs(["⚙️ Tech Admins", "🎓 Academic Admins", "🔑 Export Credentials", "📝 Audit Log"])

    # --- Tech Admins Tab ---
    with tab_ta:
        st.subheader("Tech Admins")
        st.caption(f"Manage system administrators. Limit: {TECH_ADMIN_CAP} active.")
        tech_admins = _list_tech_admins(engine)
        if tech_admins:
            df_ta = pd.DataFrame(tech_admins)
            st.dataframe(df_ta, use_container_width=True, hide_index=True, column_config={
                "user_id": None, "email": st.column_config.TextColumn("📧 Email", help="User's login email", width="medium"),
                "full_name": st.column_config.TextColumn("👤 Name", width="medium"), "employee_id": st.column_config.TextColumn("🆔 Employee ID", width="small"),
                "username": st.column_config.TextColumn("🧑‍💻 Username", width="small"), "active": st.column_config.CheckboxColumn("✅ Active?", help="Is the user account active?", width="small"),
                "first_login_pending": st.column_config.CheckboxColumn("🔒 1st Login?", help="Does the user need to change password on first login?", width="small"),
                "password_export_available": None, "roles": st.column_config.TextColumn("🎭 Roles", help="Assigned system roles"),
            })
            st.markdown("---"); st.markdown("#### Manage Tech Admins")
            for admin in tech_admins:
                is_super = 'superadmin' in (admin.get('roles') or '')
                if is_super: continue
                with st.expander(f"👤 {admin['full_name']} ({admin['email']})"):
                    col1, col2 = st.columns(2)
                    with col1:
                        if st.button("🔑 Force Password Reset", key=f"reset_ta_{admin['user_id']}", disabled=not can_manage_tech, help="Generates a new temporary password"):
                            _force_password_reset(engine, admin['user_id'], admin['full_name'], 'tech_admins')
                    with col2:
                        if st.button("🚫 Revoke tech_admin Role", key=f"revoke_ta_{admin['user_id']}", disabled=not can_manage_tech, help="Removes Tech Admin permissions"):
                            try:
                                revoked_email = admin['email']
                                revoked_id = admin['user_id']
                                revoke_role(revoked_email, "tech_admin")
                                with engine.begin() as conn:
                                     conn.execute(sa_text("DELETE FROM tech_admins WHERE user_id = :uid"), {"uid": revoked_id})
                                     conn.execute(sa_text("DELETE FROM initial_credentials WHERE user_id = :uid"), {"uid": revoked_id})
                                
                                # --- AUDIT LOG ---
                                _log_revocation_event(engine, revoked_id, revoked_email, "tech_admin", current_user_email)

                                st.success(f"Revoked tech_admin from {revoked_email} and removed specific admin record.")
                                st.rerun()
                            except Exception as ex: st.error(str(ex))
        else: st.info("No tech admins found.")

        st.markdown("---"); st.markdown("### ✨ Add New Tech Admin")
        with st.form("add_tech_admin_form"):
            ta_email = st.text_input("📧 Email*", help="Unique login email").strip().lower()
            ta_name  = st.text_input("👤 Full Name*", help="User's full name").strip()
            ta_emp_id = st.text_input("🆔 Employee ID*", help="Unique Employee Identifier").strip()
            submitted_ta = st.form_submit_button("➕ Add Tech Admin", type="primary", disabled=not can_manage_tech)
            if submitted_ta:
                error = False
                if not ta_email or not ta_name or not ta_emp_id: st.error("Email, Full name, and Employee ID are required."); error = True
                if not error and _is_email_already_admin(engine, ta_email, "tech_admins"): st.error(f"{ta_email} is already an Academic Admin."); error = True
                if not error and _count_active_tech_admins(engine) >= TECH_ADMIN_CAP: st.error(f"Active tech_admins limit reached ({TECH_ADMIN_CAP})."); error = True
                if not error:
                    with engine.begin() as conn:
                        clash_exists, clash_owner = _check_employee_id_exists(conn, ta_emp_id)
                        if clash_exists: st.error(f"❌ Employee ID '{ta_emp_id}' is already assigned to {clash_owner}. Use a unique ID."); error = True
                if not error:
                    try:
                        upsert_user(ta_email, full_name=ta_name, active=True, employee_id=ta_emp_id)
                        grant_role(ta_email, "tech_admin")
                        with engine.begin() as conn:
                            username = _generate_username(conn, ta_name, table="tech_admins")
                            digits = username[-4:] if username[-4:].isdigit() else "".join(random.choices(string.digits, k=4))
                            initial_password = _initial_password_from_name(ta_name, digits)
                            pw_hash = bcrypt.hashpw(initial_password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")
                            uid = get_user_id(conn, ta_email)
                            conn.execute(sa_text("""
                                INSERT INTO tech_admins(user_id, username, password_hash, first_login_pending, password_export_available) VALUES (:uid, :username, :hash, 1, 1)
                                ON CONFLICT(user_id) DO UPDATE SET username=excluded.username, password_hash=excluded.password_hash, first_login_pending=excluded.first_login_pending, password_export_available=excluded.password_export_available
                            """), {"uid": uid, "username": username, "hash": pw_hash})
                            # --- FIX: Removed ON CONFLICT clause ---
                            conn.execute(sa_text("""
                                INSERT INTO initial_credentials(user_id, username, plaintext) VALUES (:uid, :username, :plaintext)
                            """), {"uid": uid, "username": username, "plaintext": initial_password})
                        st.success(f"✅ Granted tech_admin to {ta_email}.")
                        with st.expander("🔑 Show initial credentials (displayed once)"):
                            st.code(f"username: {username}\npassword: {initial_password}")
                        # st.rerun()
                    except Exception as ex: st.error(f"Failed to grant role: {str(ex)}")

    # --- Academic Admins Tab ---
    with tab_aa:
        st.subheader("Academic Admins")
        st.caption("Manage academic roles like Principal, Director, Dean, HOD.")
        academic_admins = _list_academic_admins(engine)
        if academic_admins:
            df_aa = pd.DataFrame(academic_admins)
            st.dataframe(df_aa, use_container_width=True, hide_index=True, column_config={
                "user_id": None, "email": st.column_config.TextColumn("📧 Email", width="medium"),
                "full_name": st.column_config.TextColumn("👤 Name", width="medium"), "employee_id": st.column_config.TextColumn("🆔 Employee ID", width="small"),
                "username": st.column_config.TextColumn("🧑‍💻 Username", width="small"), "fixed_role": st.column_config.TextColumn("🔒 Fixed Role", help="Core role, cannot be changed here", width="small"),
                "designation": st.column_config.TextColumn("🏷️ Designation", help="Display title, can be edited below", width="medium"),
                "active": st.column_config.CheckboxColumn("✅ Active?", width="small"), "first_login_pending": st.column_config.CheckboxColumn("🔒 1st Login?", width="small"),
                "password_export_available": None, "roles": st.column_config.TextColumn("🎭 Roles"),
            })
            st.markdown("---"); st.markdown("#### Manage Academic Admins")
            for admin in academic_admins:
                 is_super = 'superadmin' in (admin.get('roles') or '')
                 if is_super: continue
                 with st.expander(f"👤 {admin['full_name']} ({admin['email']}) - {admin.get('fixed_role') or 'No Fixed Role'}"):
                    new_desig = st.text_input("Designation", value=(admin.get('designation') or ""), key=f"desig_aa_{admin['user_id']}")
                    if st.button("✏️ Update Designation", key=f"update_desig_{admin['user_id']}", disabled=not can_manage_academic):
                        with engine.begin() as conn:
                            conn.execute(sa_text("UPDATE academic_admins SET designation=:d WHERE user_id=:uid"), {"d": new_desig, "uid": admin['user_id']})
                        st.success("Designation updated."); st.rerun()
                    st.markdown("---")
                    col1, col2 = st.columns(2)
                    with col1:
                        if st.button("🔑 Force Password Reset", key=f"reset_aa_{admin['user_id']}", disabled=not can_manage_academic):
                            _force_password_reset(engine, admin['user_id'], admin['full_name'], 'academic_admins')
                    with col2:
                        # can_revoke = admin.get('fixed_role') not in FIXED_ROLES # This check was misleading, removing it. Revocation is always allowed by admin, but fixed role cleanup logic handles it.
                        if st.button("🚫 Revoke academic_admin Role", key=f"revoke_aa_{admin['user_id']}", disabled=not can_manage_academic, help="Removes Academic Admin permissions. Fixed roles will be automatically revoked too."):
                            try:
                                revoked_email = admin['email']
                                revoked_id = admin['user_id']
                                revoked_fixed_role = admin.get('fixed_role')

                                revoke_role(revoked_email, "academic_admin")
                                if revoked_fixed_role in FIXED_ROLES: 
                                    revoke_role(revoked_email, revoked_fixed_role)
                                
                                with engine.begin() as conn:
                                     conn.execute(sa_text("DELETE FROM academic_admins WHERE user_id = :uid"), {"uid": revoked_id})
                                     conn.execute(sa_text("DELETE FROM initial_credentials WHERE user_id = :uid"), {"uid": revoked_id})

                                # --- AUDIT LOG ---
                                log_role = f"academic_admin" + (f" and {revoked_fixed_role}" if revoked_fixed_role else "")
                                _log_revocation_event(engine, revoked_id, revoked_email, log_role, current_user_email)

                                st.success(f"Revoked {log_role} from {revoked_email} and removed admin record."); st.rerun()
                            except Exception as ex: st.error(str(ex))
        else: st.info("No academic admins found.")

        st.markdown("---"); st.markdown("### ✨ Add New Academic Admin")
        with st.form("add_academic_admin_form"):
            aa_email = st.text_input("📧 Email*", help="Unique login email").strip().lower()
            aa_name  = st.text_input("👤 Full Name*", help="User's full name").strip()
            aa_emp_id = st.text_input("🆔 Employee ID*", help="Unique Employee Identifier").strip()
            aa_fixed = st.selectbox("🔒 Fixed Role", options=FIXED_ROLES, help="Select core immutable role (e.g., Principal)")
            aa_desig = st.text_input("🏷️ Designation", key="aa_desig", help="Display title (e.g., 'Principal', 'Professor & Dean')").strip()
            submitted_aa = st.form_submit_button("➕ Add Academic Admin", type="primary", disabled=not can_manage_academic)
            if submitted_aa:
                error = False
                if not aa_email or not aa_name or not aa_emp_id: st.error("Email, Full name, and Employee ID are required."); error = True
                if not error and _is_email_already_admin(engine, aa_email, "tech_admins"): st.error(f"{aa_email} is already a Tech Admin."); error = True
                if not error:
                    with engine.begin() as conn:
                        clash_exists, clash_owner = _check_employee_id_exists(conn, aa_emp_id)
                        if clash_exists: st.error(f"❌ Employee ID '{aa_emp_id}' is already assigned to {clash_owner}. Use a unique ID."); error = True
                if not error:
                    try:
                        upsert_user(aa_email, full_name=aa_name, active=True, employee_id=aa_emp_id)
                        grant_role(aa_email, "academic_admin")
                        if aa_fixed: grant_role(aa_email, aa_fixed)
                        with engine.begin() as conn:
                            username = _generate_username(conn, aa_name, table="academic_admins")
                            digits = username[-4:] if username[-4:].isdigit() else "".join(random.choices(string.digits, k=4))
                            initial_password = _initial_password_from_name(aa_name, digits)
                            pw_hash = bcrypt.hashpw(initial_password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")
                            uid = get_user_id(conn, aa_email)
                            conn.execute(sa_text("""
                                INSERT INTO academic_admins(user_id, fixed_role, designation, username, password_hash, first_login_pending, password_export_available) VALUES (:uid, :fixed_role, :designation, :username, :hash, 1, 1)
                                ON CONFLICT(user_id) DO UPDATE SET fixed_role=excluded.fixed_role, designation=excluded.designation, username=excluded.username, password_hash=excluded.password_hash, first_login_pending=excluded.first_login_pending, password_export_available=excluded.password_export_available
                            """), {"uid": uid, "fixed_role": aa_fixed, "designation": aa_desig or None, "username": username, "hash": pw_hash})
                            # --- FIX: Removed ON CONFLICT clause ---
                            conn.execute(sa_text("""
                                INSERT INTO initial_credentials(user_id, username, plaintext) VALUES (:uid, :username, :plaintext)
                            """), {"uid": uid, "username": username, "plaintext": initial_password})
                        st.success(f"✅ Granted academic_admin to {aa_email} with fixed_role='{aa_fixed}'.")
                        with st.expander("🔑 Show initial credentials (displayed once)"):
                            st.code(f"username: {username}\npassword: {initial_password}")
                        # st.rerun()
                    except Exception as ex: st.error(f"Failed to grant role: {str(ex)}")

    # --- Export Credentials Tab ---
    with tab_export:
        st.subheader("Export Initial Credentials")
        st.caption("Download credentials for users who haven't logged in yet. This invalidates the export after download.")
        if not can_export: st.warning("🔒 You don't have permission to export credentials.")
        else:
            role_filter = st.selectbox("Filter by Role", ["All Pending", "Tech Admins Only", "Academic Admins Only"], key="export_filter")
            where_role = ""
            if role_filter == "Tech Admins Only": where_role = "AND ta.user_id IS NOT NULL"
            elif role_filter == "Academic Admins Only": where_role = "AND aa.user_id IS NOT NULL"
            try:
                with engine.begin() as conn:
                    query = f"""
                        SELECT u.id AS user_id, u.email, u.full_name, u.employee_id, ic.username, ic.plaintext AS initial_password,
                               CASE WHEN ta.user_id IS NOT NULL THEN 'Tech Admin' WHEN aa.user_id IS NOT NULL THEN 'Academic Admin' ELSE 'Unknown' END AS admin_type
                        FROM users u JOIN initial_credentials ic ON ic.user_id = u.id AND ic.consumed = 0
                        LEFT JOIN tech_admins ta ON ta.user_id = u.id AND ta.first_login_pending = 1 AND ta.password_export_available = 1
                        LEFT JOIN academic_admins aa ON aa.user_id = u.id AND aa.first_login_pending = 1 AND aa.password_export_available = 1
                        WHERE u.active = 1 AND (ta.user_id IS NOT NULL OR aa.user_id IS NOT NULL) {where_role} ORDER BY u.email
                    """
                    rows = conn.execute(sa_text(query)).fetchall()
                if rows:
                    data = [dict(r._mapping) for r in rows]; df_export = pd.DataFrame(data)
                    st.dataframe(df_export, use_container_width=True, hide_index=True, column_config={
                        "user_id": None, "email": st.column_config.TextColumn("📧 Email", width="medium"), "full_name": st.column_config.TextColumn("👤 Name", width="medium"),
                        "employee_id": st.column_config.TextColumn("🆔 Employee ID", width="small"), "username": st.column_config.TextColumn("🧑‍💻 Username", width="small"),
                        "initial_password": st.column_config.TextColumn("🔑 Temp Password", width="small"), "admin_type": st.column_config.TextColumn("⚙️ Type", width="small"),
                    })
                    csv = df_export.to_csv(index=False).encode("utf-8")
                    if st.download_button("⬇️ Download Pending Credentials CSV", data=csv, file_name="initial_credentials.csv", mime="text/csv", key="btn_dl_csv", help="Downloading marks these credentials as exported and consumed."):
                        user_ids_to_consume = [int(d["user_id"]) for d in data]
                        if user_ids_to_consume:
                            with engine.begin() as conn:
                                uid_params = [{"uid": uid} for uid in user_ids_to_consume]
                                conn.execute(sa_text("UPDATE initial_credentials SET consumed = 1 WHERE consumed = 0 AND user_id = :uid"), uid_params)
                                conn.execute(sa_text("UPDATE tech_admins SET password_export_available = 0 WHERE first_login_pending=1 AND user_id = :uid"), uid_params)
                                conn.execute(sa_text("UPDATE academic_admins SET password_export_available = 0 WHERE first_login_pending=1 AND user_id = :uid"), uid_params)
                            st.success(f"✅ Exported and invalidated credentials for {len(user_ids_to_consume)} user(s)."); st.rerun()
                else: st.info("✅ No pending initial credentials available for export with the selected filter.")
            except Exception as e: st.error(f"Error preparing export: {e}"); st.exception(e)

    # --- Audit Log Tab ---
    with tab_audit:
        st.subheader("Audit Log (Last 50 Events)")
        if not can_view_audit:
            st.warning("🔒 You don't have permission to view the audit log.")
        else:
            audit_logs = _list_audit_log(engine)
            if audit_logs:
                df_audit = pd.DataFrame(audit_logs)
                st.dataframe(df_audit, use_container_width=True, hide_index=True, column_config={
                    "timestamp": st.column_config.DatetimeColumn("⏰ Timestamp", format="YYYY-MM-DD HH:mm:ss", width="medium"),
                    "action": st.column_config.TextColumn("📝 Action", width="medium"),
                    "target_email": st.column_config.TextColumn("📧 Target User", width="medium"),
                    "actor_email": st.column_config.TextColumn("👤 Performed By", width="medium"),
                    "details": st.column_config.TextColumn("💬 Details", width="large"),
                })
            else:
                st.info("No audit logs found.")


    st.markdown("---")
    # render_footer_global()

# Run the app
if __name__ == "__main__":
    render()
