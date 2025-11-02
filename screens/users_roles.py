# app/pages/03_👥_Users_Roles.py
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
from sqlalchemy import text as sa_text

from core.settings import load_settings
from core.db import get_engine, init_db
from core.ui import render_footer_global

# --- THIS IMPORT HAS BEEN CORRECTED ---
# We now import the correct security decorator from policy.py
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
    return f"{base5}{last_initial}{digits}{random.choice(string.ascii_lowercase)}{''.join(random.choices(string.digits, k=3))}"

def _initial_password_from_name(full_name: str, digits: str) -> str:
    base5, last_initial, _ = _username_from_name(full_name)
    return f"{base5.lower()}{(last_initial or 'x').lower()}@{digits}"

def _table_has_column(conn, table: str, column: str) -> bool:
    info = conn.execute(sa_text(f"PRAGMA table_info({table})")).fetchall()
    return any(row[1] == column for row in info)

def _user_roles_mode(conn) -> str:
    return "by_name" if _table_has_column(conn, "user_roles", "role_name") else "by_id"

def _roles_csv_expr(conn, user_alias: str = "u") -> str:
    mode = _user_roles_mode(conn)
    if mode == "by_name":
        return f"(SELECT GROUP_CONCAT(role_name, ',') FROM user_roles ur WHERE ur.user_id={user_alias}.id)"
    else:
        return f"(SELECT GROUP_CONCAT(r.name, ',') FROM user_roles ur JOIN roles r ON r.id = ur.role_id WHERE ur.user_id = {user_alias}.id)"

def _count_active_tech_admins(engine) -> int:
    with engine.begin() as conn:
        mode = _user_roles_mode(conn)
        if mode == "by_name":
            row = conn.execute(sa_text("SELECT COUNT(*) FROM users u JOIN user_roles ur ON ur.user_id=u.id WHERE u.active=1 AND ur.role_name='tech_admin'")).fetchone()
        else:
            row = conn.execute(sa_text("SELECT COUNT(*) FROM users u JOIN user_roles ur ON ur.user_id=u.id JOIN roles r ON r.id=ur.role_id WHERE u.active=1 AND r.name='tech_admin'")).fetchone()
    return int(row[0]) if row else 0

def _list_tech_admins(engine):
    with engine.begin() as conn:
        roles_csv = _roles_csv_expr(conn, "u")
        rows = conn.execute(sa_text(f"SELECT u.id AS user_id, u.email, u.full_name, u.active, ta.username, ta.first_login_pending, ta.password_export_available, {roles_csv} AS roles FROM users u JOIN tech_admins ta ON ta.user_id=u.id ORDER BY u.email")).fetchall()
        return rows

def _list_academic_admins(engine):
    with engine.begin() as conn:
        roles_csv = _roles_csv_expr(conn, "u")
        rows = conn.execute(sa_text(f"SELECT u.id AS user_id, u.email, u.full_name, u.active, aa.username, aa.fixed_role, aa.designation, aa.first_login_pending, aa.password_export_available, {roles_csv} AS roles FROM users u JOIN academic_admins aa ON aa.user_id=u.id ORDER BY u.email")).fetchall()
        return rows
#</editor-fold>

# --- THIS DECORATOR HAS BEEN CORRECTED ---
# We now use the modern, correct security decorator from policy.py
@require_page("Users & Roles")
def render():
    settings = load_settings()
    engine = get_engine(settings.db.url)
    init_db(engine)

    user = st.session_state.get("user") or {}
    roles = set(user.get("roles") or [])

    st.title("👥 Users & Roles")
    st.caption("Create and manage Tech Admins and Academic Admins.")

    tab_ta, tab_aa, tab_export = st.tabs(["Tech Admins", "Academic Admins", "Export Initial Credentials"])

    # ============================== TECH ADMINS ==============================
    with tab_ta:
        st.subheader("Tech Admins (limit 10 active)")
        rows = _list_tech_admins(engine)
        if rows:
            df = pd.DataFrame([dict(r._mapping) for r in rows])
            st.dataframe(df, use_container_width=True, hide_index=True)
        else:
            st.info("No tech admins yet.")

        st.markdown("---")
        st.markdown("### Add Tech Admin")
        ta_email = st.text_input("Email", key="ta_email").strip().lower()
        ta_name  = st.text_input("Full name", key="ta_name").strip()

        if st.button("Grant tech_admin", key="btn_grant_ta", disabled=("superadmin" not in roles)):
            if not ta_email or not ta_name: st.error("Email and Full name are required.")
            else:
                try:
                    if _count_active_tech_admins(engine) >= TECH_ADMIN_CAP:
                        st.error(f"Active tech_admins limit reached ({TECH_ADMIN_CAP}).")
                    else:
                        upsert_user(ta_email, full_name=ta_name, active=True)
                        grant_role(ta_email, "tech_admin")
                        with engine.begin() as conn:
                            username = _generate_username(conn, ta_name, table="tech_admins")
                            digits = username[-4:] if username[-4:].isdigit() else "".join(random.choices(string.digits, k=4))
                            initial_password = _initial_password_from_name(ta_name, digits)
                            pw_hash = bcrypt.hashpw(initial_password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")
                            uid = get_user_id(conn, ta_email)
                            conn.execute(sa_text("INSERT INTO tech_admins(user_id, username, password_hash, first_login_pending, password_export_available) VALUES (:uid, :username, :hash, 1, 1) ON CONFLICT(user_id) DO UPDATE SET username=excluded.username"), {"uid": uid, "username": username, "hash": pw_hash})
                            conn.execute(sa_text("INSERT INTO initial_credentials(user_id, username, plaintext) VALUES (:uid, :username, :plaintext)"), {"uid": uid, "username": username, "plaintext": initial_password})
                        with st.expander("Show initial credentials (displayed once)"):
                            st.code(f"username: {username}\npassword: {initial_password}")
                        st.success(f"Granted tech_admin to {ta_email}.")
                        st.rerun()
                except Exception as ex: st.error(str(ex))

        st.markdown("---")
        st.markdown("### Revoke Tech Admin")
        ta_email_revoke = st.text_input("Email to revoke", key="ta_email_revoke").strip().lower()
        if st.button("Revoke tech_admin role", key="btn_revoke_ta", disabled=("superadmin" not in roles)):
            try:
                revoke_role(ta_email_revoke, "tech_admin")
                st.success(f"Revoked tech_admin from {ta_email_revoke}.")
                st.rerun()
            except Exception as ex: st.error(str(ex))

    # =========================== ACADEMIC ADMINS ============================
    with tab_aa:
        st.subheader("Academic Admins")
        rows = _list_academic_admins(engine)
        if rows:
            df = pd.DataFrame([dict(r._mapping) for r in rows])
            st.dataframe(df, use_container_width=True, hide_index=True)
        else:
            st.info("No academic admins yet.")

        st.markdown("---")
        st.markdown("### Add Academic Admin")
        aa_email = st.text_input("Email to grant academic_admin", key="aa_email").strip().lower()
        aa_name  = st.text_input("Full name", key="aa_name").strip()
        aa_fixed = st.selectbox("Fixed Role (immutable)", options=FIXED_ROLES)
        aa_desig = st.text_input("Designation (display label; editable later)", key="aa_desig").strip()

        if st.button("Grant academic_admin", key="btn_grant_aa", disabled=not roles.intersection({"superadmin", "tech_admin"})):
            if not aa_email or not aa_name: st.error("Email and Full name are required.")
            else:
                try:
                    upsert_user(aa_email, full_name=aa_name, active=True)
                    # --- MODIFICATION START ---
                    # Grant the general academic_admin role AND the specific fixed_role
                    grant_role(aa_email, "academic_admin")
                    if aa_fixed:
                        grant_role(aa_email, aa_fixed)
                    # --- MODIFICATION END ---
                    with engine.begin() as conn:
                        username = _generate_username(conn, aa_name, table="academic_admins")
                        digits = username[-4:] if username[-4:].isdigit() else "".join(random.choices(string.digits, k=4))
                        initial_password = _initial_password_from_name(aa_name, digits)
                        pw_hash = bcrypt.hashpw(initial_password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")
                        uid = get_user_id(conn, aa_email)
                        conn.execute(sa_text("INSERT INTO academic_admins(user_id, fixed_role, designation, username, password_hash, first_login_pending, password_export_available) VALUES (:uid, :fixed_role, :designation, :username, :hash, 1, 1) ON CONFLICT(user_id) DO UPDATE SET fixed_role=excluded.fixed_role, designation=excluded.designation, username=excluded.username"), {"uid": uid, "fixed_role": aa_fixed, "designation": aa_desig, "username": username, "hash": pw_hash})
                        conn.execute(sa_text("INSERT INTO initial_credentials(user_id, username, plaintext) VALUES (:uid, :username, :plaintext)"), {"uid": uid, "username": username, "plaintext": initial_password})
                    with st.expander("Show initial credentials (displayed once)"):
                        st.code(f"username: {username}\npassword: {initial_password}")
                    st.success(f"Granted academic_admin to {aa_email} with fixed_role={aa_fixed}.")
                    st.rerun()
                except Exception as ex: st.error(str(ex))

        st.markdown("---")
        st.markdown("### Edit Academic Admin Designation")
        aa_email_edit = st.text_input("Academic admin email", key="aa_email_edit").strip().lower()
        aa_new_desig  = st.text_input("New designation", key="aa_new_desig").strip()
        if st.button("Update designation", key="btn_update_desig"):
            if not aa_email_edit or not aa_new_desig: st.error("Email and new designation are required.")
            else:
                try:
                    with engine.begin() as conn:
                        uid_row = conn.execute(sa_text("SELECT id FROM users WHERE LOWER(email)=LOWER(:e)"), {"e": aa_email_edit}).fetchone()
                        if not uid_row: st.error("User not found.")
                        else:
                            conn.execute(sa_text("UPDATE academic_admins SET designation=:d WHERE user_id=:uid"), {"d": aa_new_desig, "uid": uid_row[0]})
                            st.success("Designation updated.")
                            st.rerun()
                except Exception as ex: st.error(str(ex))

        st.markdown("---")
        st.markdown("### Revoke Academic Admin")
        aa_email_revoke = st.text_input("Email to revoke academic_admin", key="aa_email_revoke").strip().lower()
        if st.button("Revoke academic_admin role", key="btn_revoke_aa"):
            if not roles.intersection({"superadmin", "tech_admin"}):
                st.error("Only superadmin/tech_admin can revoke academic admins.")
            else:
                try:
                    revoke_role(aa_email_revoke, "academic_admin")
                    st.success(f"Revoked academic_admin from {aa_email_revoke}.")
                    st.rerun()
                except Exception as ex: st.error(str(ex))

    # ======================== EXPORT INITIAL CREDENTIALS =======================
    with tab_export:
        st.subheader("Export Initial Credentials (pending first login)")
        if not roles.intersection({"superadmin", "tech_admin"}):
            st.info("You don’t have permission to export credentials.")
        else:
            role_filter = st.selectbox("Filter", ["All", "Tech Admins", "Academic Admins"], key="export_filter")
            where_role = ""
            if role_filter == "Tech Admins": where_role = "AND ta.user_id IS NOT NULL"
            elif role_filter == "Academic Admins": where_role = "AND aa.user_id IS NOT NULL"
            with engine.begin() as conn:
                rows = conn.execute(sa_text(f"SELECT u.id AS user_id, u.email, u.full_name, COALESCE(ta.username, aa.username) AS username, ic.plaintext AS initial_password, CASE WHEN ta.user_id IS NOT NULL THEN 'tech_admin' WHEN aa.user_id IS NOT NULL THEN 'academic_admin' ELSE NULL END AS role_scope FROM users u LEFT JOIN tech_admins ta ON ta.user_id=u.id LEFT JOIN academic_admins aa ON aa.user_id=u.id JOIN initial_credentials ic ON ic.user_id=u.id AND ic.consumed=0 WHERE u.active=1 AND ((ta.user_id IS NOT NULL AND ta.first_login_pending=1 AND ta.password_export_available=1) OR (aa.user_id IS NOT NULL AND aa.first_login_pending=1 AND aa.password_export_available=1)) {where_role} ORDER BY u.email")).fetchall()
            if rows:
                data = [dict(r._mapping) for r in rows]
                df = pd.DataFrame(data)
                st.dataframe(df, use_container_width=True, hide_index=True)
                csv = df.to_csv(index=False).encode("utf-8")
                if st.download_button("⬇️ Download CSV", data=csv, file_name="initial_credentials.csv", mime="text/csv", key="btn_dl_csv"):
                    user_ids = [int(d["user_id"]) for d in data]
                    with engine.begin() as conn:
                        for uid in user_ids:
                            conn.execute(sa_text("UPDATE initial_credentials SET consumed = 1 WHERE consumed = 0 AND user_id = :uid"), {"uid": uid})
                            conn.execute(sa_text("UPDATE tech_admins SET password_export_available = 0 WHERE first_login_pending=1 AND user_id = :uid"), {"uid": uid})
                            conn.execute(sa_text("UPDATE academic_admins SET password_export_available = 0 WHERE first_login_pending=1 AND user_id = :uid"), {"uid": uid})
                    st.success("Exported and invalidated plaintexts.")
                    st.rerun()
            else:
                st.info("Nothing to export.")

    st.markdown("---")
   

render()
