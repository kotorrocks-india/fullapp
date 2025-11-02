# screens/office_admin/ui.py
from __future__ import annotations
import streamlit as st
import pandas as pd
from screens.office_admin import db as odb
from screens.office_admin.utils import is_valid_email, generate_initial_password, hash_password

def _get_engine():
    return st.session_state.get("engine")

def _current_user():
    return st.session_state.get("user", {}).get("email", "")

def _load_degrees(conn):
    """Load available degrees."""
    try:
        from sqlalchemy import text as sa_text
        rows = conn.execute(sa_text("SELECT code, name FROM degrees WHERE active=1 ORDER BY name")).fetchall()
        return [{"code": r[0], "name": r[1]} for r in rows]
    except:
        return []

def _load_programs(conn, degree_code: str = None):
    """Load programs, optionally filtered by degree."""
    try:
        from sqlalchemy import text as sa_text
        if degree_code:
            sql = sa_text("SELECT id, name, degree_code FROM programs WHERE degree_code=:deg AND active=1 ORDER BY name")
            rows = conn.execute(sql, {"deg": degree_code}).fetchall()
        else:
            rows = conn.execute(sa_text("SELECT id, name, degree_code FROM programs WHERE active=1 ORDER BY name")).fetchall()
        return [{"id": r[0], "name": r[1], "degree_code": r[2]} for r in rows]
    except:
        return []

def _load_branches(conn, program_id: int = None):
    """Load branches, optionally filtered by program."""
    try:
        from sqlalchemy import text as sa_text
        if program_id:
            sql = sa_text("SELECT id, name, program_id FROM branches WHERE program_id=:pid AND active=1 ORDER BY name")
            rows = conn.execute(sql, {"pid": program_id}).fetchall()
        else:
            rows = conn.execute(sa_text("SELECT id, name, program_id FROM branches WHERE active=1 ORDER BY name")).fetchall()
        return [{"id": r[0], "name": r[1], "program_id": r[2]} for r in rows]
    except:
        return []

# ---------- ACCOUNT MANAGEMENT ----------
def render_accounts():
    st.subheader("Office Admin Accounts")
    eng = _get_engine()
    if not eng:
        st.info("No engine configured.")
        return

    status_filter = st.selectbox("Filter by Status", ["(all)", "active", "disabled"])
    
    with eng.begin() as conn:
        rows = odb.list_office_admins(conn, None if status_filter == "(all)" else status_filter)
    
    if rows:
        df = pd.DataFrame(rows)
        display_cols = ["full_name", "email", "username", "status", "scopes", "last_login", "created_at"]
        available_cols = [c for c in display_cols if c in df.columns]
        st.dataframe(df[available_cols], use_container_width=True)
    else:
        st.info("No office admin accounts found.")

    with st.expander("➕ Create New Office Admin"):
        st.markdown("**Policy:** Only superadmin and tech_admin can create office admin accounts.")
        
        with st.form(key="create_admin_form"):
            full_name = st.text_input("Full Name*")
            email = st.text_input("Email*")
            username = st.text_input("Username (optional, auto-generated if blank)")
            
            submitted = st.form_submit_button("Create Account")
        
        if submitted:
            if not full_name or not is_valid_email(email):
                st.error("Full name and valid email are required.")
            else:
                if not username:
                    username = email.split("@")[0].lower()
                
                temp_password = generate_initial_password(full_name)
                password_hash = hash_password(temp_password)
                
                with eng.begin() as conn:
                    try:
                        admin_id = odb.create_office_admin(conn, {
                            "email": email,
                            "username": username,
                            "full_name": full_name,
                            "password_hash": password_hash
                        }, created_by=_current_user())
                        
                        odb.log_audit(conn, {
                            "actor_email": _current_user(),
                            "actor_role": "superadmin",
                            "action": "create_office_admin",
                            "target_type": "office_admin",
                            "target_id": admin_id,
                            "reason": "Created new office admin account"
                        })
                        
                        st.success(f"✅ Account created for {full_name}")
                        st.info(f"**Temporary Password:** `{temp_password}` (show this once, user must change on first login)")
                        st.info("⚠️ Don't forget to assign scopes to this admin!")
                    except Exception as e:
                        st.error(f"Failed to create account: {e}")

    with st.expander("🔒 Disable Account"):
        with st.form(key="disable_form"):
            email_to_disable = st.text_input("Email to Disable")
            reason = st.text_area("Reason for Disabling*")
            submitted = st.form_submit_button("Disable Account")
        
        if submitted:
            if not email_to_disable or not reason:
                st.error("Email and reason are required.")
            else:
                with eng.begin() as conn:
                    try:
                        odb.disable_office_admin(conn, email_to_disable, reason, _current_user())
                        odb.log_audit(conn, {
                            "actor_email": _current_user(),
                            "action": "disable_office_admin",
                            "target_type": "office_admin",
                            "reason": reason
                        })
                        st.success(f"Account {email_to_disable} has been disabled.")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Failed: {e}")

    with st.expander("✅ Enable Account"):
        with st.form(key="enable_form"):
            email_to_enable = st.text_input("Email to Enable")
            submitted = st.form_submit_button("Enable Account")
        
        if submitted:
            if not email_to_enable:
                st.error("Email is required.")
            else:
                with eng.begin() as conn:
                    try:
                        odb.enable_office_admin(conn, email_to_enable)
                        odb.log_audit(conn, {
                            "actor_email": _current_user(),
                            "action": "enable_office_admin",
                            "target_type": "office_admin"
                        })
                        st.success(f"Account {email_to_enable} has been enabled.")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Failed: {e}")

# ---------- SCOPE MANAGEMENT ----------
def render_scopes():
    st.subheader("Scope Assignments")
    st.markdown("""
    **Organizational Scoping:**
    - **Global**: Access to all degrees, programs, and branches
    - **Degree**: Access to all programs/branches within a specific degree
    - **Program**: Access to all branches within a specific program  
    - **Branch**: Access to only that specific branch
    """)
    
    eng = _get_engine()
    if not eng:
        st.info("No engine configured.")
        return
    
    # Select admin to manage
    with eng.begin() as conn:
        admins = odb.list_office_admins(conn, "active")
    
    if not admins:
        st.warning("No active office admins found. Create an account first.")
        return
    
    admin_email = st.selectbox(
        "Select Office Admin",
        options=[a["email"] for a in admins],
        format_func=lambda e: f"{next((a['full_name'] for a in admins if a['email'] == e), e)} ({e})"
    )
    
    # Show current scopes
    with eng.begin() as conn:
        current_scopes = odb.get_admin_scopes(conn, admin_email)
    
    if current_scopes:
        st.write("**Current Scopes:**")
        scope_df = pd.DataFrame(current_scopes)
        display_cols = ["scope_type", "scope_value", "degree_code", "program_id", "branch_id", "created_at", "notes"]
        available_cols = [c for c in display_cols if c in scope_df.columns]
        st.dataframe(scope_df[available_cols], use_container_width=True)
        
        # Revoke scope
        with st.expander("❌ Revoke Scope"):
            scope_to_revoke = st.selectbox("Select Scope to Revoke", [s["id"] for s in current_scopes],
                                          format_func=lambda sid: next((f"{s['scope_type']}: {s['scope_value']}" for s in current_scopes if s['id'] == sid), str(sid)))
            if st.button("Revoke Selected Scope"):
                with eng.begin() as conn:
                    odb.revoke_scope(conn, scope_to_revoke)
                    odb.log_audit(conn, {
                        "actor_email": _current_user(),
                        "action": "revoke_scope",
                        "target_type": "office_admin_scope",
                        "target_id": scope_to_revoke
                    })
                st.success("Scope revoked.")
                st.rerun()
    else:
        st.info("No scopes assigned yet.")
    
    # Assign new scope
    with st.expander("➕ Assign New Scope"):
        with st.form(key="assign_scope_form"):
            scope_type = st.selectbox("Scope Type", ["global", "degree", "program", "branch"])
            
            scope_value = None
            degree_code = None
            program_id = None
            branch_id = None
            
            if scope_type == "global":
                st.info("Global scope grants access to all organizational units.")
            
            elif scope_type == "degree":
                with eng.begin() as conn:
                    degrees = _load_degrees(conn)
                if degrees:
                    selected_degree = st.selectbox("Select Degree", [d["code"] for d in degrees],
                                                   format_func=lambda c: next((d["name"] for d in degrees if d["code"] == c), c))
                    degree_code = selected_degree
                    scope_value = selected_degree
                else:
                    st.warning("No degrees available.")
            
            elif scope_type == "program":
                with eng.begin() as conn:
                    degrees = _load_degrees(conn)
                    if degrees:
                        degree_filter = st.selectbox("Filter by Degree", ["(all)"] + [d["code"] for d in degrees],
                                                     format_func=lambda c: next((d["name"] for d in degrees if d["code"] == c), c) if c != "(all)" else c)
                        programs = _load_programs(conn, None if degree_filter == "(all)" else degree_filter)
                        if programs:
                            selected_program = st.selectbox("Select Program", [p["id"] for p in programs],
                                                           format_func=lambda pid: next((p["name"] for p in programs if p["id"] == pid), str(pid)))
                            program_id = selected_program
                            scope_value = str(selected_program)
                            degree_code = next((p["degree_code"] for p in programs if p["id"] == selected_program), None)
                        else:
                            st.warning("No programs available.")
            
            elif scope_type == "branch":
                with eng.begin() as conn:
                    programs = _load_programs(conn)
                    if programs:
                        program_filter = st.selectbox("Filter by Program", [p["id"] for p in programs],
                                                     format_func=lambda pid: next((p["name"] for p in programs if p["id"] == pid), str(pid)))
                        branches = _load_branches(conn, program_filter)
                        if branches:
                            selected_branch = st.selectbox("Select Branch", [b["id"] for b in branches],
                                                          format_func=lambda bid: next((b["name"] for b in branches if b["id"] == bid), str(bid)))
                            branch_id = selected_branch
                            scope_value = str(selected_branch)
                            program_id = next((b["program_id"] for b in branches if b["id"] == selected_branch), None)
                            # Get degree from program
                            degree_code = next((p["degree_code"] for p in programs if p["id"] == program_id), None)
                        else:
                            st.warning("No branches available for this program.")
            
            notes = st.text_area("Notes (optional)")
            submitted = st.form_submit_button("Assign Scope")
        
        if submitted:
            with eng.begin() as conn:
                try:
                    scope_id = odb.assign_scope(conn, {
                        "admin_email": admin_email,
                        "scope_type": scope_type,
                        "scope_value": scope_value,
                        "degree_code": degree_code,
                        "program_id": program_id,
                        "branch_id": branch_id,
                        "notes": notes
                    }, created_by=_current_user())
                    
                    odb.log_audit(conn, {
                        "actor_email": _current_user(),
                        "action": "assign_scope",
                        "target_type": "office_admin_scope",
                        "target_id": scope_id,
                        "scope_type": scope_type,
                        "scope_value": scope_value
                    })
                    
                    st.success(f"Scope assigned: {scope_type} - {scope_value or 'global'}")
                    st.rerun()
                except Exception as e:
                    st.error(f"Failed to assign scope: {e}")

# ---------- PII ACCESS LOG ----------
def render_pii_access():
    st.subheader("PII Access Log")
    st.caption("Tracks when office admins unmask sensitive student information")
    
    eng = _get_engine()
    if not eng:
        st.info("No engine configured.")
        return
    
    with eng.begin() as conn:
        rows = odb.list_pii_access_log(conn, limit=100)
    
    if rows:
        df = pd.DataFrame(rows)
        st.dataframe(df, use_container_width=True)
    else:
        st.info("No PII access events logged yet.")

# ---------- EXPORT REQUESTS ----------
def render_export_requests():
    st.subheader("Data Export Requests")
    st.caption("Office admins can request exports (students roster, attendance, marks). Requires principal/director approval.")
    
    eng = _get_engine()
    if not eng:
        st.info("No engine configured.")
        return
    
    status_filter = st.selectbox("Filter by Status", ["(all)", "pending", "approved", "rejected", "completed"])
    
    with eng.begin() as conn:
        rows = odb.list_export_requests(conn, None if status_filter == "(all)" else status_filter)
    
    if rows:
        df = pd.DataFrame(rows)
        st.dataframe(df, use_container_width=True)
    else:
        st.info("No export requests.")
    
    with st.expander("📤 New Export Request"):
        with st.form(key="export_request_form"):
            entity_type = st.selectbox("Data Type", [
                "students_roster",
                "attendance_summary",
                "marks_summary"
            ])
            
            scope_type = st.selectbox("Scope", ["degree", "program", "branch"])
            
            # Load scope options based on type
            with eng.begin() as conn:
                if scope_type == "degree":
                    degrees = _load_degrees(conn)
                    if degrees:
                        scope_value = st.selectbox("Select Degree", [d["code"] for d in degrees],
                                                  format_func=lambda c: next((d["name"] for d in degrees if d["code"] == c), c))
                elif scope_type == "program":
                    programs = _load_programs(conn)
                    if programs:
                        scope_value = st.selectbox("Select Program", [p["id"] for p in programs],
                                                  format_func=lambda pid: next((p["name"] for p in programs if p["id"] == pid), str(pid)))
                        scope_value = str(scope_value)
                elif scope_type == "branch":
                    branches = _load_branches(conn)
                    if branches:
                        scope_value = st.selectbox("Select Branch", [b["id"] for b in branches],
                                                  format_func=lambda bid: next((b["name"] for b in branches if b["id"] == bid), str(bid)))
                        scope_value = str(scope_value)
            
            reason = st.text_area("Reason for Export*")
            submitted = st.form_submit_button("Submit Request")
        
        if submitted:
            if not reason:
                st.error("Reason is required.")
            else:
                with eng.begin() as conn:
                    request_code = odb.create_export_request(conn, {
                        "admin_email": _current_user(),
                        "entity_type": entity_type,
                        "scope_type": scope_type,
                        "scope_value": scope_value,
                        "reason": reason
                    })
                    st.success(f"Export request created: {request_code}")
                    st.info("Awaiting approval from principal or director.")
                    st.rerun()
    
    # Approval interface (for principals/directors)
    user_roles = st.session_state.get("user", {}).get("roles", set())
    if "principal" in user_roles or "director" in user_roles:
        with st.expander("✅ Approve/Reject Requests"):
            with st.form(key="approve_export_form"):
                request_code = st.text_input("Request Code")
                action = st.radio("Action", ["approve", "reject"])
                rejection_reason = st.text_input("Rejection Reason (if rejecting)")
                submitted = st.form_submit_button("Submit")
            
            if submitted:
                if not request_code:
                    st.error("Request code is required.")
                else:
                    with eng.begin() as conn:
                        if action == "approve":
                            odb.approve_export_request(conn, request_code, _current_user())
                            st.success(f"Request {request_code} approved.")
                        else:
                            if not rejection_reason:
                                st.error("Rejection reason is required.")
                            else:
                                odb.reject_export_request(conn, request_code, rejection_reason)
                                st.success(f"Request {request_code} rejected.")
                        st.rerun()

# ---------- AUDIT LOG ----------
def render_audit_log():
    st.subheader("Audit Trail")
    st.caption("All sensitive actions performed by office admins")
    
    eng = _get_engine()
    if not eng:
        st.info("No engine configured.")
        return
    
    with eng.begin() as conn:
        rows = odb.list_audit_log(conn, limit=200)
    
    if rows:
        df = pd.DataFrame(rows)
        st.dataframe(df, use_container_width=True)
    else:
        st.info("No audit events logged yet.")

# ---------- MAIN RENDER ----------
def render_office_admin():
    st.title("👥 Office Administration")
    st.markdown("""
    **Office Admins** can manage students and run reports within their assigned scope (degree/program/branch).
    
    - **Hierarchical Access**: Admins can be assigned global, degree, program, or branch-level access
    - **PII Protection**: PII is masked by default (requires step-up + approval to unmask)
    - **Export Approval**: Data exports require principal/director approval
    - **Full Audit Trail**: All sensitive actions are logged
    """)
    
    tabs = st.tabs(["Accounts", "Scope Assignments", "Export Requests", "PII Access Log", "Audit Trail"])
    
    with tabs[0]:
        render_accounts()
    
    with tabs[1]:
        render_scopes()
    
    with tabs[2]:
        render_export_requests()
    
    with tabs[3]:
        render_pii_access()
    
    with tabs[4]:
        render_audit_log()
