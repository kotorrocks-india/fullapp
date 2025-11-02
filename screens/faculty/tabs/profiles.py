# app/screens/faculty/tabs/profiles.py
from __future__ import annotations
from typing import Set, Dict, Any
import json
import random
import string

import streamlit as st
import pandas as pd
import bcrypt
from sqlalchemy import text as sa_text
from sqlalchemy.engine import Engine

from screens.faculty.utils import _handle_error
from screens.faculty.db import (
    _get_custom_profile_fields,
    _get_all_custom_field_data,
    _save_custom_field_value,
    _duplicate_candidates,
    _generate_faculty_username,
    _initial_faculty_password_from_name,
    _list_faculty_profiles_with_creds,
    _sync_academic_admins_to_faculty,
    _is_academic_admin,
)


# ----------------------------- helpers -----------------------------

def _check_faculty_has_children(conn, email: str) -> Dict[str, int]:
    children = {}

    count = conn.execute(sa_text(
        "SELECT COUNT(*) FROM faculty_affiliations WHERE lower(email)=lower(:e)"
    ), {"e": email}).scalar()
    children['affiliations'] = count or 0

    count = conn.execute(sa_text(
        "SELECT COUNT(*) FROM faculty_profile_custom_data WHERE lower(email)=lower(:e)"
    ), {"e": email}).scalar()
    children['custom_data'] = count or 0

    try:
        count = conn.execute(sa_text("""
            SELECT COUNT(*)
            FROM faculty_initial_credentials fic
            JOIN faculty_profiles fp ON fic.faculty_profile_id = fp.id
            WHERE lower(fp.email)=lower(:e)
        """), {"e": email}).scalar()
        children['credentials'] = count or 0
    except:
        children['credentials'] = 0

    return children


def _delete_faculty_profile(conn, email: str):
    # 1. custom data
    conn.execute(sa_text(
        "DELETE FROM faculty_profile_custom_data WHERE lower(email)=lower(:e)"
    ), {"e": email})
    # 2. credentials
    conn.execute(sa_text("""
        DELETE FROM faculty_initial_credentials 
        WHERE faculty_profile_id IN (SELECT id FROM faculty_profiles WHERE lower(email)=lower(:e))
    """), {"e": email})
    # 3. affiliations
    conn.execute(sa_text(
        "DELETE FROM faculty_affiliations WHERE lower(email)=lower(:e)"
    ), {"e": email})
    # 4. profile
    conn.execute(sa_text(
        "DELETE FROM faculty_profiles WHERE lower(email)=lower(:e)"
    ), {"e": email})


def _get_faculty_deletion_details(conn, email: str) -> Dict[str, Any]:
    details = {'affiliations': [], 'custom_data': [], 'credentials': None, 'profile': None}

    try:
        profile = conn.execute(sa_text(
            "SELECT name, email, phone, employee_id, status FROM faculty_profiles WHERE lower(email)=lower(:e)"
        ), {"e": email}).fetchone()
        if profile:
            details['profile'] = {
                'name': profile[0],
                'email': profile[1],
                'phone': profile[2] or 'N/A',
                'employee_id': profile[3] or 'N/A',
                'status': profile[4]
            }
    except Exception as e:
        st.warning(f"Could not load profile details: {e}")

    try:
        affiliations = conn.execute(sa_text(
            "SELECT * FROM faculty_affiliations WHERE lower(email)=lower(:e)"
        ), {"e": email}).fetchall()

        if affiliations:
            probe = conn.execute(sa_text(
                "SELECT * FROM faculty_affiliations WHERE lower(email)=lower(:e) LIMIT 1"
            ), {"e": email})
            columns = list(probe.keys()) if hasattr(probe, 'keys') else []
            for aff in affiliations:
                d = {}
                for idx, col in enumerate(columns):
                    if idx < len(aff):
                        d[col] = aff[idx] if aff[idx] is not None else 'N/A'
                details['affiliations'].append(d)
    except Exception:
        pass

    try:
        custom_data = conn.execute(sa_text(
            "SELECT field_name, field_value FROM faculty_profile_custom_data WHERE lower(email)=lower(:e)"
        ), {"e": email}).fetchall()
        for cd in custom_data:
            details['custom_data'].append({'field_name': cd[0], 'field_value': cd[1] or 'N/A'})
    except Exception:
        pass

    try:
        cred = conn.execute(sa_text("""
            SELECT fic.username FROM faculty_initial_credentials fic
            JOIN faculty_profiles fp ON fic.faculty_profile_id = fp.id
            WHERE lower(fp.email)=lower(:e)
        """), {"e": email}).fetchone()
        if cred:
            details['credentials'] = {'username': cred[0]}
    except:
        pass

    return details


def _log_faculty_deletion(conn, email: str, deletion_details: Dict[str, Any], deleted_by: str = "Admin"):
    conn.execute(sa_text("""
        CREATE TABLE IF NOT EXISTS faculty_deletion_audit (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            deleted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            deleted_by TEXT,
            faculty_name TEXT,
            faculty_email TEXT,
            profile_data TEXT,
            affiliations_count INTEGER,
            custom_data_count INTEGER,
            had_credentials BOOLEAN,
            full_details TEXT
        )
    """))
    profile = deletion_details.get('profile', {})
    affiliations = deletion_details.get('affiliations', [])
    custom_data = deletion_details.get('custom_data', [])
    credentials = deletion_details.get('credentials')

    conn.execute(sa_text("""
        INSERT INTO faculty_deletion_audit (
            deleted_by, faculty_name, faculty_email, 
            profile_data, affiliations_count, custom_data_count, 
            had_credentials, full_details
        ) VALUES (
            :deleted_by, :name, :email, 
            :profile_data, :aff_count, :custom_count, 
            :had_creds, :full_details
        )
    """), {
        "deleted_by": deleted_by,
        "name": profile.get('name', 'Unknown'),
        "email": email,
        "profile_data": json.dumps(profile),
        "aff_count": len(affiliations),
        "custom_count": len(custom_data),
        "had_creds": bool(credentials),
        "full_details": json.dumps(deletion_details)
    })


def _get_deletion_audit_trail(conn) -> list:
    try:
        rows = conn.execute(sa_text("""
            SELECT id, deleted_at, deleted_by, faculty_name, faculty_email,
                   affiliations_count, custom_data_count, had_credentials, full_details
            FROM faculty_deletion_audit
            ORDER BY deleted_at DESC
        """)).fetchall()
        out = []
        for r in rows:
            full = {}
            try:
                full = json.loads(r[8]) if r[8] else {}
            except:
                pass
            out.append({
                'id': r[0], 'deleted_at': r[1], 'deleted_by': r[2],
                'faculty_name': r[3], 'faculty_email': r[4],
                'affiliations_count': r[5], 'custom_data_count': r[6],
                'had_credentials': bool(r[7]), 'full_details': full
            })
        return out
    except Exception:
        return []


def _force_faculty_password_reset(engine: Engine, profile_id: int, full_name: str):
    if not profile_id or not full_name:
        st.error("Missing profile ID or name for password reset.")
        return
    try:
        with engine.begin() as conn:
            digits = "".join(random.choices(string.digits, k=4))
            new_password = _initial_faculty_password_from_name(full_name, digits)
            pw_hash = bcrypt.hashpw(new_password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")

            conn.execute(sa_text("""
                UPDATE faculty_profiles
                SET password_hash=:ph,
                    first_login_pending=1,
                    password_export_available=1,
                    updated_at=CURRENT_TIMESTAMP
                WHERE id=:pid
            """), {"ph": pw_hash, "pid": profile_id})

            row = conn.execute(sa_text("SELECT username FROM faculty_profiles WHERE id=:pid"),
                               {"pid": profile_id}).fetchone()
            username = (row[0] if row else None)
            if not username:
                row2 = conn.execute(sa_text("SELECT name FROM faculty_profiles WHERE id=:pid"),
                                    {"pid": profile_id}).fetchone()
                full_name = (row2[0] if row2 else full_name)
                username = _generate_faculty_username(conn, full_name)
                conn.execute(sa_text("UPDATE faculty_profiles SET username=:un WHERE id=:pid"),
                             {"un": username, "pid": profile_id})

            conn.execute(sa_text(
                "DELETE FROM faculty_initial_credentials WHERE faculty_profile_id=:pid"
            ), {"pid": profile_id})
            conn.execute(sa_text("""
                INSERT INTO faculty_initial_credentials(faculty_profile_id, username, plaintext)
                VALUES(:pid, :un, :pt)
            """), {"pid": profile_id, "un": username, "pt": new_password})

        st.success("Password reset and temporary credential created.")
        st.info(f"Temporary password: **{new_password}**")
        st.rerun()
    except Exception as e:
        _handle_error(e, f"Failed to reset password for profile ID {profile_id}.")


# ----------------------------- main -----------------------------

def render(engine: Engine, degree: str, roles: Set[str], can_edit: bool, key_prefix: str):
    st.subheader("Faculty Profiles")

    # Sync academic admins first (non-fatal)
    try:
        with engine.begin() as conn:
            _sync_academic_admins_to_faculty(conn)
    except Exception as e:
        st.warning(f"Note: Could not sync academic admins: {e}")

    st.info("""
    ℹ️ **Academic Admins:** Principal, Director, and other academic admins from User Roles 
    appear here. Their basic info is managed in User Roles, but custom fields can be edited here.
    """)

    # Filters Row
    colf1, colf2, colf3 = st.columns([2, 2, 2])
    with colf1:
        flt = st.selectbox("Filter", ["All", "First-login pending", "Academic Admins"], key=f"{key_prefix}_filter")
    with colf2:
        sort = st.selectbox("Sort by", ["name", "email"], key=f"{key_prefix}_sort")
    with colf3:
        srch = st.text_input("Search (name/email)", key=f"{key_prefix}_search")

    df = pd.DataFrame()
    active_fields: list[Dict[str, Any]] = []

    # Load current profiles + custom fields
    try:
        with engine.begin() as conn:
            faculty_list = _list_faculty_profiles_with_creds(conn)
            if faculty_list:
                df = pd.DataFrame(faculty_list)
                # unpack tuple -> bool
                df['admin_info'] = df['email'].apply(lambda e: _is_academic_admin(conn, e))
                df['is_academic_admin'] = df['admin_info'].apply(lambda x: x[0])

            custom_fields = _get_custom_profile_fields(conn)
            active_fields = [f for f in custom_fields if f.get('is_active')]
            custom_data_map = _get_all_custom_field_data(conn)

            if not df.empty and 'email' in df.columns:
                for field in active_fields:
                    fname = field['field_name']
                    dname = field['display_name']
                    df[dname] = df['email'].apply(lambda e: custom_data_map.get(str(e).lower(), {}).get(fname, ''))

    except Exception as e:
        _handle_error(e, "Could not load profiles.")
        if not isinstance(df, pd.DataFrame):
            df = pd.DataFrame()

    # Apply search/sort/filter
    display_df = df.copy()
    try:
        if not display_df.empty and srch:
            s = srch.lower().strip()
            display_df = display_df[display_df.apply(
                lambda r: s in str(r.get('name','')).lower() or s in str(r.get('email','')).lower(), axis=1)]
        if not display_df.empty and sort in display_df.columns:
            display_df = display_df.sort_values(by=sort)
        if not display_df.empty and flt == "First-login pending" and 'first_login_pending' in display_df.columns:
            display_df = display_df[display_df['first_login_pending'] == 1]
        if not display_df.empty and flt == "Academic Admins" and 'is_academic_admin' in display_df.columns:
            display_df = display_df[display_df['is_academic_admin'] == True]
    except Exception as e:
        _handle_error(e, "Could not apply filter/sort.")

    # Display grid
    if not display_df.empty:
        if 'is_academic_admin' in display_df.columns:
            display_df['Type'] = display_df['is_academic_admin'].apply(
                lambda x: '🏛️ Academic Admin' if x else 'Faculty'
            )
        base_cols = ["Type", "name", "email", "phone", "employee_id", "status", "username"]
        custom_display = [f['display_name'] for f in active_fields if f.get('display_name') in display_df.columns]
        cols = [c for c in (base_cols + custom_display) if c in display_df.columns]
        if cols:
            st.dataframe(display_df[cols], use_container_width=True, hide_index=True)
        else:
            st.info("No columns to display.")
    else:
        st.info("No faculty profiles found.")

    # ----- Custom Field Management -----
    if can_edit:
        st.divider()
        with st.expander("⚙️ Manage Custom Profile Fields"):
            st.markdown("**Add New Custom Field**")
            col1, col2, col3 = st.columns([2, 2, 1])
            with col1:
                new_field_display = st.text_input("Display Name",
                                                  key=f"{key_prefix}_new_field_display",
                                                  placeholder="e.g., Office Location")
            with col2:
                new_field_type = st.selectbox("Field Type",
                                              ["text", "number", "date", "boolean", "select"],
                                              key=f"{key_prefix}_new_field_type")
            new_field_options = None
            if new_field_type == "select":
                new_field_options = st.text_input(
                    "Options (comma-separated)",
                    key=f"{key_prefix}_new_field_options",
                    placeholder="e.g., Male, Female, Undisclosed"
                )
            with col3:
                st.write(""); st.write("")
                if st.button("Add Field", key=f"{key_prefix}_add_custom_field"):
                    if not new_field_display:
                        st.error("Display name is required")
                    else:
                        field_name = new_field_display.lower().replace(" ", "_")
                        field_name = "".join(c for c in field_name if c.isalnum() or c == "_")
                        try:
                            with engine.begin() as conn:
                                conn.execute(sa_text("""
                                    INSERT INTO faculty_profile_custom_fields(
                                        field_name, display_name, field_type, field_options, is_active
                                    )
                                    VALUES(:fn, :dn, :ft, :fo, 1)
                                """), {
                                    "fn": field_name, "dn": new_field_display,
                                    "ft": new_field_type,
                                    "fo": new_field_options if new_field_type == "select" else None
                                })
                            st.success(f"Custom field '{new_field_display}' added!")
                            st.rerun()
                        except Exception as e:
                            if "UNIQUE constraint failed" in str(e):
                                st.error("A field with this name already exists")
                            else:
                                _handle_error(e, "Failed to add custom field")

            st.markdown("---")
            st.markdown("**Existing Custom Fields**")
            try:
                with engine.begin() as conn:
                    existing_fields = _get_custom_profile_fields(conn)
                if existing_fields:
                    for field in existing_fields:
                        col1, col2, col3, col4 = st.columns([3, 2, 1, 1])
                        with col1:
                            st.text(field['display_name'])
                        with col2:
                            ftype_display = field['field_type']
                            if field['field_type'] == 'select' and field.get('field_options'):
                                ftype_display += f" ({field['field_options']})"
                            st.text(ftype_display)
                        with col3:
                            is_active = field.get('is_active', True)
                            st.text("✅ Active" if is_active else "❌ Inactive")
                        with col4:
                            toggle_key = f"{key_prefix}_toggle_{field['field_name']}"
                            if st.button("Deactivate" if is_active else "Activate", key=toggle_key):
                                try:
                                    with engine.begin() as conn:
                                        conn.execute(sa_text("""
                                            UPDATE faculty_profile_custom_fields
                                            SET is_active = :active, updated_at = CURRENT_TIMESTAMP
                                            WHERE field_name = :fn
                                        """), {"active": 0 if is_active else 1, "fn": field['field_name']})
                                    st.success("Field updated!")
                                    st.rerun()
                                except Exception as e:
                                    _handle_error(e, "Failed to toggle field")
                else:
                    st.info("No custom fields defined yet")
            except Exception as e:
                _handle_error(e, "Could not load custom fields")

    st.divider()

    # ----- Actions -----
    st.markdown("### 🎯 Actions")
    col1, col2 = st.columns(2)
    if can_edit:
        with col1:
            if st.button("➕ Create New Profile", type="primary", key=f"{key_prefix}_create_new", use_container_width=True):
                st.session_state[f"{key_prefix}_selected_profile"] = "New Profile"
                st.session_state[f"{key_prefix}_action_mode"] = "create"
                st.rerun()
        with col2:
            if not df.empty and st.button("✏️ Edit Existing Profile", type="secondary",
                                          key=f"{key_prefix}_edit_existing", use_container_width=True):
                st.session_state[f"{key_prefix}_action_mode"] = "edit"
                st.rerun()

    # ----- Selection / Editor gating (no early return; audit will always render) -----
    existing_email = None
    if st.session_state.get(f"{key_prefix}_action_mode") == "edit" and not df.empty:
        st.markdown("**Select Profile to Edit:**")
        regular_faculty = df[~df['is_academic_admin']]['email'].tolist() if 'is_academic_admin' in df.columns else []
        admin_faculty = df[df['is_academic_admin']]['email'].tolist() if 'is_academic_admin' in df.columns else []

        options = []
        if admin_faculty:
            options.append("--- Academic Admins (Edit Custom Fields Only) ---")
            options.extend(sorted(admin_faculty))
        if regular_faculty:
            options.append("--- Regular Faculty ---")
            options.extend(sorted(regular_faculty))

        if options:
            existing_email = st.selectbox("Select profile", options=options, key=f"{key_prefix}_select_profile")
            if existing_email.startswith("---"):
                st.info("Please select a profile from the list")
                existing_email = None
        else:
            st.info("No profiles available to edit")
            existing_email = None
    elif st.session_state.get(f"{key_prefix}_action_mode") == "create":
        existing_email = "New Profile"

    show_editor = existing_email is not None  # <— replaces the old early return

    # ----- Editor (wrapped) -----
    if show_editor:
        profile_id = None
        profile_data = {"name": "", "email": "", "phone": "", "employee_id": "", "status": "active"}
        is_academic_admin_flag = False

        if existing_email != "New Profile":
            try:
                with engine.begin() as conn:
                    row = conn.execute(sa_text(
                        "SELECT id, name, email, phone, employee_id, status FROM faculty_profiles WHERE lower(email)=lower(:e)"
                    ), {"e": existing_email}).fetchone()
                    if row:
                        profile_id = row[0]
                        profile_data = {"name": row[1], "email": row[2], "phone": row[3],
                                        "employee_id": row[4], "status": row[5]}
                    is_academic_admin_flag, _, _ = _is_academic_admin(conn, existing_email)
            except Exception as e:
                _handle_error(e, "Could not load profile details.")
                is_academic_admin_flag = False

        # Form fields
        if is_academic_admin_flag:
            st.info("🏛️ **Academic Admin Profile**")
            st.caption("Academic admin basic info is managed in **User Roles**. Only custom fields can be edited here.")
            st.text_input("Name (managed in User Roles)", value=profile_data.get('name', ''), disabled=True, key=f"{key_prefix}_name_ro")
            st.text_input("Email (managed in User Roles)", value=profile_data.get('email', ''), disabled=True, key=f"{key_prefix}_email_ro")
            st.text_input("Employee ID (managed in User Roles)", value=profile_data.get('employee_id', '') or 'Not set', disabled=True, key=f"{key_prefix}_empid_ro")
            name = profile_data.get('name', '')
            email = profile_data.get('email', '')
            phone = profile_data.get('phone', '')
            employee_id = profile_data.get('employee_id', '')
        else:
            c1, c2 = st.columns(2)
            with c1:
                name = st.text_input("Name *", value=profile_data.get('name', ''), key=f"{key_prefix}_name")
                phone = st.text_input("Phone", value=profile_data.get('phone', ''), key=f"{key_prefix}_phone")
            with c2:
                if existing_email == "New Profile":
                    email = st.text_input("Email *", value=profile_data.get('email', ''), key=f"{key_prefix}_email")
                else:
                    email = st.text_input("Email (cannot be changed)", value=profile_data.get('email', ''), disabled=True, key=f"{key_prefix}_email_ro")
                    email = profile_data.get('email', '')
                employee_id = st.text_input("Employee ID", value=profile_data.get('employee_id', ''), key=f"{key_prefix}_empid")

            if existing_email != "New Profile":
                st.markdown("**Status & Actions**")
                is_active = (profile_data.get('status') == 'active')
                new_active = st.checkbox("Active", value=is_active, key=f"{key_prefix}_active")
                if new_active != is_active:
                    if st.button("Update Status", key=f"{key_prefix}_status_btn"):
                        try:
                            with engine.begin() as conn:
                                conn.execute(sa_text("""
                                    UPDATE faculty_profiles
                                    SET status=:s, updated_at=CURRENT_TIMESTAMP
                                    WHERE id=:pid
                                """), {"s": ("active" if new_active else "deactivated"), "pid": profile_id})
                            st.success("Status updated.")
                            st.rerun()
                        except Exception as e:
                            _handle_error(e, "Failed to update status.")

                st.markdown("---")
                st.warning("Force Password Reset generates a new temporary password.")
                if st.button("Force Password Reset", key=f"{key_prefix}_reset_pw"):
                    _force_faculty_password_reset(engine, profile_id, profile_data.get('name', ''))

        # Custom fields editor
        st.markdown("**Custom Fields**")
        if is_academic_admin_flag:
            st.caption("✏️ Custom fields can be edited for academic admins")
        custom_values_for_save: Dict[str, Any] = {}

        try:
            with engine.begin() as conn:
                custom_fields = _get_custom_profile_fields(conn)
                active_fields = [f for f in custom_fields if f.get('is_active')]
                current_values = {}
                if existing_email != "New Profile" and profile_data.get('email'):
                    all_data = _get_all_custom_field_data(conn)
                    current_values = all_data.get(profile_data['email'].lower(), {})
        except Exception as e:
            _handle_error(e, "Failed to load custom fields.")
            active_fields, current_values = [], {}

        if active_fields:
            cols = st.columns(2)
            for idx, field in enumerate(active_fields):
                with cols[idx % 2]:
                    fkey = f"{key_prefix}_cf_{field['field_name']}_{existing_email}"
                    cur = current_values.get(field['field_name']) if current_values else None
                    ftype = field.get('field_type', 'text')

                    if ftype == 'select':
                        options_str = field.get('field_options', '')
                        options = [opt.strip() for opt in options_str.split(',') if opt.strip()]
                        if options:
                            select_options = [''] + options
                            current_idx = 0
                            if cur and cur in options:
                                current_idx = options.index(cur) + 1
                            val = st.selectbox(field['display_name'], options=select_options, index=current_idx, key=fkey)
                        else:
                            val = st.text_input(field['display_name'], value=cur or '', key=fkey)
                    elif ftype == 'boolean':
                        val = st.checkbox(field['display_name'], value=bool(int(cur)) if str(cur).isdigit() else bool(cur), key=fkey)
                    elif ftype == 'number':
                        try:
                            num = float(cur) if cur not in (None, '') else 0.0
                        except Exception:
                            num = 0.0
                        val = st.number_input(field['display_name'], value=num, key=fkey)
                    elif ftype == 'date':
                        dval = None
                        if cur:
                            try:
                                dval = pd.to_datetime(cur).date()
                            except Exception:
                                dval = None
                        val = st.date_input(field['display_name'], value=dval, key=fkey)
                    else:
                        val = st.text_input(field['display_name'], value=cur or '', key=fkey)
                    custom_values_for_save[field['field_name']] = val
        else:
            st.info("No active custom fields defined.")

        # Duplicate hint
        if not is_academic_admin_flag and (name or email):
            try:
                with engine.begin() as conn:
                    cand = _duplicate_candidates(conn, name, email)
                if cand:
                    st.info("Possible duplicates:")
                    st.dataframe(pd.DataFrame(cand, columns=["Name", "Email"]), use_container_width=True)
            except Exception:
                pass

        # Validation pre-checks
        validation_error = None
        if not is_academic_admin_flag and existing_email == "New Profile":
            if email:
                try:
                    with engine.begin() as conn:
                        existing = conn.execute(sa_text(
                            "SELECT name FROM faculty_profiles WHERE lower(email)=lower(:e)"
                        ), {"e": email}).fetchone()
                        if existing:
                            validation_error = f"❌ Email '{email}' is already registered to: {existing[0]}"
                except:
                    pass
            if employee_id and not validation_error:
                try:
                    with engine.begin() as conn:
                        existing = conn.execute(sa_text(
                            "SELECT name, email FROM faculty_profiles WHERE employee_id=:eid"
                        ), {"eid": employee_id}).fetchone()
                        if existing:
                            validation_error = f"❌ Employee ID '{employee_id}' is already in use by: {existing[0]} ({existing[1]})"
                except:
                    pass

        if validation_error:
            st.error(validation_error)
            st.info("💡 Please use a different value or leave it blank.")

        # Save / Cancel buttons (Cancel works for both create & edit)
        save_label = "💾 Save Custom Fields Only" if is_academic_admin_flag else "💾 Save Profile"
        save_disabled = bool(validation_error)
        col_save, col_cancel = st.columns([1, 1])
        with col_save:
            clicked_save = st.button(save_label, type="primary", key=f"{key_prefix}_save_profile", disabled=save_disabled)
        with col_cancel:
            clicked_cancel = st.button("❌ Cancel", type="secondary", key=f'{key_prefix}_cancel_profile')

        if clicked_cancel:
            if f"{key_prefix}_action_mode" in st.session_state:
                del st.session_state[f"{key_prefix}_action_mode"]
            if f"{key_prefix}_selected_profile" in st.session_state:
                del st.session_state[f"{key_prefix}_selected_profile"]
            # clear any confirm flags
            for k in list(st.session_state.keys()):
                if k.startswith("confirm_delete_"):
                    del st.session_state[k]
            st.rerun()
            return

        if clicked_save:
            if is_academic_admin_flag:
                if existing_email == "New Profile":
                    st.error("Cannot create new profiles for academic admins here. Create them in User Roles.")
                    return
                try:
                    with engine.begin() as conn:
                        target_email = profile_data.get('email', '').lower()
                        for fname, fval in custom_values_for_save.items():
                            sval = (str(fval) if fval is not None else None)
                            if sval is not None:
                                _save_custom_field_value(conn, target_email, fname, sval)
                    st.success("✅ Custom fields saved successfully!")
                    if f"{key_prefix}_action_mode" in st.session_state:
                        del st.session_state[f"{key_prefix}_action_mode"]
                    st.rerun()
                except Exception as ex:
                    if "unique constraint" in str(ex).lower():
                        st.error("❌ A unique constraint was violated. Please check your input.")
                    else:
                        _handle_error(ex, "Failed to save custom fields.")
                return

            if not name or not (email or existing_email != "New Profile"):
                st.error("Name and Email are required.")
                return

            try:
                with engine.begin() as conn:
                    target_email = (email or profile_data.get('email') or '').lower()
                    if existing_email == "New Profile":
                        username = _generate_faculty_username(conn, name)
                        digits = username[-4:] if username[-4:].isdigit() else "".join(random.choices(string.digits, k=4))
                        initial_password = _initial_faculty_password_from_name(name, digits)
                        pw_hash = bcrypt.hashpw(initial_password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")

                        res = conn.execute(sa_text("""
                            INSERT INTO faculty_profiles(name, email, phone, employee_id, status, username, password_hash, first_login_pending, password_export_available)
                            VALUES(:n, :e, :p, :emp, 'active', :un, :ph, 1, 1)
                            RETURNING id
                        """), {"n": name, "e": target_email, "p": (phone or None), "emp": (employee_id or None),
                               "un": username, "ph": pw_hash})
                        new_profile_id = res.scalar_one_or_none()

                        if new_profile_id:
                            try:
                                conn.execute(sa_text("""
                                    INSERT INTO faculty_initial_credentials(faculty_profile_id, username, plaintext)
                                    VALUES(:pid, :un, :pt)
                                """), {"pid": new_profile_id, "un": username, "pt": initial_password})
                            except Exception as cred_ex:
                                st.warning(f"Profile saved, but credentials not stored: {cred_ex}")
                                conn.execute(sa_text(
                                    "UPDATE faculty_profiles SET password_export_available=0 WHERE id=:pid"
                                ), {"pid": new_profile_id})

                            with st.expander("Show initial credentials (displayed once)"):
                                st.code(f"username: {username}\npassword: {initial_password}")
                        else:
                            st.error("Failed to create profile.")
                            return
                    else:
                        conn.execute(sa_text("""
                            UPDATE faculty_profiles
                            SET name=:n, phone=:p, employee_id=:emp, updated_at=CURRENT_TIMESTAMP
                            WHERE lower(email)=lower(:e)
                        """), {"n": name, "p": (phone or None), "emp": (employee_id or None),
                               "e": profile_data.get('email').lower()})
                        target_email = profile_data.get('email').lower()

                    # Save custom fields
                    for fname, fval in custom_values_for_save.items():
                        sval = (str(fval) if fval is not None else None)
                        if sval is not None:
                            _save_custom_field_value(conn, target_email, fname, sval)

                st.success("✅ Profile saved successfully!")
                if f"{key_prefix}_action_mode" in st.session_state:
                    del st.session_state[f"{key_prefix}_action_mode"]
                st.rerun()
            except Exception as ex:
                msg = str(ex).lower()
                if "unique constraint failed: faculty_profiles.employee_id" in msg:
                    try:
                        with engine.begin() as conn:
                            existing = conn.execute(sa_text(
                                "SELECT name, email FROM faculty_profiles WHERE employee_id=:eid"
                            ), {"eid": employee_id}).fetchone()
                            if existing:
                                st.error(f"❌ Employee ID '{employee_id}' is already in use by: **{existing[0]}** ({existing[1]})")
                            else:
                                st.error(f"❌ Employee ID '{employee_id}' is already in use.")
                    except:
                        st.error(f"❌ Employee ID '{employee_id}' is already in use.")
                    st.info("💡 **Solution:** Use a different Employee ID or leave it blank.")
                elif "unique constraint failed: faculty_profiles.email" in msg:
                    st.error(f"❌ Email '{email}' is already registered.")
                    st.info("💡 **Solution:** Use a different email address.")
                elif "unique constraint failed: faculty_profiles.username" in msg:
                    st.error("❌ Username conflict occurred. Please try again.")
                    st.info("💡 If this persists, contact your administrator.")
                else:
                    _handle_error(ex, "Failed to save profile.")

        # Delete (only for existing, non-admin)
        if existing_email != "New Profile" and not is_academic_admin_flag:
            st.markdown("---")
            st.markdown("### 🗑️ Remove Faculty Profile")
            st.caption("⚠️ **Warning:** This action cannot be undone and will permanently delete all associated data.")
            try:
                with engine.begin() as conn:
                    deletion_details = _get_faculty_deletion_details(conn, existing_email)
                    children = _check_faculty_has_children(conn, existing_email)
                    total_children = sum(children.values())
            except Exception as e:
                st.error(f"Failed to load deletion details: {e}")
                deletion_details = {}; children = {}; total_children = 0

            if deletion_details:
                with st.expander("📊 Preview: What Will Be Deleted", expanded=True):
                    st.warning("⚠️ The following data will be **permanently deleted**:")
                    if deletion_details.get('profile'):
                        st.markdown("**👤 Profile Information:**")
                        st.dataframe(pd.DataFrame([deletion_details['profile']]),
                                     use_container_width=True, hide_index=True)
                    if deletion_details.get('affiliations'):
                        st.markdown(f"**🏢 Affiliations ({len(deletion_details['affiliations'])}):**")
                        st.dataframe(pd.DataFrame(deletion_details['affiliations']),
                                     use_container_width=True, hide_index=True)
                    else:
                        st.info("No affiliations to delete")
                    if deletion_details.get('custom_data'):
                        st.markdown(f"**🎯 Custom Field Data ({len(deletion_details['custom_data'])}):**")
                        st.dataframe(pd.DataFrame(deletion_details['custom_data']),
                                     use_container_width=True, hide_index=True)
                    else:
                        st.info("No custom field data to delete")
                    if deletion_details.get('credentials'):
                        st.markdown("**🔐 Login Credentials:**")
                        st.dataframe(pd.DataFrame([deletion_details['credentials']]),
                                     use_container_width=True, hide_index=True)
                    else:
                        st.info("No credentials to delete")

            st.markdown("---")
            if total_children > 0:
                st.warning(f"⚠️ This profile has {total_children} related record(s)")
                st.success("✅ This profile can be deleted (all related data will be removed automatically)")
            else:
                st.success("✅ This profile has no related data and can be deleted directly")

            col1, col2, _ = st.columns([1, 1, 2])
            with col1:
                if st.button("🗑️ Delete Profile", type="secondary", key=f"{key_prefix}_delete_now"):
                    if f"confirm_delete_{existing_email}" not in st.session_state:
                        st.session_state[f"confirm_delete_{existing_email}"] = True
                        st.rerun()

            if f"confirm_delete_{existing_email}" in st.session_state:
                st.markdown("---")
                st.error(f"⚠️ **FINAL CONFIRMATION: Delete {profile_data.get('name')}?**")
                summary = []
                if deletion_details.get('profile'): summary.append("• Faculty profile")
                if deletion_details.get('credentials'): summary.append("• Login credentials")
                if deletion_details.get('affiliations'): summary.append(f"• {len(deletion_details['affiliations'])} affiliation(s)")
                if deletion_details.get('custom_data'): summary.append(f"• {len(deletion_details['custom_data'])} custom field value(s)")
                st.write("This will permanently remove:"); [st.write(x) for x in summary]

                c1, c2, _ = st.columns([1, 1, 2])
                with c1:
                    if st.button("✅ Yes, Delete Permanently", type="primary", key=f"{key_prefix}_confirm_yes"):
                        try:
                            with engine.begin() as conn:
                                _log_faculty_deletion(conn, existing_email, deletion_details, deleted_by="Admin")
                                _delete_faculty_profile(conn, existing_email)
                            st.success(f"✅ Profile for {profile_data.get('name')} has been deleted and logged to audit trail.")
                            del st.session_state[f"confirm_delete_{existing_email}"]
                            if f"{key_prefix}_action_mode" in st.session_state:
                                del st.session_state[f"{key_prefix}_action_mode"]
                            st.rerun()
                        except Exception as e:
                            st.error(f"Failed to delete profile: {e}")
                with c2:
                    if st.button("❌ Cancel", type="secondary", key=f"{key_prefix}_confirm_no"):
                        del st.session_state[f"confirm_delete_{existing_email}"]
                        st.rerun()

    # ----- Deletion Audit Trail (ALWAYS VISIBLE) -----
    st.divider()
    st.markdown("### 🗂️ Deletion Audit Trail")
    st.caption("Track all faculty profile deletions with complete details")

    try:
        with engine.begin() as conn:
            audit_records = _get_deletion_audit_trail(conn)
    except Exception as e:
        st.warning(f"Could not load deletion audit trail: {e}")
        audit_records = []

    if audit_records:
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Deletions", len(audit_records))
        with col2:
            recent = [r for r in audit_records if r['deleted_at']]
            st.metric("Records Available", len(recent))
        with col3:
            total_aff = sum(r.get('affiliations_count', 0) for r in audit_records)
            st.metric("Total Affiliations Deleted", total_aff)

        with st.expander("📋 View Detailed Deletion History", expanded=False):
            for idx, rec in enumerate(audit_records):
                st.markdown(f"#### 🗑️ Deletion #{rec['id']} - {rec['faculty_name']}")
                c1, c2, c3 = st.columns(3)
                with c1: st.write(f"**Email:** {rec['faculty_email']}")
                with c2: st.write(f"**Deleted By:** {rec['deleted_by']}")
                with c3: st.write(f"**Date:** {rec['deleted_at']}")
                c1, c2, c3 = st.columns(3)
                with c1: st.metric("Affiliations", rec.get('affiliations_count', 0))
                with c2: st.metric("Custom Fields", rec.get('custom_data_count', 0))
                with c3: st.write(f"**Had Credentials:** {'Yes' if rec.get('had_credentials') else 'No'}")

                if rec.get('full_details'):
                    with st.expander(f"📄 Full Details for {rec['faculty_name']}", expanded=False):
                        full = rec['full_details']
                        if full.get('profile'):
                            st.markdown("**Profile Information:**")
                            st.dataframe(pd.DataFrame([full['profile']]), use_container_width=True, hide_index=True)
                        if full.get('affiliations'):
                            st.markdown(f"**Affiliations ({len(full['affiliations'])}):**")
                            st.dataframe(pd.DataFrame(full['affiliations']), use_container_width=True, hide_index=True)
                        if full.get('custom_data'):
                            st.markdown(f"**Custom Field Data ({len(full['custom_data'])}):**")
                            st.dataframe(pd.DataFrame(full['custom_data']), use_container_width=True, hide_index=True)
                        if full.get('credentials'):
                            st.markdown("**Credentials:**")
                            st.dataframe(pd.DataFrame([full['credentials']]), use_container_width=True, hide_index=True)

                if idx < len(audit_records) - 1:
                    st.markdown("---")
    else:
        st.info("📭 No deletion records found. The audit trail will appear here when faculty profiles are deleted.")
