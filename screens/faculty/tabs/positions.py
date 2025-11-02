# app/screens/faculty/tabs/positions.py
from __future__ import annotations
from typing import Set, List, Tuple, Dict, Any

import streamlit as st
import pandas as pd
from sqlalchemy import text as sa_text
from sqlalchemy.engine import Engine
from datetime import date

from screens.faculty.utils import _handle_error
from screens.faculty.db import (
    _get_curriculum_groups_for_degree,
    _degree_has_branches,
    _degree_has_curriculum_groups,
)

# --- Schema Check Function ---
def _check_positions_schema(conn) -> tuple[bool, str | None]:
    """Checks if required tables and columns for positions exist."""
    try:
        # administrative_positions
        has_admin_pos = conn.execute(sa_text(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='administrative_positions'"
        )).fetchone()
        if not has_admin_pos:
            return False, "❌ Table `administrative_positions` is missing."
        admin_pos_info = conn.execute(sa_text("PRAGMA table_info(administrative_positions)")).fetchall()
        has_default_relief = any(col[1] == 'default_credit_relief' for col in admin_pos_info)  # noqa: F841

        # position_assignments
        has_pos_assign = conn.execute(sa_text(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='position_assignments'"
        )).fetchone()
        if not has_pos_assign:
            return False, "❌ Table `position_assignments` is missing."
        pos_assign_info = conn.execute(sa_text("PRAGMA table_info(position_assignments)")).fetchall()
        col_names = {c[1] for c in pos_assign_info}
        missing = []
        if 'credit_relief' not in col_names: missing.append('credit_relief')
        if 'group_code'   not in col_names: missing.append('group_code')
        if missing:
            return False, f"❌ Column(s) `{'`, `'.join(missing)}` missing from `position_assignments`."
        return True, "✅ Positions schema OK."
    except Exception as e:
        return False, f"❌ Error checking schema: {e}"

# --- Helper Queries ---
def _get_active_positions(conn) -> List[Tuple]:
    ap_cols = conn.execute(sa_text("PRAGMA table_info(administrative_positions)")).fetchall()
    has_default_relief = any(col[1] == 'default_credit_relief' for col in ap_cols)
    relief_sql = "COALESCE(default_credit_relief,0) AS default_credit_relief" if has_default_relief else "0 AS default_credit_relief"
    rows = conn.execute(sa_text(
        f"SELECT position_code, position_title, description, scope, is_active, {relief_sql} "
        "FROM administrative_positions ORDER BY is_active DESC, position_title"
    )).fetchall()
    return rows

def _get_position_assignments(conn, degree_code: str | None = None) -> List[Tuple]:
    pa_cols = conn.execute(sa_text("PRAGMA table_info(position_assignments)")).fetchall()
    has_program = any(col[1] == 'program_code' for col in pa_cols)
    has_group   = any(col[1] == 'group_code'   for col in pa_cols)
    program_sql = "pa.program_code" if has_program else "NULL AS program_code"
    group_sql   = "pa.group_code"   if has_group   else "NULL AS group_code"

    base = (
        "SELECT pa.id, pa.position_code, ap.position_title, pa.assignee_email, pa.assignee_type, "
        f"pa.degree_code, {program_sql}, pa.branch_code, {group_sql}, "
        "pa.start_date, pa.end_date, pa.is_active, pa.credit_relief, "
        "COALESCE(fp.name, u.full_name, pa.assignee_email) AS assignee_name "
        "FROM position_assignments pa "
        "JOIN administrative_positions ap ON ap.position_code = pa.position_code "
        "LEFT JOIN faculty_profiles fp ON lower(pa.assignee_email) = lower(fp.email) "
        "LEFT JOIN users u ON lower(pa.assignee_email) = lower(u.email) "
        "WHERE pa.is_active = 1 "
    )
    params = {}
    if degree_code:
        base += "AND (pa.degree_code = :d OR pa.degree_code IS NULL) "
        params["d"] = degree_code
    base += "ORDER BY ap.position_title, assignee_name"
    return conn.execute(sa_text(base), params).fetchall()

def _get_assignable_people(conn) -> List[Tuple[str, str, str]]:
    """
    Return (email, label, type) for assignable people.
    Excludes ALL tech admins and MR from the 'faculty' list.
    Allows principal/director as immutable admins.
    """
    people: list[tuple[str, str, str]] = []

    # Faculty (exclude academic admins & tech admins)
    faculty_rows = conn.execute(sa_text("""
        SELECT fp.email, fp.name
        FROM faculty_profiles fp
        WHERE fp.status = 'active'
          AND NOT EXISTS (
              SELECT 1 FROM academic_admins aa
              JOIN users u ON aa.user_id = u.id
              WHERE lower(u.email) = lower(fp.email)
                AND u.active = 1
          )
          AND NOT EXISTS (
              SELECT 1 FROM tech_admins ta
              JOIN users tu ON ta.user_id = tu.id
              WHERE lower(tu.email) = lower(fp.email)
                AND tu.active = 1
          )
        ORDER BY fp.name
    """)).fetchall()
    for email, name in faculty_rows:
        people.append((email, name, 'faculty'))

    # Immutable admins (principal/director) as "Academic Admin"
    immutable_rows = conn.execute(sa_text("""
        SELECT u.email, u.full_name, aa.designation
        FROM academic_admins aa
        JOIN users u ON aa.user_id = u.id
        WHERE aa.fixed_role IN ('principal','director')
          AND u.active = 1
        ORDER BY u.full_name
    """)).fetchall()
    for email, fullname, designation in immutable_rows:
        people.append((email, f"{fullname} [{designation or 'Academic Admin'}]", 'Academic Admin'))

    return people

def _get_degree_structure(conn, degree_code: str) -> Dict[str, Any] | None:
    try:
        cols = {c[1] for c in conn.execute(sa_text("PRAGMA table_info(degrees)")).fetchall()}
        needed = {'cohort_splitting_mode','cg_degree','cg_program','cg_branch'}
        if not needed.issubset(cols):
            st.error(f"DEBUG: Degrees table missing {needed - cols}")
            return None
        row = conn.execute(sa_text("""
            SELECT cohort_splitting_mode, cg_degree, cg_program, cg_branch
            FROM degrees WHERE lower(code)=lower(:d)
        """), {"d": degree_code}).fetchone()
        return dict(row._mapping) if row else None
    except Exception as e:
        st.error(f"Could not read structure for {degree_code}: {e}")
        return None

# --- Main ---
def render(engine: Engine, degree: str, roles: Set[str], can_edit: bool, key_prefix: str):
    st.subheader("Administrative Positions Management")

    try:
        with engine.connect() as conn:
            schema_ok, schema_msg = _check_positions_schema(conn)
    except Exception as e:
        schema_ok, schema_msg = False, f"❌ Error connecting: {e}"
    if not schema_ok:
        st.error(f"**Schema Problem:** {schema_msg}")
        st.warning("Install/patch schema (administrative_positions, position_assignments) and retry.")
        return

    tab1, tab2 = st.tabs(["📋 Current Assignments", "⚙️ Manage Position Types"])

    # === TAB 1: Current Assignments ===
    with tab1:
        st.markdown("### Current Position Assignments")
        show_all = st.checkbox("Show all degrees", value=False, key=f"{key_prefix}_show_all")
        filter_degree = None if show_all else degree

        try:
            with engine.begin() as conn:
                rows = _get_position_assignments(conn, filter_degree)
        except Exception as e:
            _handle_error(e, "Could not load assignments")
            return

        if rows:
            df = pd.DataFrame(rows, columns=[
                "ID","Position Code","Position","Email","Type","Degree",
                "Program","Branch","Group","Start Date","End Date","Active",
                "Credit Relief","Name"
            ])

            display = df[["Name","Email","Position","Degree","Program","Branch","Group","Type","Credit Relief"]].copy()
            def _scope(r):
                if r["Program"]: return f"{r['Degree']}/{r['Program']}"
                if r["Branch"] : return f"{r['Degree']}/{r['Branch']}"
                if r["Group"]  : return f"{r['Degree']}/{r['Group']}"
                return r["Degree"] or "Institution"
            display["Scope"] = df.apply(_scope, axis=1)
            display["Type"] = display["Type"].apply(lambda x:
                "🔒 Academic Admin" if x == "Academic Admin" else ("👤 Faculty" if x == "faculty" else x)
            )
            st.dataframe(
                display[["Name","Email","Position","Scope","Type","Credit Relief"]],
                use_container_width=True, hide_index=True
            )

            if can_edit:
                st.divider()
                st.markdown("#### Remove Assignment")
                options = {}
                for _, r in df.iterrows():
                    scope_str = (_scope(r))
                    label = f"{r['Name']} - {r['Position']} ({scope_str})"
                    options[label] = r['ID']
                choice = st.selectbox("Select assignment to remove", options=[""] + sorted(options.keys()),
                                      key=f"{key_prefix}_rm_sel")
                if choice and st.button("🗑️ Remove Assignment", key=f"{key_prefix}_rm_btn"):
                    try:
                        with engine.begin() as conn:
                            conn.execute(sa_text("""
                                UPDATE position_assignments
                                SET is_active = 0, updated_at = CURRENT_TIMESTAMP
                                WHERE id = :id
                            """), {"id": options[choice]})
                        st.success("✅ Assignment removed")
                        st.rerun()
                    except Exception as e:
                        _handle_error(e, "Failed to remove assignment")
        else:
            st.info("No active position assignments for the selected scope.")

        # Add new assignment
        if not can_edit:
            st.caption("🔒 View-only")
        else:
            st.divider()
            st.markdown("### ➕ Add New Assignment")

            try:
                with engine.begin() as conn:
                    positions = conn.execute(sa_text("""
                        SELECT position_code, position_title, description, scope, is_active,
                               COALESCE(default_credit_relief,0) AS default_credit_relief
                        FROM administrative_positions WHERE is_active=1 ORDER BY position_title
                    """)).fetchall()
                    people = _get_assignable_people(conn)
                    deg_rows = conn.execute(sa_text(
                        "SELECT code FROM degrees WHERE active=1 ORDER BY code"
                    )).fetchall()
                    deg_codes = [r[0] for r in deg_rows]
                    deg_struct = {d: _get_degree_structure(conn, d) for d in deg_codes}
            except Exception as e:
                _handle_error(e, "Could not load reference data")
                return

            if not positions:
                st.warning("No active position types defined.")
                return
            if not people:
                st.warning("No eligible people to assign.")
                return

            c1, c2 = st.columns(2)
            selected_position = None
            with c1:
                label_map = {
                    "institution": "Institution",
                    "degree": "Degree",
                    "program": "Program",
                    "branch": "Branch",
                    "curriculum_group": "Group",
                }
                pos_map = {f"{p.position_title} ({label_map.get(p.scope,p.scope)})": p for p in positions}
                pos_label = st.selectbox("Position Type *", [""] + list(pos_map.keys()),
                                         key=f"{key_prefix}_new_pos")
                selected_position = pos_map.get(pos_label)

            with c2:
                ppl = {f"{nm} ({typ})": (em, typ) for em, nm, typ in people}
                person_label = st.selectbox("Assign To *", [""] + list(ppl.keys()),
                                            key=f"{key_prefix}_new_person")
                selected_person = ppl.get(person_label, (None, None))

            assign_degree = assign_program = assign_branch = assign_group = None
            scope_ok = True

            c3, c4 = st.columns(2)
            with c3:
                if selected_position and selected_position.scope in ("degree","program","branch","curriculum_group"):
                    if not deg_codes:
                        st.error("No active degrees found.")
                        scope_ok = False
                    else:
                        assign_degree = st.selectbox("Degree *", deg_codes,
                                                     index=max(deg_codes.index(degree),0) if degree in deg_codes else 0,
                                                     key=f"{key_prefix}_new_deg")
                elif selected_position and selected_position.scope == "institution":
                    st.caption("Institution-wide position")

            with c4:
                if selected_position and assign_degree:
                    ds = deg_struct.get(assign_degree)
                    if ds is None:
                        st.error(f"Structure missing for {assign_degree}")
                        scope_ok = False
                    else:
                        if selected_position.scope == "branch":
                            uses = ds.get("cohort_splitting_mode") in ("both","branch_only","program_or_branch")
                            if uses:
                                try:
                                    with engine.begin() as conn:
                                        branches = conn.execute(sa_text("""
                                            SELECT branch_code, branch_name
                                            FROM branches WHERE degree_code=:d AND active=1
                                            ORDER BY branch_code
                                        """), {"d": assign_degree}).fetchall()
                                    if branches:
                                        opt = {f"{b.branch_name} ({b.branch_code})": b.branch_code for b in branches}
                                        sel = st.selectbox("Branch *", [""] + list(opt.keys()),
                                                           key=f"{key_prefix}_new_branch")
                                        assign_branch = opt.get(sel)
                                    else:
                                        st.warning(f"No active branches for {assign_degree}")
                                        scope_ok = False
                                except Exception as e:
                                    _handle_error(e, "Could not load branches")
                                    scope_ok = False
                            else:
                                st.error(f"{assign_degree} does not use Branches.")
                                scope_ok = False

                        elif selected_position.scope == "program":
                            uses = ds.get("cohort_splitting_mode") in ("both","program_only","program_or_branch")
                            if uses:
                                try:
                                    with engine.begin() as conn:
                                        prows = conn.execute(sa_text("""
                                            SELECT program_code, program_name
                                            FROM programs WHERE degree_code=:d AND active=1
                                            ORDER BY program_code
                                        """), {"d": assign_degree}).fetchall()
                                    if prows:
                                        opt = {f"{p.program_name} ({p.program_code})": p.program_code for p in prows}
                                        sel = st.selectbox("Program *", [""] + list(opt.keys()),
                                                           key=f"{key_prefix}_new_prog")
                                        assign_program = opt.get(sel)
                                    else:
                                        st.warning(f"No active programs for {assign_degree}")
                                        scope_ok = False
                                except Exception as e:
                                    _handle_error(e, "Could not load programs")
                                    scope_ok = False
                            else:
                                st.error(f"{assign_degree} does not use Programs.")
                                scope_ok = False

                        elif selected_position.scope == "curriculum_group":
                            uses = any(ds.get(f"cg_{lvl}") for lvl in ("degree","program","branch"))
                            if uses:
                                try:
                                    with engine.begin() as conn:
                                        has_groups = _degree_has_curriculum_groups(conn, assign_degree)
                                    if has_groups:
                                        groups = _get_curriculum_groups_for_degree(conn, assign_degree)
                                        opt = {f"{g['group_name']} ({g['group_code']})": g['group_code'] for g in groups}
                                        sel = st.selectbox("Curriculum Group *", [""] + list(opt.keys()),
                                                           key=f"{key_prefix}_new_group")
                                        assign_group = opt.get(sel)
                                    else:
                                        st.warning(f"No active groups for {assign_degree}")
                                        scope_ok = False
                                except Exception as e:
                                    _handle_error(e, "Could not load groups")
                                    scope_ok = False

            c5, c6 = st.columns(2)
            start_date = c5.date_input("Start Date", value=date.today(), key=f"{key_prefix}_start")
            if c6.checkbox("Set End Date", key=f"{key_prefix}_use_end"):
                end_date = c6.date_input("End Date", min_value=start_date, value=start_date,
                                         key=f"{key_prefix}_end")
            else:
                end_date = None

            if st.button("➕ Add Assignment", type="primary",
                         key=f"{key_prefix}_add_assignment", disabled=not scope_ok):
                errors = []
                if not selected_position or not selected_person or not selected_person[0]:
                    errors.append("Position Type and Assign To are required.")
                if selected_position and selected_position.scope in ("degree","program","branch","curriculum_group") and not assign_degree:
                    errors.append("Degree is required for this scope.")
                if selected_position and selected_position.scope == "program" and not assign_program:
                    errors.append("Program is required.")
                if selected_position and selected_position.scope == "branch" and not assign_branch:
                    errors.append("Branch is required.")
                if selected_position and selected_position.scope == "curriculum_group" and not assign_group:
                    errors.append("Curriculum Group is required.")
                if end_date and end_date < start_date:
                    errors.append("End date cannot be before start date.")
                if errors:
                    for e in errors: st.error(e)
                else:
                    try:
                        with engine.begin() as conn:
                            relief = int(getattr(selected_position, "default_credit_relief", 0) or 0)
                            conn.execute(sa_text("""
                                INSERT INTO position_assignments(
                                    position_code, assignee_email, assignee_type,
                                    degree_code, program_code, branch_code, group_code,
                                    start_date, end_date, credit_relief, is_active
                                )
                                VALUES(:pos, :email, :type, :deg, :prog, :branch, :grp,
                                       :start, :end, :relief, 1)
                            """), {
                                "pos": selected_position.position_code,
                                "email": selected_person[0],
                                "type": selected_person[1],
                                "deg": assign_degree,
                                "prog": assign_program,
                                "branch": assign_branch,
                                "grp": assign_group,
                                "start": start_date.isoformat() if start_date else None,
                                "end": end_date.isoformat() if end_date else None,
                                "relief": relief,
                            })
                        st.success("✅ Assignment created")
                        st.rerun()
                    except Exception as e:
                        _handle_error(e, "Failed to create assignment")

    # === TAB 2: Manage Position Types ===
    with tab2:
        st.markdown("### Position Types")
        try:
            with engine.begin() as conn:
                rows = _get_active_positions(conn)
        except Exception as e:
            _handle_error(e, "Could not load position types")
            return

        if rows:
            df = pd.DataFrame([(p[0], p[1], p[2], p[3], p[5], p[4]) for p in rows],
                              columns=["Code","Title","Description","Scope","Default Relief","Active"])
            df["Active"] = df["Active"].apply(lambda x: "✅ Yes" if x else "❌ No")
            st.dataframe(df, use_container_width=True, hide_index=True)
        else:
            st.info("No position types defined yet.")

        if not can_edit:
            st.caption("🔒 View-only")
            return

        st.divider()
        st.markdown("### ➕ Add / Edit Position Type")

        pos_dict = {p[0]: p for p in rows}
        edit_choices = ["✨ Create New Position Type"] + sorted([f"{p[1]} ({p[0]})" for p in rows])
        sel = st.selectbox("Select to edit or create new", edit_choices, key=f"{key_prefix}_edit_sel")
        curr = None
        if sel != "✨ Create New Position Type":
            code = sel[sel.rfind("(")+1:-1]
            curr = pos_dict.get(code)

        curr_code  = curr[0] if curr else ""
        curr_title = curr[1] if curr else ""
        curr_desc  = curr[2] if curr else ""
        curr_scope = curr[3] if curr else "institution"
        curr_rel   = int(curr[5]) if curr else 0
        curr_act   = bool(curr[4]) if curr else True

        c1, c2 = st.columns(2)
        with c1:
            # Allow code edit via a guarded toggle (safe rename)
            allow_code_edit = st.checkbox("Advanced: edit position code", value=False,
                                          key=f"{key_prefix}_edit_code_toggle",
                                          help="Renaming updates all existing assignments too.")
            pos_code = st.text_input("Position Code *",
                                     value=curr_code, placeholder="e.g., dean, hod",
                                     key=f"{key_prefix}_pos_code").lower().strip()
            if curr and not allow_code_edit:
                st.caption("Code is locked. Enable the toggle to rename safely.")
            scope_opts = ["institution","degree","program","branch","curriculum_group"]
            pos_scope = st.selectbox("Scope *", options=scope_opts,
                                     index=scope_opts.index(curr_scope) if curr_scope in scope_opts else 0,
                                     key=f"{key_prefix}_pos_scope")
        with c2:
            pos_title = st.text_input("Position Title *", value=curr_title, key=f"{key_prefix}_pos_title")
            pos_relief = st.number_input("Default Credit Relief", value=int(curr_rel),
                                         min_value=0, max_value=20, step=1, key=f"{key_prefix}_pos_relief")
        pos_desc = st.text_area("Description", value=curr_desc, key=f"{key_prefix}_pos_desc")

        is_new = curr is None
        btn_lbl = "➕ Create" if is_new else "💾 Update"
        if st.button(btn_lbl, type="primary", key=f"{key_prefix}_save_pos"):
            if not pos_code or not pos_title:
                st.error("Code and Title are required.")
            elif is_new and not pos_code.replace("_","").isalnum():
                st.error("Code must be alphanumeric/underscores.")
            else:
                try:
                    with engine.begin() as conn:
                        ap_cols = {c[1] for c in conn.execute(sa_text("PRAGMA table_info(administrative_positions)")).fetchall()}
                        has_rel_col = "default_credit_relief" in ap_cols

                        if is_new:
                            conn.execute(sa_text(f"""
                                INSERT INTO administrative_positions
                                    (position_code, position_title, description, scope,
                                     {"default_credit_relief," if has_rel_col else ""} is_active)
                                VALUES (:c,:t,:d,:s, {" :r," if has_rel_col else ""} 1)
                            """), {"c": pos_code, "t": pos_title, "d": pos_desc or None,
                                   "s": pos_scope, **({"r": pos_relief} if has_rel_col else {})})
                            st.success("✅ Created")
                        else:
                            # If user renamed code, propagate to assignments + row
                            if allow_code_edit and pos_code != curr_code:
                                # ensure target code not already present
                                exists = conn.execute(sa_text(
                                    "SELECT 1 FROM administrative_positions WHERE lower(position_code)=lower(:c)"
                                ), {"c": pos_code}).fetchone()
                                if exists:
                                    st.error(f"Code '{pos_code}' already exists.")
                                else:
                                    conn.execute(sa_text("""
                                        UPDATE position_assignments
                                        SET position_code = :new, updated_at = CURRENT_TIMESTAMP
                                        WHERE lower(position_code)=lower(:old)
                                    """), {"new": pos_code, "old": curr_code})
                                    conn.execute(sa_text("""
                                        UPDATE administrative_positions
                                        SET position_code = :new
                                        WHERE lower(position_code)=lower(:old)
                                    """), {"new": pos_code, "old": curr_code})
                                    curr_code = pos_code  # continue updating other fields on new code

                            conn.execute(sa_text(f"""
                                UPDATE administrative_positions
                                SET position_title=:t, description=:d, scope=:s,
                                    {"default_credit_relief=:r," if has_rel_col else ""} updated_at=CURRENT_TIMESTAMP
                                WHERE lower(position_code)=lower(:c)
                            """), {"c": curr_code, "t": pos_title, "d": pos_desc or None,
                                   "s": pos_scope, **({"r": pos_relief} if has_rel_col else {})})

                            # Sync default relief to *active* assignments of this position
                            if has_rel_col:
                                conn.execute(sa_text("""
                                    UPDATE position_assignments AS pa
                                    SET credit_relief = ap.default_credit_relief,
                                        updated_at = CURRENT_TIMESTAMP
                                    FROM administrative_positions AS ap
                                    WHERE lower(pa.position_code)=lower(ap.position_code)
                                      AND lower(ap.position_code)=lower(:c)
                                      AND pa.is_active=1
                                """), {"c": curr_code})
                            st.success("✅ Updated & synced relief")

                    st.rerun()
                except Exception as e:
                    _handle_error(e, "Failed to save position type")

        # Activate / Deactivate / Delete
        if curr:
            st.markdown("---")
            left, mid, right = st.columns([1,1,1])
            with left:
                if st.button(("🔴 Deactivate" if curr_act else "🟢 Activate"),
                             key=f"{key_prefix}_toggle_active"):
                    try:
                        with engine.begin() as conn:
                            conn.execute(sa_text("""
                                UPDATE administrative_positions
                                SET is_active=:a, updated_at=CURRENT_TIMESTAMP
                                WHERE lower(position_code)=lower(:c)
                            """), {"a": 0 if curr_act else 1, "c": curr_code})
                        st.rerun()
                    except Exception as e:
                        _handle_error(e, "Failed to toggle active")
            with mid:
                st.caption("Deactivate to prevent new assignments.")
            with right:
                can_delete = st.checkbox("Allow delete (no assignments exist)", key=f"{key_prefix}_allow_del")
                if st.button("🗑️ Delete Position Type", disabled=not can_delete, key=f"{key_prefix}_del_btn"):
                    try:
                        with engine.begin() as conn:
                            exists = conn.execute(sa_text(
                                "SELECT 1 FROM position_assignments WHERE lower(position_code)=lower(:c) AND is_active=1"
                            ), {"c": curr_code}).fetchone()
                            if exists:
                                st.error("Cannot delete: active assignments exist. Deactivate instead.")
                            else:
                                conn.execute(sa_text(
                                    "DELETE FROM administrative_positions WHERE lower(position_code)=lower(:c)"
                                ), {"c": curr_code})
                                st.success("✅ Deleted")
                                st.rerun()
                    except Exception as e:
                        _handle_error(e, "Failed to delete")
