from __future__ import annotations

import pandas as pd
import streamlit as st
from sqlalchemy import text as sa_text

from core.settings import load_settings
from core.db import get_engine, init_db
from core.policy import require_page, can_edit_page, user_roles
from core.ui import render_footer_global

# ─────────────────────────── DB helpers ───────────────────────────

def _fetch_degree(conn, degree_code: str):
    # Added cg flags to the query to respect degree settings
    return conn.execute(sa_text("""
        SELECT code,
               title,
               cohort_splitting_mode,
               roll_number_scope,
               active,
               sort_order,
               logo_file_name,
               cg_degree,
               cg_program,
               cg_branch
          FROM degrees
         WHERE code = :c
    """), {"c": degree_code}).fetchone()

def _degrees_df(conn):
    rows = conn.execute(sa_text("""
        SELECT code, title, cohort_splitting_mode, roll_number_scope, active, sort_order, logo_file_name
          FROM degrees
         ORDER BY sort_order, code
    """)).fetchall()
    cols = ["code","title","cohort_splitting_mode","roll_number_scope","active","sort_order","logo_file_name"]
    if not rows:
        return pd.DataFrame(columns=cols)
    return pd.DataFrame([dict(r._mapping) for r in rows], columns=cols)

def _programs_df(conn, degree_filter: str | None = None):
    cols = ["id","program_code","program_name","degree_code","active","sort_order","logo_file_name","description"]
    q = f"SELECT {', '.join(cols)} FROM programs"
    params = {}
    if degree_filter:
        q += " WHERE degree_code=:d"
        params["d"] = degree_filter
    q += " ORDER BY degree_code, sort_order, lower(program_code)"
    rows = conn.execute(sa_text(q), params).fetchall()
    if not rows:
        return pd.DataFrame(columns=cols)
    return pd.DataFrame([dict(r._mapping) for r in rows], columns=cols)

def _table_cols(conn, table: str) -> set[str]:
    try:
        return {c[1] for c in conn.execute(sa_text(f"PRAGMA table_info({table})")).fetchall()}
    except:
        return set()

def _branches_df(conn, degree_filter: str | None = None, program_id: int | None = None):
    """List branches; supports schemas with or without degree_code on branches."""
    bcols = _table_cols(conn, "branches")
    has_pid = "program_id" in bcols
    has_deg = "degree_code" in bcols

    params = {}
    if has_pid:
        wh = []
        if program_id:
            wh.append("b.program_id=:pid"); params["pid"] = program_id
        if degree_filter:
            wh.append("p.degree_code=:deg"); params["deg"] = degree_filter
        where = (" WHERE " + " AND ".join(wh)) if wh else ""
        rows = conn.execute(sa_text(f"""
            SELECT b.id, b.branch_code, b.branch_name, p.program_code, p.degree_code,
                   b.active, b.sort_order, b.logo_file_name, b.description
              FROM branches b
              LEFT JOIN programs p ON p.id=b.program_id
              {where}
             ORDER BY p.degree_code, p.program_code, b.sort_order, lower(b.branch_code)
        """), params).fetchall()
        cols = ["id","branch_code","branch_name","program_code","degree_code",
                "active","sort_order","logo_file_name","description"]
    elif has_deg:
        wh = []
        if degree_filter:
            wh.append("degree_code=:deg"); params["deg"] = degree_filter
        where = (" WHERE " + " AND ".join(wh)) if wh else ""
        rows = conn.execute(sa_text(f"""
            SELECT id, branch_code, branch_name, degree_code,
                   active, sort_order, logo_file_name, description
              FROM branches
              {where}
             ORDER BY degree_code, sort_order, lower(branch_code)
        """), params).fetchall()
        cols = ["id","branch_code","branch_name","degree_code",
                "active","sort_order","logo_file_name","description"]
    else:
        return pd.DataFrame(columns=["id","branch_code","branch_name","active","sort_order","logo_file_name","description"])

    if not rows:
        return pd.DataFrame(columns=cols)
    return pd.DataFrame([dict(r._mapping) for r in rows], columns=cols)

def _program_id_by_code(conn, degree_code: str, program_code: str) -> int | None:
    row = conn.execute(sa_text("""
        SELECT id FROM programs
         WHERE degree_code=:d AND lower(program_code)=lower(:pc)
         LIMIT 1
    """), {"d": degree_code, "pc": program_code}).fetchone()
    return int(row.id) if row else None

# DB helpers for Curriculum Groups
def _curriculum_groups_df(conn, degree_filter: str):
    rows = conn.execute(sa_text("""
        SELECT id, group_code, group_name, kind, active, sort_order, description
          FROM curriculum_groups
         WHERE degree_code=:d
         ORDER BY sort_order, group_code
    """), {"d": degree_filter}).fetchall()
    return pd.DataFrame([dict(r._mapping) for r in rows]) if rows else pd.DataFrame()

def _curriculum_group_links_df(conn, degree_filter: str):
    rows = conn.execute(sa_text("""
        SELECT cgl.id, cg.group_code, cgl.program_code, cgl.branch_code
          FROM curriculum_group_links cgl
          JOIN curriculum_groups cg ON cg.id = cgl.group_id
         WHERE cgl.degree_code = :d
         ORDER BY cg.group_code, cgl.program_code, cgl.branch_code
    """), {"d": degree_filter}).fetchall()
    return pd.DataFrame([dict(r._mapping) for r in rows]) if rows else pd.DataFrame()

# helpers for semester structure map
def _get_semester_binding(conn, degree_code: str) -> str | None:
    row = conn.execute(sa_text("SELECT binding_mode FROM semester_binding WHERE degree_code=:dc"), {"dc": degree_code}).fetchone()
    return row.binding_mode if row else None

def _get_degree_struct(conn, degree_code: str) -> tuple | None:
    row = conn.execute(sa_text("SELECT years, terms_per_year FROM degree_semester_struct WHERE degree_code=:k"), {"k": degree_code}).fetchone()
    return (row.years, row.terms_per_year) if row else None

def _get_program_structs_for_degree(conn, degree_code: str) -> dict:
    rows = conn.execute(sa_text("""
        SELECT p.program_code, s.years, s.terms_per_year
        FROM programs p
        JOIN program_semester_struct s ON p.id = s.program_id
        WHERE p.degree_code = :dc
    """), {"dc": degree_code}).fetchall()
    return {r.program_code: (r.years, r.terms_per_year) for r in rows}

def _get_branch_structs_for_degree(conn, degree_code: str) -> dict:
    q = """
        SELECT b.branch_code, s.years, s.terms_per_year
        FROM branches b
        JOIN branch_semester_struct s ON b.id = s.branch_id
    """
    if 'degree_code' in _table_cols(conn, 'branches'):
        q += " WHERE b.degree_code = :dc"
    else:
        q += " JOIN programs p ON p.id = b.program_id WHERE p.degree_code = :dc"

    rows = conn.execute(sa_text(q), {"dc": degree_code}).fetchall()
    return {r.branch_code: (r.years, r.terms_per_year) for r in rows}


# ───────────────── schema-aware audits / approvals ─────────────────

def _audit_program(conn, action: str, actor: str, row: dict, note: str = ""):
    cols = _table_cols(conn, "programs_audit")
    payload = { "action": action, "actor": actor, "note": note, **row }
    fields = [k for k in payload.keys() if k in cols]
    params = {k: payload[k] for k in fields}
    conn.execute(sa_text(
        f"INSERT INTO programs_audit({', '.join(fields)}) VALUES({', '.join(':'+f for f in fields)})"
    ), params)

def _audit_branch(conn, action: str, actor: str, row: dict, note: str = ""):
    cols = _table_cols(conn, "branches_audit")
    payload = { "action": action, "actor": actor, "note": note, **row }
    fields = [k for k in payload.keys() if k in cols]
    params = {k: payload[k] for k in fields}
    conn.execute(sa_text(
        f"INSERT INTO branches_audit({', '.join(fields)}) VALUES({', '.join(':'+f for f in fields)})"
    ), params)

def _audit_curriculum_group(conn, action: str, actor: str, row: dict, note: str = ""):
    cols = _table_cols(conn, "curriculum_groups_audit")
    payload = { "action": action, "actor": actor, "note": note, **row }
    fields = [k for k in payload.keys() if k in cols]
    params = {k: payload[k] for k in fields}
    conn.execute(sa_text(
        f"INSERT INTO curriculum_groups_audit({', '.join(fields)}) VALUES({', '.join(':'+f for f in fields)})"
    ), params)

def _approvals_columns(conn) -> set[str]:
    return _table_cols(conn, "approvals")

def _queue_approval(conn, *, object_type: str, object_id: str, action: str,
                    requester_email: str | None, reason_note: str, rule_value: str | None = None):
    cols = _approvals_columns(conn)
    fields = ["object_type", "object_id", "action", "status"]
    params = {"object_type": object_type, "object_id": object_id, "action": action, "status": "pending"}
    if "requester_email" in cols and requester_email:
        fields.append("requester_email"); params["requester_email"] = requester_email
    elif "requester" in cols and requester_email:
        fields.append("requester"); params["requester"] = requester_email
    if "rule" in cols and rule_value:
        fields.append("rule"); params["rule"] = rule_value
    if "reason_note" in cols:
        fields.append("reason_note"); params["reason_note"] = reason_note
    conn.execute(sa_text(
        f"INSERT INTO approvals({', '.join(fields)}) VALUES({', '.join(':'+f for f in fields)})"
    ), params)

# ───────────────── cohort helpers ─────────────────

COHORT_BOTH              = "both"
COHORT_PROGRAM_OR_BRANCH = "program_or_branch"
COHORT_PROGRAM_ONLY      = "program_only"
COHORT_BRANCH_ONLY       = "branch_only"
COHORT_NONE              = "none"

def allow_programs_for(mode: str) -> bool:
    return mode in {COHORT_BOTH, COHORT_PROGRAM_OR_BRANCH, COHORT_PROGRAM_ONLY}

def allow_branches_for(mode: str) -> bool:
    return mode in {COHORT_BOTH, COHORT_PROGRAM_OR_BRANCH, COHORT_BRANCH_ONLY}

def branches_require_program(mode: str) -> bool:
    return mode == COHORT_BOTH

# ─────────────────────────── Page ───────────────────────────

@require_page("Programs / Branches")
def render():
    settings = load_settings()
    engine = get_engine(settings.db.url)
    init_db(engine)

    user = st.session_state.get("user") or {}
    actor = (user.get("email") or user.get("full_name") or "system")
    
    roles = user_roles()
    CAN_EDIT = can_edit_page("Programs / Branches", roles)
    
    if not CAN_EDIT:
        st.info("📖 Read-only mode: You have view access but cannot modify data.")

    st.title("📚 Programs, Branches & Curriculum")

    with engine.begin() as conn:
        ddf = _degrees_df(conn)

    if ddf.empty:
        st.info("No degrees found.")
        render_footer_global()
        return

    deg_codes = ddf["code"].tolist()
    degree_sel = st.selectbox("Degree", options=deg_codes, key="pb_deg_sel")

    with engine.begin() as conn:
        deg = _fetch_degree(conn, degree_sel)
        dfp = _programs_df(conn, degree_sel)
        dfb_all = _branches_df(conn, degree_sel, program_id=None)
        
        SHOW_CG = bool(deg.cg_degree or deg.cg_program or deg.cg_branch)
        
        df_cg = _curriculum_groups_df(conn, degree_sel) if SHOW_CG else pd.DataFrame()
        df_cgl = _curriculum_group_links_df(conn, degree_sel) if SHOW_CG else pd.DataFrame()
        
        sem_binding = _get_semester_binding(conn, degree_sel) or 'degree'
        deg_struct = _get_degree_struct(conn, degree_sel)
        prog_structs = _get_program_structs_for_degree(conn, degree_sel)
        branch_structs = _get_branch_structs_for_degree(conn, degree_sel)

        bcols = _table_cols(conn, "branches")
        BR_HAS_PID = "program_id" in bcols
        BR_HAS_DEG = "degree_code" in bcols

    mode = str(deg.cohort_splitting_mode or "both").lower()
    st.caption(f"Degree: **{deg.title}** • Cohort mode: `{mode}` • Active: `{bool(deg.active)}`")

    st.markdown("---")
    with st.expander("Show full degree structure map", expanded=True):
        
        map_md = f"**Degree:** {deg.title} (`{degree_sel}`)\n"
        if sem_binding == 'degree' and deg_struct:
            map_md += f"- *Semester Structure: {deg_struct[0]} Years, {deg_struct[1]} Terms/Year*\n"
        map_md += "\n"

        if mode == 'both':
            map_md += "**Hierarchy:** `Degree → Program → Branch`\n"
            if not dfp.empty:
                for _, prog_row in dfp.iterrows():
                    prog_code = prog_row['program_code']
                    map_md += f"- **Program:** {prog_row['program_name']} (`{prog_code}`)\n"
                    if sem_binding == 'program' and prog_code in prog_structs:
                        p_struct = prog_structs[prog_code]
                        map_md += f"  - *Semester Structure: {p_struct[0]} Years, {p_struct[1]} Terms/Year*\n"
                    
                    if deg.cg_program:
                        linked_cgs_prog = df_cgl[df_cgl['program_code'] == prog_code] if not df_cgl.empty else pd.DataFrame()
                        for _, cg_link_row in linked_cgs_prog.iterrows():
                            map_md += f"  - *Curriculum Group:* `{cg_link_row['group_code']}`\n"

                    child_branches = dfb_all[dfb_all['program_code'] == prog_code] if not dfb_all.empty else pd.DataFrame()
                    if not child_branches.empty:
                        for _, branch_row in child_branches.iterrows():
                            branch_code = branch_row['branch_code']
                            map_md += f"  - **Branch:** {branch_row['branch_name']} (`{branch_code}`)\n"
                            if sem_binding == 'branch' and branch_code in branch_structs:
                                b_struct = branch_structs[branch_code]
                                map_md += f"    - *Semester Structure: {b_struct[0]} Years, {b_struct[1]} Terms/Year*\n"
                            
                            if deg.cg_branch:
                                linked_cgs_branch = df_cgl[df_cgl['branch_code'] == branch_code] if not df_cgl.empty else pd.DataFrame()
                                for _, cg_link_row in linked_cgs_branch.iterrows():
                                    map_md += f"    - *Curriculum Group:* `{cg_link_row['group_code']}`\n"
                    else:
                        map_md += "  - *(No branches defined for this program)*\n"
            else:
                map_md += "*(No programs defined for this degree)*\n"
        
        # (Rest of cohort modes can be added with similar detail if needed)

        if SHOW_CG:
            map_md += "\n---\n"
            cg_list = df_cg["group_name"].tolist() if not df_cg.empty else []
            map_md += f"**All Defined Curriculum Groups (for this degree):** {', '.join(cg_list) if cg_list else 'None'}"
        
        st.markdown(map_md)
    st.markdown("---")


    allow_programs = allow_programs_for(mode)
    allow_branches = allow_branches_for(mode)

    supports_degree_level_branches = BR_HAS_DEG
    if not supports_degree_level_branches:
        st.info("Schema note: your 'branches' table has no degree_code column, so all branches must be attached to a Program.")

    labels = []
    if allow_programs:
        labels.append("Programs")
    if allow_branches:
        labels.append("Branches")
    if SHOW_CG:
        labels.append("Curriculum Groups")
    if not labels:
        labels.append("View")

    tabs = st.tabs(labels)

    # ───────────────────────── Programs Tab ─────────────────────────
    if "Programs" in labels:
        with tabs[labels.index("Programs")]:
            st.subheader("Programs (per degree)")
            st.markdown("**Existing Programs**")
            st.dataframe(dfp, use_container_width=True, hide_index=True)

            if not CAN_EDIT:
                st.info("You don't have permissions to create or edit Programs.")
            else:
                st.markdown("### Create Program")
                c1, c2 = st.columns(2)
                with c1:
                    pc = st.text_input("Program code", key="prog_create_code").strip()
                    pn = st.text_input("Program name", key="prog_create_name").strip()
                    pactive = st.checkbox("Active", value=True, key="prog_create_active")
                    psort = st.number_input("Sort order", 1, 10000, 100, step=1, key="prog_create_sort")
                with c2:
                    plogo = st.text_input("Logo file name (optional)", key="prog_create_logo")
                    pdesc = st.text_area("Description", "", key="prog_create_desc")

                if st.button("Create Program", disabled=not CAN_EDIT, key="prog_create_btn"):
                    if not pc or not pn:
                        st.error("Program code and name are required.")
                    else:
                        try:
                            with engine.begin() as conn:
                                exists = conn.execute(sa_text(
                                    "SELECT 1 FROM programs WHERE lower(program_code)=lower(:pc)"
                                ), {"pc": pc}).fetchone()
                                if exists:
                                    st.error("Program code must be unique.")
                                else:
                                    conn.execute(sa_text("""
                                        INSERT INTO programs(program_code, program_name, degree_code, active, sort_order, logo_file_name, description)
                                        VALUES(:pc, :pn, :deg, :act, :so, :logo, :desc)
                                    """), {
                                        "pc": pc, "pn": pn, "deg": degree_sel,
                                        "act": 1 if pactive else 0, "so": int(psort),
                                        "logo": (plogo or None), "desc": (pdesc or None)
                                    })
                                    _audit_program(conn, "create", actor, {
                                        "degree_code": degree_sel, "program_code": pc, "program_name": pn,
                                        "active": 1 if pactive else 0, "sort_order": int(psort),
                                        "logo_file_name": (plogo or None), "description": (pdesc or None)
                                    })
                            st.success("Program created.")
                            st.rerun()
                        except Exception as ex:
                            st.error(str(ex))

                st.markdown("---")
                st.markdown("### Edit / Delete Program")

                prog_codes = dfp["program_code"].tolist() if "program_code" in dfp.columns else []
                sel_pc = st.selectbox("Select program_code", [""] + prog_codes, key="prog_edit_pick")
                if sel_pc:
                    with engine.begin() as conn:
                        prow = conn.execute(sa_text("""
                            SELECT id, program_code, program_name, degree_code, active, sort_order, logo_file_name, description
                              FROM programs
                             WHERE degree_code=:d AND lower(program_code)=lower(:pc)
                             LIMIT 1
                        """), {"d": degree_sel, "pc": sel_pc}).fetchone()

                    if prow:
                        e1, e2 = st.columns(2)
                        with e1:
                            editable_name = st.text_input("Program name", prow.program_name or "", key=f"prog_edit_name_{sel_pc}")
                            editable_active = st.checkbox("Active", value=bool(prow.active), key=f"prog_edit_active_{sel_pc}")
                            editable_so = st.number_input("Sort order", 1, 10000, int(prow.sort_order), step=1, key=f"prog_edit_sort_{sel_pc}")
                        with e2:
                            editable_logo = st.text_input("Logo file name (optional)", prow.logo_file_name or "", key=f"prog_edit_logo_{sel_pc}")
                            editable_desc = st.text_area("Description", prow.description or "", key=f"prog_edit_desc_{sel_pc}")

                        c3, c4 = st.columns(2)
                        with c3:
                            if st.button("Save changes", disabled=not CAN_EDIT, key=f"prog_edit_save_{sel_pc}"):
                                try:
                                    with engine.begin() as conn:
                                        conn.execute(sa_text("""
                                            UPDATE programs
                                               SET program_name=:pn, active=:act, sort_order=:so, logo_file_name=:logo, description=:desc,
                                                   updated_at=CURRENT_TIMESTAMP
                                             WHERE id=:id
                                        """), {
                                            "pn": (editable_name or None),
                                            "act": 1 if editable_active else 0,
                                            "so": int(editable_so),
                                            "logo": (editable_logo or None),
                                            "desc": (editable_desc or None),
                                            "id": int(prow.id)
                                        })
                                        _audit_program(conn, "edit", actor, {
                                            "degree_code": degree_sel, "program_code": prow.program_code,
                                            "program_name": (editable_name or None), "active": 1 if editable_active else 0,
                                            "sort_order": int(editable_so), "logo_file_name": (editable_logo or None),
                                            "description": (editable_desc or None)
                                        })
                                    st.success("Saved.")
                                    st.rerun()
                                except Exception as ex:
                                    st.error(str(ex))
                        with c4:
                            if st.button("Request Delete", disabled=not CAN_EDIT, key=f"prog_delete_req_{sel_pc}"):
                                try:
                                    with engine.begin() as conn:
                                        _queue_approval(
                                            conn, object_type="program", object_id=prow.program_code, action="delete",
                                            requester_email=actor, reason_note="Program delete (requires approval)", rule_value="either_one"
                                        )
                                        _audit_program(conn, "delete_request", actor, dict(prow._mapping), note="Approval requested")
                                    st.success("Delete request submitted.")
                                    st.rerun()
                                except Exception as ex:
                                    st.error(str(ex))

    # ───────────────────────── Branches Tab ─────────────────────────
    if "Branches" in labels:
        with tabs[labels.index("Branches")]:
            st.subheader("Branches")

            with engine.begin() as conn:
                dfp2 = _programs_df(conn, degree_sel)

            # CORRECTED LOGIC: Only show warning for 'both' mode
            if mode == 'both' and dfp2.empty:
                st.warning("This degree requires Program → Branch structure. Create a Program first.")
                st.markdown("---")
                st.markdown("**Existing Branches**")
                st.dataframe(pd.DataFrame(columns=['id', 'branch_code', 'branch_name']), use_container_width=True, hide_index=True)
            else:
                prog_pick_codes = dfp2["program_code"].tolist() if "program_code" in dfp2.columns else []
                filter_pc = st.selectbox(
                    "Filter branches by program_code (optional)", [""] + prog_pick_codes, key="branch_filter_prog"
                )
                filter_pid = None
                with engine.begin() as conn:
                    if filter_pc:
                        filter_pid = _program_id_by_code(conn, degree_sel, filter_pc)
                    dfb = _branches_df(conn, degree_sel, program_id=filter_pid)

                st.markdown("**Existing Branches**")
                st.dataframe(dfb, use_container_width=True, hide_index=True)

                if not CAN_EDIT:
                    st.info("You don't have permissions to create or edit Branches.")
                else:
                    st.markdown("### Create Branch")
                    c1, c2 = st.columns(2)
                    with c1:
                        parent_pc = ""
                        # Logic to determine if parent program dropdown should be shown
                        if mode == 'both' or (mode == 'program_or_branch' and not dfp2.empty) or not supports_degree_level_branches:
                             parent_pc = st.selectbox(
                                "Parent program_code",
                                options=([""] + prog_pick_codes),
                                key="branch_create_parent_pc"
                            )
                        bc = st.text_input("Branch code", key="branch_create_code").strip()
                        bn = st.text_input("Branch name", key="branch_create_name").strip()
                        bactive = st.checkbox("Active", value=True, key="branch_create_active")
                        bsort = st.number_input("Sort order", 1, 10000, 100, step=1, key="branch_create_sort")
                    with c2:
                        blogo = st.text_input("Logo file name (optional)", key="branch_create_logo")
                        bdesc = st.text_area("Description", "", key="branch_create_desc")
                    
                    # A program is required if mode is 'both' OR if the schema physically can't support degree-level branches.
                    parent_required = (mode == 'both') or (not supports_degree_level_branches)

                    if st.button("Create Branch", disabled=not CAN_EDIT, key="branch_create_btn"):
                        if parent_required and not parent_pc:
                            st.error("Select a parent program.")
                        elif not bc or not bn:
                            st.error("Branch code and name are required.")
                        else:
                            try:
                                with engine.begin() as conn:
                                    pid = _program_id_by_code(conn, degree_sel, parent_pc) if parent_pc else None
                                    if parent_pc and not pid:
                                        st.error("Parent program not found."); raise RuntimeError("parent program missing")

                                    exists = conn.execute(sa_text("SELECT 1 FROM branches WHERE lower(branch_code)=lower(:bc)"), {"bc": bc}).fetchone()
                                    if exists:
                                        st.error("Branch code must be unique.")
                                    else:
                                        if pid is not None:
                                            base_payload = {
                                                "bc": bc, "bn": bn, "pid": int(pid), "act": 1 if bactive else 0,
                                                "so": int(bsort), "logo": (blogo or None), "desc": (bdesc or None)
                                            }
                                            audit_payload = {
                                                "branch_code": bc, "branch_name": bn, "program_id": int(pid), 
                                                "active": 1 if bactive else 0, "sort_order": int(bsort), 
                                                "logo_file_name": (blogo or None), "description": (bdesc or None)
                                            }

                                            if BR_HAS_DEG:
                                                sql = """
                                                    INSERT INTO branches(branch_code, branch_name, program_id, degree_code, active, sort_order, logo_file_name, description)
                                                    VALUES(:bc, :bn, :pid, :deg, :act, :so, :logo, :desc)
                                                """
                                                base_payload["deg"] = degree_sel
                                                audit_payload["degree_code"] = degree_sel
                                            else:
                                                sql = """
                                                    INSERT INTO branches(branch_code, branch_name, program_id, active, sort_order, logo_file_name, description)
                                                    VALUES(:bc, :bn, :pid, :act, :so, :logo, :desc)
                                                """
                                            conn.execute(sa_text(sql), base_payload)
                                            _audit_branch(conn, "create", actor, audit_payload)

                                        elif BR_HAS_DEG:
                                            conn.execute(sa_text("""
                                                INSERT INTO branches(branch_code, branch_name, degree_code, active, sort_order, logo_file_name, description)
                                                VALUES(:bc, :bn, :deg, :act, :so, :logo, :desc)
                                            """), {
                                                "bc": bc, "bn": bn, "deg": degree_sel, "act": 1 if bactive else 0,
                                                "so": int(bsort), "logo": (blogo or None), "desc": (bdesc or None)
                                            })
                                            _audit_branch(conn, "create", actor, {"degree_code": degree_sel, "branch_code": bc, "branch_name": bn, "active": 1 if bactive else 0, "sort_order": int(bsort), "logo_file_name": (blogo or None), "description": (bdesc or None)})
                                        else:
                                            raise ValueError("Schema requires branches to be attached to a Program.")

                                st.success("Branch created.")
                                st.rerun()
                            except Exception as ex:
                                st.error(str(ex))

                    st.markdown("---")
                    st.markdown("### Edit / Delete Branch")

                    br_codes = dfb["branch_code"].tolist() if "branch_code" in dfb.columns else []
                    sel_bc = st.selectbox("Select branch_code", [""] + br_codes, key="branch_edit_pick")

                    if sel_bc:
                        with engine.begin() as conn:
                            brow = conn.execute(sa_text("""
                                SELECT b.id, b.branch_code, b.branch_name, b.active, b.sort_order, b.logo_file_name, b.description,
                                       p.program_code, p.degree_code, b.program_id
                                  FROM branches b
                                  LEFT JOIN programs p ON p.id=b.program_id
                                 WHERE (p.degree_code=:deg OR b.degree_code=:deg) AND lower(b.branch_code)=lower(:bc)
                                 LIMIT 1
                            """), {"deg": degree_sel, "bc": sel_bc}).fetchone()

                        if brow:
                            e1, e2 = st.columns(2)
                            with e1:
                                editable_name = st.text_input("Branch name", brow.branch_name or "", key=f"branch_edit_name_{sel_bc}")
                                editable_active = st.checkbox("Active", value=bool(brow.active), key=f"branch_edit_active_{sel_bc}")
                                editable_so = st.number_input("Sort order", 1, 10000, int(brow.sort_order), step=1, key=f"branch_edit_sort_{sel_bc}")
                            with e2:
                                editable_logo = st.text_input("Logo file name (optional)", brow.logo_file_name or "", key=f"branch_edit_logo_{sel_bc}")
                                editable_desc = st.text_area("Description", brow.description or "", key=f"branch_edit_desc_{sel_bc}")

                            c3, c4 = st.columns(2)
                            with c3:
                                if st.button("Save changes", disabled=not CAN_EDIT, key=f"branch_edit_save_{sel_bc}"):
                                    try:
                                        with engine.begin() as conn:
                                            conn.execute(sa_text("""
                                                UPDATE branches
                                                   SET branch_name=:bn, active=:act, sort_order=:so, logo_file_name=:logo, description=:desc,
                                                       updated_at=CURRENT_TIMESTAMP
                                                 WHERE id=:id
                                            """), {
                                                "bn": (editable_name or None), "act": 1 if editable_active else 0, "so": int(editable_so),
                                                "logo": (editable_logo or None), "desc": (editable_desc or None), "id": int(brow.id)
                                            })
                                            _audit_branch(conn, "edit", actor, {"program_id": brow.program_id, "degree_code": brow.degree_code, "branch_code": brow.branch_code, "branch_name": editable_name, "active": 1 if editable_active else 0, "sort_order": int(editable_so), "logo_file_name": (editable_logo or None), "description": (editable_desc or None)})
                                        st.success("Saved.")
                                        st.rerun()
                                    except Exception as ex:
                                        st.error(str(ex))
                            with c4:
                                if st.button("Request Delete", disabled=not CAN_EDIT, key=f"branch_delete_req_{sel_bc}"):
                                    try:
                                        with engine.begin() as conn:
                                            _queue_approval(
                                                conn, object_type="branch", object_id=brow.branch_code, action="delete",
                                                requester_email=actor, reason_note="Branch delete (requires approval)", rule_value="either_one"
                                            )
                                            _audit_branch(conn, "delete_request", actor, dict(brow._mapping), note="Approval requested")
                                        st.success("Delete request submitted.")
                                        st.rerun()
                                    except Exception as ex:
                                        st.error(str(ex))
    
    # ───────────────────── Curriculum Groups Tab ─────────────────────
    if "Curriculum Groups" in labels:
        with tabs[labels.index("Curriculum Groups")]:
            st.subheader("Curriculum Groups")
            
            st.markdown("**Existing Groups**")
            st.dataframe(df_cg, use_container_width=True, hide_index=True)
            
            st.markdown("**Existing Links**")
            st.dataframe(df_cgl, use_container_width=True, hide_index=True)

            if not CAN_EDIT:
                st.info("You don't have permissions to create or edit Curriculum Groups.")
            else:
                st.markdown("---")
                st.markdown("### Create Curriculum Group")
                c1, c2 = st.columns(2)
                with c1:
                    gc = st.text_input("Group code", key="cg_create_code").strip()
                    gn = st.text_input("Group name", key="cg_create_name").strip()
                    gkind = st.selectbox("Group Kind", ["pseudo", "cohort"], key="cg_create_kind")
                with c2:
                    gactive = st.checkbox("Active", value=True, key="cg_create_active")
                    gsort = st.number_input("Sort order", 1, 10000, 100, step=1, key="cg_create_sort")
                gdesc = st.text_area("Description", "", key="cg_create_desc")

                if st.button("Create Group", disabled=not CAN_EDIT, key="cg_create_btn"):
                    if not gc or not gn:
                        st.error("Group code and name are required.")
                    else:
                        try:
                            with engine.begin() as conn:
                                exists = conn.execute(sa_text(
                                    "SELECT 1 FROM curriculum_groups WHERE degree_code=:d AND lower(group_code)=lower(:gc)"
                                ), {"d": degree_sel, "gc": gc}).fetchone()
                                if exists:
                                    st.error("Group code must be unique for this degree.")
                                else:
                                    conn.execute(sa_text("""
                                        INSERT INTO curriculum_groups(degree_code, group_code, group_name, kind, active, sort_order, description)
                                        VALUES(:deg, :gc, :gn, :kind, :act, :so, :desc)
                                    """), {
                                        "deg": degree_sel, "gc": gc, "gn": gn, "kind": gkind,
                                        "act": 1 if gactive else 0, "so": int(gsort), "desc": (gdesc or None)
                                    })
                                    _audit_curriculum_group(conn, "create", actor, {
                                        "degree_code": degree_sel, "group_code": gc, "group_name": gn, "kind": gkind,
                                        "active": 1 if gactive else 0, "sort_order": int(gsort), "description": (gdesc or None)
                                    })
                            st.success("Curriculum Group created.")
                            st.rerun()
                        except Exception as ex:
                            st.error(str(ex))

                st.markdown("---")
                if (deg.cg_program or deg.cg_branch) and (not dfp.empty or not dfb_all.empty):
                    st.markdown("### Link Group to Program/Branch")
                    
                    group_codes = df_cg["group_code"].tolist() if not df_cg.empty else []
                    prog_codes = dfp["program_code"].tolist() if not dfp.empty else []
                    branch_codes = dfb_all["branch_code"].tolist() if not dfb_all.empty else []
                    
                    link_targets = [""]
                    link_targets.extend([f"Program: {pc}" for pc in prog_codes])
                    link_targets.extend([f"Branch: {bc}" for bc in branch_codes])
                    
                    sel_group = st.selectbox("Select Group to Link", [""] + group_codes, key="cg_link_group_sel")
                    sel_target = st.selectbox("Select Program or Branch to Link to", link_targets, key="cg_link_target_sel")
                    
                    if st.button("Link Group", disabled=(not CAN_EDIT or not sel_group or not sel_target), key="cg_link_btn"):
                        try:
                            target_type, target_code = sel_target.split(": ", 1)
                            prog_code_to_link = target_code if target_type == "Program" else None
                            branch_code_to_link = target_code if target_type == "Branch" else None

                            with engine.begin() as conn:
                                group_id_row = conn.execute(sa_text("SELECT id FROM curriculum_groups WHERE degree_code=:d AND group_code=:gc"), {"d": degree_sel, "gc": sel_group}).fetchone()
                                if not group_id_row:
                                    st.error("Selected group not found."); raise RuntimeError("Group missing")
                                
                                conn.execute(sa_text("""
                                    INSERT INTO curriculum_group_links(group_id, degree_code, program_code, branch_code)
                                    VALUES(:gid, :deg, :pc, :bc)
                                """), {
                                    "gid": group_id_row.id, "deg": degree_sel,
                                    "pc": prog_code_to_link, "bc": branch_code_to_link
                                })
                            st.success(f"Linked '{sel_group}' to '{sel_target}'.")
                            st.rerun()
                        except Exception as ex:
                            st.error(f"Failed to create link. It might already exist. Details: {ex}")
                else:
                    st.info("Linking is not available. Enable curriculum groups at the program/branch level on the Degrees page and ensure programs/branches exist.")

    if "View" in labels:
        with tabs[labels.index("View")]:
            st.info("This degree's cohort mode does not allow Programs or Branches.")

    st.markdown("---")
    
render()
