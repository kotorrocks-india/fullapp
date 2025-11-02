# app/screens/semesters.py
import json
import pandas as pd
import streamlit as st
from sqlalchemy import text as sa_text
from core.db import get_engine, init_db
from core.policy import require_page, can_edit_page
from core.policy import can_view_page
from core.theme_toggle import render_theme_toggle
from core.settings import load_settings

PAGE_KEY = "Semesters"

#  HELPER FUNCTIONS (to robustly check schema)
def _table_exists(conn, table_name: str) -> bool:
    row = conn.execute(sa_text(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=:t"
    ), {"t": table_name}).fetchone()
    return bool(row)

def _has_column(conn, table_name: str, col: str) -> bool:
    rows = conn.execute(sa_text(f"PRAGMA table_info({table_name})")).fetchall()
    return any(r[1] == col for r in rows)


def _approvals_columns(conn):
    cols = {r[1] for r in conn.execute(sa_text("PRAGMA table_info(approvals)")).fetchall()}
    return cols

def _queue_approval(conn, object_type, object_id, action, requester_email, payload: dict):
    cols = _approvals_columns(conn)
    # Try to include the most complete set your schema supports
    fields = ["object_type","object_id","action","status"]
    values = [":ot",":oid",":ac","'pending'"]
    params = {"ot": object_type, "oid": object_id, "ac": action}

    if "requester_email" in cols:
        fields.append("requester_email"); values.append(":re"); params["re"] = requester_email
    if "rule" in cols:
        fields.append("rule"); values.append(":rl"); params["rl"] = "either_one"
    if "payload" in cols:
        fields.append("payload"); values.append(":pl"); params["pl"] = json.dumps(payload)
    if "reason_note" in cols:
        fields.append("reason_note"); values.append(":rn"); params["rn"] = payload.get("note","")

    sql = f"INSERT INTO approvals({', '.join(fields)}) VALUES({', '.join(values)})"
    conn.execute(sa_text(sql), params)

def _degrees(conn):
    # Modified to return all degrees with active status
    return conn.execute(sa_text("""
        SELECT code, title, active, cohort_splitting_mode FROM degrees ORDER BY sort_order, code
    """)).fetchall()

def _programs_for_degree(conn, degree_code):
    return conn.execute(sa_text("""
        SELECT id, program_code, program_name
          FROM programs
         WHERE lower(degree_code)=lower(:dc)
         ORDER BY sort_order, lower(program_code)
    """), {"dc": degree_code}).fetchall()

def _branches_for_degree(conn, degree_code):
    """Fetches branches for a degree, supporting schemas with or without
    a direct degree_code column on the branches table."""
    if not _table_exists(conn, "branches"):
        return []

    try:
        # Schema 1: branches table has a direct degree_code link
        if _has_column(conn, "branches", "degree_code"):
            return conn.execute(sa_text("""
                SELECT id, branch_code, branch_name, program_id,
                       (SELECT program_code FROM programs WHERE id=branches.program_id) as program_code
                  FROM branches
                 WHERE lower(degree_code)=lower(:dc)
                 ORDER BY sort_order, lower(branch_code)
            """), {"dc": degree_code}).fetchall()

        # Schema 2: branches are linked via programs (branches.program_id -> programs.degree_code)
        elif _has_column(conn, "branches", "program_id"):
            return conn.execute(sa_text("""
                SELECT b.id, b.branch_code, b.branch_name, b.program_id, p.program_code
                  FROM branches b
                  JOIN programs p ON p.id = b.program_id
                 WHERE lower(p.degree_code)=lower(:dc)
                 ORDER BY b.sort_order, lower(b.branch_code)
            """), {"dc": degree_code}).fetchall()

        # Fallback if neither schema matches
        return []
    except Exception as e:
        # If there's an error (like table doesn't exist or schema issue), return empty list
        return []


def _binding(conn, degree_code):
    return conn.execute(sa_text("""
        SELECT binding_mode, label_mode
          FROM semester_binding WHERE degree_code=:dc
    """), {"dc": degree_code}).fetchone()

def _set_binding(conn, degree_code, binding_mode, label_mode):
    conn.execute(sa_text("""
        INSERT INTO semester_binding(degree_code, binding_mode, label_mode)
        VALUES(:dc, :bm, :lm)
        ON CONFLICT(degree_code) DO UPDATE SET
            binding_mode=excluded.binding_mode,
            label_mode=excluded.label_mode,
            updated_at=CURRENT_TIMESTAMP
    """), {"dc": degree_code, "bm": binding_mode, "lm": label_mode})

def _struct_for_target(conn, target, key):
    if target == "degree":
        return conn.execute(sa_text("""
            SELECT years, terms_per_year, active
              FROM degree_semester_struct WHERE degree_code=:k
        """), {"k": key}).fetchone()
    if target == "program":
        return conn.execute(sa_text("""
            SELECT years, terms_per_year, active
              FROM program_semester_struct WHERE program_id=:k
        """), {"k": key}).fetchone()
    if target == "branch":
        return conn.execute(sa_text("""
            SELECT years, terms_per_year, active
              FROM branch_semester_struct WHERE branch_id=:k
        """), {"k": key}).fetchone()

def _upsert_struct(conn, target, key, years, tpy):
    table = {
        "degree":  "degree_semester_struct",
        "program": "program_semester_struct",
        "branch":  "branch_semester_struct",
    }[target]
    keycol = "degree_code" if target == "degree" else f"{target}_id"
    conn.execute(sa_text(f"""
        INSERT INTO {table}({keycol}, years, terms_per_year, active)
        VALUES(:k, :y, :t, 1)
        ON CONFLICT({keycol}) DO UPDATE SET
            years=excluded.years,
            terms_per_year=excluded.terms_per_year,
            active=1,
            updated_at=CURRENT_TIMESTAMP
    """), {"k": key, "y": int(years), "t": int(tpy)})

def _has_child_semesters(conn, degree_code, target, key):
    if target == "degree":
        row = conn.execute(sa_text("""
            SELECT 1 FROM semesters
             WHERE degree_code=:dc AND program_id IS NULL AND branch_id IS NULL
             LIMIT 1
        """), {"dc": degree_code}).fetchone()
        return bool(row)
    if target == "program":
        row = conn.execute(sa_text("""
            SELECT 1 FROM semesters WHERE program_id=:k LIMIT 1
        """), {"k": key}).fetchone()
        return bool(row)
    if target == "branch":
        row = conn.execute(sa_text("""
            SELECT 1 FROM semesters WHERE branch_id=:k LIMIT 1
        """), {"k": key}).fetchone()
        return bool(row)

def _delete_struct(conn, target, key):
    table = {
        "degree":  "degree_semester_struct",
        "program": "program_semester_struct",
        "branch":  "branch_semester_struct",
    }[target]
    keycol = "degree_code" if target == "degree" else f"{target}_id"
    conn.execute(sa_text(f"DELETE FROM {table} WHERE {keycol}=:k"), {"k": key})

def _rebuild_semesters(conn, degree_code, binding_mode, label_mode):
    # clear existing for degree
    conn.execute(sa_text("DELETE FROM semesters WHERE degree_code=:dc"), {"dc": degree_code})

    def label(y, t, n):
        if label_mode == "year_term":
            return f"Year {y} • Term {t}"
        else:
            return f"Semester {n}"

    if binding_mode == "degree":
        row = conn.execute(sa_text("""
            SELECT years, terms_per_year FROM degree_semester_struct WHERE degree_code=:dc
        """), {"dc": degree_code}).fetchone()
        if not row:
            return 0
        years, tpy = int(row[0]), int(row[1])
        n = 0
        for y in range(1, years+1):
            for t in range(1, tpy+1):
                n += 1
                conn.execute(sa_text("""
                    INSERT INTO semesters(degree_code, year_index, term_index, semester_number, label, active)
                    VALUES(:dc, :y, :t, :n, :lbl, 1)
                """), {"dc": degree_code, "y": y, "t": t, "n": n, "lbl": label(y,t,n)})
        return n

    if binding_mode == "program":
        prows = conn.execute(sa_text("""
            SELECT p.id, s.years, s.terms_per_year
              FROM programs p
         LEFT JOIN program_semester_struct s ON s.program_id=p.id
             WHERE lower(p.degree_code)=lower(:dc)
        """), {"dc": degree_code}).fetchall()
        total = 0
        for pid, years, tpy in prows:
            if years is None or tpy is None:
                continue
            n = 0
            for y in range(1, int(years)+1):
                for t in range(1, int(tpy)+1):
                    n += 1
                    total += 1
                    conn.execute(sa_text("""
                        INSERT INTO semesters(degree_code, program_id, year_index, term_index, semester_number, label, active)
                        VALUES(:dc, :pid, :y, :t, :n, :lbl, 1)
                    """), {"dc": degree_code, "pid": pid, "y": y, "t": t, "n": n, "lbl": label(y,t,n)})
        return total

    if binding_mode == "branch":
        b_has_deg = _has_column(conn, "branches", "degree_code")
        if b_has_deg:
            sql = """
                SELECT b.id, s.years, s.terms_per_year
                  FROM branches b
             LEFT JOIN branch_semester_struct s ON s.branch_id=b.id
                 WHERE lower(b.degree_code)=lower(:dc)
            """
        else: # Fallback to joining through programs
            sql = """
                SELECT b.id, s.years, s.terms_per_year
                  FROM branches b
                  JOIN programs p ON p.id = b.program_id
             LEFT JOIN branch_semester_struct s ON s.branch_id=b.id
                 WHERE lower(p.degree_code)=lower(:dc)
            """
        brows = conn.execute(sa_text(sql), {"dc": degree_code}).fetchall()
        
        total = 0
        for bid, years, tpy in brows:
            if years is None or tpy is None:
                continue
            n = 0
            for y in range(1, int(years)+1):
                for t in range(1, int(tpy)+1):
                    n += 1
                    total += 1
                    conn.execute(sa_text("""
                        INSERT INTO semesters(degree_code, branch_id, year_index, term_index, semester_number, label, active)
                        VALUES(:dc, :bid, :y, :t, :n, :lbl, 1)
                    """), {"dc": degree_code, "bid": bid, "y": y, "t": t, "n": n, "lbl": label(y,t,n)})
        return total

# NEW helpers for the map
def _curriculum_groups_df(conn, degree_filter: str):
    rows = conn.execute(sa_text("""
        SELECT id, group_code, group_name
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
    """), {"d": degree_filter}).fetchall()
    return pd.DataFrame([dict(r._mapping) for r in rows]) if rows else pd.DataFrame()

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
    if _has_column(conn, 'branches', 'degree_code'):
        q += " WHERE b.degree_code = :dc"
    else:
        q += " JOIN programs p ON p.id = b.program_id WHERE p.degree_code = :dc"

    rows = conn.execute(sa_text(q), {"dc": degree_code}).fetchall()
    return {r.branch_code: (r.years, r.terms_per_year) for r in rows}


@require_page(PAGE_KEY)
def render():
    settings = load_settings()
    engine = get_engine(settings.db.url)
    
    init_db(engine)

    

    st.title("Semesters / Terms")
    st.caption("Curriculum groups do not affect semesters.")

    with engine.begin() as conn:
        degs = _degrees(conn)
    
    if not degs:
        st.info("No degrees found. Please create a degree on the 'Degrees' page first.")
        return

    degree_options = []
    deg_map = {f"{d.code} — {d.title}{' (Active)' if d.active else ' (Inactive)'}": (d.code, d.cohort_splitting_mode, d.title) for d in degs}
    
    deg_label = st.selectbox("Degree", options=list(deg_map.keys()), key="sem_deg_sel")
    
    degree_code, cohort_mode, degree_title = deg_map[deg_label]
    mode = str(cohort_mode or "both").lower()

    # NEW: Fetch all data for the map at the top
    with engine.begin() as conn:
        dfp = pd.DataFrame(_programs_for_degree(conn, degree_code))
        dfb_all = pd.DataFrame(_branches_for_degree(conn, degree_code))
        df_cg = _curriculum_groups_df(conn, degree_code)
        df_cgl = _curriculum_group_links_df(conn, degree_code)
        
        binding_mode = _binding(conn, degree_code)
        sem_binding = binding_mode[0] if binding_mode else 'degree'
        
        deg_struct = _get_degree_struct(conn, degree_code)
        prog_structs = _get_program_structs_for_degree(conn, degree_code)
        branch_structs = _get_branch_structs_for_degree(conn, degree_code)

    # NEW: Degree Map from programs_branches.py, adapted for this page
    with st.expander("Show full degree structure map", expanded=False):
        map_md = f"**Degree:** {degree_title} (`{degree_code}`)\n"
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
                            
                            linked_cgs_branch = df_cgl[df_cgl['branch_code'] == branch_code] if not df_cgl.empty else pd.DataFrame()
                            for _, cg_link_row in linked_cgs_branch.iterrows():
                                map_md += f"    - *Curriculum Group:* `{cg_link_row['group_code']}`\n"
                    else:
                        map_md += "  - *(No branches defined for this program)*\n"
            else:
                map_md += "*(No programs defined for this degree)*\n"

        elif mode == 'program_or_branch':
            map_md += "**Hierarchy:** `Degree → Program/Branch` (Independent)\n"
            if not dfp.empty:
                map_md += "**Programs:**\n"
                for _, prog_row in dfp.iterrows():
                    prog_code = prog_row['program_code']
                    map_md += f"- {prog_row['program_name']} (`{prog_code}`)\n"
                    linked_cgs_prog = df_cgl[df_cgl['program_code'] == prog_code] if not df_cgl.empty else pd.DataFrame()
                    for _, cg_link_row in linked_cgs_prog.iterrows():
                        map_md += f"  - *Curriculum Group:* `{cg_link_row['group_code']}`\n"
            else:
                map_md += "**Programs:** None\n"

            if not dfb_all.empty:
                map_md += "\n**Branches:**\n"
                for _, branch_row in dfb_all.iterrows():
                    branch_code = branch_row['branch_code']
                    parent_prog = branch_row.get('program_code')
                    if parent_prog:
                        map_md += f"- {branch_row['branch_name']} (`{branch_code}`) *(under Program: {parent_prog})*\n"
                    else:
                        map_md += f"- {branch_row['branch_name']} (`{branch_code}`) *(direct to Degree)*\n"
                    
                    linked_cgs_branch = df_cgl[df_cgl['branch_code'] == branch_code] if not df_cgl.empty else pd.DataFrame()
                    for _, cg_link_row in linked_cgs_branch.iterrows():
                        map_md += f"  - *Curriculum Group:* `{cg_link_row['group_code']}`\n"
            else:
                map_md += "\n**Branches:** None\n"
        
        elif mode == 'program_only':
            map_md += "**Hierarchy:** `Degree → Program`\n"
            if not dfp.empty:
                map_md += "**Programs:**\n"
                for _, prog_row in dfp.iterrows():
                    prog_code = prog_row['program_code']
                    map_md += f"- {prog_row['program_name']} (`{prog_code}`)\n"
                    linked_cgs_prog = df_cgl[df_cgl['program_code'] == prog_code] if not df_cgl.empty else pd.DataFrame()
                    for _, cg_link_row in linked_cgs_prog.iterrows():
                        map_md += f"  - *Curriculum Group:* `{cg_link_row['group_code']}`\n"
            else:
                map_md += "**Programs:** None\n"
            map_md += "\n**Branches:** *Not applicable in this mode.*\n"

        elif mode == 'branch_only':
            map_md += "**Hierarchy:** `Degree → Branch`\n"
            map_md += "**Programs:** *Not applicable in this mode.*\n"
            if not dfb_all.empty:
                map_md += "\n**Branches:**\n"
                for _, branch_row in dfb_all.iterrows():
                    branch_code = branch_row['branch_code']
                    map_md += f"- {branch_row['branch_name']} (`{branch_code}`)\n"
                    linked_cgs_branch = df_cgl[df_cgl['branch_code'] == branch_code] if not df_cgl.empty else pd.DataFrame()
                    for _, cg_link_row in linked_cgs_branch.iterrows():
                        map_md += f"  - *Curriculum Group:* `{cg_link_row['group_code']}`\n"
            else:
                map_md += "\n**Branches:** None\n"

        elif mode == 'none':
            map_md += "**Hierarchy:** `Degree Only`\n"
            map_md += "**Programs:** *Not applicable in this mode.*\n"
            map_md += "**Branches:** *Not applicable in this mode.*\n"

        map_md += "\n---\n"
        cg_list = df_cg["group_name"].tolist() if not df_cg.empty else []
        map_md += f"**All Defined Curriculum Groups (for this degree):** {', '.join(cg_list) if cg_list else 'None'}"
        
        st.markdown(map_md)
    st.markdown("---")


    user = st.session_state.get("user") or {}
    roles = set(user.get("roles") or [])
    actor = user.get("email") or "system"

    can_edit = bool(roles.intersection({"superadmin","principal","director"}))
    mr_view_only = ("management_representative" in roles) and not can_edit

    # Binding + label mode
    with engine.begin() as conn:
        row = _binding(conn, degree_code)
    current_binding = (row[0] if row else None)
    current_label_mode = (row[1] if row else "year_term")

    st.subheader("Binding")
    
    with engine.begin() as conn:
        has_prog = bool(_programs_for_degree(conn, degree_code))
        # FIXED: Use robust helper function to check for branches
        has_branch = bool(_branches_for_degree(conn, degree_code))

    binding_options = ["degree","program","branch"]
    if not (has_prog or has_branch):
        binding_options = ["degree"]

    bcol1, bcol2 = st.columns([1,1])
    with bcol1:
        sel_binding = st.radio("Binding mode", binding_options,
                               index=(binding_options.index(current_binding) if current_binding in binding_options else 0),
                               horizontal=True,
                               key=f"sem_bind_{degree_code}",
                               disabled=(mr_view_only))
    with bcol2:
        sel_label = st.radio("Label mode", ["year_term","semester_n"],
                             index=(0 if current_label_mode=="year_term" else 1),
                             horizontal=True,
                             key=f"sem_label_{degree_code}",
                             disabled=(mr_view_only))

    # Handle binding / label changes
    if (sel_binding != current_binding or sel_label != current_label_mode):
        if not can_edit or mr_view_only:
            st.warning("You do not have permission to change binding or labels.")
        else:
            if sel_binding != current_binding and current_binding is not None:
                with engine.begin() as conn:
                    _queue_approval(conn, "semesters", degree_code, "binding_change",
                                    requester_email=actor,
                                    payload={"from": current_binding, "to": sel_binding, "auto_rebuild": True})
                st.info("Binding change submitted for approval.")
                st.stop()
            else:
                with engine.begin() as conn:
                    _set_binding(conn, degree_code, sel_binding, sel_label)
                    conn.execute(sa_text("""
                        INSERT INTO semesters_audit(action, actor, degree_code, payload)
                        VALUES('edit', :actor, :dc, :pl)
                    """), {"actor": actor, "dc": degree_code,
                           "pl": json.dumps({"label_mode": sel_label})})
                st.success("Label mode updated.")
                st.rerun()

    if not current_binding:
        with engine.begin() as conn:
            _set_binding(conn, degree_code, sel_binding, sel_label)
        current_binding = sel_binding
        current_label_mode = sel_label

    st.divider()
    st.subheader("Structure")

    def _structure_editor(target, key, title):
        with engine.begin() as conn:
            srow = _struct_for_target(conn, target, key)
            have_semesters = _has_child_semesters(conn, degree_code, target, key)

        years_val = int(srow[0]) if srow else 4
        tpy_val = int(srow[1]) if srow else 2

        c1, c2, c3 = st.columns([1,1,2])
        with c1:
            y = st.number_input(f"{title}: Years", 1, 10, years_val, step=1,
                                key=f"yrs_{target}_{key}")
        with c2:
            t = st.number_input(f"{title}: Terms/Year", 1, 5, tpy_val, step=1,
                                key=f"tpy_{target}_{key}")
        with c3:
            st.write("")

        save_disabled = (mr_view_only or not can_edit)
        if st.button(f"Save {title}", key=f"save_{target}_{key}", disabled=save_disabled):
            if have_semesters and (int(y) != years_val or int(t) != tpy_val):
                with engine.begin() as conn:
                    _queue_approval(conn, "semesters", f"{target}:{key}", "edit_structure",
                                    requester_email=actor,
                                    payload={"years_from": years_val, "tpy_from": tpy_val,
                                             "years_to": int(y), "tpy_to": int(t),
                                             "reason": "Edit years/terms requires approval when child data exists"})
                st.info("Edit submitted for approval (child data detected).")
            else:
                with engine.begin() as conn:
                    _upsert_struct(conn, target, key, int(y), int(t))
                    conn.execute(sa_text("""
                        INSERT INTO semesters_audit(action, actor, degree_code, payload)
                        VALUES('edit', :actor, :dc, :pl)
                    """), {"actor": actor, "dc": degree_code,
                           "pl": json.dumps({"target": target, "key": key, "years": int(y), "tpy": int(t)})})
                st.success("Saved.")
            st.rerun()

        if st.button(f"Rebuild {title} semesters", key=f"rebuild_{target}_{key}",
                     disabled=(mr_view_only or not can_edit)):
            with engine.begin() as conn:
                cnt = _rebuild_semesters(conn, degree_code, current_binding, current_label_mode)
                conn.execute(sa_text("""
                    INSERT INTO semesters_audit(action, actor, degree_code, payload)
                    VALUES('rebuild', :actor, :dc, :pl)
                """), {"actor": actor, "dc": degree_code,
                       "pl": json.dumps({"target": target, "key": key, "count": cnt})})
            st.success("Rebuilt.")
            st.rerun()

        st.caption("Note: Delete of structure always requires approval.")

    if current_binding == "degree":
        _structure_editor("degree", degree_code, f"Degree {degree_code}")

    elif current_binding == "program":
        with engine.begin() as conn:
            prows = _programs_for_degree(conn, degree_code)
        if not prows:
            st.info("No programs under this degree.")
        else:
            labels = {f"{pcode} — {pname}": pid for (pid, pcode, pname) in prows}
            sel = st.selectbox("Program", list(labels.keys()), key=f"sem_prog_{degree_code}")
            _structure_editor("program", labels[sel], f"Program {sel.split(' — ')[0]}")

    elif current_binding == "branch":
        with engine.begin() as conn:
            brows = _branches_for_degree(conn, degree_code)
        if not brows:
            st.info("No branches under this degree.")
        else:
            def _fmt(br):
                bid, bcode, bname, pid, pcode = br
                code = bcode or f"#{bid}"
                return f"{code} — {bname or 'Branch'}"
            labels = {_fmt(b): b[0] for b in brows}
            sel = st.selectbox("Branch", list(labels.keys()), key=f"sem_branch_{degree_code}")
            _structure_editor("branch", labels[sel], f"Branch {sel.split(' — ')[0]}")

    st.divider()
    st.subheader("Current Semesters (flat)")

    with engine.begin() as conn:
        rows = conn.execute(sa_text("""
            SELECT degree_code, program_id, branch_id, year_index, term_index, semester_number, label, active, updated_at
              FROM semesters
             WHERE lower(degree_code)=lower(:dc)
             ORDER BY program_id NULLS FIRST, branch_id NULLS FIRST, year_index, term_index
        """), {"dc": degree_code}).fetchall()
    if rows:
        df = pd.DataFrame(rows, columns=["degree","program_id","branch_id","year","term","sem_no","label","active","updated"])
        st.dataframe(df, use_container_width=True, hide_index=True)
    else:
        st.info("No semesters yet. Save structure and rebuild.")
render()
