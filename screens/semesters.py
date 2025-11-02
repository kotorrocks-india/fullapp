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

# HELPER FUNCTIONS
def _table_exists(conn, table_name: str) -> bool:
    row = conn.execute(sa_text(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=:t"
    ), {"t": table_name}).fetchone()
    return bool(row)

def _has_column(conn, table_name: str, col: str) -> bool:
    # FIX: Patched SQL injection vulnerability.
    # Uses parameterized query instead of f-string.
    row = conn.execute(sa_text(
        "SELECT 1 FROM pragma_table_info(:table) WHERE name = :col"
    ), {"table": table_name, "col": col}).fetchone()
    return bool(row)

def _approvals_columns(conn):
    cols = {r[1] for r in conn.execute(sa_text("PRAGMA table_info(approvals)")).fetchall()}
    return cols

def _queue_approval(conn, object_type, object_id, action, requester_email, payload: dict):
    cols = _approvals_columns(conn)
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
    if not _table_exists(conn, "branches"):
        return []

    # FIX: Removed generic try...except block that was hiding errors.
    if _has_column(conn, "branches", "degree_code"):
        return conn.execute(sa_text("""
            SELECT id, branch_code, branch_name, program_id,
                   (SELECT program_code FROM programs WHERE id=branches.program_id) as program_code
              FROM branches
             WHERE lower(degree_code)=lower(:dc)
             ORDER BY sort_order, lower(branch_code)
        """), {"dc": degree_code}).fetchall()
    elif _has_column(conn, "branches", "program_id"):
        return conn.execute(sa_text("""
            SELECT b.id, b.branch_code, b.branch_name, b.program_id, p.program_code
              FROM branches b
              JOIN programs p ON p.id = b.program_id
             WHERE lower(p.degree_code)=lower(:dc)
             ORDER BY b.sort_order, lower(b.branch_code)
        """), {"dc": degree_code}).fetchall()
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

# REFACTOR: Function updated to support rebuilding a single target (program/branch)
# or all semesters for the degree.
def _rebuild_semesters(conn, degree_code, binding_mode, label_mode, target_id=None):
    params = {"dc": degree_code}
    
    # clear existing for degree
    if binding_mode == "program" and target_id:
        conn.execute(sa_text("DELETE FROM semesters WHERE program_id=:tid"), {"tid": target_id})
    elif binding_mode == "branch" and target_id:
        conn.execute(sa_text("DELETE FROM semesters WHERE branch_id=:tid"), {"tid": target_id})
    elif binding_mode == "degree" and target_id: # target_id will be degree_code
        conn.execute(sa_text("DELETE FROM semesters WHERE degree_code=:dc AND program_id IS NULL AND branch_id IS NULL"), {"dc": degree_code})
    elif not target_id: # "Rebuild All" quick action
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
        sql = """
            SELECT p.id, s.years, s.terms_per_year
              FROM programs p
         LEFT JOIN program_semester_struct s ON s.program_id=p.id
             WHERE lower(p.degree_code)=lower(:dc)
        """
        if target_id:
            sql += " AND p.id = :tid"
            params["tid"] = target_id
            
        prows = conn.execute(sa_text(sql), params).fetchall()
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
        else:
            sql = """
                SELECT b.id, s.years, s.terms_per_year
                  FROM branches b
                  JOIN programs p ON p.id = b.program_id
             LEFT JOIN branch_semester_struct s ON s.branch_id=b.id
                 WHERE lower(p.degree_code)=lower(:dc)
            """
        
        if target_id:
            sql += " AND b.id = :tid"
            params["tid"] = target_id
            
        brows = conn.execute(sa_text(sql), params).fetchall()
        
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

# Structure helpers
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

# Approval helpers
def _get_pending_approvals(conn, degree_code: str) -> list:
    rows = conn.execute(sa_text("""
        SELECT id, object_type, object_id, action, status, requester, created_at, payload
        FROM approvals 
        WHERE (object_id LIKE :degree_pattern OR object_id = :degree_code)
          AND status IN ('pending', 'under_review')
        ORDER BY created_at DESC
    """), {"degree_pattern": f"%{degree_code}%", "degree_code": degree_code}).fetchall()
    
    results = []
    for r in rows:
        d = dict(r._mapping)
        try:
            d['payload'] = json.loads(d.get('payload', '{}'))
        except (json.JSONDecodeError, TypeError):
            d['payload'] = {}
        results.append(d)
    return results

def _get_approved_changes(conn, degree_code: str) -> list:
    rows = conn.execute(sa_text("""
        SELECT id, object_type, object_id, action, status, requester, approver, decided_at
        FROM approvals 
        WHERE (object_id LIKE :degree_pattern OR object_id = :degree_code)
          AND status = 'approved'
          AND decided_at >= datetime('now', '-1 day')
        ORDER BY decided_at DESC
    """), {"degree_pattern": f"%{degree_code}%", "degree_code": degree_code}).fetchall()
    return [dict(r._mapping) for r in rows]

@require_page(PAGE_KEY)
def render():
    settings = load_settings()
    engine = get_engine(settings.db.url)
    init_db(engine)

    st.title("Semesters / Terms")
    st.caption("Configure semester structure for degrees, programs, and branches")

    with engine.begin() as conn:
        degs = _degrees(conn)
    
    if not degs:
        st.info("No degrees found. Please create a degree on the 'Degrees' page first.")
        return

    degree_options = []
    deg_map = {f"{d.code} — {d.title}{' (Active)' if d.active else ' (Inactive)'}": (d.code, d.cohort_splitting_mode, d.title) for d in degs}
    
    deg_label = st.selectbox("Select Degree", options=list(deg_map.keys()), key="sem_deg_sel")
    degree_code, cohort_mode, degree_title = deg_map[deg_label]
    mode = str(cohort_mode or "both").lower()

    # Get current state
    with engine.begin() as conn:
        binding_row = _binding(conn, degree_code)
        # Initialize binding if not exists
        if not binding_row:
            _set_binding(conn, degree_code, "degree", "year_term")
            binding_row = _binding(conn, degree_code)
            
        pending_approvals = _get_pending_approvals(conn, degree_code)
        approved_changes = _get_approved_changes(conn, degree_code)
        
        programs = _programs_for_degree(conn, degree_code)
        branches = _branches_for_degree(conn, degree_code)
        
        deg_struct = _get_degree_struct(conn, degree_code)
        prog_structs = _get_program_structs_for_degree(conn, degree_code)
        branch_structs = _get_branch_structs_for_degree(conn, degree_code)
    
    current_binding = binding_row[0] if binding_row else 'degree'
    current_label_mode = binding_row[1] if binding_row else 'year_term'

    # UI FIX: Check for pending binding change to lock config
    pending_binding_change = next((p for p in pending_approvals if p['action'] == 'binding_change'), None)
    config_locked = bool(pending_binding_change)

    # Show approval status
    if pending_approvals:
        with st.expander("🕒 Pending Approvals", expanded=True):
            for approval in pending_approvals:
                st.warning(
                    f"**{approval['object_type']}.{approval['action']}** - "
                    f"Object: `{approval['object_id']}` - "
                    f"Requested by: `{approval['requester']}` - "
                    f"Status: `{approval['status']}`"
                )
    
    if approved_changes:
        with st.expander("✅ Recently Approved Changes", expanded=True):
            for approval in approved_changes:
                st.success(
                    f"**{approval['object_type']}.{approval['action']}** - "
                    f"Object: `{approval['object_id']}` - "
                    f"Approved by: `{approval['approver']}` - "
                    f"Ready for use"
                )

    # Degree Structure Overview
    st.subheader("📊 Degree Structure Overview")
    with st.expander("View Degree Hierarchy", expanded=True):
        map_md = f"**Degree:** {degree_title} (`{degree_code}`)\n"
        map_md += f"- **Hierarchy Mode:** {mode.upper()}\n"
        map_md += f"- **Current Binding:** {current_binding.upper()}\n"
        
        if current_binding == 'degree' and deg_struct:
            map_md += f"- *Degree Structure: {deg_struct[0]} Years, {deg_struct[1]} Terms/Year*\n"
        map_md += "\n"

        if mode == 'both':
            map_md += "**Structure:** `Degree → Program → Branch`\n"
            if programs:
                map_md += f"\n**Programs ({len(programs)}):**\n"
                for pid, pcode, pname in programs:
                    map_md += f"- **{pname}** (`{pcode}`)\n"
                    if current_binding == 'program' and pcode in prog_structs:
                        p_struct = prog_structs[pcode]
                        map_md += f"  - *Structure: {p_struct[0]} Years, {p_struct[1]} Terms/Year*\n"
                    
                    program_branches = [b for b in branches if b[3] == pid]  # branches for this program
                    if program_branches:
                        map_md += f"  - **Branches ({len(program_branches)}):**\n"
                        for bid, bcode, bname, _, _ in program_branches:
                            map_md += f"    - {bname} (`{bcode}`)\n"
                            if current_binding == 'branch' and bcode in branch_structs:
                                b_struct = branch_structs[bcode]
                                map_md += f"      - *Structure: {b_struct[0]} Years, {b_struct[1]} Terms/Year*\n"
                    else:
                        map_md += "  - *No branches*\n"
            else:
                map_md += "*(No programs defined)*\n"

        elif mode == 'program_or_branch':
            map_md += "**Structure:** `Degree → Program/Branch` (Independent)\n"
            if programs:
                map_md += f"\n**Programs ({len(programs)}):**\n"
                for _, pcode, pname in programs:
                    map_md += f"- {pname} (`{pcode}`)\n"
            else:
                map_md += "**Programs:** None\n"

            if branches:
                map_md += f"\n**Branches ({len(branches)}):**\n"
                for _, bcode, bname, _, pcode in branches:
                    parent_info = f"(under {pcode})" if pcode else "(direct)"
                    map_md += f"- {bname} (`{bcode}`) {parent_info}\n"
            else:
                map_md += "\n**Branches:** None\n"
        
        elif mode == 'program_only':
            map_md += "**Structure:** `Degree → Program`\n"
            if programs:
                map_md += f"\n**Programs ({len(programs)}):**\n"
                for _, pcode, pname in programs:
                    map_md += f"- {pname} (`{pcode}`)\n"
            else:
                map_md += "**Programs:** None\n"
            map_md += "\n**Branches:** *Not applicable in this mode*\n"

        elif mode == 'branch_only':
            map_md += "**Structure:** `Degree → Branch`\n"
            map_md += "**Programs:** *Not applicable in this mode*\n"
            if branches:
                map_md += f"\n**Branches ({len(branches)}):**\n"
                for _, bcode, bname, _, _ in branches:
                    map_md += f"- {bname} (`{bcode}`)\n"
            else:
                map_md += "\n**Branches:** None\n"

        elif mode == 'none':
            map_md += "**Structure:** `Degree Only`\n"
            map_md += "**Programs:** *Not applicable in this mode*\n"
            map_md += "**Branches:** *Not applicable in this mode*\n"

        st.markdown(map_md)

    st.markdown("---")

    user = st.session_state.get("user") or {}
    roles = set(user.get("roles") or [])
    actor = user.get("email") or "system"
    
    # BUG FIX: Use policy function for edit rights, not hardcoded roles
    can_edit = can_edit_page(PAGE_KEY, roles)
    
    # Keep page-specific logic for a "view-only" mode
    mr_view_only = ("management_representative" in roles) and not can_edit

    # Binding Configuration
    st.subheader("⚙️ Binding Configuration")
    
    if config_locked:
        st.warning(f"**Binding change to '{pending_binding_change['payload']['to'].upper()}' is pending approval.**")
        st.info("Configuration is locked until this change is approved or rejected.")
    
    col1, col2 = st.columns(2)
    with col1:
        st.metric("Current Binding", current_binding.upper())
    with col2:
        st.metric("Label Format", "Year/Term" if current_label_mode == "year_term" else "Semester")

    # Determine available binding options
    binding_options = ["degree"]
    if programs:
        binding_options.append("program")
    if branches:
        binding_options.append("branch")

    st.info(f"**Available binding levels:** {', '.join([b.upper() for b in binding_options])}")

    # UI FIX: User-friendly labels for Binding Level
    binding_format_options = {
        "degree": "Degree",
        "program": "Program",
        "branch": "Branch"
    }
    
    new_binding = st.radio("Select Semester Binding Level:", 
                          options=binding_options, # This is the list like ['degree', 'program']
                          index=binding_options.index(current_binding) if current_binding in binding_options else 0,
                          format_func=lambda k: binding_format_options.get(k, k.capitalize()), # Map to friendly label
                          horizontal=True,
                          # UI FIX: Disable if config is locked
                          disabled=(config_locked or mr_view_only or not can_edit),
                          help="Choose at which level semesters should be configured")

    # FIX: This 'if' statement must be on its own line and dedented
    if new_binding != current_binding:
        if st.button("🔄 Change Binding Level", 
                    type="primary",
                    disabled=(config_locked or mr_view_only or not can_edit)):
            with engine.begin() as conn:
                _queue_approval(conn, "semesters", degree_code, "binding_change",
                              requester_email=actor,
                              payload={
                                  "from": current_binding, 
                                  "to": new_binding, 
                                  "auto_rebuild": True,
                                  "note": f"Change semester binding from {current_binding} to {new_binding}"
                              })
            st.success(f"Binding change to '{new_binding.upper()}' submitted for approval.")
            st.info("After approval, you'll be able to configure semesters at the selected level.")
            # Original code already correctly used st.stop() here.
            st.stop()

    # Label mode (immediate change)
    # UI FIX: User-friendly labels for Semester Label Format
    label_format_options = {
        "year_term": "Year / Term",
        "semester_n": "Semester Number"
    }
    label_options_keys = list(label_format_options.keys()) # ['year_term', 'semester_n']
    
    new_label = st.radio("Semester Label Format:", 
                        options=label_options_keys,
                        index=label_options_keys.index(current_label_mode),
                        format_func=lambda k: label_format_options[k], # This displays the friendly labels
                        horizontal=True,
                        disabled=(config_locked or mr_view_only or not can_edit),
                        help="Choose how semesters are labeled")

    if new_label != current_label_mode:
        if st.button("Update Label Format", 
                    # UI FIX: Disable if config is locked
                    disabled=(config_locked or mr_view_only or not can_edit)):
            with engine.begin() as conn:
                _set_binding(conn, degree_code, current_binding, new_label)
            st.success("Label format updated.")
            st.rerun()

    st.markdown("---")
    st.subheader("🎯 Semester Structure Configuration")

    def _structure_editor(target, key, title, code):
        with engine.begin() as conn:
            srow = _struct_for_target(conn, target, key)
            have_semesters = _has_child_semesters(conn, degree_code, target, key)

        years_val = int(srow[0]) if srow else 4
        tpy_val = int(srow[1]) if srow else 2

        st.write(f"**{title}** (`{code}`)")
        col1, col2, col3 = st.columns([1,1,2])
        with col1:
            y = st.number_input(f"Years", 1, 10, years_val, step=1,
                                key=f"yrs_{target}_{key}")
        with col2:
            t = st.number_input(f"Terms/Year", 1, 5, tpy_val, step=1,
                                key=f"tpy_{target}_{key}")
        with col3:
            status_text = "⚠️ Has existing semesters" if have_semesters else "✅ No existing semesters"
            st.write(status_text)

        save_disabled = (mr_view_only or not can_edit)
        col1, col2 = st.columns(2)
        
        with col1:
            if st.button(f"💾 Save Structure", 
                        key=f"save_{target}_{key}", 
                        disabled=save_disabled,
                        use_container_width=True):
                if have_semesters and (int(y) != years_val or int(t) != tpy_val):
                    with engine.begin() as conn:
                        _queue_approval(conn, "semesters", f"{target}:{key}", "edit_structure",
                                      requester_email=actor,
                                      payload={
                                          "years_from": years_val, 
                                          "tpy_from": tpy_val,
                                          "years_to": int(y), 
                                          "tpy_to": int(t),
                                          "reason": "Edit years/terms requires approval when child data exists"
                                      })
                    st.info("Structure change submitted for approval (existing semesters detected).")
                    # UI FIX: Use st.stop() instead of st.rerun() to keep inputs
                    st.stop()
                else:
                    with engine.begin() as conn:
                        _upsert_struct(conn, target, key, int(y), int(t))
                    st.success("Structure saved.")
                    st.rerun()

        with col2:
            # BUG FIX: Rebuild button now checks for approval
            if st.button(f"🔄 Rebuild Semesters", 
                        key=f"rebuild_{target}_{key}",
                        disabled=(mr_view_only or not can_edit),
                        use_container_width=True):
                if have_semesters:
                    with engine.begin() as conn:
                        _queue_approval(conn, "semesters", f"{target}:{key}", "rebuild_semesters",
                                      requester_email=actor,
                                      payload={
                                          "note": f"Request to rebuild semesters for {target} {key}",
                                          "binding_mode": current_binding,
                                          "label_mode": current_label_mode,
                                          "target_id": key
                                      })
                    st.info("Rebuild request submitted for approval (existing semesters detected).")
                    st.stop()
                else:
                    with engine.begin() as conn:
                        # UI FIX: Pass target_id to only rebuild this item
                        cnt = _rebuild_semesters(conn, degree_code, current_binding, current_label_mode, target_id=key)
                    st.success(f"Rebuilt {cnt} semesters.")
                    st.rerun()

    # Show appropriate structure editor based on current binding
    # UI FIX: Check if config is locked
    if config_locked:
        st.info("Semester structure configuration is locked due to a pending binding change.")
    elif current_binding == "degree":
        _structure_editor("degree", degree_code, "Degree Structure", degree_code)

    elif current_binding == "program":
        if not programs:
            st.info("No programs found for this degree. Please create programs first.")
        else:
            program_options = [f"{pcode} — {pname}" for (_, pcode, pname) in programs]
            selected_program = st.selectbox("Select Program to Configure:", 
                                          program_options,
                                          key=f"sem_prog_{degree_code}")
            
            selected_program_id = next(pid for (pid, pcode, pname) in programs if f"{pcode} — {pname}" == selected_program)
            selected_program_code = next(pcode for (pid, pcode, pname) in programs if f"{pcode} — {pname}" == selected_program)
            
            _structure_editor("program", selected_program_id, "Program Structure", selected_program_code)

    elif current_binding == "branch":
        if not branches:
            st.info("No branches found for this degree. Please create branches first.")
        else:
            branch_options = [f"{bcode} — {bname}" for (_, bcode, bname, _, _) in branches]
            selected_branch = st.selectbox("Select Branch to Configure:", 
                                         branch_options,
                                         key=f"sem_branch_{degree_code}")
            
            selected_branch_id = next(bid for (bid, bcode, bname, _, _) in branches if f"{bcode} — {bname}" == selected_branch)
            selected_branch_code = next(bcode for (bid, bcode, bname, _, _) in branches if f"{bcode} — {bname}" == selected_branch)
            
            _structure_editor("branch", selected_branch_id, "Branch Structure", selected_branch_code)

    st.markdown("---")
    st.subheader("📋 Current Semesters")

    with engine.begin() as conn:
        df_rows = conn.execute(sa_text("""
            SELECT degree_code, program_id, branch_id, year_index, term_index, semester_number, label, active, updated_at
              FROM semesters
             WHERE lower(degree_code)=lower(:dc)
             ORDER BY program_id NULLS FIRST, branch_id NULLS FIRST, year_index, term_index
        """), {"dc": degree_code}).fetchall()
    
    semesters_exist = bool(df_rows)
    
    if semesters_exist:
        df = pd.DataFrame(df_rows, columns=["degree","program_id","branch_id","year","term","sem_no","label","active","updated"])
        st.dataframe(df, use_container_width=True, hide_index=True)
        st.caption(f"Total semesters: {len(df_rows)}")
        
        # Show semester statistics
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Semesters", len(df_rows))
        with col2:
            active_count = len([r for r in df_rows if r[7] == 1])
            st.metric("Active Semesters", active_count)
        with col3:
            unique_years = len(set(r[3] for r in df_rows))
            st.metric("Years Covered", unique_years)
    else:
        st.info("No semesters found. Configure the structure above and rebuild semesters.")

    # Quick Actions
    st.markdown("---")
    st.subheader("⚡ Quick Actions")
    
    col1, col2, col3 = st.columns(3)
    quick_actions_disabled = (mr_view_only or not can_edit)
    
    with col1:
        # BUG FIX: Add approval check and permission check
        if st.button("🗑️ Clear All Semesters", 
                    type="secondary", 
                    use_container_width=True,
                    disabled=quick_actions_disabled):
            if semesters_exist:
                with engine.begin() as conn:
                    _queue_approval(conn, "semesters", degree_code, "clear_all_semesters",
                                  requester_email=actor,
                                  payload={"note": "Request to clear all semesters for degree."})
                st.info("Request to clear all semesters submitted for approval.")
                st.stop()
            else:
                with engine.begin() as conn:
                    conn.execute(sa_text("DELETE FROM semesters WHERE degree_code=:dc"), {"dc": degree_code})
                st.success("All semesters cleared (no existing semesters).")
                st.rerun()
    
    with col2:
        # BUG FIX: Add approval check and permission check
        if st.button("🔄 Rebuild All Semesters", 
                    type="primary", 
                    use_container_width=True,
                    disabled=quick_actions_disabled):
            if semesters_exist:
                with engine.begin() as conn:
                    _queue_approval(conn, "semesters", degree_code, "rebuild_all_semesters",
                                  requester_email=actor,
                                  payload={
                                      "note": "Request to rebuild all semesters for degree.",
                                      "binding_mode": current_binding,
                                      "label_mode": current_label_mode
                                  })
                st.info("Rebuild request submitted for approval (existing semesters detected).")
                st.stop()
            else:
                with engine.begin() as conn:
                    cnt = _rebuild_semesters(conn, degree_code, current_binding, current_label_mode, target_id=None)
                st.success(f"Rebuilt {cnt} semesters with {current_binding} binding.")
                st.rerun()
    
    with col3:
        # BUG FIX: Add permission check
        st.button("📊 View Detailed Report (Future)", 
                    type="secondary", 
                    use_container_width=True,
                    disabled=True, # Disabled until feature is implemented
                    help="This feature is not yet implemented.")

render()
