# screens/academic_years/ui.py
from __future__ import annotations

import traceback
import pandas as pd
import streamlit as st
from typing import Optional, Sequence
from sqlalchemy.engine import Engine
import json 

# Utilities (with soft fallbacks so UI doesn't go blank if utils move)
try:
    from screens.academic_years.utils import (
        is_valid_ay_code,
        get_next_ay_code,
        _handle_error,
        compute_term_windows_for_ay,
        validate_ay_code_dates,
    )
except Exception:  # soft fallback
    def is_valid_ay_code(code: str) -> bool:  # type: ignore
        return isinstance(code, str) and len(code) >= 4
    def get_next_ay_code(latest: Optional[str]) -> str:  # type: ignore
        return "" if not latest else latest
    def _handle_error(e: Exception, prefix: str = "Error"):
        st.error(f"{prefix}: {e}")
        st.code(traceback.format_exc())
    def compute_term_windows_for_ay(code: str):  # type: ignore
        return [], []
    def validate_ay_code_dates(ay_code, start_date): return True 

# DB accessors (soft fallbacks keep the page alive even if you’re refactoring)
try:
    from screens.academic_years.db import (
        # AY CRUD
        get_all_ays,
        get_ay_by_code,
        insert_ay,
        update_ay_dates,
        update_ay_status,
        delete_ay,
        check_overlap,
        get_latest_ay_code,
        # Catalogs
        get_all_degrees,
        get_degree_duration, 
        get_degree_terms_per_year, # NEW
        get_programs_for_degree,
        get_branches_for_degree_program,
        # Calendar profiles & term calc
        get_assignable_calendar_profiles,
        get_calendar_profile_by_id, 
        get_profile_term_count, # NEW
        insert_calendar_profile,  
        compute_terms_with_validation,
        # Assignment CUD
        insert_calendar_assignment,
    )
except Exception:  # soft fallbacks
    def get_all_ays(conn): return []
    def get_ay_by_code(conn, code): return None
    def insert_ay(conn, code, start_date, end_date): return True
    def update_ay_dates(conn, code, start_date, end_date): return True
    def update_ay_status(conn, code, status): return True
    def delete_ay(conn, code): return True
    def check_overlap(conn, start_date, end_date, exclude_code=None): return None
    def get_latest_ay_code(conn): return None
    def get_all_degrees(conn): return []
    def get_degree_duration(conn, code): return 10 
    def get_degree_terms_per_year(conn, code): return 0 # NEW Fallback
    def get_programs_for_degree(conn, d): return []
    def get_branches_for_degree_program(conn, d, p): return []
    def get_assignable_calendar_profiles(conn): return []
    def get_calendar_profile_by_id(conn, id): return None
    def get_profile_term_count(conn, id): return 0 # NEW Fallback
    def insert_calendar_profile(conn, code, name, model, anchor, spec_json): pass
    def compute_terms_with_validation(conn, ay, d, p, b, progression_year): return ([], [])
    def insert_calendar_assignment(conn, level, d, p, b, ay, py, cid, shift, actor): pass

# ---- tiny helpers ----
def _safe_conn(engine: Engine):
    try:
        return engine.connect()
    except Exception as e:
        _handle_error(e, "Failed to connect to database")
        st.stop()

def _df_or_empty(rows, columns) -> pd.DataFrame:
    try:
        return pd.DataFrame(rows, columns=columns)
    except Exception:
        return pd.DataFrame(columns=columns)

def render_ay_list(engine: Engine) -> None:
    """Render the Academic Year list with filters and pagination."""
    st.subheader("🗓️ Academic Year List")
    
    # Create a unique container to ensure widgets don't duplicate
    with st.container():
        fc1, fc2 = st.columns([1, 2])
        
        with fc1:
            statuses = st.multiselect(
                "Status",
                ["planned", "open", "closed"],
                default=[],
                key="aylist_status_filter",
            )
        
        with fc2:
            query = st.text_input(
                "Search AY code",
                placeholder="e.g., 2025-26",
                key="aylist_search_q",
            )
        
        # Fetch data
        with _safe_conn(engine) as conn:
            rows = get_all_ays(conn) or []
        
        df = _df_or_empty(rows, columns=["code", "start_date", "end_date", "status", "updated_at"])
        
        # Apply filters
        if statuses:
            df = df[df["status"].isin(statuses)]
        if query:
            q = str(query).strip().lower()
            df = df[df["code"].str.lower().str.contains(q, na=False)]
        
        st.caption(f"{len(df)} result(s)")
        
        # Pagination
        if len(df) == 0:
            st.info("No academic years found.")
            return
        
        page_size = st.selectbox(
            "Rows per page",
            options=[10, 25, 50, 100, len(df)],
            index=1 if len(df) >= 25 else 0,
            format_func=lambda x: "All" if x == len(df) else str(x),
            key="aylist_page_size",
        )
        
        if page_size and page_size != len(df):
            pages = (len(df) + page_size - 1) // page_size
            page = st.number_input(
                "Page",
                min_value=1,
                max_value=int(pages),
                value=1,
                step=1,
                key="aylist_page_num",
            )
            start = (int(page) - 1) * page_size
            end = start + page_size
            st.dataframe(df.iloc[start:end].reset_index(drop=True), use_container_width=True)
        else:
            st.dataframe(df.reset_index(drop=True), use_container_width=True)

def render_ay_editor(engine: Engine, roles: Sequence[str], email: str) -> None:
    st.subheader("✏️ Create / Edit Academic Year")
    # ... (code omitted for brevity) ...
    mode = st.radio(
        "Mode",
        options=["Create", "Edit"],
        horizontal=True,
        key="ayed_mode",
    )
    edit_mode = mode == "Edit"
    with _safe_conn(engine) as conn:
        latest_code = get_latest_ay_code(conn)
        ay_codes = [r["code"] if isinstance(r, dict) else (r[0] if r else "") for r in get_all_ays(conn) or []]
    if edit_mode:
        code = st.selectbox("Select AY", options=[""] + sorted(ay_codes, reverse=True), key="ayed_select_code")
        if not code:
            st.info("Choose an AY to edit.")
            return
        with _safe_conn(engine) as conn:
            record = get_ay_by_code(conn, code) or {}
        default_code = code
        default_start = record.get("start_date") if isinstance(record, dict) else None
        default_end = record.get("end_date") if isinstance(record, dict) else None
    else:
        default_code = get_next_ay_code(latest_code) if latest_code else ""
        default_start, default_end = None, None
    c1, c2, c3 = st.columns(3)
    with c1:
        ay_code = st.text_input("AY Code", value=default_code or "", key="ayed_code")
    with c2:
        start_date = st.date_input("Start Date", value=default_start, key="ayed_start")
    with c3:
        end_date = st.date_input("End Date", value=default_end, key="ayed_end")
    st.caption("Format examples: 2025-26, 2025/26, AY2025-26")
    col_a, col_b = st.columns([1, 1])
    with col_a:
        if st.button("💾 Save", key="ayed_save"):
            if not ay_code or not is_valid_ay_code(ay_code):
                st.error("Please enter a valid AY code (e.g., 2025-26 or AY2025-26).")
                return
            if not start_date or not end_date or start_date >= end_date:
                st.error("Start Date must be before End Date.")
                return
            if not validate_ay_code_dates(ay_code, start_date):
                st.error(f"AY Code {ay_code} start year does not logically match Start Date {start_date.isoformat()}. Check if the date is within the expected academic year range.")
                return
            try:
                with engine.begin() as conn:
                    conflict = check_overlap(conn, start_date.isoformat(), end_date.isoformat(), exclude_code=ay_code if edit_mode else None)
                    if conflict:
                        st.error(f"Date range overlaps with {conflict}")
                        return
                    if edit_mode:
                        update_ay_dates(conn, ay_code, start_date.isoformat(), end_date.isoformat(), actor=email)
                        st.success(f"Updated AY {ay_code}.")
                    else:
                        insert_ay(conn, ay_code, start_date.isoformat(), end_date.isoformat(), actor=email)
                        st.success(f"Created AY {ay_code}.")
            except Exception as e:
                st.error(f"Failed to save AY: {e}")
                st.code(traceback.format_exc())
    with col_b:
        if edit_mode and st.button("🗑️ Delete", key="ayed_delete"):
            try:
                with engine.begin() as conn:
                    delete_ay(conn, ay_code, actor=email)
                st.success(f"Deleted AY {ay_code}.")
            except Exception as e:
                _handle_error(e, "Failed to delete AY")

# ----------------------------
# AY STATUS
# ... (No changes in this section) ...
# ----------------------------
def render_ay_status_changer(engine: Engine, roles: Sequence[str], email: str) -> None:
    st.subheader("🔄 Change AY Status")
    # ... (code omitted for brevity) ...
    with _safe_conn(engine) as conn:
        rows = get_all_ays(conn) or []
    codes = [r["code"] if isinstance(r, dict) else (r[0] if r else "") for r in rows]
    statuses = ["planned", "open", "closed"]
    c1, c2 = st.columns(2)
    with c1:
        code = st.selectbox("Select AY", options=[""] + sorted(codes, reverse=True), key="aystatus_code")
    with c2:
        new_status = st.selectbox("New Status", options=statuses, key="aystatus_new_status")
    if st.button("Update Status", key="aystatus_update_btn"):
        if not code:
            st.error("Select an academic year.")
            return
        try:
            with engine.begin() as conn:
                update_ay_status(conn, code, new_status, actor=email)
            st.success(f"Status for {code} set to {new_status}.")
        except Exception as e:
            _handle_error(e, "Failed to update status")
            
# ----------------------------
# CALENDAR PROFILE EDITOR (REBUILT)
# ----------------------------
def render_calendar_profiles(engine: Engine, roles: Sequence[str], email: str) -> None:
    st.subheader("🗂️ Calendar Profile Editor")

    with _safe_conn(engine) as conn:
        profiles = get_assignable_calendar_profiles(conn) or []
    
    profile_map = {p['name']: p['id'] for p in profiles}
    profile_id_map = {p['id']: p for p in profiles}

    st.markdown("Select an existing profile to clone it into the editor, or create a new one.")
    
    clone_name = st.selectbox("Clone from Profile", 
                              options=[""] + sorted(profile_map.keys()),
                              key="profedit_clone_name")

    with st.form(key="profile_editor_form"):
        # Load defaults if cloning
        default_terms = [{"label": "Term 1", "start_mmdd": "07-01", "end_mmdd": "12-15"}]
        defaults = {"code": "", "name": "", "anchor": "07-01", "model": "2-Term"}
        
        if clone_name and 'clone_data' not in st.session_state:
            st.session_state.clone_data = profile_id_map.get(profile_map[clone_name])
        
        if 'clone_data' in st.session_state and st.session_state.clone_data:
            clone = st.session_state.clone_data
            defaults["code"] = f"{clone['code']}_clone"
            defaults["name"] = f"{clone['name']} (Clone)"
            defaults["anchor"] = clone['anchor_mmdd']
            defaults["model"] = clone['model']
            try:
                default_terms = json.loads(clone['term_spec_json']) 
            except Exception:
                pass 
            
            if st.button("Clear clone data", key="profedit_clear_clone"):
                del st.session_state.clone_data
                st.rerun()

        st.text("Note: Editing locked or system profiles is disabled. They must be cloned.")
        
        c1, c2 = st.columns(2)
        with c1:
            code = st.text_input("Profile Code (Unique)", value=defaults["code"])
            name = st.text_input("Profile Name", value=defaults["name"])
        with c2:
            model = st.text_input("Model (Label)", value=defaults["model"], help="e.g., '2-Term', 'Quarter', '5-Year/10-Term'")
            anchor_mmdd = st.text_input("Anchor MM-DD", value=defaults["anchor"], help="Informational, e.g., '07-01'")

        st.divider()
        
        # --- DATA EDITOR for TERMS ---
        st.subheader("Term Specification")
        st.caption("Use the 'Add Row' button below to add more terms (e.g., Sem 1, Sem 2, etc.)")
        
        edited_terms = st.data_editor(
            default_terms,
            num_rows="dynamic",
            column_config={
                "label": st.column_config.TextColumn("Term Label", help="e.g., 'Sem 1', 'Term 1', 'Q1'", required=True),
                "start_mmdd": st.column_config.TextColumn("Start (MM-DD)", help="Format 'MM-DD', e.g., '10-01'", required=True),
                "end_mmdd": st.column_config.TextColumn("End (MM-DD)", help="Format 'MM-DD', e.g., '02-15'", required=True),
            },
            key="profedit_term_editor"
        )
        # --- END: DATA EDITOR ---

        submitted = st.form_submit_button("💾 Save New Profile")
        if submitted:
            valid = True
            if not code or not name or not anchor_mmdd or not model:
                st.error("All profile fields (Code, Name, Model, Anchor) are required.")
                valid = False
            
            if not edited_terms:
                st.error("At least one term is required in the Term Specification.")
                valid = False
            
            for i, term in enumerate(edited_terms):
                if not term.get("label") or not term.get("start_mmdd") or not term.get("end_mmdd"):
                    st.error(f"Row {i+1} in Term Specification is missing data.")
                    valid = False
                if len(term.get("start_mmdd", "")) != 5 or len(term.get("end_mmdd", "")) != 5:
                    st.error(f"Row {i+1}: Dates must be in MM-DD format (e.g., '10-01').")
                    valid = False
            
            if valid:
                try:
                    spec_json = json.dumps(edited_terms) 
                    
                    with engine.begin() as conn:
                        insert_calendar_profile(conn, code, name, model, anchor_mmdd, spec_json)
                    st.success(f"Profile '{name}' created successfully.")
                    if 'clone_data' in st.session_state:
                        del st.session_state.clone_data 
                except Exception as e:
                    _handle_error(e, "Failed to save profile (is the 'code' unique?)")

    st.divider()
    st.subheader("Existing Profiles")
    df = _df_or_empty(profiles, columns=["id", "name", "code", "model", "locked", "is_system"])
    st.dataframe(df, use_container_width=True)


# ----------------------------
# ASSIGNMENT EDITOR (UPDATED with Validation)
# ----------------------------
def render_calendar_assignment_editor(engine: Engine, roles: Sequence[str], email: str) -> None:
    st.subheader("📝 Create / Edit Calendar Assignment")
    st.info("Here you define the rules that link a Degree/Program/Branch (for a specific AY and Year of Study) to a Calendar Profile.")

    with _safe_conn(engine) as conn:
        degrees = [d["code"] for d in get_all_degrees(conn) or []]
        ay_rows = get_all_ays(conn) or []
        all_ays = [r["code"] for r in ay_rows]
        profiles = get_assignable_calendar_profiles(conn) or []
        profile_map = {p['name']: p['id'] for p in profiles}

    level = st.radio(
        "Assignment Level",
        options=["degree", "program", "branch"],
        format_func=str.capitalize,
        horizontal=True,
        key="caledit_level",
    )

    c1, c2, c3 = st.columns(3)
    with c1:
        deg = st.selectbox("Degree", options=[""] + degrees, key="caledit_deg")
    
    max_duration = 10 
    if deg:
        with _safe_conn(engine) as conn:
            max_duration = get_degree_duration(conn, deg)
    
    progs = []
    if deg:
        with _safe_conn(engine) as conn:
            progs = [p["program_code"] for p in get_programs_for_degree(conn, deg) or []]
    
    branches = []
    current_prog = st.session_state.get("caledit_prog")
    if deg and current_prog:
        with _safe_conn(engine) as conn:
            branches = [b["branch_code"] for b in get_branches_for_degree_program(conn, deg, current_prog) or []]

    with c2:
        prog_disabled = level == 'degree' or not deg
        prog = st.selectbox("Program", 
                            options=[""] + progs, 
                            key="caledit_prog",
                            disabled=prog_disabled)

    with c3:
        branch_disabled = level != 'branch' or not prog
        br = st.selectbox("Branch", 
                          options=[""] + branches, 
                          key="caledit_branch",
                          disabled=branch_disabled)
    
    st.divider()
    
    c4, c5, c6, c7 = st.columns(4)
    with c4:
        ay = st.selectbox("Effective From AY", 
                          options=[""] + all_ays, 
                          key="caledit_ay",
                          help="This rule will apply from this AY onwards, until a new rule overrides it.")
    with c5:
        prog_year = st.number_input("Year of Study", 
                                    min_value=1, 
                                    max_value=max_duration,
                                    value=1, 
                                    step=1, 
                                    key="caledit_progyear")
    with c6:
        cal_name = st.selectbox("Calendar Profile", 
                                options=[""] + sorted(profile_map.keys()), 
                                key="caledit_cal_name",
                                help="The calendar to apply for this rule.")
    with c7:
        shift_days = st.number_input("Shift Days", 
                                     min_value=-30, 
                                     max_value=30, 
                                     value=0, 
                                     step=1, 
                                     key="caledit_shift",
                                     help="Shift all dates in the profile by this many days.")

    if st.button("💾 Save Assignment", key="caledit_save"):
        valid = True
        if not deg:
            st.error("Degree is required.")
            valid = False
        if level == 'program' and not prog:
            st.error("Program is required for Program-level assignment.")
            valid = False
        if level == 'branch' and not br:
            st.error("Branch is required for Branch-level assignment.")
            valid = False
        if not ay:
            st.error("Effective From AY is required.")
            valid = False
        if not cal_name:
            st.error("Calendar Profile is required.")
            valid = False
        
        if valid:
            try:
                cal_id = profile_map[cal_name]
                prog_param = prog if level in ('program', 'branch') else None
                branch_param = br if level == 'branch' else None

                with engine.begin() as conn:
                    
                    # --- NEW: MISMATCH VALIDATION & ROLE CHECK ---
                    expected_terms = get_degree_terms_per_year(conn, deg)
                    profile_terms = get_profile_term_count(conn, cal_id)
                    
                    mismatch = False
                    if expected_terms > 0 and profile_terms > 0 and expected_terms != profile_terms:
                        mismatch = True

                    is_superadmin = any(r in ('superadmin', 'director', 'principal') for r in roles)

                    if mismatch and not is_superadmin:
                        st.error(f"Validation Error: This degree is defined with {expected_terms} terms per year, but the selected profile has {profile_terms} terms. Only a superadmin or director can make this assignment.")
                        return # Block the save
                    
                    if mismatch and is_superadmin:
                        st.warning(f"Admin Override: You are applying a {profile_terms}-term profile to a {expected_terms}-term degree structure.")
                    # --- END: VALIDATION ---

                    insert_calendar_assignment(
                        conn,
                        level=level,
                        degree_code=deg,
                        program_code=prog_param,
                        branch_code=branch_param,
                        effective_from_ay=ay,
                        progression_year=prog_year,
                        calendar_id=cal_id,
                        shift_days=shift_days,
                        actor=email
                    )
                st.success("Calendar assignment saved successfully.")
            except Exception as e:
                _handle_error(e, "Failed to save assignment")


# ----------------------------
# ASSIGNMENT PREVIEW (Rebuilt for 'All Years')
# ----------------------------
def render_calendar_assignments(engine: Engine, roles: Sequence[str], email: str) -> None:
    st.subheader("📌 Assignment Preview (Full Degree)")
    st.info("Preview the calculated terms for all years of a degree in a specific AY.")

    with _safe_conn(engine) as conn:
        degrees = [d["code"] for d in get_all_degrees(conn) or []]

    level = st.radio(
        "Assignment Level",
        options=["degree", "program", "branch"],
        format_func=str.capitalize,
        horizontal=True,
        key="calassn_level",
    )

    c1, c2, c3, c4, c5 = st.columns(5)
    
    with c1:
        deg = st.selectbox("Degree", options=[""] + degrees, key="calassn_deg")
    
    default_duration = 5  
    if deg:
        with _safe_conn(engine) as conn:
            default_duration = get_degree_duration(conn, deg)
    
    progs = []
    if deg:
        with _safe_conn(engine) as conn:
            progs = [p["program_code"] for p in get_programs_for_degree(conn, deg) or []]
    
    branches = []
    current_prog = st.session_state.get("calassn_prog")
    if deg and current_prog:
        with _safe_conn(engine) as conn:
            branches = [b["branch_code"] for b in get_branches_for_degree_program(conn, deg, current_prog) or []]

    with c2:
        prog_disabled = level == 'degree' or not deg
        prog = st.selectbox("Program", 
                            options=[""] + progs, 
                            key="calassn_prog",
                            disabled=prog_disabled)

    with c3:
        branch_disabled = level != 'branch' or not prog
        br = st.selectbox("Branch", 
                          options=[""] + branches, 
                          key="calassn_branch",
                          disabled=branch_disabled)

    with c4:
        with _safe_conn(engine) as conn:
            ay_rows = get_all_ays(conn) or []
        all_ays = [r["code"] for r in ay_rows]
        ay = st.selectbox("Preview AY", options=[""] + all_ays, key="calassn_ay")
        
    with c5:
        total_years = st.number_input("Total Years in Degree", 
                                    min_value=1, 
                                    max_value=10, 
                                    value=default_duration, 
                                    step=1, 
                                    key="calassn_totalyears")

    ready_to_preview = bool(deg and ay and total_years)
    if level == 'program' and not prog:
        ready_to_preview = False
        st.warning("Please select a Program.")
    elif level == 'branch' and not br:
        ready_to_preview = False
        st.warning("Please select a Branch.")
    if not ready_to_preview:
        st.info(f"Choose an AY, {level.capitalize()}, and Total Years to preview.")
        return

    prog_param = prog if level in ('program', 'branch') and prog else None
    branch_param = br if level == 'branch' and br else None

    if st.button("Preview Full Schedule", key="calassn_preview_btn"):
        st.subheader("Calculated Terms by Year of Study")
        st.caption(f"Showing schedule for **{deg}{f' / {prog}' if prog else ''}{f' / {br}' if br else ''}** in AY **{ay}**.")
        
        try:
            with _safe_conn(engine) as conn:
                for year in range(1, total_years + 1):
                    terms, warnings = compute_terms_with_validation(
                        conn, ay, deg, prog_param, branch_param, progression_year=year
                    )
                    with st.expander(f"**Year {year} of Study**", expanded=year==1):
                        if warnings:
                            for w in warnings:
                                st.warning(w)
                        if terms:
                            term_df = pd.DataFrame(terms)
                            st.dataframe(term_df, use_container_width=True)
                        else:
                            st.info("No terms calculated based on current assignments for this year.")
        except Exception as e:
            st.error(f"Failed to preview terms: {e}")
            st.code(traceback.format_exc())
