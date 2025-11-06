# screens/academic_years/ui.py
# -------------------------------------------------------------------
# MODIFIED VERSION
# - Batches-aware Assignment Preview.
# - "Total Years" is driven by degree_semester_struct.years.
# - Assignment Preview radio options adapt to whether the degree
#   actually has programs / branches.
# - All previous UI functions are preserved.
# -------------------------------------------------------------------
from __future__ import annotations

import traceback
from typing import Optional, Sequence

import pandas as pd
import streamlit as st
from sqlalchemy.engine import Engine
import json


# ------------------------------------------------------------
# Utility imports (soft-fail with fallbacks so UI doesn't die)
# ------------------------------------------------------------
try:
    from screens.academic_years.utils import (
        is_valid_ay_code,
        parse_date_range,
        validate_ay_dates,
        get_next_ay_code,
        get_ay_status_display,
        _get_year_from_ay_code,
    )
except Exception:
    # VERY small no-op fallbacks so the whole UI doesn't crash
    def is_valid_ay_code(code: str) -> bool:
        return bool(code and len(str(code)) >= 4)

    def parse_date_range(start, end):
        return start, end

    def validate_ay_dates(start, end):
        return []

    def get_next_ay_code(latest: Optional[str]) -> str:
        return ""

    def get_ay_status_display(status: str) -> str:
        return status or ""

    def _get_year_from_ay_code(code: str) -> Optional[int]:
        try:
            if not code:
                return None
            parts = str(code).split("-")
            return int(parts[0])
        except Exception:
            return None


# ------------------------------------------------------------
# DB imports (soft-fail with trivial fallbacks)
# ------------------------------------------------------------
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
        # Degree + structure
        get_all_degrees,
        get_degree_duration,
        get_degree_terms_per_year,
        get_programs_for_degree,
        get_branches_for_degree_program,
        # Calendar profiles
        get_assignable_calendar_profiles,
        get_calendar_profile_by_id,
        get_profile_term_count,
        insert_calendar_profile,
        # Calendar assignments & term computation
        insert_calendar_assignment,
        compute_terms_with_validation,
        # Batch / student helpers
        _db_check_batch_has_students,
        get_semester_mapping_for_year,
        _db_get_batches_for_degree,
    )
except Exception:
    # These are only to keep the UI from exploding if something is missing;
    # in a real app your actual db.py should be imported successfully.
    def get_all_ays(conn): return []
    def get_ay_by_code(conn, code): return None
    def insert_ay(conn, code, start_date, end_date, actor=None): return True
    def update_ay_dates(conn, code, start_date, end_date, actor=None): return True
    def update_ay_status(conn, code, status): return True
    def delete_ay(conn, code, actor=None): return True
    def check_overlap(conn, start_date, end_date, exclude_code=None): return None
    def get_latest_ay_code(conn): return None
    def get_all_degrees(conn): return []
    def get_degree_duration(conn, code): return 4
    def get_degree_terms_per_year(conn, code): return 0
    def get_programs_for_degree(conn, d): return []
    def get_branches_for_degree_program(conn, d, p): return []
    def get_assignable_calendar_profiles(conn): return []
    def get_calendar_profile_by_id(conn, id): return None
    def get_profile_term_count(conn, id): return 0
    def insert_calendar_profile(conn, code, name, model, anchor, spec_json): pass
    def insert_calendar_assignment(conn, level, degree_code, program_code,
                                   branch_code, effective_from_ay,
                                   progression_year, calendar_id, shift_days,
                                   actor=None): pass
    def compute_terms_with_validation(conn, ay, d, p, b, progression_year):
        return [], ["Fallback term computation (db import failed)."]
    def _db_check_batch_has_students(conn, degree_code, batch_code): return False
    def get_semester_mapping_for_year(conn, degree_code, year_index, program_code=None, branch_code=None): return {}
    def _db_get_batches_for_degree(conn, degree_code): return []

# ------------------------------------------------------------
# Import from Students module (for batch-based preview)
# ------------------------------------------------------------
#try:
#    from screens.students.db import _db_get_batches_for_degree
#except Exception:
#    st.warning("Student module not found. Batch-based Assignment Preview will be limited.")

#    def _db_get_batches_for_degree(conn, degree_code):
#        return []


# ------------------------------------------------------------
# Small helpers
# ------------------------------------------------------------
def _safe_conn(engine: Engine):
    """Context manager-ish wrapper to get a connection or stop the app."""
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


def _handle_error(e: Exception, message: str) -> None:
    st.error(f"{message}: {e}")
    st.code(traceback.format_exc())


# ------------------------------------------------------------
# Academic Year List
# ------------------------------------------------------------
def render_ay_list(engine: Engine) -> None:
    st.subheader("🗓️ Academic Years")

    # Filters
    fc1, fc2 = st.columns([1, 2])
    with fc1:
        status_filter = st.multiselect(
            "Filter by Status",
            options=["planned", "open", "closed"],
            default=[],
            key="aylist_status_filter",
        )
    with fc2:
        query = st.text_input(
            "Search AY code",
            placeholder="e.g., 2025-26",
            key="aylist_search_q",
        )

    # Data
    with _safe_conn(engine) as conn:
        rows = get_all_ays(conn) or []

    # Expect columns: code, start_date, end_date, status, updated_at (if present)
    # Be tolerant if updated_at is missing.
    cols = ["code", "start_date", "end_date", "status"]
    has_updated = any("updated_at" in r for r in rows) if rows else False
    if has_updated:
        cols.append("updated_at")

    df = _df_or_empty(rows, columns=cols)

    # Filters
    if status_filter:
        df = df[df["status"].isin(status_filter)]
    if query:
        q = str(query).strip().lower()
        df = df[df["code"].str.lower().str.contains(q, na=False)]

    st.caption(f"{len(df)} result(s)")
    if df.empty:
        st.info("No academic years found.")
        return

    # Simple pagination
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
        df_to_show = df.iloc[start:end]
    else:
        df_to_show = df

    st.dataframe(df_to_show, use_container_width=True)


# ------------------------------------------------------------
# Academic Year Editor
# ------------------------------------------------------------
def render_ay_editor(engine: Engine, roles: Sequence[str], email: str) -> None:
    st.subheader("✏️ Create / Edit Academic Year")

    if "admin" not in roles and "superadmin" not in roles:
        st.info("You do not have permission to edit Academic Years.")
        return

    with _safe_conn(engine) as conn:
        rows = get_all_ays(conn) or []
        latest_code = get_latest_ay_code(conn)

    codes = [r["code"] if isinstance(r, dict) else r[0] for r in rows] if rows else []

    selected_code = st.selectbox(
        "Select AY to edit (or blank for new):",
        options=[""] + codes,
        key="ayed_select_code",
    )

    edit_mode = bool(selected_code)

    if edit_mode:
        with _safe_conn(engine) as conn:
            record = get_ay_by_code(conn, selected_code) or {}
        # record may be a dict or Row
        if isinstance(record, dict):
            default_code = record.get("code", selected_code)
            default_start = record.get("start_date")
            default_end = record.get("end_date")
        else:
            # row-like
            default_code = getattr(record, "code", selected_code)
            default_start = getattr(record, "start_date", None)
            default_end = getattr(record, "end_date", None)
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

    col_a, col_b = st.columns(2)
    with col_a:
        if st.button("💾 Save", key="ayed_save"):
            # Validation
            if not ay_code:
                st.error("AY Code is required.")
                return
            if not is_valid_ay_code(ay_code):
                st.error("AY Code format is invalid.")
                return
            errs = validate_ay_dates(start_date, end_date)
            if errs:
                for e in errs:
                    st.error(e)
                return

            try:
                with engine.begin() as conn:
                    conflict = check_overlap(
                        conn,
                        start_date.isoformat(),
                        end_date.isoformat(),
                        exclude_code=ay_code if edit_mode else None,
                    )
                    if conflict:
                        st.error(f"Date range overlaps with {conflict}")
                        return

                    if edit_mode:
                        # We do not support changing AY code for now.
                        update_ay_dates(
                            conn,
                            ay_code,
                            start_date.isoformat(),
                            end_date.isoformat(),
                            actor=email,
                        )
                        st.success(f"Updated AY {ay_code}.")
                    else:
                        insert_ay(
                            conn,
                            ay_code,
                            start_date.isoformat(),
                            end_date.isoformat(),
                            actor=email,
                        )
                        st.success(f"Created AY {ay_code}.")
            except Exception as e:
                _handle_error(e, "Failed to save AY")

    with col_b:
        if edit_mode and st.button("🗑️ Delete", key="ayed_delete"):
            try:
                with engine.begin() as conn:
                    delete_ay(conn, ay_code, actor=email)
                st.success(f"Deleted AY {ay_code}.")
            except Exception as e:
                _handle_error(e, "Failed to delete AY")


# ------------------------------------------------------------
# AY Status Changer
# ------------------------------------------------------------
def render_ay_status_changer(engine: Engine, roles: Sequence[str], email: str) -> None:
    st.subheader("🔄 Change AY Status")

    if "admin" not in roles and "superadmin" not in roles:
        st.info("You do not have permission to change AY status.")
        return

    with _safe_conn(engine) as conn:
        rows = get_all_ays(conn) or []

    codes = [r["code"] if isinstance(r, dict) else r[0] for r in rows] if rows else []
    if not codes:
        st.info("No Academic Years found.")
        return

    c1, c2 = st.columns(2)
    with c1:
        code = st.selectbox("Select AY", options=[""] + codes, key="aystat_code")
    with c2:
        status = st.selectbox(
            "New Status",
            options=["planned", "open", "closed"],
            key="aystat_status",
        )

    if st.button("Update Status", key="aystat_btn"):
        if not code:
            st.warning("Please select an AY.")
            return
        try:
            with engine.begin() as conn:
                update_ay_status(conn, code, status)
            st.success(f"Updated status of {code} to {status}.")
        except Exception as e:
            _handle_error(e, "Failed to update AY status")


# ------------------------------------------------------------
# Calendar Profiles
# ------------------------------------------------------------
def render_calendar_profiles(engine: Engine, roles: Sequence[str], email: str) -> None:
    st.subheader("📐 Calendar Profiles")

    if "admin" not in roles and "superadmin" not in roles:
        st.info("You do not have permission to manage calendar profiles.")
        return

    # Load profiles once
    with _safe_conn(engine) as conn:
        profiles_raw = get_assignable_calendar_profiles(conn) or []

    profiles = []
    with _safe_conn(engine) as conn:
        for pr in profiles_raw:
            d = dict(pr)
            try:
                d["terms_per_year"] = get_profile_term_count(conn, d["id"])
            except Exception:
                d["terms_per_year"] = None
            profiles.append(d)

    st.caption(
        "Profiles define how an Academic Year is broken into terms "
        "(semesters, trimesters, etc.)."
    )

    model_options = ["2-Term", "3-Term", "Custom"]
    default_model = "2-Term"

    # --- Create / Edit ---
    with st.expander("➕ Create / Edit Profile", expanded=True):
        profile_map = {p["name"]: p["id"] for p in profiles}
        profile_id_map = {p["id"]: p for p in profiles}

        selected_name = st.selectbox(
            "Existing Profile (optional):",
            options=[""] + sorted(profile_map.keys()),
            key="profedit_selected_name",
        )
        edit_mode = bool(selected_name)

        clone_name = st.selectbox(
            "Clone from Profile",
            options=[""] + sorted(profile_map.keys()),
            key="profedit_clone_name",
        )

        # Defaults
        default_terms = [
            {"label": "Term 1", "start_mmdd": "07-01", "end_mmdd": "12-15"},
        ]
        defaults = {
            "code": "",
            "name": "",
            "anchor": "07-01",
            "model": "2-Term",
        }

        # Load clone defaults (outside the form)
        if clone_name and "clone_data" not in st.session_state:
            st.session_state.clone_data = profile_id_map.get(profile_map[clone_name])

        if "clone_data" in st.session_state and st.session_state.clone_data:
            clone = st.session_state.clone_data
            defaults["code"] = f"{clone['code']}_clone"
            defaults["name"] = f"{clone['name']} (Clone)"
            defaults["anchor"] = clone.get("anchor_mmdd", "07-01")
            defaults["model"] = clone.get("model", "2-Term")
            try:
                default_terms = json.loads(clone.get("term_spec_json", "[]")) or default_terms
            except Exception:
                default_terms = default_terms

        # Edit mode overrides clone defaults
        if edit_mode:
            p = profile_id_map[profile_map[selected_name]]
            defaults["code"] = p.get("code", "")
            defaults["name"] = p.get("name", "")
            defaults["anchor"] = p.get("anchor_mmdd", "07-01")
            defaults["model"] = p.get("model", "2-Term")
            try:
                default_terms = json.loads(p.get("term_spec_json", "[]")) or default_terms
            except Exception:
                default_terms = default_terms

        # --- Form ---
        with st.form("calendar_profile_form"):
            c1, c2, c3 = st.columns([2, 2, 1])
            with c1:
                code = st.text_input(
                    "Profile Code",
                    value=defaults["code"],
                    key="profedit_code",
                )
            with c2:
                name = st.text_input(
                    "Profile Name",
                    value=defaults["name"],
                    key="profedit_name",
                )
            with c3:
                model = st.selectbox(
                    "Model",
                    options=model_options,
                    index=model_options.index(
                        defaults["model"] if defaults["model"] in model_options else default_model
                    ),
                    key="profedit_model",
                )

            anchor_mmdd = st.text_input(
                "Anchor Date (MM-DD)",
                value=defaults["anchor"],
                key="profedit_anchor",
            )

            st.markdown("**Terms per Year**")
            terms = default_terms.copy()
            edited_terms = []
            for idx, term in enumerate(terms):
                col1, col2, col3, col4 = st.columns([2, 2, 2, 1])
                with col1:
                    label = st.text_input(
                        f"Label {idx+1}",
                        value=term.get("label", f"Term {idx+1}"),
                        key=f"term_label_{idx}",
                    )
                with col2:
                    start_mmdd = st.text_input(
                        f"Start (MM-DD) {idx+1}",
                        value=term.get("start_mmdd", "07-01"),
                        key=f"term_start_{idx}",
                    )
                with col3:
                    end_mmdd = st.text_input(
                        f"End (MM-DD) {idx+1}",
                        value=term.get("end_mmdd", "12-15"),
                        key=f"term_end_{idx}",
                    )
                with col4:
                    st.write(" ")
                    remove = st.checkbox("Remove", key=f"term_remove_{idx}")

                if not remove:
                    edited_terms.append(
                        {
                            "label": label,
                            "start_mmdd": start_mmdd,
                            "end_mmdd": end_mmdd,
                        }
                    )

            if st.form_submit_button("Save Profile"):
                if not code or not name:
                    st.error("Code and Name are required.")
                elif not anchor_mmdd:
                    st.error("Anchor date is required.")
                elif not edited_terms:
                    st.error("At least one term is required.")
                else:
                    valid = True
                    for t in edited_terms:
                        if not t["label"] or not t["start_mmdd"] or not t["end_mmdd"]:
                            st.error("All term fields are required.")
                            valid = False
                            break

                    if valid:
                        try:
                            spec_json = json.dumps(edited_terms)
                            with engine.begin() as conn:
                                insert_calendar_profile(
                                    conn,
                                    code,
                                    name,
                                    model,
                                    anchor_mmdd,
                                    spec_json,
                                )
                            st.success(f"Profile '{name}' saved successfully.")
                            if "clone_data" in st.session_state:
                                del st.session_state.clone_data
                        except Exception as e:
                            _handle_error(e, "Failed to save profile (is the 'code' unique?)")

    # --- Existing Profiles table ---
    st.divider()
    st.subheader("Existing Profiles")
    df = _df_or_empty(
        profiles,
        columns=["id", "name", "code", "model", "locked", "is_system", "terms_per_year"],
    )
    if df.empty:
        st.info("No profiles configured yet.")
        return
    st.dataframe(df, use_container_width=True)


# ------------------------------------------------------------
# Calendar Assignment Editor (rules)
# ------------------------------------------------------------
def render_calendar_assignment_editor(engine: Engine, roles: Sequence[str], email: str) -> None:
    st.subheader("📝 Create / Edit Calendar Assignment")
    st.info(
        "Here you define the rules that link a Degree / Program / Branch "
        "(for a specific AY and Year of Study) to a Calendar Profile."
    )

    if "admin" not in roles and "superadmin" not in roles:
        st.info("You do not have permission to edit calendar assignments.")
        return

    with _safe_conn(engine) as conn:
        degrees = [d["code"] for d in get_all_degrees(conn) or []]
        ay_rows = get_all_ays(conn) or []
        all_ays = [r["code"] for r in ay_rows]
        profiles = get_assignable_calendar_profiles(conn) or []
        profile_map = {p["name"]: p["id"] for p in profiles}

    # --- Degree selection first ---
    c_deg, _, _ = st.columns(3)
    with c_deg:
        deg = st.selectbox("Degree", options=[""] + degrees, key="caledit_deg")

    # Determine what structures actually exist for this degree
    progs: list[str] = []
    has_programs = False
    has_branches = False

    if deg:
        with _safe_conn(engine) as conn:
            progs = [p["program_code"] for p in get_programs_for_degree(conn, deg) or []]
            has_programs = len(progs) > 0
            all_branches_for_degree = [
                b["branch_code"]
                for b in get_branches_for_degree_program(conn, deg, None) or []
            ]
            has_branches = len(all_branches_for_degree) > 0

    # --- Assignment Level radio, restricted by structure ---
    allowed_levels = ["degree"]
    if has_programs:
        allowed_levels.append("program")
    if has_branches:
        allowed_levels.append("branch")

    prev_level = st.session_state.get("caledit_level", "degree")
    if prev_level not in allowed_levels:
        prev_level = "degree"
        st.session_state["caledit_level"] = "degree"

    level = st.radio(
        "Assignment Level",
        options=allowed_levels,
        index=allowed_levels.index(prev_level),
        format_func=str.capitalize,
        horizontal=True,
        key="caledit_level",
    )

    # Degree duration for Year-of-Study max
    max_duration = 10
    if deg:
        with _safe_conn(engine) as conn:
            max_duration = get_degree_duration(conn, deg)

    # Programs & branches for this degree
    branches: list[str] = []
    current_prog_state = st.session_state.get("caledit_prog")
    current_prog = current_prog_state if current_prog_state else None

    if deg and current_prog:
        with _safe_conn(engine) as conn:
            branches = [
                b["branch_code"]
                for b in get_branches_for_degree_program(conn, deg, current_prog) or []
            ]

    c1, c2, c3 = st.columns(3)
    with c1:
        st.write("")  # Degree already chosen above (kept column structure)

    with c2:
        prog_disabled = (level == "degree") or (not deg) or (not has_programs)
        prog = st.selectbox(
            "Program",
            options=[""] + progs,
            key="caledit_prog",
            disabled=prog_disabled,
        )

    # Branch options depend on the chosen program
    branches = []
    if deg and prog:
        with _safe_conn(engine) as conn:
            branches = [
                b["branch_code"]
                for b in get_branches_for_degree_program(conn, deg, prog) or []
            ]

    with c3:
        branch_disabled = (level != "branch") or (not prog) or (not has_branches)
        br = st.selectbox(
            "Branch",
            options=[""] + branches,
            key="caledit_branch",
            disabled=branch_disabled,
        )

    st.divider()

    c4, c5, c6, c7 = st.columns(4)
    with c4:
        ay = st.selectbox(
            "Effective From AY",
            options=[""] + all_ays,
            key="caledit_ay",
            help=(
                "This rule will apply from this AY onwards, until a new rule "
                "overrides it."
            ),
        )
    with c5:
        prog_year = st.number_input(
            "Year of Study",
            min_value=1,
            max_value=max_duration,
            value=1,
            step=1,
            key="caledit_progyear",
        )
    with c6:
        cal_name = st.selectbox(
            "Calendar Profile",
            options=[""] + sorted(profile_map.keys()),
            key="caledit_cal_name",
            help="The calendar profile to apply for this rule.",
        )
    with c7:
        shift_days = st.number_input(
            "Shift Days",
            min_value=-30,
            max_value=30,
            value=0,
            step=1,
            key="caledit_shift",
            help="Shift all dates in the selected profile by this many days.",
        )

    # --- Save ---
    if st.button("💾 Save Assignment", key="caledit_save"):
        valid = True
        if not deg:
            st.error("Degree is required.")
            valid = False
        if level == "program" and not prog:
            st.error("Program is required for Program-level assignment.")
            valid = False
        if level == "branch" and not br:
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
                prog_param = prog if level in ("program", "branch") else None
                branch_param = br if level == "branch" else None

                with engine.begin() as conn:
                    # --- Term-count mismatch check ---
                    expected_terms = get_degree_terms_per_year(conn, deg)
                    profile_terms = get_profile_term_count(conn, cal_id)

                    mismatch = False
                    if (
                        expected_terms > 0
                        and profile_terms > 0
                        and expected_terms != profile_terms
                    ):
                        mismatch = True

                    is_superadmin = any(
                        r in ("superadmin", "director", "principal") for r in roles
                    )

                    if mismatch and not is_superadmin:
                        st.error(
                            "This calendar profile's term count does not match the "
                            "degree's expected terms per year. Only Principal / "
                            "Director / Superadmin can override this."
                        )
                        return

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
                        actor=email,
                    )

                st.success("Calendar assignment saved successfully.")
            except Exception as e:
                _handle_error(e, "Failed to save assignment")


# ------------------------------------------------------------
# Assignment Preview (batch-aware)
# ------------------------------------------------------------
def render_calendar_assignments(engine: Engine, roles: Sequence[str], email: str) -> None:
    st.subheader("📌 Assignment Preview (Full Degree)")
    st.info(
        "Preview the calculated terms for all years of study of a degree in a given AY. "
        "This uses **batches** from the Student Module to infer which batch is in which year."
    )

    with _safe_conn(engine) as conn:
        degrees = [d["code"] for d in get_all_degrees(conn) or []]
        if not degrees:
            st.warning("No active degrees found.")
            return

    # --- Degree / AY / Years ---
    c1, c2, c3 = st.columns(3)
    with c1:
        deg = st.selectbox("Degree", options=[""] + degrees, key="calassn_deg")

    with c2:
        with _safe_conn(engine) as conn:
            ay_rows = get_all_ays(conn) or []
        all_ays = [r["code"] for r in ay_rows]
        ay = st.selectbox("Preview AY", options=[""] + all_ays, key="calassn_ay")

    # Duration (from degree_semester_struct)
    fallback_duration = 1
    duration = fallback_duration
    if deg:
        with _safe_conn(engine) as conn:
            duration = get_degree_duration(conn, deg)

    # Always sync the widget state to DB value (field is disabled anyway)
    st.session_state["calassn_totalyears"] = duration

    with c3:
        total_years = st.number_input(
            "Total Years in Degree",
            min_value=1,
            max_value=20,
            value=st.session_state["calassn_totalyears"],
            step=1,
            key="calassn_totalyears",
            disabled=True,
            help="Driven by the degree's structure (degree_semester_struct.years).",
        )

    # --- Determine existing structure (programs / branches) ---
    progs: list[str] = []
    has_programs = False
    has_branches = False

    if deg:
        with _safe_conn(engine) as conn:
            progs = [p["program_code"] for p in get_programs_for_degree(conn, deg) or []]
            has_programs = len(progs) > 0
            all_branches_for_degree = [
                b["branch_code"]
                for b in get_branches_for_degree_program(conn, deg, None) or []
            ]
            has_branches = len(all_branches_for_degree) > 0

    # --- Assignment Level radio, restricted by structure ---
    allowed_levels = ["degree"]
    if has_programs:
        allowed_levels.append("program")
    if has_branches:
        allowed_levels.append("branch")

    prev_level = st.session_state.get("calassn_level", "degree")
    if prev_level not in allowed_levels:
        prev_level = "degree"
        st.session_state["calassn_level"] = "degree"

    level = st.radio(
        "Assignment Level",
        options=allowed_levels,
        index=allowed_levels.index(prev_level),
        format_func=str.capitalize,
        horizontal=True,
        key="calassn_level",
    )

    c4, c5 = st.columns(2)

    # Program selector (if the degree has programs)
    with c4:
        prog_disabled = (level == "degree") or (not deg) or (not has_programs)
        prog = st.selectbox(
            "Program",
            options=[""] + progs,
            key="calassn_prog",
            disabled=prog_disabled,
        )

    # Branch selector depends on program & structure
    branches: list[str] = []
    if deg and prog:
        with _safe_conn(engine) as conn:
            branches = [
                b["branch_code"]
                for b in get_branches_for_degree_program(conn, deg, prog) or []
            ]

    with c5:
        branch_disabled = (level != "branch") or (not prog) or (not has_branches)
        br = st.selectbox(
            "Branch",
            options=[""] + branches,
            key="calassn_branch",
            disabled=branch_disabled,
        )

    # --- Ready to preview? ---
    ready_to_preview = bool(deg and ay and total_years)
    if level == "program" and not prog:
        ready_to_preview = False
        st.warning("Please select a Program.")
    elif level == "branch" and not br:
        ready_to_preview = False
        st.warning("Please select a Branch.")

    if not ready_to_preview:
        st.info("Choose a Degree and AY (and, if applicable, Program/Branch) to preview.")
        return

    st.divider()
    st.subheader("Calculated Terms by Year of Study")
    st.caption(
        f"Showing schedule for **{deg}"
        f"{f' / {prog}' if prog else ''}"
        f"{f' / {br}' if br else ''}** in AY **{ay}**."
    )

    # --- Batch-aware logic ---
    try:
        ay_start_year = _get_year_from_ay_code(ay)
        if not ay_start_year:
            st.error("Could not parse year from AY code.")
            return

        with _safe_conn(engine) as conn:
            batches = _db_get_batches_for_degree(conn, deg) or []

        # Build lookup by intake_year -> batch
        batch_lookup = {}
        for b in batches:
            if isinstance(b, dict):
                code = b.get("code")
                intake_year = b.get("intake_year")
            else:
                code = b[0]
                intake_year = None

            if not intake_year and code and len(str(code)) >= 4:
                try:
                    intake_year = int(str(code)[:4])
                except Exception:
                    intake_year = None

            if intake_year:
                batch_lookup[str(intake_year)] = b

        st.markdown(
            """
            **Logic**

            - For each *year of study* (1..N), we compute the intake year as  
              `intake_year = ay_start_year - (year_of_study - 1)`.
            - We look up a batch whose intake year matches.
            - We show whether that batch currently has students.
            """
        )

        for year in range(1, int(total_years) + 1):
            target_batch_start_year = ay_start_year - (year - 1)
            target_batch_code = str(target_batch_start_year)

            found_batch = batch_lookup.get(target_batch_code)

            exp_title = f"**Year {year} of Study**"
            if found_batch:
                b_code = found_batch["code"] if isinstance(found_batch, dict) else found_batch[0]
                exp_title += f" (Batch: {b_code})"
            else:
                exp_title += f" (Batch: '{target_batch_code}' not found)"

            with st.expander(exp_title, expanded=(year == 1)):
                if not found_batch:
                    st.warning(
                        f"No batch found for expected intake year {target_batch_start_year}. "
                        "Terms cannot be computed until such a batch exists."
                    )
                    continue

                if isinstance(found_batch, dict):
                    batch_code = found_batch.get("code")
                    intake_year = found_batch.get("intake_year")
                    shift = found_batch.get("shift")
                    section = found_batch.get("section")
                else:
                    batch_code = found_batch[0]
                    intake_year = None
                    shift = None
                    section = None

                with _safe_conn(engine) as conn:
                    has_students = _db_check_batch_has_students(conn, deg, batch_code)

                if not has_students:
                    st.warning(
                        f"Batch {batch_code} has **no students** in the database. "
                        "Assignments may not apply in practice."
                    )

                st.write(f"**Batch Code:** {batch_code}")
                if intake_year:
                    st.write(f"**Intake Year:** {intake_year}")
                if shift is not None:
                    st.write(f"**Shift:** {shift}")
                if section:
                    st.write(f"**Section:** {section}")

                # Compute terms for this progression year
                with _safe_conn(engine) as conn:
                    prog_param = prog if level in ("program", "branch") else None
                    branch_param = br if level == "branch" else None

                    # 1) Compute the raw term windows from calendar assignments
                    terms, warnings = compute_terms_with_validation(
                        conn, ay, deg, prog_param, branch_param, progression_year=year
                    )

                    # 2) Get official semester numbers / labels from semesters table
                    sem_map = get_semester_mapping_for_year(
                        conn,
                        degree_code=deg,
                        year_index=year,          # semesters.year_index = year of study
                        program_code=prog_param,
                        branch_code=branch_param,
                    )

                # --- outside the DB connection block ---

                if warnings:
                    for w in warnings:
                        st.warning(w)

                if terms:
                    # If there is a mapping from semesters.py, apply it
                    for idx, t in enumerate(terms, start=1):
                        # calendar computation may or may not provide term_index;
                        # default to 1..N if missing.
                        term_index = t.get("term_index") or idx

                        if term_index in sem_map:
                            t["semester_number"] = sem_map[term_index]["semester_number"]
                            t["label"] = sem_map[term_index]["label"]
                        else:
                            # Fallback: keep whatever label was there
                            # (or compute something if you want a formula)
                            pass

                    term_df = pd.DataFrame(terms)
                    st.dataframe(term_df, use_container_width=True)
                else:
                    st.error("No terms calculated based on current assignments for this year.")

    except Exception as e:
        st.error(f"Failed to preview terms: {e}")
        st.code(traceback.format_exc())
