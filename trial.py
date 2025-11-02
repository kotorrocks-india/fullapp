from __future__ import annotations

import pandas as pd
import streamlit as st
from sqlalchemy import text as sa_text
from sqlalchemy.exc import IntegrityError

from core.settings import load_settings
from core.db import get_engine, init_db
from core.policy import require_page, can_edit_page, user_roles
from core.ui import render_footer_global

import io
import csv
import re
from typing import List, Dict, Any, Tuple

PROGRAM_IMPORT_COLS = [
    "program_code", "program_name", "active", "sort_order", "description"
]
BRANCH_IMPORT_COLS = [
    "branch_code", "branch_name", "program_code", "active", "sort_order", "description"
]
CG_IMPORT_COLS = [
    "group_code", "group_name", "kind", "active", "sort_order", "description"
]
CGL_IMPORT_COLS = [
    "group_code", "program_code", "branch_code"
]

CODE_RE = re.compile(r"^[A-Z0-9_-]+$")

# Fetch functions and helpers for all tables (degree, program, branch, curriculum group, links, etc.)
# Cache and schema-awareness helpers are also included

# ...[existing fetch and cache helpers for degrees, programs, branches, curriculum_groups, curriculum_group_links...]

# --- Import/Export Logic ---

def import_programs(conn, df_import: pd.DataFrame, degree_code: str, actor: str) -> Tuple[int, int, List[str]]:
    # [Same logic as previous import_programs, omitted here for brevity]

def import_branches(conn, df_import: pd.DataFrame, degree_code: str, actor: str, br_has_pid: bool) -> Tuple[int, int, List[str]]:
    # [Same logic as previous import_branches, omitted here for brevity]

def import_cgs(conn, df_import: pd.DataFrame, degree_code: str, actor: str, cg_allowed: bool) -> Tuple[int, int, List[str]]:
    created, updated, errors = 0, 0, []
    if not cg_allowed:
        errors.append("Import failed: This degree’s cohort mode does not support curriculum groups.")
        return 0, 0, errors

    df_import.columns = [col.strip() for col in df_import.columns]
    req_cols = set(CG_IMPORT_COLS)
    if not req_cols.issubset(df_import.columns):
        missing = list(req_cols - set(df_import.columns))
        errors.append(f"Import file is missing required columns: {', '.join(missing)}")
        return 0, 0, errors

    for row in df_import.itertuples():
        code = str(getattr(row, "group_code", "")).strip()
        name = str(getattr(row, "group_name", "")).strip()
        kind = str(getattr(row, "kind", "")).strip()
        active = bool(int(getattr(row, "active", 1)))
        sort_order = int(getattr(row, "sort_order", 0))
        desc = str(getattr(row, "description", "")).strip()

        if not code:
            errors.append(f"Skipped row {row.Index}: 'group_code' is missing.")
            continue
        if not CODE_RE.match(code):
            errors.append(f"Skipped row {row.Index} ({code}): 'group_code' contains invalid characters.")
            continue
        if not name:
            errors.append(f"Skipped row {row.Index} ({code}): 'group_name' is missing.")
            continue
        if kind not in ("pseudo", "cohort"):
            errors.append(f"Skipped row {row.Index} ({code}): 'kind' must be 'pseudo' or 'cohort'.")
            continue

        existing = conn.execute(sa_text("""
            SELECT * FROM curriculum_groups WHERE degree_code = :dc AND group_code = :gc
        """), {"dc": degree_code, "gc": code}).fetchone()

        new_data = {
            "group_name": name,
            "kind": kind,
            "active": active,
            "sort_order": sort_order,
            "description": desc
        }

        if existing:
            # UPDATE
            old_data = {k: getattr(existing, k) for k in new_data}
            changes = {k: v for k, v in new_data.items() if str(v) != str(old_data[k])}
            if not changes:
                continue
            conn.execute(sa_text(f"""
                UPDATE curriculum_groups
                SET {', '.join([f"{k} = :{k}" for k in changes])}
                WHERE id = :id
            """), {**changes, "id": existing.id})
            updated += 1
        else:
            # CREATE
            conn.execute(sa_text("""
                INSERT INTO curriculum_groups (degree_code, group_code, group_name, kind, active, sort_order, description)
                VALUES (:dc, :gc, :gn, :kind, :active, :sort_order, :desc)
            """), {
                "dc": degree_code, "gc": code, "gn": name, "kind": kind, "active": active, "sort_order": sort_order, "desc": desc
            })
            created += 1

    return created, updated, errors

def import_cg_links(conn, df_import: pd.DataFrame, degree_code: str, actor: str, cg_allowed: bool, group_codes, program_codes, branch_codes) -> Tuple[int, int, List[str]]:
    created, updated, errors = 0, 0, []
    if not cg_allowed:
        errors.append("Import failed: This degree’s cohort mode does not support curriculum group links.")
        return 0, 0, errors
    df_import.columns = [col.strip() for col in df_import.columns]
    req_cols = set(CGL_IMPORT_COLS)
    if not req_cols.issubset(df_import.columns):
        missing = list(req_cols - set(df_import.columns))
        errors.append(f"Import file is missing required columns: {', '.join(missing)}")
        return 0, 0, errors

    for row in df_import.itertuples():
        group_code = str(getattr(row, "group_code", "")).strip()
        program_code = str(getattr(row, "program_code", "")).strip()
        branch_code = str(getattr(row, "branch_code", "")).strip()

        if not group_code or group_code not in group_codes:
            errors.append(f"Skipped row {row.Index}: curriculum group '{group_code}' not found.")
            continue
        if program_code and program_code not in program_codes:
            errors.append(f"Skipped row {row.Index}: program_code '{program_code}' not found.")
            continue
        if branch_code and branch_code not in branch_codes:
            errors.append(f"Skipped row {row.Index}: branch_code '{branch_code}' not found.")
            continue

        group_id_row = conn.execute(sa_text("""
            SELECT id FROM curriculum_groups WHERE degree_code = :dc AND group_code = :gc
        """), {"dc": degree_code, "gc": group_code}).fetchone()

        if not group_id_row:
            errors.append(f"Skipped row {row.Index}: curriculum group '{group_code}' not found in DB.")
            continue

        # Check duplicate
        duplicate = conn.execute(sa_text("""
            SELECT id FROM curriculum_group_links
            WHERE degree_code=:dc AND group_id=:gid AND program_code=:pc AND branch_code=:bc
        """), {"dc": degree_code, "gid": group_id_row.id, "pc": program_code or None, "bc": branch_code or None}).fetchone()

        if duplicate:
            continue  # Consider as already present, skip

        conn.execute(sa_text("""
            INSERT INTO curriculum_group_links (group_id, degree_code, program_code, branch_code)
            VALUES (:gid, :dc, :pc, :bc)
        """), {"gid": group_id_row.id, "dc": degree_code, "pc": program_code or None, "bc": branch_code or None})

        created += 1

    return created, updated, errors

# --- Export logic for all types (just use Pandas to_csv on corresponding DataFrames) ---

def _df_to_csv(df_to_conv: pd.DataFrame):
    with io.StringIO() as buffer:
        df_to_conv.to_csv(buffer, index=False, quoting=csv.QUOTE_ALL)
        return buffer.getvalue().encode('utf-8')

# --- UI Integration: Modify your tab/expander logic to add "Curriculum Groups Import/Export" expander in the CG tab, with download, upload, and button calls as above ---

# --- rest of your app logic ---

# Place this in the Curriculum Groups tab:
if active_tab == "Curriculum Groups":
    cg_allowed = bool(deg.cg_degree or deg.cg_program or deg.cg_branch)
    with st.expander("Import / Export Curriculum Groups"):
        df_cg_template = pd.DataFrame(columns=CG_IMPORT_COLS)
        st.download_button(
            label="Download CG Template (CSV)",
            data=_df_to_csv(df_cg_template),
            file_name=f"{degree_sel}_cgs_template.csv",
            mime="text/csv",
            key="dload_cg_template",
            disabled=not cg_allowed
        )
        cg_file = st.file_uploader("Upload CG CSV", type=["csv"], key="cg_uploader", disabled=not CAN_EDIT or not cg_allowed)
        if st.button("Import CGs", key="import_cg_btn", disabled=not CAN_EDIT or not cg_file or not cg_allowed):
            try:
                df_import = pd.read_csv(cg_file, dtype=str).fillna("")
                # Call import function
                c_count, u_count, errors = import_cgs(conn, df_import, degree_sel, actor, cg_allowed)
                # Show results...
            except Exception as e:
                st.error(f"An error occurred during CG import: {e}")

        # Export existing CGs:
        if not df_cg.empty and cg_allowed:
            st.download_button(
                label=f"Export {len(df_cg)} Curriculum Groups (CSV)",
                data=_df_to_csv(df_cg[CG_IMPORT_COLS]),
                file_name=f"{degree_sel}_cgs_export.csv",
                mime="text/csv",
                key="export_cg_btn"
            )
        else:
            st.caption("No curriculum groups to export.")

    # Repeat expander for CG Links
    with st.expander("Import / Export CG Links"):
        df_cgl_template = pd.DataFrame(columns=CGL_IMPORT_COLS)
        st.download_button(
            label="Download CG Links Template (CSV)",
            data=_df_to_csv(df_cgl_template),
            file_name=f"{degree_sel}_cg_links_template.csv",
            mime="text/csv",
            key="dload_cgl_template",
            disabled=not cg_allowed
        )
        cgl_file = st.file_uploader("Upload CG Links CSV", type=["csv"], key="cgl_uploader", disabled=not CAN_EDIT or not cg_allowed)
        group_codes = df_cg["group_code"].tolist() if not df_cg.empty else []
        program_codes = dfp["program_code"].tolist() if not dfp.empty else []
        branch_codes = dfb_all["branch_code"].tolist() if not dfb_all.empty else []
        if st.button("Import CG Links", key="import_cgl_btn", disabled=not CAN_EDIT or not cgl_file or not cg_allowed):
            try:
                df_import = pd.read_csv(cgl_file, dtype=str).fillna("")
                c_count, u_count, errors = import_cg_links(conn, df_import, degree_sel, actor, cg_allowed, group_codes, program_codes, branch_codes)
                # Show import result
            except Exception as e:
                st.error(f"An error occurred during CG Link import: {e}")
        # Export existing CG links
        if not df_cgl.empty and cg_allowed:
            st.download_button(
                label=f"Export {len(df_cgl)} Curriculum Group Links (CSV)",
                data=_df_to_csv(df_cgl[CGL_IMPORT_COLS]),
                file_name=f"{degree_sel}_cg_links_export.csv",
                mime="text/csv",
                key="export_cgl_btn"
            )
        else:
            st.caption("No curriculum group links to export.")

# (Rest of your render function and footer)

render()
