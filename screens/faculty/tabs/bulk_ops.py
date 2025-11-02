from __future__ import annotations
from typing import Set, List, Dict, Any, Tuple
from dataclasses import dataclass
from datetime import date
import random

import pandas as pd
import streamlit as st
from sqlalchemy.engine import Engine

# Reuse your existing Import/Export UI
from screens.faculty.importer import (
    _add_combined_import_export_section,
    _add_positions_import_export_section,   # NEW: wire positions section
)

# Read-only DB helpers (no DDL here)
from screens.faculty.db import (
    _active_degrees,
    _branches_for_degree,
    _get_programs_for_degree,
    _get_curriculum_groups_for_degree,
    _get_all_positions,
)

# -------------------------------------------------------------------
# Data model for helper tab
# -------------------------------------------------------------------

@dataclass
class FacultySeed:
    name: str
    email: str
    phone: str
    employee_id: str
    designation: str
    ftype: str = "core"  # core/visiting

# -------------------------------------------------------------------
# Fixed sample names, emails, designations (as requested)
# -------------------------------------------------------------------

def _fixed_faculty(domain: str = "bharat.edu", seed_idx: int = 0) -> List[FacultySeed]:
    """
    Returns exactly four faculty in the requested order with the requested designations.
    Employee IDs vary by selected degree index (seed_idx) to keep them stable but unique.
    - 0: Rani Laxmibai            -> Professor
    - 1: Sardar Patel             -> Associate Professor
    - 2: Netaji Subhashchandra Bose -> Assistant Professor
    - 3: Shaheed Bhagat Singh     -> Assistant Professor
    """
    def email_for(full_name: str) -> str:
        parts = [p for p in full_name.strip().split() if p]
        return ".".join(p.lower() for p in parts) + f"@{domain}"

    start_emp = 100 + seed_idx * 10  # 100, 101, 102, 103... varies per degree index

    names_designations = [
        ("Rani Laxmibai", "Professor"),
        ("Sardar Patel", "Associate Professor"),
        ("Netaji Subhashchandra Bose", "Assistant Professor"),
        ("Shaheed Bhagat Singh", "Assistant Professor"),
    ]

    seeds: List[FacultySeed] = []
    for i, (full_name, desg) in enumerate(names_designations):
        seeds.append(
            FacultySeed(
                name=full_name,
                email=email_for(full_name),
                phone="123456",
                employee_id=str(start_emp + i),
                designation=desg,
                ftype="core",
            )
        )
    return seeds

# -------------------------------------------------------------------
# Scenario options and helpers
# -------------------------------------------------------------------

def _scenario_choices() -> Dict[str, Dict[str, Any]]:
    # Four patterns you requested (CG = Curriculum Group)
    return {
        "Degree + Program + Branch (+CG optional)": {
            "needs_program": True, "needs_branch": True, "cg_optional": True
        },
        "Degree + Program (+CG optional)": {
            "needs_program": True, "needs_branch": False, "cg_optional": True
        },
        "Degree + Branch (+CG optional)": {
            "needs_program": False, "needs_branch": True, "cg_optional": True
        },
        "Degree only (+CG optional)": {
            "needs_program": False, "needs_branch": False, "cg_optional": True
        },
    }

def _df_download_button(name: str, df: pd.DataFrame, key: str):
    csv = df.to_csv(index=False)
    st.download_button(
        label=f"⬇️ Download {name} CSV",
        data=csv,
        file_name=f"{name.lower().replace(' ', '_')}.csv",
        mime="text/csv",
        key=key,
    )

# -------------------------------------------------------------------
# Build sample dataframes (Combined Import + Admin Positions)
# -------------------------------------------------------------------

def _build_affiliations_sample(
    engine: Engine,
    degree: str,
    scenario_key: str,
    include_cg: bool,
    seeds: List[FacultySeed],
) -> pd.DataFrame:
    """
    Creates a Combined-Import-format DataFrame (profiles + affiliations)
    honoring the chosen scenario and CG toggle. Ensures one faculty (F0) is present
    in multiple scopes (program/branch) where applicable.
    """
    with engine.begin() as conn:
        programs = _get_programs_for_degree(conn, degree)
        branches = _branches_for_degree(conn, degree)
        groups   = _get_curriculum_groups_for_degree(conn, degree)

    scen = _scenario_choices()[scenario_key]
    needs_program = scen["needs_program"]
    needs_branch  = scen["needs_branch"]

    # Columns expected by importer (combined)
    columns = [
        "name","email","phone","employee_id","status","first_login_pending",
        "degree_code","program_code","branch_code","group_code",
        "designation","type","allowed_credit_override","active"
    ]

    rows: List[Dict[str, Any]] = []

    def add_row(s: FacultySeed, prog: str|None, br: str|None, grp: str|None):
        rows.append({
            "name": s.name,
            "email": s.email,
            "phone": s.phone,
            "employee_id": s.employee_id,
            "status": "active",
            "first_login_pending": 0,
            "degree_code": degree,
            "program_code": (prog or ""),
            "branch_code":  (br or ""),
            "group_code":   (grp or ""),
            "designation": s.designation,
            "type": s.ftype,
            "allowed_credit_override": 0,
            "active": 1,
        })

    # Safe picks in case any list is empty
    progA = programs[0] if programs else ""
    progB = programs[1] if len(programs) > 1 else (programs[0] if programs else "")
    brA   = branches[0] if branches else ""
    brB   = branches[1] if len(branches) > 1 else (branches[0] if branches else "")
    grpA  = groups[0] if groups else ""
    grpB  = groups[1] if len(groups) > 1 else (groups[0] if groups else "")

    # Ensure one faculty appears in multiple scopes -> use F0
    F0, F1, F2, F3 = seeds[0], seeds[1], seeds[2], seeds[3]

    if scenario_key.startswith("Degree + Program + Branch"):
        add_row(F0, progA, brA, (grpA if include_cg else ""))
        add_row(F0, progB, brB, (grpB if include_cg else ""))
        add_row(F1, progA, brA, (grpB if include_cg else ""))
        add_row(F2, progB, brA, (grpA if include_cg else ""))
        add_row(F3, progA, brB, (grpA if include_cg else ""))

    elif scenario_key.startswith("Degree + Program"):
        add_row(F0, progA, "", (grpA if include_cg else ""))
        add_row(F0, progB, "", (grpB if include_cg else ""))
        add_row(F1, progA, "", (grpB if include_cg else ""))
        add_row(F2, progB, "", (grpA if include_cg else ""))
        add_row(F3, progA, "", (grpA if include_cg else ""))

    elif scenario_key.startswith("Degree + Branch"):
        add_row(F0, "", brA, (grpA if include_cg else ""))
        add_row(F0, "", brB, (grpB if include_cg else ""))
        add_row(F1, "", brA, (grpB if include_cg else ""))
        add_row(F2, "", brB, (grpA if include_cg else ""))
        add_row(F3, "", brA, (grpA if include_cg else ""))

    else:  # Degree only (+CG optional)
        add_row(F0, "", "", (grpA if include_cg else ""))
        add_row(F1, "", "", (grpB if include_cg else ""))
        add_row(F2, "", "", (grpA if include_cg else ""))
        add_row(F3, "", "", (grpB if include_cg else ""))

    df = pd.DataFrame(rows, columns=columns)
    return df

def _build_admin_positions_sample(
    engine: Engine,
    degree: str,
    scenario_key: str,
    include_cg: bool,
    seeds: List[FacultySeed],
) -> pd.DataFrame:
    """
    Creates a sample DataFrame for administrative positions import.
    If a position is degree-specific, scopes are assigned per the chosen scenario.
    Institution-wide positions get empty scopes.
    """
    with engine.begin() as conn:
        positions = [p for p in _get_all_positions(conn) if p.get("is_active")]
        positions = sorted(positions, key=lambda x: x["position_code"]) if positions else []

        programs = _get_programs_for_degree(conn, degree)
        branches = _branches_for_degree(conn, degree)
        groups   = _get_curriculum_groups_for_degree(conn, degree)

    posA = positions[0] if positions else None
    posB = positions[1] if len(positions) > 1 else (positions[0] if positions else None)

    def scope_tuple(is_degree_specific: int, which: str) -> Tuple[str,str,str,str]:
        # (deg, prog, br, grp)
        deg = degree if is_degree_specific else ""
        prog = br = grp = ""
        if is_degree_specific:
            if scenario_key.startswith("Degree + Program + Branch"):
                prog = (programs[0] if which == "A" else (programs[1] if len(programs) > 1 else (programs[0] if programs else ""))) if programs else ""
                br   = (branches[0] if which == "A" else (branches[1] if len(branches) > 1 else (branches[0] if branches else ""))) if branches else ""
                if include_cg and groups:
                    grp = (groups[0] if which == "A" else (groups[1] if len(groups) > 1 else groups[0]))
            elif scenario_key.startswith("Degree + Program"):
                prog = (programs[0] if which == "A" else (programs[1] if len(programs) > 1 else (programs[0] if programs else ""))) if programs else ""
                if include_cg and groups:
                    grp = (groups[0] if which == "A" else (groups[1] if len(groups) > 1 else groups[0]))
            elif scenario_key.startswith("Degree + Branch"):
                br = (branches[0] if which == "A" else (branches[1] if len(branches) > 1 else (branches[0] if branches else ""))) if branches else ""
                if include_cg and groups:
                    grp = (groups[0] if which == "A" else (groups[1] if len(groups) > 1 else groups[0]))
            else:
                if include_cg and groups:
                    grp = (groups[0] if which == "A" else (groups[1] if len(groups) > 1 else groups[0]))
        return (deg or "", prog or "", br or "", grp or "")

    rows: List[Dict[str, Any]] = []

    def add_pos_row(seed: FacultySeed, pos: Dict[str, Any] | None, which_scope: str):
        if not pos:
            return
        is_deg_spec = int(pos.get("is_degree_specific") or 0)
        deg, prog, br, grp = scope_tuple(is_deg_spec, which_scope)
        rows.append({
            "faculty_email": seed.email,
            "position_code": pos["position_code"],
            "degree_code":   deg,
            "program_code":  prog,
            "branch_code":   br,
            "group_code":    grp,
            "start_date":    date.today().isoformat(),
            "credit_relief": int(pos.get("default_credit_relief") or 0),
            "notes":         f"Playground sample ({which_scope})",
        })

    # Use first four seeds; F0 gets two entries
    F0, F1, F2, F3 = seeds[0], seeds[1], seeds[2], seeds[3]
    add_pos_row(F0, posA, "A")
    add_pos_row(F0, posB, "B")
    add_pos_row(F1, posA, "A")
    add_pos_row(F2, posB, "B")
    add_pos_row(F3, posA, "A")

    columns = [
        "faculty_email", "position_code", "degree_code",
        "program_code", "branch_code", "group_code",
        "start_date", "credit_relief", "notes",
    ]
    return pd.DataFrame(rows, columns=columns)

# -------------------------------------------------------------------
# Public render
# -------------------------------------------------------------------

def render(engine: Engine, degree: str, roles: Set[str], can_edit: bool, key_prefix: str):
    st.subheader("Bulk Operations")

    tab_import, tab_helper = st.tabs(["📥 Import / Export", "🧪 Helper: Import Logic Playground"])

    # ---- Tab 1: Import / Export ----
    with tab_import:
        if can_edit:
            _add_combined_import_export_section(engine)
            _add_positions_import_export_section(engine)   # NEW: visible Import/Export for Admin Positions
        else:
            st.info("Edit permission required for bulk operations")

    # ---- Tab 2: Helper Playground ----
    with tab_helper:
        st.caption("Generate sample CSVs that demonstrate how the importer expects data for different scope patterns.")

        with engine.begin() as conn:
            degrees = _active_degrees(conn)

        if not degrees:
            st.warning("No active degrees found. Initialize degrees first.")
            return

        colA, colB = st.columns([2, 1])
        with colA:
            d_index = degrees.index(degree) if degree in degrees else 0
            chosen_degree = st.selectbox("Degree", options=degrees, index=d_index, key=f"{key_prefix}_deg")
            scenario_key = st.selectbox("Scenario", options=list(_scenario_choices().keys()), key=f"{key_prefix}_scenario")
        with colB:
            include_cg = st.checkbox("Include Curriculum Group (optional)", value=True, key=f"{key_prefix}_cgopt")

        seeds = _fixed_faculty(domain="bharat.edu", seed_idx=(degrees.index(chosen_degree) if chosen_degree in degrees else 0))

        # Combined import sample (profiles + affiliations)
        st.markdown("### 👥 Faculty Profiles + Affiliations (Combined Import Format)")
        aff_df = _build_affiliations_sample(engine, chosen_degree, scenario_key, include_cg, seeds)
        st.dataframe(aff_df, use_container_width=True, hide_index=True)
        _df_download_button("sample_combined_import", aff_df, key=f"{key_prefix}_dl_combined")

        # Administrative positions sample
        st.markdown("### 👔 Administrative Positions (Import Format)")
        admin_df = _build_admin_positions_sample(engine, chosen_degree, scenario_key, include_cg, seeds)
        if admin_df.empty:
            st.info("No active positions found. Create positions in the 'Administrative Positions' tab first.")
        else:
            st.dataframe(admin_df, use_container_width=True, hide_index=True)
            _df_download_button("sample_admin_positions_import", admin_df, key=f"{key_prefix}_dl_admin")

        st.markdown("#### Notes")
        st.markdown(
            "- **Exactly one** of `program_code` / `branch_code` / `group_code` should be filled per row (this helper follows your scenarios).  \n"
            "- For **institution-wide** positions (`is_degree_specific` = 0), keep all scope columns empty.  \n"
            "- Use the downloads above with **Dry Run** / **Execute Import** in the Import / Export tab."
        )
