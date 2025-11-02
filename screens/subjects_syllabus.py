# app/screens/subjects_syllabus.py
"""
Subjects & Syllabus Management - Template-Based Architecture
Clean implementation for new databases.
"""

from __future__ import annotations

import io
import csv
import json
import re
from typing import Tuple, Dict, Any, List, Optional
from datetime import datetime

import pandas as pd
import streamlit as st
from sqlalchemy import text as sa_text

from core.settings import load_settings
from core.db import get_engine, init_db, SessionLocal
from core.forms import tagline, success
from core.policy import require_page, can_edit_page, user_roles

# Import template schema
from schemas.syllabus_templates_schema import (
    install_template_syllabus_schema,
    get_effective_syllabus_for_offering
)

# Import template operations
from screens.subjects_syllabus_templates_db import (
    create_syllabus_template,
    list_templates_for_subject,
    get_current_template_for_subject,
    assign_template_to_offering,
    bulk_assign_template,
    create_syllabus_override,
    get_syllabus_summary
)

# Import template UI
from screens.subjects_syllabus_templates_ui import add_template_tabs_to_main

# ... (Keep all your existing helper functions and imports)
# _table_exists, _has_column, _audit_subject, _validate_subject, etc.

# ===========================================================================
# UPDATED: EXPORT SYLLABUS (Now uses templates)
# ===========================================================================

def export_syllabus_with_templates(
    engine, 
    degree_code: str = None, 
    ay_label: str = None,
    year: int = None, 
    term: int = None, 
    fmt: str = "csv"
) -> Tuple[str, bytes]:
    """
    Export syllabus points using template-based system.
    Automatically merges template + overrides.
    """
    with engine.begin() as conn:
        # Get matching offerings
        query = """
            SELECT so.id, so.subject_code, sc.subject_name,
                   so.degree_code, so.program_code, so.branch_code,
                   so.ay_label, so.year, so.term,
                   so.status, so.syllabus_template_id, so.syllabus_customized
            FROM subject_offerings so
            LEFT JOIN subjects_catalog sc ON sc.subject_code = so.subject_code
            WHERE 1=1
        """
        params = {}
        
        if degree_code:
            query += " AND so.degree_code = :d"
            params["d"] = degree_code
        
        if ay_label:
            query += " AND so.ay_label = :ay"
            params["ay"] = ay_label
        
        if year:
            query += " AND so.year = :y"
            params["y"] = year
        
        if term:
            query += " AND so.term = :t"
            params["t"] = term
        
        query += " ORDER BY so.ay_label, so.year, so.term, so.subject_code"
        
        offerings = conn.execute(sa_text(query), params).fetchall()
        
        # Collect all points
        all_points = []
        for offering in offerings:
            offering_id = offering[0]
            
            # Get effective syllabus (template + overrides merged)
            effective_points = get_effective_syllabus_for_offering(conn, offering_id)
            
            for point in effective_points:
                all_points.append({
                    "subject_code": offering[1],
                    "subject_name": offering[2],
                    "degree_code": offering[3],
                    "program_code": offering[4] or "",
                    "branch_code": offering[5] or "",
                    "ay_label": offering[6],
                    "year": offering[7],
                    "term": offering[8],
                    "status": offering[9],
                    "uses_template": bool(offering[10]),
                    "is_customized": bool(offering[11]),
                    "sequence": point["sequence"],
                    "title": point["title"],
                    "description": point.get("description", ""),
                    "tags": point.get("tags", ""),
                    "resources": point.get("resources", ""),
                    "hours_weight": point.get("hours_weight", 0),
                    "is_overridden": point.get("is_overridden", False),
                    "__export_version": "2.0.0-template"
                })
    
    df = pd.DataFrame(all_points)
    
    if df.empty:
        df = pd.DataFrame(columns=[
            "subject_code", "subject_name", "degree_code", "program_code", "branch_code",
            "ay_label", "year", "term", "sequence", "title", "description",
            "tags", "resources", "hours_weight", "__export_version"
        ])
    
    if fmt == "excel":
        buf = io.BytesIO()
        df.to_excel(buf, index=False)
        return "syllabus_export_template.xlsx", buf.getvalue()
    
    out = io.StringIO()
    df.to_csv(out, index=False)
    return "syllabus_export_template.csv", out.getvalue().encode("utf-8")

# ===========================================================================
# UPDATED: IMPORT SYLLABUS (Now creates overrides)
# ===========================================================================

def import_syllabus_with_templates(
    engine, 
    df: pd.DataFrame, 
    dry_run: bool = True
) -> Tuple[pd.DataFrame, int]:
    """
    Import syllabus points. If offering has template, creates overrides.
    If no template, suggests creating one.
    """
    errors = []
    upserted = 0
    actor = (st.session_state.get("user", {}) or {}).get("email", "system")
    
    for idx, row in df.iterrows():
        try:
            data = {
                "degree_code": str(row.get("degree_code", "") or "").strip().upper(),
                "program_code": str(row.get("program_code", "") or "").strip() or None,
                "branch_code": str(row.get("branch_code", "") or "").strip() or None,
                "ay_label": str(row.get("ay_label", "") or "").strip(),
                "year": int(row.get("year", 0) or 0),
                "term": int(row.get("term", 0) or 0),
                "subject_code": str(row.get("subject_code", "") or "").strip().upper(),
                "sequence": int(row.get("sequence", 0) or 0),
                "title": str(row.get("title", "") or "").strip(),
                "description": str(row.get("description", "") or "").strip() or None,
                "tags": str(row.get("tags", "") or "").strip() or None,
                "resources": str(row.get("resources", "") or "").strip() or None,
                "hours_weight": float(row.get("hours_weight", 0) or 0) if row.get("hours_weight") else None,
            }
            
            # Validate required fields
            if not all([data["degree_code"], data["ay_label"], data["subject_code"], 
                       data["year"] > 0, data["term"] > 0, data["sequence"] > 0, data["title"]]):
                errors.append({"row": idx + 2, "error": "Missing required fields"})
                continue
            
            if dry_run:
                # Just validate that offering exists
                with engine.begin() as conn:
                    offering = conn.execute(sa_text("""
                        SELECT id, syllabus_template_id 
                        FROM subject_offerings
                        WHERE subject_code = :sc
                        AND degree_code = :dc
                        AND COALESCE(program_code, '') = COALESCE(:pc, '')
                        AND COALESCE(branch_code, '') = COALESCE(:bc, '')
                        AND ay_label = :ay
                        AND year = :y
                        AND term = :t
                    """), {
                        "sc": data["subject_code"],
                        "dc": data["degree_code"],
                        "pc": data["program_code"],
                        "bc": data["branch_code"],
                        "ay": data["ay_label"],
                        "y": data["year"],
                        "t": data["term"]
                    }).fetchone()
                    
                    if not offering:
                        errors.append({
                            "row": idx + 2,
                            "error": f"Offering not found: {data['subject_code']} {data['ay_label']} Y{data['year']}T{data['term']}"
                        })
                continue
            
            # Actual import
            with engine.begin() as conn:
                offering = conn.execute(sa_text("""
                    SELECT id, syllabus_template_id 
                    FROM subject_offerings
                    WHERE subject_code = :sc
                    AND degree_code = :dc
                    AND COALESCE(program_code, '') = COALESCE(:pc, '')
                    AND COALESCE(branch_code, '') = COALESCE(:bc, '')
                    AND ay_label = :ay
                    AND year = :y
                    AND term = :t
                """), {
                    "sc": data["subject_code"],
                    "dc": data["degree_code"],
                    "pc": data["program_code"],
                    "bc": data["branch_code"],
                    "ay": data["ay_label"],
                    "y": data["year"],
                    "t": data["term"]
                }).fetchone()
                
                if not offering:
                    errors.append({
                        "row": idx + 2,
                        "error": f"Offering not found: {data['subject_code']} {data['ay_label']} Y{data['year']}T{data['term']}"
                    })
                    continue
                
                offering_id = offering[0]
                has_template = offering[1] is not None
                
                if has_template:
                    # Create/update override
                    create_syllabus_override(
                        engine,
                        offering_id=offering_id,
                        sequence=data["sequence"],
                        override_type="replace",
                        actor=actor,
                        title=data["title"],
                        description=data["description"],
                        tags=data["tags"],
                        resources=data["resources"],
                        hours_weight=data["hours_weight"],
                        reason="Imported from CSV/Excel"
                    )
                else:
                    # No template - suggest creating one
                    errors.append({
                        "row": idx + 2,
                        "error": f"Offering has no template. Create template first for {data['subject_code']}"
                    })
                    continue
                
                upserted += 1
                
        except Exception as e:
            errors.append({"row": idx + 2, "error": str(e)})
    
    return pd.DataFrame(errors), upserted

# ===========================================================================
# STREAMLIT PAGE
# ===========================================================================

@require_page("Subjects & Syllabus")
def render():
    st.title("Subjects & Syllabus Management")
    tagline()
    
    settings = load_settings()
    engine = get_engine(settings.db.url)
    
    # Install schema (safe to run multiple times)
    install_template_syllabus_schema(engine, seed_examples=False)
    
    init_db(engine)
    SessionLocal.configure(bind=engine)
    
    user = st.session_state.get("user") or {}
    actor = user.get("email", "system")
    roles = user_roles()
    CAN_EDIT = can_edit_page("Subjects & Syllabus", roles)
    
    if not CAN_EDIT:
        st.info("📖 Read-only mode: You have view access but cannot modify subjects.")
    
    # Main navigation
    tab1, tab2, tab3, tab4 = st.tabs([
        "Subjects Catalog", 
        "Import/Export Subjects", 
        "Syllabus Export",
        "Audit Trail"
    ])
    
    # ======================================================================
    # TAB 1: SUBJECTS CATALOG (Keep your existing implementation)
    # ======================================================================
    with tab1:
        st.subheader("Subjects Catalog")
        st.caption("Manage subjects at degree/program/branch level")
        # ... (keep your existing subjects catalog code)
    
    # ======================================================================
    # TAB 2: IMPORT/EXPORT SUBJECTS (Keep your existing implementation)
    # ======================================================================
    with tab2:
        st.subheader("Import/Export Subjects Catalog")
        # ... (keep your existing import/export code)
    
    # ======================================================================
    # TAB 3: SYLLABUS EXPORT (UPDATED to use templates)
    # ======================================================================
    with tab3:
        st.subheader("Syllabus Export")
        st.caption("Export syllabus with templates and overrides merged")
        
        col1, col2 = st.columns(2)
        
        with col1:
            if st.button("Export Syllabus to CSV", key="syl_export_csv"):
                name, data = export_syllabus_with_templates(engine, fmt="csv")
                st.download_button("Download CSV", data, file_name=name, mime="text/csv")
        
        with col2:
            if st.button("Export Syllabus to Excel", key="syl_export_excel"):
                name, data = export_syllabus_with_templates(engine, fmt="excel")
                st.download_button("Download Excel", data, file_name=name,
                                 mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        
        # Import section
        if CAN_EDIT:
            st.markdown("---")
            st.markdown("### Import Syllabus")
            st.warning("⚠️ Import now creates overrides on existing templates. Create templates first!")
            
            upload = st.file_uploader("Upload CSV/Excel", type=["csv", "xlsx", "xls"], key="syl_import_upload")
            
            if upload:
                if upload.name.lower().endswith(".csv"):
                    df = pd.read_csv(upload)
                else:
                    df = pd.read_excel(upload)
                
                st.dataframe(df.head(10), use_container_width=True)
                
                col1, col2 = st.columns(2)
                
                with col1:
                    if st.button("Dry Run", key="syl_import_dry"):
                        errs, _ = import_syllabus_with_templates(engine, df, dry_run=True)
                        if len(errs):
                            st.error(f"Found {len(errs)} issues")
                            st.dataframe(errs)
                        else:
                            st.success("No issues found!")
                
                with col2:
                    if st.button("Import", key="syl_import_now"):
                        errs, count = import_syllabus_with_templates(engine, df, dry_run=False)
                        if len(errs):
                            st.warning(f"Imported {count} with {len(errs)} issues")
                            st.dataframe(errs)
                        else:
                            success(f"Imported {count} points!")
                        st.rerun()
    
    # ======================================================================
    # TAB 4: AUDIT TRAIL (Keep your existing implementation)
    # ======================================================================
    with tab4:
        st.subheader("Audit Trail")
        # ... (keep your existing audit code)
    
    # ======================================================================
    # NEW: TEMPLATE MANAGEMENT TABS
    # ======================================================================
    st.markdown("---")
    st.markdown("## 📋 Template-Based Syllabus Management")
    st.caption("🎯 Reduce data duplication by 90% - Create reusable templates for recurring subjects")
    
    add_template_tabs_to_main(engine, actor, CAN_EDIT)

render()
