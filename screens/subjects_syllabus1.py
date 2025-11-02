# app/screens/subjects_syllabus.py
"""
Subjects & Syllabus Management
- Subjects catalog (degree/program/branch level)
- Subject offerings per AY-term
- Syllabus points management
- Import/Export functionality
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
from core.policy import require_page, can_edit_page, user_roles, can_request
from core.approvals_policy import approver_roles, rule, requires_reason
from schemas.subjects_offerings_schema import migrate_subjects_offerings

# ===========================================================================
# CONSTANTS & VALIDATION
# ===========================================================================

# Subject code validation (uppercase alphanumeric with hyphens/underscores)
SUBJECT_CODE_RE = re.compile(r"^[A-Z0-9_-]+$")
SUBJECT_NAME_RE = re.compile(r"^[A-Za-z0-9 &/\-\.,()]+$")

# Subject types
DEFAULT_SUBJECT_TYPES = ["Core", "Elective", "Audit", "Honors", "Project", "Internship"]

# Status values
STATUS_VALUES = ["active", "inactive", "archived"]
OFFERING_STATUS_VALUES = ["draft", "published", "archived"]

# Assessment source modes
ATTAINMENT_SOURCE_MODES = ["overall", "selected_components"]

# ===========================================================================
# HELPERS
# ===========================================================================

def _table_exists(conn, table: str) -> bool:
    """Check if table exists."""
    result = conn.execute(sa_text(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=:t"
    ), {"t": table}).fetchone()
    return result is not None

def _has_column(conn, table: str, col: str) -> bool:
    """Check if column exists in table."""
    rows = conn.execute(sa_text(f"PRAGMA table_info({table})")).fetchall()
    return any(r[1].lower() == col.lower() for r in rows)

def _audit_subject(conn, subject_id: int, subject_code: str, degree_code: str,
                   program_code: str, branch_code: str, action: str, actor: str, note: str = "",
                   changed_fields: Dict[str, Any] = None):
    """Write audit log for subject catalog changes."""
    conn.execute(sa_text("""
        INSERT INTO subjects_catalog_audit 
        (subject_id, subject_code, degree_code, program_code, branch_code, action, note, changed_fields, actor)
        VALUES (:sid, :sc, :dc, :pc, :bc, :act, :note, :fields, :actor)
    """), {
        "sid": subject_id,
        "sc": subject_code,
        "dc": degree_code,
        "pc": program_code,
        "bc": branch_code,
        "act": action,
        "note": note or "",
        "fields": json.dumps(changed_fields) if changed_fields else None,
        "actor": actor or "system"
    })

def _audit_offering(conn, offering_id: int, subject_code: str, degree_code: str,
                    program_code: str, branch_code: str, ay_label: str, year: int, term: int,
                    action: str, actor: str, note: str = "", changed_fields: Dict[str, Any] = None):
    """Write audit log for offering changes."""
    conn.execute(sa_text("""
        INSERT INTO subject_offerings_audit 
        (offering_id, subject_code, degree_code, program_code, branch_code, 
         ay_label, year, term, action, note, changed_fields, actor)
        VALUES (:oid, :sc, :dc, :pc, :bc, :ay, :y, :t, :act, :note, :fields, :actor)
    """), {
        "oid": offering_id,
        "sc": subject_code,
        "dc": degree_code,
        "pc": program_code,
        "bc": branch_code,
        "ay": ay_label,
        "y": year,
        "t": term,
        "act": action,
        "note": note or "",
        "fields": json.dumps(changed_fields) if changed_fields else None,
        "actor": actor or "system"
    })

def _audit_syllabus(conn, point_id: int, offering_id: int, subject_code: str,
                    action: str, actor: str, note: str = "", changed_fields: Dict[str, Any] = None):
    """Write audit log for syllabus changes."""
    conn.execute(sa_text("""
        INSERT INTO syllabus_points_audit 
        (point_id, offering_id, subject_code, action, note, changed_fields, actor)
        VALUES (:pid, :oid, :sc, :act, :note, :fields, :actor)
    """), {
        "pid": point_id,
        "oid": offering_id,
        "sc": subject_code,
        "act": action,
        "note": note or "",
        "fields": json.dumps(changed_fields) if changed_fields else None,
        "actor": actor or "system"
    })

# ===========================================================================
# VALIDATION FUNCTIONS
# ===========================================================================

def _validate_subject(data: Dict[str, Any], editing: bool = False) -> Tuple[bool, str]:
    """Validate subject catalog data."""
    code = (data.get("subject_code") or "").strip().upper()
    name = (data.get("subject_name") or "").strip()
    degree_code = (data.get("degree_code") or "").strip().upper()
    
    if not code or not SUBJECT_CODE_RE.match(code):
        return False, "Subject code must match ^[A-Z0-9_-]+$"
    
    if not name or not SUBJECT_NAME_RE.match(name):
        return False, "Subject name contains invalid characters"
    
    if not degree_code:
        return False, "Degree code is required"
    
    # Validate credits
    try:
        credits_total = float(data.get("credits_total", 0))
        if credits_total < 0 or credits_total > 40:
            return False, "Credits total must be between 0 and 40"
        
        L = int(data.get("L", 0))
        T = int(data.get("T", 0))
        P = int(data.get("P", 0))
        S = int(data.get("S", 0))
        
        if any(x < 0 for x in [L, T, P, S]):
            return False, "L/T/P/S values cannot be negative"
        
        if L > 12 or T > 12 or P > 20 or S > 20:
            return False, "L/T must be ≤12, P/S must be ≤20"
    except (ValueError, TypeError):
        return False, "Invalid numeric values for credits"
    
    # Validate marks
    try:
        internal = int(data.get("internal_marks_max", 40))
        exam = int(data.get("exam_marks_max", 60))
        jury_viva = int(data.get("jury_viva_marks_max", 0))
        
        if any(x < 0 for x in [internal, exam, jury_viva]):
            return False, "Marks values cannot be negative"
    except (ValueError, TypeError):
        return False, "Invalid marks values"
    
    return True, ""

# ===========================================================================
# FETCH FUNCTIONS
# ===========================================================================

@st.cache_data(ttl=300)
def _fetch_degrees(_conn):
    """Fetch all degrees with their cohort settings."""
    rows = _conn.execute(sa_text("""
        SELECT code, title, cohort_splitting_mode, 
               cg_degree, cg_program, cg_branch, active
        FROM degrees
        WHERE active = 1
        ORDER BY sort_order, code
    """)).fetchall()
    return [dict(r._mapping) for r in rows]

@st.cache_data(ttl=300)
def _fetch_programs(_conn, degree_code: str):
    """Fetch programs for a degree."""
    rows = _conn.execute(sa_text("""
        SELECT program_code, program_name, active
        FROM programs
        WHERE degree_code = :d AND active = 1
        ORDER BY sort_order, program_code
    """), {"d": degree_code}).fetchall()
    return [dict(r._mapping) for r in rows]

@st.cache_data(ttl=300)
def _fetch_branches(_conn, degree_code: str, program_code: Optional[str] = None):
    """Fetch branches for degree/program."""
    if program_code:
        # Fetch branches for specific program
        rows = _conn.execute(sa_text("""
            SELECT b.branch_code, b.branch_name, b.active
            FROM branches b
            JOIN programs p ON p.id = b.program_id
            WHERE p.degree_code = :d AND p.program_code = :p AND b.active = 1
            ORDER BY b.sort_order, b.branch_code
        """), {"d": degree_code, "p": program_code}).fetchall()
    else:
        # Fetch all branches for degree
        rows = _conn.execute(sa_text("""
            SELECT b.branch_code, b.branch_name, b.active
            FROM branches b
            LEFT JOIN programs p ON p.id = b.program_id
            WHERE (p.degree_code = :d OR b.degree_code = :d) AND b.active = 1
            ORDER BY b.sort_order, b.branch_code
        """), {"d": degree_code}).fetchall()
    return [dict(r._mapping) for r in rows]

@st.cache_data(ttl=300)
def _fetch_academic_years(_conn):
    """Fetch academic years."""
    rows = _conn.execute(sa_text("""
        SELECT ay_label
        FROM academic_years
        WHERE status = 'active'
        ORDER BY start_date DESC
    """)).fetchall()
    return [r[0] for r in rows]

@st.cache_data(ttl=300)
def _fetch_subject(conn, subject_id: int):
    """Fetch a single subject by ID."""
    row = conn.execute(sa_text("""
        SELECT * FROM subjects_catalog WHERE id = :id
    """), {"id": subject_id}).fetchone()
    return dict(row._mapping) if row else None

def _fetch_subjects(conn, degree_code: str, program_code: Optional[str] = None,
                   branch_code: Optional[str] = None, active_only: bool = True):
    """Fetch subjects for a given scope."""
    query = "SELECT * FROM subjects_catalog WHERE degree_code = :d"
    params = {"d": degree_code}
    
    if program_code:
        query += " AND (program_code = :p OR program_code IS NULL)"
        params["p"] = program_code
    
    if branch_code:
        query += " AND (branch_code = :b OR branch_code IS NULL)"
        params["b"] = branch_code
    
    if active_only:
        query += " AND active = 1"
    
    query += " ORDER BY sort_order, subject_code"
    
    rows = conn.execute(sa_text(query), params).fetchall()
    return [dict(r._mapping) for r in rows]

# ===========================================================================
# CRUD OPERATIONS - SUBJECTS CATALOG
# ===========================================================================

def create_subject(engine, data: Dict[str, Any], actor: str) -> int:
    """Create a new subject in catalog."""
    ok, msg = _validate_subject(data)
    if not ok:
        raise ValueError(msg)
    
    with engine.begin() as conn:
        # Check if subject already exists in this scope
        existing = conn.execute(sa_text("""
            SELECT id FROM subjects_catalog 
            WHERE subject_code = :code 
            AND degree_code = :deg
            AND COALESCE(program_code, '') = COALESCE(:prog, '')
            AND COALESCE(branch_code, '') = COALESCE(:branch, '')
        """), {
            "code": data["subject_code"],
            "deg": data["degree_code"],
            "prog": data.get("program_code"),
            "branch": data.get("branch_code")
        }).fetchone()
        
        if existing:
            raise ValueError("Subject already exists in this scope")
        
        # Insert subject
        result = conn.execute(sa_text("""
            INSERT INTO subjects_catalog (
                subject_code, subject_name, subject_type,
                degree_code, program_code, branch_code,
                credits_total, L, T, P, S,
                student_credits, teaching_credits,
                internal_marks_max, exam_marks_max, jury_viva_marks_max,
                min_internal_percent, min_external_percent, min_overall_percent,
                direct_source_mode, direct_internal_threshold_percent,
                direct_external_threshold_percent, direct_internal_weight_percent,
                direct_external_weight_percent, direct_target_students_percent,
                indirect_target_students_percent, indirect_min_response_rate_percent,
                overall_direct_weight_percent, overall_indirect_weight_percent,
                description, status, active, sort_order
            ) VALUES (
                :code, :name, :type,
                :deg, :prog, :branch,
                :credits, :L, :T, :P, :S,
                :sc, :tc,
                :int_max, :exam_max, :jury_max,
                :min_int, :min_ext, :min_overall,
                :dsm, :dit, :det, :diw, :dew, :dts,
                :its, :imr,
                :odw, :oiw,
                :desc, :status, :active, :sort
            )
        """), {
            "code": data["subject_code"],
            "name": data["subject_name"],
            "type": data.get("subject_type", "Core"),
            "deg": data["degree_code"],
            "prog": data.get("program_code"),
            "branch": data.get("branch_code"),
            "credits": data.get("credits_total", 0),
            "L": data.get("L", 0),
            "T": data.get("T", 0),
            "P": data.get("P", 0),
            "S": data.get("S", 0),
            "sc": data.get("student_credits"),
            "tc": data.get("teaching_credits"),
            "int_max": data.get("internal_marks_max", 40),
            "exam_max": data.get("exam_marks_max", 60),
            "jury_max": data.get("jury_viva_marks_max", 0),
            "min_int": data.get("min_internal_percent", 50.0),
            "min_ext": data.get("min_external_percent", 40.0),
            "min_overall": data.get("min_overall_percent", 40.0),
            "dsm": data.get("direct_source_mode", "overall"),
            "dit": data.get("direct_internal_threshold_percent", 50.0),
            "det": data.get("direct_external_threshold_percent", 40.0),
            "diw": data.get("direct_internal_weight_percent", 40.0),
            "dew": data.get("direct_external_weight_percent", 60.0),
            "dts": data.get("direct_target_students_percent", 50.0),
            "its": data.get("indirect_target_students_percent", 50.0),
            "imr": data.get("indirect_min_response_rate_percent", 75.0),
            "odw": data.get("overall_direct_weight_percent", 80.0),
            "oiw": data.get("overall_indirect_weight_percent", 20.0),
            "desc": data.get("description"),
            "status": data.get("status", "active"),
            "active": 1 if data.get("active", True) else 0,
            "sort": data.get("sort_order", 100)
        })
        
        subject_id = result.lastrowid
        
        # Audit
        _audit_subject(conn, subject_id, data["subject_code"], data["degree_code"],
                      data.get("program_code"), data.get("branch_code"),
                      "create", actor, f"Created subject: {data['subject_name']}")
        
        return subject_id

def update_subject(engine, subject_id: int, data: Dict[str, Any], actor: str):
    """Update an existing subject."""
    ok, msg = _validate_subject(data, editing=True)
    if not ok:
        raise ValueError(msg)
    
    with engine.begin() as conn:
        # Fetch existing
        existing = _fetch_subject(conn, subject_id)
        if not existing:
            raise ValueError("Subject not found")
        
        # Update
        conn.execute(sa_text("""
            UPDATE subjects_catalog SET
                subject_name = :name,
                subject_type = :type,
                credits_total = :credits,
                L = :L, T = :T, P = :P, S = :S,
                student_credits = :sc,
                teaching_credits = :tc,
                internal_marks_max = :int_max,
                exam_marks_max = :exam_max,
                jury_viva_marks_max = :jury_max,
                min_internal_percent = :min_int,
                min_external_percent = :min_ext,
                min_overall_percent = :min_overall,
                direct_source_mode = :dsm,
                direct_internal_threshold_percent = :dit,
                direct_external_threshold_percent = :det,
                direct_internal_weight_percent = :diw,
                direct_external_weight_percent = :dew,
                direct_target_students_percent = :dts,
                indirect_target_students_percent = :its,
                indirect_min_response_rate_percent = :imr,
                overall_direct_weight_percent = :odw,
                overall_indirect_weight_percent = :oiw,
                description = :desc,
                status = :status,
                active = :active,
                sort_order = :sort,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = :id
        """), {
            "id": subject_id,
            "name": data["subject_name"],
            "type": data.get("subject_type", "Core"),
            "credits": data.get("credits_total", 0),
            "L": data.get("L", 0),
            "T": data.get("T", 0),
            "P": data.get("P", 0),
            "S": data.get("S", 0),
            "sc": data.get("student_credits"),
            "tc": data.get("teaching_credits"),
            "int_max": data.get("internal_marks_max", 40),
            "exam_max": data.get("exam_marks_max", 60),
            "jury_max": data.get("jury_viva_marks_max", 0),
            "min_int": data.get("min_internal_percent", 50.0),
            "min_ext": data.get("min_external_percent", 40.0),
            "min_overall": data.get("min_overall_percent", 40.0),
            "dsm": data.get("direct_source_mode", "overall"),
            "dit": data.get("direct_internal_threshold_percent", 50.0),
            "det": data.get("direct_external_threshold_percent", 40.0),
            "diw": data.get("direct_internal_weight_percent", 40.0),
            "dew": data.get("direct_external_weight_percent", 60.0),
            "dts": data.get("direct_target_students_percent", 50.0),
            "its": data.get("indirect_target_students_percent", 50.0),
            "imr": data.get("indirect_min_response_rate_percent", 75.0),
            "odw": data.get("overall_direct_weight_percent", 80.0),
            "oiw": data.get("overall_indirect_weight_percent", 20.0),
            "desc": data.get("description"),
            "status": data.get("status", "active"),
            "active": 1 if data.get("active", True) else 0,
            "sort": data.get("sort_order", 100)
        })
        
        # Audit
        _audit_subject(conn, subject_id, existing["subject_code"], existing["degree_code"],
                      existing.get("program_code"), existing.get("branch_code"),
                      "update", actor, f"Updated subject: {data['subject_name']}")

def delete_subject(engine, subject_id: int, actor: str):
    """Delete a subject (soft delete by marking inactive)."""
    with engine.begin() as conn:
        existing = _fetch_subject(conn, subject_id)
        if not existing:
            raise ValueError("Subject not found")
        
        # Check if subject has offerings
        has_offerings = conn.execute(sa_text("""
            SELECT COUNT(*) FROM subject_offerings WHERE subject_id = :id
        """), {"id": subject_id}).fetchone()[0]
        
        if has_offerings > 0:
            raise ValueError("Cannot delete subject with existing offerings")
        
        # Soft delete
        conn.execute(sa_text("""
            UPDATE subjects_catalog SET active = 0, status = 'inactive', updated_at = CURRENT_TIMESTAMP
            WHERE id = :id
        """), {"id": subject_id})
        
        # Audit
        _audit_subject(conn, subject_id, existing["subject_code"], existing["degree_code"],
                      existing.get("program_code"), existing.get("branch_code"),
                      "delete", actor, "Subject marked inactive")

# ===========================================================================
# IMPORT/EXPORT - SUBJECTS CATALOG
# ===========================================================================

SUBJECTS_EXPORT_COLS = [
    "subject_code", "subject_name", "subject_type",
    "degree_code", "program_code", "branch_code",
    "credits_total", "L", "T", "P", "S",
    "student_credits", "teaching_credits",
    "internal_marks_max", "exam_marks_max", "jury_viva_marks_max",
    "min_internal_percent", "min_external_percent", "min_overall_percent",
    "direct_source_mode", "direct_internal_threshold_percent", "direct_external_threshold_percent",
    "direct_internal_weight_percent", "direct_external_weight_percent", "direct_target_students_percent",
    "indirect_target_students_percent", "indirect_min_response_rate_percent",
    "overall_direct_weight_percent", "overall_indirect_weight_percent",
    "description", "status", "active", "sort_order",
    "__export_version"
]

def export_subjects(engine, degree_code: str = None, program_code: str = None,
                   branch_code: str = None, fmt: str = "csv") -> Tuple[str, bytes]:
    """Export subjects catalog to CSV or Excel."""
    with engine.begin() as conn:
        query = "SELECT * FROM subjects_catalog WHERE 1=1"
        params = {}
        
        if degree_code:
            query += " AND degree_code = :d"
            params["d"] = degree_code
        
        if program_code:
            query += " AND program_code = :p"
            params["p"] = program_code
        
        if branch_code:
            query += " AND branch_code = :b"
            params["b"] = branch_code
        
        query += " ORDER BY degree_code, program_code, branch_code, sort_order, subject_code"
        
        rows = conn.execute(sa_text(query), params).fetchall()
    
    df = pd.DataFrame([dict(r._mapping) for r in rows])
    df["__export_version"] = "1.0.0"
    
    # Select only export columns that exist
    export_cols = [c for c in SUBJECTS_EXPORT_COLS if c in df.columns or c == "__export_version"]
    df = df[export_cols]
    
    if fmt == "excel":
        buf = io.BytesIO()
        df.to_excel(buf, index=False)
        return "subjects_catalog_export.xlsx", buf.getvalue()
    
    out = io.StringIO()
    df.to_csv(out, index=False)
    return "subjects_catalog_export.csv", out.getvalue().encode("utf-8")

def import_subjects(engine, df: pd.DataFrame, dry_run: bool = True) -> Tuple[pd.DataFrame, int]:
    """Import subjects catalog from CSV/Excel."""
    errors = []
    upserted = 0
    actor = (st.session_state.get("user", {}) or {}).get("email", "system")
    
    for idx, row in df.iterrows():
        data = {
            "subject_code": str(row.get("subject_code", "") or "").strip().upper(),
            "subject_name": str(row.get("subject_name", "") or "").strip(),
            "subject_type": str(row.get("subject_type", "Core") or "Core"),
            "degree_code": str(row.get("degree_code", "") or "").strip().upper(),
            "program_code": str(row.get("program_code", "") or "").strip() or None,
            "branch_code": str(row.get("branch_code", "") or "").strip() or None,
            "credits_total": float(row.get("credits_total", 0) or 0),
            "L": int(row.get("L", 0) or 0),
            "T": int(row.get("T", 0) or 0),
            "P": int(row.get("P", 0) or 0),
            "S": int(row.get("S", 0) or 0),
            "student_credits": row.get("student_credits"),
            "teaching_credits": row.get("teaching_credits"),
            "internal_marks_max": int(row.get("internal_marks_max", 40) or 40),
            "exam_marks_max": int(row.get("exam_marks_max", 60) or 60),
            "jury_viva_marks_max": int(row.get("jury_viva_marks_max", 0) or 0),
            "min_internal_percent": float(row.get("min_internal_percent", 50.0) or 50.0),
            "min_external_percent": float(row.get("min_external_percent", 40.0) or 40.0),
            "min_overall_percent": float(row.get("min_overall_percent", 40.0) or 40.0),
            "direct_source_mode": str(row.get("direct_source_mode", "overall") or "overall"),
            "direct_internal_threshold_percent": float(row.get("direct_internal_threshold_percent", 50.0) or 50.0),
            "direct_external_threshold_percent": float(row.get("direct_external_threshold_percent", 40.0) or 40.0),
            "direct_internal_weight_percent": float(row.get("direct_internal_weight_percent", 40.0) or 40.0),
            "direct_external_weight_percent": float(row.get("direct_external_weight_percent", 60.0) or 60.0),
            "direct_target_students_percent": float(row.get("direct_target_students_percent", 50.0) or 50.0),
            "indirect_target_students_percent": float(row.get("indirect_target_students_percent", 50.0) or 50.0),
            "indirect_min_response_rate_percent": float(row.get("indirect_min_response_rate_percent", 75.0) or 75.0),
            "overall_direct_weight_percent": float(row.get("overall_direct_weight_percent", 80.0) or 80.0),
            "overall_indirect_weight_percent": float(row.get("overall_indirect_weight_percent", 20.0) or 20.0),
            "description": str(row.get("description", "") or "").strip() or None,
            "status": str(row.get("status", "active") or "active"),
            "active": bool(row.get("active", True)),
            "sort_order": int(row.get("sort_order", 100) or 100)
        }
        
        ok, msg = _validate_subject(data)
        if not ok:
            errors.append({"row": idx + 2, "error": msg})
            continue
        
        if dry_run:
            continue
        
        try:
            with engine.begin() as conn:
                # Check if exists
                existing = conn.execute(sa_text("""
                    SELECT id FROM subjects_catalog 
                    WHERE subject_code = :code 
                    AND degree_code = :deg
                    AND COALESCE(program_code, '') = COALESCE(:prog, '')
                    AND COALESCE(branch_code, '') = COALESCE(:branch, '')
                """), {
                    "code": data["subject_code"],
                    "deg": data["degree_code"],
                    "prog": data.get("program_code"),
                    "branch": data.get("branch_code")
                }).fetchone()
                
                if existing:
                    # Update
                    conn.execute(sa_text("""
                        UPDATE subjects_catalog SET
                            subject_name = :name, subject_type = :type,
                            credits_total = :credits, L = :L, T = :T, P = :P, S = :S,
                            student_credits = :sc, teaching_credits = :tc,
                            internal_marks_max = :int_max, exam_marks_max = :exam_max,
                            jury_viva_marks_max = :jury_max,
                            min_internal_percent = :min_int, min_external_percent = :min_ext,
                            min_overall_percent = :min_overall,
                            direct_source_mode = :dsm,
                            direct_internal_threshold_percent = :dit,
                            direct_external_threshold_percent = :det,
                            direct_internal_weight_percent = :diw,
                            direct_external_weight_percent = :dew,
                            direct_target_students_percent = :dts,
                            indirect_target_students_percent = :its,
                            indirect_min_response_rate_percent = :imr,
                            overall_direct_weight_percent = :odw,
                            overall_indirect_weight_percent = :oiw,
                            description = :desc, status = :status,
                            active = :active, sort_order = :sort,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE id = :id
                    """), {"id": existing[0], **data})
                    
                    _audit_subject(conn, existing[0], data["subject_code"], data["degree_code"],
                                  data.get("program_code"), data.get("branch_code"),
                                  "import_update", actor, "Updated via import")
                else:
                    # Insert
                    result = conn.execute(sa_text("""
                        INSERT INTO subjects_catalog (
                            subject_code, subject_name, subject_type,
                            degree_code, program_code, branch_code,
                            credits_total, L, T, P, S,
                            student_credits, teaching_credits,
                            internal_marks_max, exam_marks_max, jury_viva_marks_max,
                            min_internal_percent, min_external_percent, min_overall_percent,
                            direct_source_mode, direct_internal_threshold_percent,
                            direct_external_threshold_percent, direct_internal_weight_percent,
                            direct_external_weight_percent, direct_target_students_percent,
                            indirect_target_students_percent, indirect_min_response_rate_percent,
                            overall_direct_weight_percent, overall_indirect_weight_percent,
                            description, status, active, sort_order
                        ) VALUES (
                            :subject_code, :subject_name, :subject_type,
                            :degree_code, :program_code, :branch_code,
                            :credits_total, :L, :T, :P, :S,
                            :student_credits, :teaching_credits,
                            :internal_marks_max, :exam_marks_max, :jury_viva_marks_max,
                            :min_internal_percent, :min_external_percent, :min_overall_percent,
                            :direct_source_mode, :direct_internal_threshold_percent,
                            :direct_external_threshold_percent, :direct_internal_weight_percent,
                            :direct_external_weight_percent, :direct_target_students_percent,
                            :indirect_target_students_percent, :indirect_min_response_rate_percent,
                            :overall_direct_weight_percent, :overall_indirect_weight_percent,
                            :description, :status, :active, :sort_order
                        )
                    """), data)
                    
                    subject_id = result.lastrowid
                    _audit_subject(conn, subject_id, data["subject_code"], data["degree_code"],
                                  data.get("program_code"), data.get("branch_code"),
                                  "import_create", actor, "Created via import")
                
                upserted += 1
        except Exception as e:
            errors.append({"row": idx + 2, "error": str(e)})
    
    return pd.DataFrame(errors), upserted

# ===========================================================================
# EXPORT - SYLLABUS POINTS
# ===========================================================================

SYLLABUS_EXPORT_COLS = [
    "degree_code", "program_code", "branch_code",
    "ay_label", "year", "term",
    "subject_code", "subject_name", "status",
    "sequence", "title", "description", "tags", "resources", "hours_weight",
    "last_updated_at", "last_updated_by",
    "__export_version"
]

def export_syllabus(engine, degree_code: str = None, ay_label: str = None,
                   year: int = None, term: int = None, fmt: str = "csv") -> Tuple[str, bytes]:
    """Export syllabus points to CSV or Excel."""
    with engine.begin() as conn:
        query = """
            SELECT 
                sp.degree_code, sp.program_code, sp.branch_code,
                sp.ay_label, sp.year, sp.term,
                sp.subject_code, sc.subject_name, so.status,
                sp.sequence, sp.title, sp.description, sp.tags, sp.resources, sp.hours_weight,
                sp.updated_at as last_updated_at, sp.last_updated_by
            FROM syllabus_points sp
            LEFT JOIN subject_offerings so ON so.id = sp.offering_id
            LEFT JOIN subjects_catalog sc ON sc.subject_code = sp.subject_code 
                AND sc.degree_code = sp.degree_code
            WHERE 1=1
        """
        params = {}
        
        if degree_code:
            query += " AND sp.degree_code = :d"
            params["d"] = degree_code
        
        if ay_label:
            query += " AND sp.ay_label = :ay"
            params["ay"] = ay_label
        
        if year:
            query += " AND sp.year = :y"
            params["y"] = year
        
        if term:
            query += " AND sp.term = :t"
            params["t"] = term
        
        query += " ORDER BY sp.degree_code, sp.ay_label, sp.year, sp.term, sp.subject_code, sp.sequence"
        
        rows = conn.execute(sa_text(query), params).fetchall()
    
    df = pd.DataFrame([dict(r._mapping) for r in rows])
    if df.empty:
        df = pd.DataFrame(columns=SYLLABUS_EXPORT_COLS)
    
    df["__export_version"] = "1.0.0"
    
    if fmt == "excel":
        buf = io.BytesIO()
        df.to_excel(buf, index=False)
        return "syllabus_points_export.xlsx", buf.getvalue()
    
    out = io.StringIO()
    df.to_csv(out, index=False)
    return "syllabus_points_export.csv", out.getvalue().encode("utf-8")

# ===========================================================================
# SYLLABUS IMPORT FUNCTION
# ===========================================================================
# Add this function to your subjects_syllabus.py file
# Place it after the export_syllabus() function
# ===========================================================================

def import_syllabus(engine, df: pd.DataFrame, dry_run: bool = True) -> Tuple[pd.DataFrame, int]:
    """
    Import syllabus points from CSV/Excel.
    
    Args:
        engine: SQLAlchemy engine
        df: DataFrame with syllabus point data
        dry_run: If True, only validates without importing
    
    Returns:
        Tuple of (errors_df, upserted_count)
    """
    errors = []
    upserted = 0
    actor = (st.session_state.get("user", {}) or {}).get("email", "system")
    
    for idx, row in df.iterrows():
        try:
            # Extract and validate data
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
            if not data["degree_code"]:
                errors.append({"row": idx + 2, "error": "degree_code is required"})
                continue
            
            if not data["ay_label"]:
                errors.append({"row": idx + 2, "error": "ay_label is required"})
                continue
            
            if not data["subject_code"]:
                errors.append({"row": idx + 2, "error": "subject_code is required"})
                continue
            
            if data["year"] <= 0:
                errors.append({"row": idx + 2, "error": "year must be positive"})
                continue
            
            if data["term"] <= 0:
                errors.append({"row": idx + 2, "error": "term must be positive"})
                continue
            
            if data["sequence"] <= 0:
                errors.append({"row": idx + 2, "error": "sequence must be positive"})
                continue
            
            if not data["title"]:
                errors.append({"row": idx + 2, "error": "title is required"})
                continue
            
            if len(data["title"]) > 200:
                errors.append({"row": idx + 2, "error": "title too long (max 200 chars)"})
                continue
            
            if dry_run:
                # In dry run, just validate that offering exists
                with engine.begin() as conn:
                    offering = conn.execute(sa_text("""
                        SELECT id FROM subject_offerings
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
                            "error": f"Subject offering not found: {data['subject_code']} for {data['degree_code']} {data['ay_label']} Y{data['year']}T{data['term']}"
                        })
                        continue
                
                continue  # Skip actual import in dry run
            
            # Actual import
            with engine.begin() as conn:
                # Find or create offering
                offering = conn.execute(sa_text("""
                    SELECT id FROM subject_offerings
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
                        "error": f"Subject offering not found: {data['subject_code']} for {data['degree_code']} {data['ay_label']} Y{data['year']}T{data['term']}"
                    })
                    continue
                
                offering_id = offering[0]
                
                # Check if syllabus point exists
                existing = conn.execute(sa_text("""
                    SELECT id FROM syllabus_points
                    WHERE offering_id = :oid AND sequence = :seq
                """), {"oid": offering_id, "seq": data["sequence"]}).fetchone()
                
                if existing:
                    # Update existing point
                    conn.execute(sa_text("""
                        UPDATE syllabus_points SET
                            title = :title,
                            description = :desc,
                            tags = :tags,
                            resources = :res,
                            hours_weight = :hw,
                            updated_at = CURRENT_TIMESTAMP,
                            last_updated_by = :actor
                        WHERE id = :id
                    """), {
                        "id": existing[0],
                        "title": data["title"],
                        "desc": data["description"],
                        "tags": data["tags"],
                        "res": data["resources"],
                        "hw": data["hours_weight"],
                        "actor": actor
                    })
                    
                    _audit_syllabus(conn, existing[0], offering_id, data["subject_code"],
                                  "import_update", actor, "Updated via import")
                else:
                    # Insert new point
                    result = conn.execute(sa_text("""
                        INSERT INTO syllabus_points (
                            offering_id, subject_code,
                            degree_code, program_code, branch_code,
                            ay_label, year, term,
                            sequence, title, description, tags, resources, hours_weight,
                            last_updated_by
                        ) VALUES (
                            :oid, :sc,
                            :dc, :pc, :bc,
                            :ay, :y, :t,
                            :seq, :title, :desc, :tags, :res, :hw,
                            :actor
                        )
                    """), {
                        "oid": offering_id,
                        "sc": data["subject_code"],
                        "dc": data["degree_code"],
                        "pc": data["program_code"],
                        "bc": data["branch_code"],
                        "ay": data["ay_label"],
                        "y": data["year"],
                        "t": data["term"],
                        "seq": data["sequence"],
                        "title": data["title"],
                        "desc": data["description"],
                        "tags": data["tags"],
                        "res": data["resources"],
                        "hw": data["hours_weight"],
                        "actor": actor
                    })
                    
                    point_id = result.lastrowid
                    _audit_syllabus(conn, point_id, offering_id, data["subject_code"],
                                  "import_create", actor, "Created via import")
                
                upserted += 1
                
        except ValueError as e:
            errors.append({"row": idx + 2, "error": f"Invalid data: {str(e)}"})
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
    
    # Run migrations
    migrate_subjects_offerings(engine)
    
    init_db(engine)
    SessionLocal.configure(bind=engine)
    
    user = st.session_state.get("user") or {}
    actor = user.get("email", "system")
    
    # Check permissions
    roles = user_roles()
    CAN_EDIT = can_edit_page("Subjects & Syllabus", roles)
    
    if not CAN_EDIT:
        st.info("📖 Read-only mode: You have view access but cannot modify subjects.")
    
    # Tab navigation
    tab1, tab2, tab3, tab4 = st.tabs([
        "Subjects Catalog", 
        "Import/Export Subjects", 
        "Syllabus Import/Export",
        "Audit Trail"
    ])
    
    # ======================================================================
    # TAB 1: SUBJECTS CATALOG
    # ======================================================================
    with tab1:
        st.subheader("Subjects Catalog")
        st.caption("Manage subjects at degree/program/branch level based on cohort settings")
        
        with engine.begin() as conn:
            degrees = _fetch_degrees(conn)
        
        if not degrees:
            st.warning("No degrees found. Please create degrees first.")
            return
        
        # Filters
        col1, col2, col3 = st.columns(3)
        
        with col1:
            degree_code = st.selectbox(
                "Degree",
                options=[d["code"] for d in degrees],
                format_func=lambda x: next((d["title"] for d in degrees if d["code"] == x), x)
            )
        
        # Get selected degree info
        degree = next((d for d in degrees if d["code"] == degree_code), None)
        
        program_code = None
        branch_code = None
        
        if degree:
            cohort_mode = degree["cohort_splitting_mode"]
            cg_program = degree.get("cg_program", 0)
            cg_branch = degree.get("cg_branch", 0)
            
            # Show program selector if applicable
            if cohort_mode in ["both", "program_only"] or cg_program:
                with col2:
                    with engine.begin() as conn:
                        programs = _fetch_programs(conn, degree_code)
                    
                    if programs:
                        program_code = st.selectbox(
                            "Program (optional)",
                            options=[None] + [p["program_code"] for p in programs],
                            format_func=lambda x: "All Programs" if x is None else 
                                next((p["program_name"] for p in programs if p["program_code"] == x), x)
                        )
            
            # Show branch selector if applicable
            if cohort_mode in ["both", "branch_only"] or cg_branch:
                with col3:
                    with engine.begin() as conn:
                        branches = _fetch_branches(conn, degree_code, program_code)
                    
                    if branches:
                        branch_code = st.selectbox(
                            "Branch (optional)",
                            options=[None] + [b["branch_code"] for b in branches],
                            format_func=lambda x: "All Branches" if x is None else 
                                next((b["branch_name"] for b in branches if b["branch_code"] == x), x)
                        )
        
        # Display subjects
        st.markdown("---")
        with engine.begin() as conn:
            subjects = _fetch_subjects(conn, degree_code, program_code, branch_code, active_only=False)
        
        if subjects:
            df = pd.DataFrame(subjects)
            # Select display columns
            display_cols = ["subject_code", "subject_name", "subject_type", 
                          "credits_total", "L", "T", "P", "S", "status", "active"]
            display_df = df[[col for col in display_cols if col in df.columns]]
            st.dataframe(display_df, use_container_width=True)
        else:
            st.info(f"No subjects found for the selected scope: {degree_code}" +
                   (f" / {program_code}" if program_code else "") +
                   (f" / {branch_code}" if branch_code else ""))
        
        # Create/Edit form
        if CAN_EDIT:
            st.markdown("---")
            st.subheader("Create/Edit Subject")
            
            with st.form("subject_form"):
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    subject_code = st.text_input(
                        "Subject Code*",
                        placeholder="e.g., CS101",
                        help="Uppercase alphanumeric with hyphens/underscores"
                    ).upper()
                
                with col2:
                    subject_name = st.text_input(
                        "Subject Name*",
                        placeholder="e.g., Data Structures"
                    )
                
                with col3:
                    subject_type = st.selectbox(
                        "Subject Type*",
                        options=DEFAULT_SUBJECT_TYPES
                    )
                
                # Credits
                st.markdown("**Credits Configuration**")
                col1, col2, col3, col4, col5 = st.columns(5)
                
                with col1:
                    credits_total = st.number_input("Total Credits", min_value=0.0, max_value=40.0, value=4.0, step=0.5)
                with col2:
                    L = st.number_input("L", min_value=0, max_value=12, value=3)
                with col3:
                    T = st.number_input("T", min_value=0, max_value=12, value=0)
                with col4:
                    P = st.number_input("P", min_value=0, max_value=20, value=2)
                with col5:
                    S = st.number_input("S", min_value=0, max_value=20, value=0)
                
                # Marks
                st.markdown("**Assessment Marks**")
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    internal_marks = st.number_input("Internal Marks Max", min_value=0, value=40)
                with col2:
                    exam_marks = st.number_input("Exam Marks Max", min_value=0, value=60)
                with col3:
                    jury_marks = st.number_input("Jury/Viva Marks Max", min_value=0, value=0)
                
                # Pass criteria
                st.markdown("**Pass/Fail Criteria (%)**")
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    min_internal_pct = st.number_input("Min Internal %", min_value=0.0, max_value=100.0, value=50.0)
                with col2:
                    min_external_pct = st.number_input("Min External %", min_value=0.0, max_value=100.0, value=40.0)
                with col3:
                    min_overall_pct = st.number_input("Min Overall %", min_value=0.0, max_value=100.0, value=40.0)
                
                description = st.text_area("Description (optional)")
                
                col1, col2 = st.columns(2)
                with col1:
                    active = st.checkbox("Active", value=True)
                with col2:
                    sort_order = st.number_input("Sort Order", min_value=1, value=100)
                
                submitted = st.form_submit_button("Create Subject")
                
                if submitted:
                    try:
                        data = {
                            "subject_code": subject_code,
                            "subject_name": subject_name,
                            "subject_type": subject_type,
                            "degree_code": degree_code,
                            "program_code": program_code,
                            "branch_code": branch_code,
                            "credits_total": credits_total,
                            "L": L, "T": T, "P": P, "S": S,
                            "internal_marks_max": internal_marks,
                            "exam_marks_max": exam_marks,
                            "jury_viva_marks_max": jury_marks,
                            "min_internal_percent": min_internal_pct,
                            "min_external_percent": min_external_pct,
                            "min_overall_percent": min_overall_pct,
                            "description": description,
                            "active": active,
                            "sort_order": sort_order
                        }
                        
                        subject_id = create_subject(engine, data, actor)
                        success(f"Subject {subject_code} created successfully!")
                        st.cache_data.clear()
                        st.rerun()
                    except Exception as e:
                        st.error(f"Error creating subject: {str(e)}")
    
    # ======================================================================
    # TAB 2: IMPORT/EXPORT SUBJECTS
    # ======================================================================
    with tab2:
        st.subheader("Import/Export Subjects Catalog")
        
        # Export
        st.markdown("### Export")
        col1, col2 = st.columns(2)
        
        with col1:
            if st.button("Export to CSV"):
                name, data = export_subjects(engine, fmt="csv")
                st.download_button("Download CSV", data, file_name=name, mime="text/csv")
        
        with col2:
            if st.button("Export to Excel"):
                name, data = export_subjects(engine, fmt="excel")
                st.download_button("Download Excel", data, file_name=name, 
                                 mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        
        # Import
        if CAN_EDIT:
            st.markdown("---")
            st.markdown("### Import")
            st.caption("Required columns: subject_code, subject_name, degree_code")
            st.caption("Optional columns: program_code, branch_code, subject_type, credits_total, L, T, P, S, etc.")
            
            upload = st.file_uploader("Upload CSV/Excel", type=["csv", "xlsx", "xls"])
            
            if upload is not None:
                if upload.name.lower().endswith(".csv"):
                    df = pd.read_csv(upload)
                else:
                    df = pd.read_excel(upload)
                
                st.dataframe(df.head(10))
                
                col1, col2 = st.columns(2)
                
                with col1:
                    if st.button("Dry Run (Validate)"):
                        errs, _ = import_subjects(engine, df, dry_run=True)
                        if len(errs):
                            st.error(f"Found {len(errs)} issues.")
                            st.dataframe(errs)
                        else:
                            st.success("No issues found. You can proceed to import.")
                
                with col2:
                    if st.button("Import Now"):
                        errs, count = import_subjects(engine, df, dry_run=False)
                        if len(errs):
                            st.error(f"Imported {count} rows with {len(errs)} issues.")
                            st.dataframe(errs)
                        else:
                            success(f"Imported {count} subjects successfully!")
                        st.cache_data.clear()
                        st.rerun()
    
# Replace TAB 3 section in your subjects_syllabus.py with this:

    # ======================================================================
    # TAB 3: SYLLABUS IMPORT/EXPORT
    # ======================================================================
    with tab3:
        st.subheader("Syllabus Points Import/Export")
        st.caption("Import and export syllabus points for specific AY-term offerings")
        
        # Export Section
        st.markdown("### Export Syllabus")
        
        col1, col2 = st.columns(2)
        
        with col1:
            if st.button("Export Syllabus to CSV", key="syllabus_export_csv"):
                name, data = export_syllabus(engine, fmt="csv")
                st.download_button("Download CSV", data, file_name=name, mime="text/csv")
        
        with col2:
            if st.button("Export Syllabus to Excel", key="syllabus_export_excel"):
                name, data = export_syllabus(engine, fmt="excel")
                st.download_button("Download Excel", data, file_name=name,
                                 mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        
        # Import Section
        if CAN_EDIT:
            st.markdown("---")
            st.markdown("### Import Syllabus")
            st.caption("**Required columns:** degree_code, ay_label, year, term, subject_code, sequence, title")
            st.caption("**Optional columns:** program_code, branch_code, description, tags, resources, hours_weight")
            
            with st.expander("ℹ️ Import Instructions", expanded=False):
                st.markdown("""
                **Requirements:**
                - The subject offering must already exist (created in the system)
                - Each syllabus point needs: degree, AY, year, term, subject code, sequence number, and title
                - Sequence numbers determine the order of syllabus points
                - If a point with the same offering_id + sequence exists, it will be updated
                
                **Expected Format:**
                ```
                degree_code,program_code,branch_code,ay_label,year,term,subject_code,sequence,title,description,tags,resources,hours_weight
                BTECH,CSE,,2024-25,1,1,CS101,1,Introduction to Programming,Basic concepts,...,Textbook Ch1,2.0
                BTECH,CSE,,2024-25,1,1,CS101,2,Variables and Data Types,Understanding variables,...,Textbook Ch2,3.0
                ```
                """)
            
            upload = st.file_uploader(
                "Upload CSV/Excel file with syllabus points", 
                type=["csv", "xlsx", "xls"],
                key="syllabus_upload"
            )
            
            if upload is not None:
                try:
                    # Read file
                    if upload.name.lower().endswith(".csv"):
                        df = pd.read_csv(upload)
                    else:
                        df = pd.read_excel(upload)
                    
                    st.success(f"File loaded: {len(df)} rows")
                    
                    # Show preview
                    with st.expander("Preview Data", expanded=True):
                        st.dataframe(df.head(10), use_container_width=True)
                    
                    # Validation summary
                    st.markdown("### Validation")
                    
                    required_cols = ["degree_code", "ay_label", "year", "term", "subject_code", "sequence", "title"]
                    missing_cols = [col for col in required_cols if col not in df.columns]
                    
                    if missing_cols:
                        st.error(f"❌ Missing required columns: {', '.join(missing_cols)}")
                    else:
                        st.success("✅ All required columns present")
                    
                    # Action buttons
                    st.markdown("---")
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        if st.button("🔍 Dry Run (Validate Only)", key="syllabus_dry_run", use_container_width=True):
                            with st.spinner("Validating..."):
                                errs, _ = import_syllabus(engine, df, dry_run=True)
                                
                                if len(errs) > 0:
                                    st.error(f"❌ Found {len(errs)} validation issues:")
                                    st.dataframe(errs, use_container_width=True)
                                else:
                                    st.success("✅ No issues found! Ready to import.")
                    
                    with col2:
                        if st.button("📥 Import Now", key="syllabus_import_now", type="primary", use_container_width=True):
                            with st.spinner("Importing syllabus points..."):
                                errs, count = import_syllabus(engine, df, dry_run=False)
                                
                                if len(errs) > 0:
                                    st.warning(f"⚠️ Imported {count} rows with {len(errs)} issues:")
                                    st.dataframe(errs, use_container_width=True)
                                else:
                                    success(f"✅ Successfully imported {count} syllabus points!")
                                
                                st.cache_data.clear()
                                st.rerun()
                
                except Exception as e:
                    st.error(f"Error reading file: {str(e)}")
        else:
            st.info("📖 Import is only available for users with edit permissions")    
    # ======================================================================
    # TAB 4: AUDIT TRAIL
    # ======================================================================
    with tab4:
        st.subheader("Audit Trail")
        
        with engine.begin() as conn:
            logs = conn.execute(sa_text("""
                SELECT subject_code, degree_code, program_code, branch_code, 
                       action, note, actor, at
                FROM subjects_catalog_audit
                ORDER BY id DESC LIMIT 50
            """)).fetchall()
        
        if logs:
            df = pd.DataFrame([dict(r._mapping) for r in logs])
            st.dataframe(df, use_container_width=True)
        else:
            st.info("No audit logs found")

render()
