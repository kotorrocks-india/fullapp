from __future__ import annotations

# --- Original Imports ---
import pandas as pd
import streamlit as st
from sqlalchemy import text as sa_text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.engine import Engine

from core.settings import load_settings
from core.db import get_engine, init_db
from core.policy import require_page, can_edit_page, user_roles
from core.ui import render_footer_global

# --- ADDED FOR MIGRATION ---
from schemas.degrees_schema import migrate_degrees
# --- END ADDED ---

# --- ADDED FOR IMPORT/EXPORT ---
import io
import csv
import re
import json  # ADDED FOR PAYLOAD FIX
from typing import List, Tuple, Dict, Any
# --- END ADDED ---


# --- ADDED FOR IMPORT/EXPORT ---
# Column definitions for import/export
PROGRAM_IMPORT_COLS = ["program_code", "program_name", "active", "sort_order", "description"]
BRANCH_IMPORT_COLS = ["branch_code", "branch_name", "program_code", "active", "sort_order", "description"]
CG_IMPORT_COLS = ["group_code", "group_name", "kind", "active", "sort_order", "description"]
CGL_IMPORT_COLS = ["group_code", "program_code", "branch_code"]

# Validation Regex
CODE_RE = re.compile(r"^[A-Z0-9_-]+$")
# --- END ADDED ---


# ─────────────────────────── DB helpers ───────────────────────────

# --- ADDED FOR MIGRATION ---
def _ensure_curriculum_columns(engine: Engine):
    """Ensure the curriculum group columns exist in the degrees table."""
    try:
        with engine.begin() as conn:
            # Check if columns exist
            columns = conn.execute(sa_text("PRAGMA table_info(degrees)")).fetchall()
            column_names = [col[1] for col in columns]

            # Add missing columns
            if 'cg_degree' not in column_names:
                conn.execute(sa_text("ALTER TABLE degrees ADD COLUMN cg_degree INTEGER NOT NULL DEFAULT 0"))
            if 'cg_program' not in column_names:
                conn.execute(sa_text("ALTER TABLE degrees ADD COLUMN cg_program INTEGER NOT NULL DEFAULT 0"))
            if 'cg_branch' not in column_names:
                conn.execute(sa_text("ALTER TABLE degrees ADD COLUMN cg_branch INTEGER NOT NULL DEFAULT 0"))
    except Exception as e:
        # Don't show sidebar info here, as it's not the main page for it
        # This will silently fail if, e.g., the degrees table doesn't exist yet
        pass
# --- END ADDED ---


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

@st.cache_data
def _degrees_df(_engine: Engine): # <-- FIXED: Argument renamed
    cols = ["code","title","cohort_splitting_mode","roll_number_scope","active","sort_order","logo_file_name"]
    with _engine.begin() as conn: # <-- FIXED: Variable renamed
        rows = conn.execute(sa_text("""
            SELECT code, title, cohort_splitting_mode, roll_number_scope, active, sort_order, logo_file_name
              FROM degrees
             ORDER BY sort_order, code
        """)).fetchall()
    if not rows:
        return pd.DataFrame(columns=cols)
    return pd.DataFrame([dict(r._mapping) for r in rows], columns=cols)

@st.cache_data
def _programs_df(_engine: Engine, degree_filter: str | None = None): # <-- FIXED: Argument renamed
    cols = ["id","program_code","program_name","degree_code","active","sort_order","logo_file_name","description"]
    q = f"SELECT {', '.join(cols)} FROM programs"
    params = {}
    if degree_filter:
        q += " WHERE degree_code=:d"
        params["d"] = degree_filter
    q += " ORDER BY degree_code, sort_order, lower(program_code)"
    with _engine.begin() as conn: # <-- FIXED: Variable renamed
        rows = conn.execute(sa_text(q), params).fetchall()
    if not rows:
        return pd.DataFrame(columns=cols)
    return pd.DataFrame([dict(r._mapping) for r in rows], columns=cols)

@st.cache_data
def _table_cols(_engine: Engine, table: str) -> set[str]: # <-- FIXED: Argument renamed
    try:
        with _engine.begin() as conn: # <-- FIXED: Variable renamed
            return {c[1] for c in conn.execute(sa_text(f"PRAGMA table_info({table})")).fetchall()}
    except:
        return set()

@st.cache_data
def _branches_df(_engine: Engine, degree_filter: str | None = None, program_id: int | None = None): # <-- FIXED: Argument renamed
    """List branches; supports schemas with or without degree_code on branches."""
    bcols = _table_cols(_engine, "branches") # <-- FIXED: Variable renamed
    has_pid = "program_id" in bcols
    has_deg = "degree_code" in bcols
    params = {}
    
    # Case 1: Schema supports linking branches to BOTH programs and degrees
    if has_pid and has_deg:
        wh = []
        if degree_filter:
            params["deg"] = degree_filter
            if program_id:
                wh.append("b.program_id = :pid")
                params["pid"] = program_id
                wh.append("p.degree_code = :deg")
            elif degree_filter:
                wh.append("(p.degree_code = :deg OR b.degree_code = :deg)")
        where = (" WHERE " + " AND ".join(wh)) if wh else ""
        with _engine.begin() as conn: # <-- FIXED: Variable renamed
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
    
    # Case 2: Schema ONLY supports linking branches to programs
    elif has_pid:
        wh = []
        if program_id:
            wh.append("b.program_id=:pid"); params["pid"] = program_id
        if degree_filter:
            wh.append("p.degree_code=:deg"); params["deg"] = degree_filter
        where = (" WHERE " + " AND ".join(wh)) if wh else ""
        with _engine.begin() as conn: # <-- FIXED: Variable renamed
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
    
    # Case 3: Schema ONLY supports linking branches to degrees
    elif has_deg:
        wh = []
        if degree_filter:
            wh.append("degree_code=:deg"); params["deg"] = degree_filter
        where = (" WHERE " + " AND ".join(wh)) if wh else ""
        with _engine.begin() as conn: # <-- FIXED: Variable renamed
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

# --- ADDED: New helpers for import logic (uncached for transactional safety) ---

def _fetch_program_by_code(conn, degree_code: str, program_code: str):
    """Fetches a single program by its degree and code."""
    return conn.execute(sa_text("""
        SELECT * FROM programs WHERE degree_code = :dc AND program_code = :pc
    """), {"dc": degree_code, "pc": program_code}).fetchone()

def _fetch_branch_by_code(conn, degree_code: str, branch_code: str):
    """Fetches a single branch by its degree and code."""
    bcols = _table_cols(conn.engine, "branches") # Pass engine from connection
    if "degree_code" in bcols:
        return conn.execute(sa_text("""
            SELECT * FROM branches WHERE degree_code = :dc AND branch_code = :bc
        """), {"dc": degree_code, "bc": branch_code}).fetchone()
    else:
        return conn.execute(sa_text("""
            SELECT b.* FROM branches b
            LEFT JOIN programs p ON p.id = b.program_id
            WHERE p.degree_code = :dc AND b.branch_code = :bc
        """), {"dc": degree_code, "bc": branch_code}).fetchone()

def _program_id_by_code(conn, degree_code: str, program_code: str) -> int | None:
    """Finds a program's primary key (id) from its code and degree."""
    row = conn.execute(sa_text("""
        SELECT id FROM programs
         WHERE degree_code=:d AND lower(program_code)=lower(:pc)
         LIMIT 1
    """), {"d": degree_code, "pc": program_code}).fetchone()
    return int(row.id) if row else None

# --- END ADDED ---

# DB helpers for Curriculum Groups
@st.cache_data
def _curriculum_groups_df(_engine: Engine, degree_filter: str): # <-- FIXED: Argument renamed
    with _engine.begin() as conn: # <-- FIXED: Variable renamed
        rows = conn.execute(sa_text("""
            SELECT id, group_code, group_name, kind, active, sort_order, description
              FROM curriculum_groups
             WHERE degree_code=:d
             ORDER BY sort_order, group_code
        """), {"d": degree_filter}).fetchall()
    return pd.DataFrame([dict(r._mapping) for r in rows]) if rows else pd.DataFrame()

@st.cache_data
def _curriculum_group_links_df(_engine: Engine, degree_filter: str): # <-- FIXED: Argument renamed
    with _engine.begin() as conn: # <-- FIXED: Variable renamed
        rows = conn.execute(sa_text("""
            SELECT cgl.id, cg.group_code, cgl.program_code, cgl.branch_code
              FROM curriculum_group_links cgl
              JOIN curriculum_groups cg ON cg.id = cgl.group_id
             WHERE cgl.degree_code = :d
             ORDER BY cg.group_code, cgl.program_code, cgl.branch_code
        """), {"d": degree_filter}).fetchall()
    return pd.DataFrame([dict(r._mapping) for r in rows]) if rows else pd.DataFrame()

@st.cache_data
def _get_approvals_df(_engine: Engine, object_types: list[str]): # <-- FIXED: Argument renamed
    """Fetches approval requests for specific object types."""
    cols = _table_cols(_engine, "approvals") # <-- FIXED: Variable renamed
    select_cols = ["id", "object_type", "object_id", "action", "status"]
    if "requester_email" in cols:
        select_cols.append("requester_email")
    elif "requester" in cols:
        select_cols.append("requester AS requester_email")
    if "reason_note" in cols:
        select_cols.append("reason_note")
    if "requested_at" in cols:
        select_cols.append("requested_at")
    if "decided_at" in cols:
        select_cols.append("decided_at")
    if "decider_email" in cols:
        select_cols.append("decider_email")
    
    placeholders = ", ".join([f"'{t}'" for t in object_types])
    order_by = "ORDER BY id DESC"
    if "requested_at" in cols:
        order_by = "ORDER BY requested_at DESC"
    
    with _engine.begin() as conn: # <-- FIXED: Variable renamed
        rows = conn.execute(sa_text(f"""
            SELECT {', '.join(select_cols)}
              FROM approvals
             WHERE object_type IN ({placeholders})
            {order_by}
        """)).fetchall()
    
    selected_final_cols = [c.split(" AS ")[-1] for c in select_cols]
    return pd.DataFrame([dict(r._mapping) for r in rows], columns=selected_final_cols) if rows else pd.DataFrame(columns=selected_final_cols)

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
    if 'degree_code' in _table_cols(conn.engine, 'branches'): # <-- FIXED: Pass engine from connection
        q += " WHERE b.degree_code = :dc"
    else:
        q += " JOIN programs p ON p.id = b.program_id WHERE p.degree_code = :dc"
    
    rows = conn.execute(sa_text(q), {"dc": degree_code}).fetchall()
    return {r.branch_code: (r.years, r.terms_per_year) for r in rows}

# ───────────────── schema-aware audits / approvals ─────────────────

def _audit_program(conn, action: str, actor: str, row: dict, note: str = ""):
    cols = _table_cols(conn.engine, "programs_audit") # <-- FIXED: Pass engine from connection
    audit_row = {k: v for k, v in row.items() if k != 'id'}
    payload = { "action": action, "actor": actor, "note": note, **audit_row }
    fields = [k for k in payload.keys() if k in cols]
    params = {k: payload[k] for k in fields}
    conn.execute(sa_text(
        f"INSERT INTO programs_audit({', '.join(fields)}) VALUES({', '.join(':'+f for f in fields)})"
    ), params)

def _audit_branch(conn, action: str, actor: str, row: dict, note: str = ""):
    cols = _table_cols(conn.engine, "branches_audit") # <-- FIXED: Pass engine from connection
    audit_row = {k: v for k, v in row.items() if k != 'id'}
    payload = { "action": action, "actor": actor, "note": note, **audit_row }
    fields = [k for k in payload.keys() if k in cols]
    params = {k: payload[k] for k in fields}
    conn.execute(sa_text(
        f"INSERT INTO branches_audit({', '.join(fields)}) VALUES({', '.join(':'+f for f in fields)})"
    ), params)

def _audit_curriculum_group(conn, action: str, actor: str, row: dict, note: str = ""):
    cols = _table_cols(conn.engine, "curriculum_groups_audit") # <-- FIXED: Pass engine from connection
    audit_row = {k: v for k, v in row.items() if k != 'id'}
    payload = { "action": action, "actor": actor, "note": note, **audit_row }
    fields = [k for k in payload.keys() if k in cols]
    params = {k: payload[k] for k in fields}
    conn.execute(sa_text(
        f"INSERT INTO curriculum_groups_audit({', '.join(fields)}) VALUES({', '.join(':'+f for f in fields)})"
    ), params)

def _audit_curriculum_group_link(conn, action: str, actor: str, row: dict, note: str = ""):
    cols = _table_cols(conn.engine, "curriculum_group_links_audit") # <-- FIXED: Pass engine from connection
    if not cols: return
    audit_row = {k: v for k, v in row.items() if k != 'id'}
    payload = { "action": action, "actor": actor, "note": note, **audit_row }
    fields = [k for k in payload.keys() if k in cols]
    params = {k: payload[k] for k in fields}
    conn.execute(sa_text(
        f"INSERT INTO curriculum_group_links_audit({', '.join(fields)}) VALUES({', '.join(':'+f for f in fields)})"
    ), params)

def _approvals_columns(conn) -> set[str]:
    return _table_cols(conn.engine, "approvals") # <-- FIXED: Pass engine from connection

# ============================================================================
# FIXED: _queue_approval function with payload support
# ============================================================================
def _queue_approval(conn, *, object_type: str, object_id: str, action: str,
                    requester_email: str | None, reason_note: str, 
                    rule_value: str | None = None, payload: dict | None = None):
    """
    Queue an approval request in the approvals table.
    
    FIXED: Now accepts and stores payload parameter as JSON.
    """
    cols = _approvals_columns(conn)
    fields = ["object_type", "object_id", "action", "status"]
    params = {
        "object_type": object_type, 
        "object_id": object_id, 
        "action": action, 
        "status": "pending"
    }
    
    if "requester_email" in cols and requester_email:
        fields.append("requester_email")
        params["requester_email"] = requester_email
    elif "requester" in cols and requester_email:
        fields.append("requester")
        params["requester"] = requester_email
    
    if "rule" in cols and rule_value:
        fields.append("rule")
        params["rule"] = rule_value
    
    if "reason_note" in cols:
        fields.append("reason_note")
        params["reason_note"] = reason_note
    
    # CRITICAL FIX: Store payload as JSON
    if "payload" in cols and payload:
        fields.append("payload")
        params["payload"] = json.dumps(payload)
    
    conn.execute(sa_text(
        f"INSERT INTO approvals({', '.join(fields)}) VALUES({', '.join(':'+f for f in fields)})"
    ), params)

# ============================================================================
# FIXED: _request_deletion function with payload building
# ============================================================================
def _request_deletion(
    conn,
    *,
    object_type: str,
    object_id: str | int,
    actor: str,
    audit_function: callable,
    audit_row: dict,
    reason_note: str,
    rule_value: str | None = "either_one",
    additional_payload: dict | None = None
) -> Tuple[bool, Exception | None]:
    """
    Universal handler to queue an item for deletion via the approvals table.
    
    FIXED: Now builds proper payload with object-specific information.
    
    Args:
        conn: The SQLAlchemy connection.
        object_type: The type of object (e.g., "program", "branch", "curriculum_group").
        object_id: The primary key of the object.
        actor: The email/name of the user requesting deletion.
        audit_function: The specific audit function to call (e.g., _audit_program).
        audit_row: The full data row of the object being deleted (as a dict).
        reason_note: The note to show in the approval request.
        rule_value: The approval rule (e.g., "either_one").
        additional_payload: Extra data to merge into payload (optional).

    Returns:
        A tuple (success: bool, error: Exception | None).
    """
    try:
        # Build the payload with information needed by action handlers
        payload = {}
        
        # For programs, include program_code and cascade flag
        if object_type == "program":
            payload["program_code"] = audit_row.get("program_code")
            payload["cascade"] = True  # Allow cascade deletion of branches/semesters
            payload["degree_code"] = audit_row.get("degree_code")
        
        # For branches, include branch identifiers
        elif object_type == "branch":
            payload["branch_code"] = audit_row.get("branch_code")
            payload["branch_id"] = object_id  # Numeric ID for deletion
            payload["degree_code"] = audit_row.get("degree_code")
            payload["program_code"] = audit_row.get("program_code")
        
        # For curriculum groups
        elif object_type == "curriculum_group":
            payload["group_code"] = audit_row.get("group_code")
            payload["degree_code"] = audit_row.get("degree_code")
            payload["group_id"] = object_id
        
        # Merge any additional payload data
        if additional_payload:
            payload.update(additional_payload)
        
        # 1. Queue the approval WITH PAYLOAD
        _queue_approval(
            conn,
            object_type=object_type,
            object_id=str(object_id),
            action="delete",
            requester_email=actor,
            reason_note=reason_note,
            rule_value=rule_value,
            payload=payload  # CRITICAL: Pass the payload!
        )
        
        # 2. Log the delete request to the specific audit table
        # Remove 'id' as audit tables don't typically mirror the PK.
        audit_payload = {k: v for k, v in audit_row.items() if k != 'id'}
        
        audit_function(
            conn,
            action="delete_request",
            actor=actor,
            row=audit_payload,
            note="Approval requested"
        )
        
        return True, None  # Success
        
    except Exception as e:
        # Propagate the error to be caught by the UI
        return False, e

# ───────────────── cohort helpers ─────────────────

COHORT_BOTH = "both"
COHORT_PROGRAM_OR_BRANCH = "program_or_branch"
COHORT_PROGRAM_ONLY = "program_only"
COHORT_BRANCH_ONLY = "branch_only"
COHORT_NONE = "none"

def allow_programs_for(mode: str) -> bool:
    return mode in {COHORT_BOTH, COHORT_PROGRAM_OR_BRANCH, COHORT_PROGRAM_ONLY}

def allow_branches_for(mode: str) -> bool:
    return mode in {COHORT_BOTH, COHORT_PROGRAM_OR_BRANCH, COHORT_BRANCH_ONLY}

def branches_require_program(mode: str) -> bool:
    return mode == COHORT_BOTH


# ============================================================================
# ENHANCED import_programs WITH DRY-RUN & DEBUG
# ============================================================================

def import_programs(
    conn, 
    df_import: pd.DataFrame, 
    degree_code: str, 
    actor: str,
    engine = None,
    dry_run: bool = False,
    debug: bool = False
) -> Tuple[int, int, List[str]]:
    """
    Imports Programs from a DataFrame, scoped to a specific Degree.
    
    Args:
        conn: Database connection
        df_import: DataFrame with program data
        degree_code: Degree code to import under
        actor: User performing import
        engine: Database engine (optional)
        dry_run: If True, simulate import without saving (default: False)
        debug: If True, show detailed debug info (default: False)
    
    Returns:
        (created_count, updated_count, errors)
    """
    created_count = 0
    updated_count = 0
    errors = []
    debug_info = []
    
    if dry_run:
        debug_info.append("🔍 DRY-RUN MODE: No changes will be saved to database")
    
    # Clean column names
    df_import.columns = [col.strip() for col in df_import.columns]
    
    if debug:
        debug_info.append(f"📊 Input DataFrame: {len(df_import)} rows, {len(df_import.columns)} columns")
        debug_info.append(f"📋 Columns found: {list(df_import.columns)}")
    
    # Check for required columns
    req_cols = set(PROGRAM_IMPORT_COLS)
    if not req_cols.issubset(df_import.columns):
        missing = list(req_cols - set(df_import.columns))
        errors.append(f"Import file is missing required columns: {', '.join(missing)}")
        return 0, 0, errors

    if debug:
        debug_info.append(f"✅ All required columns present: {PROGRAM_IMPORT_COLS}")
        debug_info.append(f"🎯 Target degree: {degree_code}")
        debug_info.append(f"👤 Actor: {actor}")
        debug_info.append("---")

    for idx, row in enumerate(df_import.itertuples(), start=1):
        code = ""
        try:
            # 1. Get data & validate
            code = str(getattr(row, "program_code", "")).strip().upper()
            name = str(getattr(row, "program_name", "")).strip()
            active = bool(int(getattr(row, "active", 1)))
            sort_order = int(getattr(row, "sort_order", 0))
            desc = str(getattr(row, "description", "")).strip()

            if debug:
                debug_info.append(f"📝 Row {idx}: Processing '{code}'")

            if not code:
                errors.append(f"Skipped row {row.Index}: 'program_code' is missing.")
                if debug:
                    debug_info.append(f"  ❌ Skipped: No program_code")
                continue
                
            if not CODE_RE.match(code):
                errors.append(f"Skipped row {row.Index} ({code}): 'program_code' contains invalid characters.")
                if debug:
                    debug_info.append(f"  ❌ Skipped: Invalid code format '{code}'")
                continue
                
            if not name:
                errors.append(f"Skipped row {row.Index} ({code}): 'program_name' is missing.")
                if debug:
                    debug_info.append(f"  ❌ Skipped: No program_name")
                continue

            # 2. Check for existing program
            existing = _fetch_program_by_code(conn, degree_code, code)
            
            new_data = {
                "program_name": name,
                "active": active,
                "sort_order": sort_order,
                "description": desc,
            }

            if existing:
                # --- UPDATE ---
                action = "update"
                old_data = {k: getattr(existing, k) for k in new_data}
                changes = {k: v for k, v in new_data.items() if str(v) != str(old_data[k])}
                
                if not changes:
                    if debug:
                        debug_info.append(f"  ⏭️  No changes needed (data identical)")
                    continue

                if debug:
                    debug_info.append(f"  🔄 UPDATE: {len(changes)} field(s) changed")
                    for k, v in changes.items():
                        debug_info.append(f"     • {k}: '{old_data[k]}' → '{v}'")

                if not dry_run:
                    conn.execute(sa_text(f"""
                        UPDATE programs
                           SET {', '.join([f"{k} = :{k}" for k in changes])}
                         WHERE id = :id
                    """), {**changes, "id": existing.id})
                    
                    if debug:
                        debug_info.append(f"  ✅ SQL executed successfully")
                
                updated_count += 1
                audit_note = "Import: Updated" if not dry_run else "Import: Updated (DRY-RUN)"
                audit_payload = {"degree_code": degree_code, "program_code": code, **changes}

            else:
                # --- CREATE ---
                action = "create"
                
                if debug:
                    debug_info.append(f"  ➕ CREATE: New program")
                    debug_info.append(f"     • program_code: {code}")
                    debug_info.append(f"     • program_name: {name}")
                    debug_info.append(f"     • active: {active}")
                    debug_info.append(f"     • sort_order: {sort_order}")

                if not dry_run:
                    conn.execute(sa_text("""
                        INSERT INTO programs (degree_code, program_code, program_name, active, sort_order, description)
                        VALUES(:dc, :pc, :name, :active, :sort, :desc)
                    """), {
                        "dc": degree_code,
                        "pc": code,
                        "name": name,
                        "active": active,
                        "sort": sort_order,
                        "desc": desc,
                    })
                    
                    if debug:
                        debug_info.append(f"  ✅ SQL executed successfully")
                
                created_count += 1
                audit_note = "Import: Created" if not dry_run else "Import: Created (DRY-RUN)"
                audit_payload = {"degree_code": degree_code, "program_code": code, **new_data}
            
            # 3. Audit (skip in dry-run)
            if not dry_run:
                _audit_program(conn, action, actor, audit_payload, note=audit_note)

        except Exception as e:
            error_msg = f"Error on row {row.Index} (Program '{code}'): {e}"
            errors.append(error_msg)
            if debug:
                debug_info.append(f"  ❌ ERROR: {e}")

    # Show debug info if enabled
    if debug or dry_run:
        st.info("**Import Debug Information:**")
        for line in debug_info:
            st.text(line)
    
    # Verify results if not dry-run
    if not dry_run and debug:
        st.write("---")
        st.write("**Verification: Checking database...**")
        try:
            verify_count = conn.execute(sa_text(
                "SELECT COUNT(*) FROM programs WHERE degree_code = :dc"
            ), {"dc": degree_code}).fetchone()[0]
            st.success(f"✅ Database now has {verify_count} program(s) for degree '{degree_code}'")
        except Exception as e:
            st.error(f"❌ Verification failed: {e}")

    return created_count, updated_count, errors


# ============================================================================
# ENHANCED import_branches WITH DRY-RUN & DEBUG
# ============================================================================

def import_branches(
    conn, 
    df_import: pd.DataFrame, 
    degree_code: str, 
    actor: str,
    br_has_pid: bool,
    engine = None,
    dry_run: bool = False,
    debug: bool = False
) -> Tuple[int, int, List[str]]:
    """
    Imports Branches from a DataFrame, scoped to a specific Degree.
    
    Args:
        conn: Database connection
        df_import: DataFrame with branch data
        degree_code: Degree code to import under
        actor: User performing import
        br_has_pid: Whether branches table has program_id column
        engine: Database engine (optional)
        dry_run: If True, simulate import without saving (default: False)
        debug: If True, show detailed debug info (default: False)
    
    Returns:
        (created_count, updated_count, errors)
    """
    created_count = 0
    updated_count = 0
    errors = []
    debug_info = []
    
    if dry_run:
        debug_info.append("🔍 DRY-RUN MODE: No changes will be saved to database")
    
    # Clean column names
    df_import.columns = [col.strip() for col in df_import.columns]

    if debug:
        debug_info.append(f"📊 Input DataFrame: {len(df_import)} rows, {len(df_import.columns)} columns")
        debug_info.append(f"📋 Columns found: {list(df_import.columns)}")

    # Check for required columns
    req_cols = set(BRANCH_IMPORT_COLS)
    if not req_cols.issubset(df_import.columns):
        missing = list(req_cols - set(df_import.columns))
        errors.append(f"Import file is missing required columns: {', '.join(missing)}")
        return 0, 0, errors

    if debug:
        debug_info.append(f"✅ All required columns present: {BRANCH_IMPORT_COLS}")
        debug_info.append(f"🎯 Target degree: {degree_code}")
        debug_info.append(f"👤 Actor: {actor}")
        debug_info.append(f"🔗 Schema uses program_id: {br_has_pid}")
        debug_info.append("---")

    for idx, row in enumerate(df_import.itertuples(), start=1):
        code = ""
        prog_code = ""
        try:
            # 1. Get data & validate
            code = str(getattr(row, "branch_code", "")).strip().upper()
            name = str(getattr(row, "branch_name", "")).strip()
            prog_code = str(getattr(row, "program_code", "")).strip().upper()
            active = bool(int(getattr(row, "active", 1)))
            sort_order = int(getattr(row, "sort_order", 0))
            desc = str(getattr(row, "description", "")).strip()

            if debug:
                debug_info.append(f"📝 Row {idx}: Processing '{code}'")

            if not code:
                errors.append(f"Skipped row {row.Index}: 'branch_code' is missing.")
                if debug:
                    debug_info.append(f"  ❌ Skipped: No branch_code")
                continue
                
            if not CODE_RE.match(code):
                errors.append(f"Skipped row {row.Index} ({code}): 'branch_code' contains invalid characters.")
                if debug:
                    debug_info.append(f"  ❌ Skipped: Invalid code format '{code}'")
                continue
                
            if not name:
                errors.append(f"Skipped row {row.Index} ({code}): 'branch_name' is missing.")
                if debug:
                    debug_info.append(f"  ❌ Skipped: No branch_name")
                continue
            
            # 2. Resolve Program ID
            if not prog_code:
                errors.append(f"Skipped row {row.Index} ({code}): 'program_code' is missing.")
                if debug:
                    debug_info.append(f"  ❌ Skipped: No program_code")
                continue
            
            program_id = _program_id_by_code(conn, degree_code, prog_code)
            if br_has_pid and not program_id:
                errors.append(f"Skipped row {row.Index} ({code}): Program '{prog_code}' not found in degree '{degree_code}'. Import programs first.")
                if debug:
                    debug_info.append(f"  ❌ Skipped: Parent program '{prog_code}' doesn't exist")
                continue

            if debug and program_id:
                debug_info.append(f"  🔗 Linked to program '{prog_code}' (ID: {program_id})")

            # 3. Check for existing branch
            existing = _fetch_branch_by_code(conn, degree_code, code)
            
            new_data = {
                "branch_name": name,
                "active": active,
                "sort_order": sort_order,
                "description": desc,
            }
            if br_has_pid and program_id:
                new_data["program_id"] = program_id

            if existing:
                # --- UPDATE ---
                action = "update"
                old_data = {k: getattr(existing, k) for k in new_data if hasattr(existing, k)}
                changes = {k: v for k, v in new_data.items() if k not in old_data or str(v) != str(old_data[k])}
                
                if not changes:
                    if debug:
                        debug_info.append(f"  ⏭️  No changes needed (data identical)")
                    continue

                if debug:
                    debug_info.append(f"  🔄 UPDATE: {len(changes)} field(s) changed")
                    for k, v in changes.items():
                        debug_info.append(f"     • {k}: '{old_data.get(k)}' → '{v}'")

                if not dry_run:
                    conn.execute(sa_text(f"""
                        UPDATE branches
                           SET {', '.join([f"{k} = :{k}" for k in changes])}
                         WHERE id = :id
                    """), {**changes, "id": existing.id})
                    
                    if debug:
                        debug_info.append(f"  ✅ SQL executed successfully")
                
                updated_count += 1
                audit_note = "Import: Updated" if not dry_run else "Import: Updated (DRY-RUN)"
                audit_payload = {"degree_code": degree_code, "branch_code": code, **changes}
            
            else:
                # --- CREATE ---
                action = "create"
                
                if debug:
                    debug_info.append(f"  ➕ CREATE: New branch")
                    debug_info.append(f"     • branch_code: {code}")
                    debug_info.append(f"     • branch_name: {name}")
                    debug_info.append(f"     • program_code: {prog_code}")
                    debug_info.append(f"     • active: {active}")

                if not dry_run:
                    insert_data = new_data.copy()
                    insert_data["degree_code"] = degree_code
                    insert_data["branch_code"] = code
                    
                    bcols = _table_cols(engine if engine else conn.engine, "branches")
                    insert_cols = {k: v for k, v in insert_data.items() if k in bcols}
                    
                    col_names = ", ".join(insert_cols.keys())
                    col_params = ", ".join([f":{k}" for k in insert_cols.keys()])

                    conn.execute(sa_text(f"""
                        INSERT INTO branches ({col_names})
                        VALUES ({col_params})
                    """), insert_cols)
                    
                    if debug:
                        debug_info.append(f"  ✅ SQL executed successfully")
                
                created_count += 1
                audit_note = "Import: Created" if not dry_run else "Import: Created (DRY-RUN)"
                audit_payload = {"degree_code": degree_code, "branch_code": code, **new_data}

            # 3. Audit (skip in dry-run)
            if not dry_run:
                _audit_branch(conn, action, actor, audit_payload, note=audit_note)

        except Exception as e:
            error_msg = f"Error on row {row.Index} (Branch '{code}'): {e}"
            errors.append(error_msg)
            if debug:
                debug_info.append(f"  ❌ ERROR: {e}")

    # Show debug info if enabled
    if debug or dry_run:
        st.info("**Import Debug Information:**")
        for line in debug_info:
            st.text(line)
    
    # Verify results if not dry-run
    if not dry_run and debug:
        st.write("---")
        st.write("**Verification: Checking database...**")
        try:
            # Count branches for this degree
            if br_has_pid:
                verify_count = conn.execute(sa_text("""
                    SELECT COUNT(*) FROM branches b
                    JOIN programs p ON b.program_id = p.id
                    WHERE p.degree_code = :dc
                """), {"dc": degree_code}).fetchone()[0]
            else:
                verify_count = conn.execute(sa_text(
                    "SELECT COUNT(*) FROM branches WHERE degree_code = :dc"
                ), {"dc": degree_code}).fetchone()[0]
            
            st.success(f"✅ Database now has {verify_count} branch(es) for degree '{degree_code}'")
        except Exception as e:
            st.error(f"❌ Verification failed: {e}")

    return created_count, updated_count, errors


# ============================================================================
# ENHANCED import_cgs WITH DRY-RUN & DEBUG
# ============================================================================

def import_cgs(
    conn, 
    df_import: pd.DataFrame, 
    degree_code: str, 
    actor: str, 
    cg_allowed: bool,
    engine = None,
    dry_run: bool = False,
    debug: bool = False
) -> Tuple[int, int, List[str]]:
    """
    Imports Curriculum Groups from a DataFrame, scoped to a specific Degree.
    
    Args:
        conn: Database connection
        df_import: DataFrame with curriculum group data
        degree_code: Degree code to import under
        actor: User performing import
        cg_allowed: Whether curriculum groups are allowed for this degree
        engine: Database engine (optional)
        dry_run: If True, simulate import without saving (default: False)
        debug: If True, show detailed debug info (default: False)
    
    Returns:
        (created_count, updated_count, errors)
    """
    created_count = 0
    updated_count = 0
    errors = []
    debug_info = []
    
    if dry_run:
        debug_info.append("🔍 DRY-RUN MODE: No changes will be saved to database")
    
    if not cg_allowed:
        errors.append("Import failed: This degree's cohort mode does not support curriculum groups.")
        return 0, 0, errors

    # Clean column names
    df_import.columns = [col.strip() for col in df_import.columns]
    
    if debug:
        debug_info.append(f"📊 Input DataFrame: {len(df_import)} rows, {len(df_import.columns)} columns")
        debug_info.append(f"📋 Columns found: {list(df_import.columns)}")
    
    # Check for required columns
    req_cols = set(CG_IMPORT_COLS)
    if not req_cols.issubset(df_import.columns):
        missing = list(req_cols - set(df_import.columns))
        errors.append(f"Import file is missing required columns: {', '.join(missing)}")
        return 0, 0, errors

    if debug:
        debug_info.append(f"✅ All required columns present: {CG_IMPORT_COLS}")
        debug_info.append(f"🎯 Target degree: {degree_code}")
        debug_info.append(f"👤 Actor: {actor}")
        debug_info.append("---")

    for idx, row in enumerate(df_import.itertuples(), start=1):
        code = ""
        try:
            # 1. Get data & validate
            code = str(getattr(row, "group_code", "")).strip().upper()
            name = str(getattr(row, "group_name", "")).strip()
            kind = str(getattr(row, "kind", "")).strip()
            active = bool(int(getattr(row, "active", 1)))
            sort_order = int(getattr(row, "sort_order", 0))
            desc = str(getattr(row, "description", "")).strip()

            if debug:
                debug_info.append(f"📝 Row {idx}: Processing '{code}'")

            if not code:
                errors.append(f"Skipped row {row.Index}: 'group_code' is missing.")
                if debug:
                    debug_info.append(f"  ❌ Skipped: No group_code")
                continue
                
            if not CODE_RE.match(code):
                errors.append(f"Skipped row {row.Index} ({code}): 'group_code' contains invalid characters.")
                if debug:
                    debug_info.append(f"  ❌ Skipped: Invalid code format '{code}'")
                continue
                
            if not name:
                errors.append(f"Skipped row {row.Index} ({code}): 'group_name' is missing.")
                if debug:
                    debug_info.append(f"  ❌ Skipped: No group_name")
                continue
                
            if kind not in ("pseudo", "cohort"):
                errors.append(f"Skipped row {row.Index} ({code}): 'kind' must be 'pseudo' or 'cohort'.")
                if debug:
                    debug_info.append(f"  ❌ Skipped: Invalid kind '{kind}'")
                continue

            # 2. Check for existing curriculum group
            existing = conn.execute(sa_text("""
                SELECT * FROM curriculum_groups WHERE degree_code = :dc AND group_code = :gc
            """), {"dc": degree_code, "gc": code}).fetchone()
            
            new_data = {
                "group_name": name,
                "kind": kind,
                "active": active,
                "sort_order": sort_order,
                "description": desc,
            }

            if existing:
                # --- UPDATE ---
                action = "update"
                old_data = {k: getattr(existing, k) for k in new_data}
                changes = {k: v for k, v in new_data.items() if str(v) != str(old_data[k])}
                
                if not changes:
                    if debug:
                        debug_info.append(f"  ⏭️  No changes needed (data identical)")
                    continue

                if debug:
                    debug_info.append(f"  🔄 UPDATE: {len(changes)} field(s) changed")
                    for k, v in changes.items():
                        debug_info.append(f"     • {k}: '{old_data[k]}' → '{v}'")

                if not dry_run:
                    conn.execute(sa_text(f"""
                        UPDATE curriculum_groups
                           SET {', '.join([f"{k} = :{k}" for k in changes])}
                         WHERE id = :id
                    """), {**changes, "id": existing.id})
                    
                    if debug:
                        debug_info.append(f"  ✅ SQL executed successfully")
                
                updated_count += 1
                audit_note = "Import: Updated" if not dry_run else "Import: Updated (DRY-RUN)"
                audit_payload = {"degree_code": degree_code, "group_code": code, **changes}

            else:
                # --- CREATE ---
                action = "create"
                
                if debug:
                    debug_info.append(f"  ➕ CREATE: New curriculum group")
                    debug_info.append(f"     • group_code: {code}")
                    debug_info.append(f"     • group_name: {name}")
                    debug_info.append(f"     • kind: {kind}")
                    debug_info.append(f"     • active: {active}")

                if not dry_run:
                    conn.execute(sa_text("""
                        INSERT INTO curriculum_groups (degree_code, group_code, group_name, kind, active, sort_order, description)
                        VALUES(:dc, :gc, :gn, :kind, :active, :sort_order, :desc)
                    """), {
                        "dc": degree_code,
                        "gc": code,
                        "gn": name,
                        "kind": kind,
                        "active": active,
                        "sort_order": sort_order,
                        "desc": desc,
                    })
                    
                    if debug:
                        debug_info.append(f"  ✅ SQL executed successfully")
                
                created_count += 1
                audit_note = "Import: Created" if not dry_run else "Import: Created (DRY-RUN)"
                audit_payload = {"degree_code": degree_code, "group_code": code, **new_data}
                
            # 3. Audit (skip in dry-run)
            if not dry_run:
                _audit_curriculum_group(conn, action, actor, audit_payload, note=audit_note)

        except Exception as e:
            error_msg = f"Error on row {row.Index} (Curriculum Group '{code}'): {e}"
            errors.append(error_msg)
            if debug:
                debug_info.append(f"  ❌ ERROR: {e}")

    # Show debug info if enabled
    if debug or dry_run:
        st.info("**Import Debug Information:**")
        for line in debug_info:
            st.text(line)
    
    # Verify results if not dry-run
    if not dry_run and debug:
        st.write("---")
        st.write("**Verification: Checking database...**")
        try:
            verify_count = conn.execute(sa_text(
                "SELECT COUNT(*) FROM curriculum_groups WHERE degree_code = :dc"
            ), {"dc": degree_code}).fetchone()[0]
            st.success(f"✅ Database now has {verify_count} curriculum group(s) for degree '{degree_code}'")
        except Exception as e:
            st.error(f"❌ Verification failed: {e}")

    return created_count, updated_count, errors


# ============================================================================
# ENHANCED import_cg_links WITH DRY-RUN & DEBUG
# ============================================================================

def import_cg_links(
    conn, 
    df_import: pd.DataFrame, 
    degree_code: str, 
    actor: str, 
    cg_allowed: bool,
    group_codes: List[str],
    program_codes: List[str],
    branch_codes: List[str],
    engine = None,
    dry_run: bool = False,
    debug: bool = False
) -> Tuple[int, int, List[str]]:
    """
    Imports Curriculum Group Links from a DataFrame.
    
    Args:
        conn: Database connection
        df_import: DataFrame with curriculum group link data
        degree_code: Degree code to import under
        actor: User performing import
        cg_allowed: Whether curriculum groups are allowed for this degree
        group_codes: List of valid group codes
        program_codes: List of valid program codes
        branch_codes: List of valid branch codes
        engine: Database engine (optional)
        dry_run: If True, simulate import without saving (default: False)
        debug: If True, show detailed debug info (default: False)
    
    Returns:
        (created_count, updated_count, errors)
    """
    created_count = 0
    updated_count = 0  # Not used for links since we don't update
    errors = []
    debug_info = []
    
    if dry_run:
        debug_info.append("🔍 DRY-RUN MODE: No changes will be saved to database")
    
    if not cg_allowed:
        errors.append("Import failed: This degree's cohort mode does not support curriculum group links.")
        return 0, 0, errors

    # Clean column names
    df_import.columns = [col.strip() for col in df_import.columns]
    
    if debug:
        debug_info.append(f"📊 Input DataFrame: {len(df_import)} rows, {len(df_import.columns)} columns")
        debug_info.append(f"📋 Columns found: {list(df_import.columns)}")

    # Check for required columns
    req_cols = set(CGL_IMPORT_COLS)
    if not req_cols.issubset(df_import.columns):
        missing = list(req_cols - set(df_import.columns))
        errors.append(f"Import file is missing required columns: {', '.join(missing)}")
        return 0, 0, errors

    if debug:
        debug_info.append(f"✅ All required columns present: {CGL_IMPORT_COLS}")
        debug_info.append(f"🎯 Target degree: {degree_code}")
        debug_info.append(f"👤 Actor: {actor}")
        debug_info.append(f"📊 Valid groups: {len(group_codes)}, programs: {len(program_codes)}, branches: {len(branch_codes)}")
        debug_info.append("---")

    for idx, row in enumerate(df_import.itertuples(), start=1):
        try:
            # 1. Get data & validate
            group_code = str(getattr(row, "group_code", "")).strip().upper()
            program_code = str(getattr(row, "program_code", "")).strip().upper()
            branch_code = str(getattr(row, "branch_code", "")).strip().upper()

            if debug:
                debug_info.append(f"📝 Row {idx}: Processing link for group '{group_code}', program '{program_code}', branch '{branch_code}'")

            if not group_code:
                errors.append(f"Skipped row {row.Index}: 'group_code' is missing.")
                if debug:
                    debug_info.append(f"  ❌ Skipped: No group_code")
                continue
                
            if group_code not in group_codes:
                errors.append(f"Skipped row {row.Index}: curriculum group '{group_code}' not found.")
                if debug:
                    debug_info.append(f"  ❌ Skipped: Group '{group_code}' not in valid groups")
                continue
                
            if program_code and program_code not in program_codes:
                errors.append(f"Skipped row {row.Index}: program_code '{program_code}' not found.")
                if debug:
                    debug_info.append(f"  ❌ Skipped: Program '{program_code}' not in valid programs")
                continue
                
            if branch_code and branch_code not in branch_codes:
                errors.append(f"Skipped row {row.Index}: branch_code '{branch_code}' not found.")
                if debug:
                    debug_info.append(f"  ❌ Skipped: Branch '{branch_code}' not in valid branches")
                continue

            # 2. Get group_id
            group_id_row = conn.execute(sa_text("""
                SELECT id FROM curriculum_groups WHERE degree_code = :dc AND group_code = :gc
            """), {"dc": degree_code, "gc": group_code}).fetchone()

            if not group_id_row:
                errors.append(f"Skipped row {row.Index}: curriculum group '{group_code}' not found in DB.")
                if debug:
                    debug_info.append(f"  ❌ Skipped: Group '{group_code}' not found in database")
                continue

            # 3. Check for duplicate link
            duplicate = conn.execute(sa_text("""
                SELECT id FROM curriculum_group_links
                WHERE degree_code=:dc AND group_id=:gid AND program_code=:pc AND branch_code=:bc
            """), {
                "dc": degree_code, 
                "gid": group_id_row.id, 
                "pc": program_code or None, 
                "bc": branch_code or None
            }).fetchone()

            if duplicate:
                if debug:
                    debug_info.append(f"  ⏭️  Link already exists, skipping")
                continue  # Link already exists, skip

            # 4. Create new link
            if debug:
                debug_info.append(f"  ➕ CREATE: New link")
                debug_info.append(f"     • group_id: {group_id_row.id}")
                debug_info.append(f"     • program_code: {program_code}")
                debug_info.append(f"     • branch_code: {branch_code}")

            if not dry_run:
                conn.execute(sa_text("""
                    INSERT INTO curriculum_group_links (group_id, degree_code, program_code, branch_code)
                    VALUES (:gid, :dc, :pc, :bc)
                """), {
                    "gid": group_id_row.id,
                    "dc": degree_code,
                    "pc": program_code or None,
                    "bc": branch_code or None
                })
                
                if debug:
                    debug_info.append(f"  ✅ SQL executed successfully")
            
            created_count += 1
            
            # 5. Audit (skip in dry-run)
            if not dry_run:
                _audit_curriculum_group_link(
                    conn,
                    "create",
                    actor,
                    {
                        "group_id": group_id_row.id,
                        "degree_code": degree_code,
                        "program_code": program_code or None,
                        "branch_code": branch_code or None
                    },
                    note="Import: Created"
                )

        except Exception as e:
            error_msg = f"Error on row {row.Index}: {e}"
            errors.append(error_msg)
            if debug:
                debug_info.append(f"  ❌ ERROR: {e}")

    # Show debug info if enabled
    if debug or dry_run:
        st.info("**Import Debug Information:**")
        for line in debug_info:
            st.text(line)
    
    # Verify results if not dry-run
    if not dry_run and debug:
        st.write("---")
        st.write("**Verification: Checking database...**")
        try:
            verify_count = conn.execute(sa_text(
                "SELECT COUNT(*) FROM curriculum_group_links WHERE degree_code = :dc"
            ), {"dc": degree_code}).fetchone()[0]
            st.success(f"✅ Database now has {verify_count} curriculum group link(s) for degree '{degree_sel}'")
        except Exception as e:
            st.error(f"❌ Verification failed: {e}")

    return created_count, updated_count, errors


# ─────────────────────────── Page ───────────────────────────

@require_page("Programs / Branches")
def render():
    settings = load_settings()
    engine = get_engine(settings.db.url) # <-- DEFINED as 'engine'
    
    # --- ADDED FOR MIGRATION ---
    # Ensure degrees table and cg_ columns exist *before* any reads.
    migrate_degrees(engine)
    _ensure_curriculum_columns(engine)
    # --- END ADDED ---
    
    init_db(engine)
    
    user = st.session_state.get("user") or {}
    actor = (user.get("email") or user.get("full_name") or "system")
    roles = user_roles()
    CAN_EDIT = can_edit_page("Programs / Branches", roles)
    
    # --- ADDED FOR ERROR CATCHING (FOR ALL 3 FORMS) ---
    if "prog_create_error" not in st.session_state:
        st.session_state.prog_create_error = None
    if "branch_create_error" not in st.session_state:
        st.session_state.branch_create_error = None
    if "cg_create_error" not in st.session_state:
        st.session_state.cg_create_error = None
    # --- END ADDED ---
    
    if not CAN_EDIT:
        st.info("📖 Read-only mode: You have view access but cannot modify data.")
    
    st.title("📚 Programs, Branches & Curriculum")
    
    try:
        # --- FIXED: Pass 'engine' to cached function
        ddf = _degrees_df(engine)
    except Exception as e:
        st.error(f"Failed to load degrees. Has the database been initialized? Error: {e}")
        st.warning("If this is a new setup, please visit the 'Degrees' page first to create the necessary tables.")
        return

    if ddf.empty:
        st.info("No degrees found. Please create a degree on the 'Degrees' page first.")
        return
    
    deg_codes = ddf["code"].tolist()
    degree_sel = st.selectbox("Degree", options=deg_codes, key="pb_deg_sel")
    
    # --- FIXED: Use 'engine' (no underscore) inside render()
    with engine.begin() as conn: # <-- THIS IS THE FIX for NameError: _engine
        deg = _fetch_degree(conn, degree_sel)
        dfp = _programs_df(engine, degree_sel) # <-- FIXED
        dfb_all = _branches_df(engine, degree_sel, program_id=None) # <-- FIXED
        
        SHOW_CG = bool(deg.cg_degree or deg.cg_program or deg.cg_branch)
        df_cg = _curriculum_groups_df(engine, degree_sel) if SHOW_CG else pd.DataFrame() # <-- FIXED
        df_cgl = _curriculum_group_links_df(engine, degree_sel) if SHOW_CG else pd.DataFrame() # <-- FIXED
        df_approvals = _get_approvals_df(engine, ["program", "branch", "curriculum_group"]) # <-- FIXED
        
        sem_binding = _get_semester_binding(conn, degree_sel) or 'degree'
        deg_struct = _get_degree_struct(conn, degree_sel)
        prog_structs = _get_program_structs_for_degree(conn, degree_sel)
        branch_structs = _get_branch_structs_for_degree(conn, degree_sel)
        
        bcols = _table_cols(engine, "branches") # <-- FIXED
        BR_HAS_PID = "program_id" in bcols
        BR_HAS_DEG = "degree_code" in bcols
    
    mode = str(deg.cohort_splitting_mode or "both").lower()
    
    st.caption(f"Degree: **{deg.title}** • Cohort mode: `{mode}` • Active: `{bool(deg.active)}`")
    st.markdown("---")
    
    # Show degree structure map
    with st.expander("Show full degree structure map", expanded=True):
        map_md = f"**Degree:** {deg.title} (`{degree_sel}`)\n"
        if sem_binding == 'degree' and deg_struct:
            map_md += f"- *Semester Structure: {deg_struct[0]} Years, {deg_struct[1]} Terms/Year*\n"
        
        if deg.cg_degree:
            linked_cgs_deg = df_cgl[df_cgl['program_code'].isnull() & df_cgl['branch_code'].isnull()] if not df_cgl.empty else pd.DataFrame()
            for _, cg_link_row in linked_cgs_deg.iterrows():
                map_md += f"- *Curriculum Group:* `{cg_link_row['group_code']}`\n"
        
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
                        linked_cgs_prog = df_cgl[
                            (df_cgl['program_code'] == prog_code) & (df_cgl['branch_code'].isnull())
                        ] if not df_cgl.empty else pd.DataFrame()
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
    
    # Define tab labels
    labels = []
    if allow_programs:
        labels.append("Programs")
    if allow_branches:
        labels.append("Branches")
    if SHOW_CG:
        labels.append("Curriculum Groups")
    if not labels:
        labels.append("View")
    
    # Initialize or update the session state for the active tab
    page_tab_key = f"pb_active_tab_{degree_sel}"
    if page_tab_key not in st.session_state:
        st.session_state[page_tab_key] = labels[0]
    
    # Check if the stored tab is still valid
    if st.session_state[page_tab_key] not in labels:
        st.session_state[page_tab_key] = labels[0]
    
    # Get the index of the currently active tab
    try:
        active_tab_index = labels.index(st.session_state[page_tab_key])
    except ValueError:
        active_tab_index = 0
    
    # Create the st.radio element that looks and acts like tabs
    active_tab = st.radio(
        "Navigation",
        options=labels,
        index=active_tab_index,
        key=page_tab_key,
        horizontal=True,
        label_visibility="collapsed"
    )
    
    # ───────────────────────── IMPORT/EXPORT SECTION (ENHANCED) ─────────────────────────
    
    if active_tab in ["Programs", "Branches", "Curriculum Groups"]:
        # Helper to create in-memory CSVs
        def _df_to_csv(df_to_conv: pd.DataFrame):
            with io.StringIO() as buffer:
                df_to_conv.to_csv(buffer, index=False, quoting=csv.QUOTE_ALL)
                return buffer.getvalue().encode('utf-8')

        # --- ADDED: COHORT MODE VALIDATION ---
        allow_prog_import = mode in ["both", "program_or_branch", "program_only"]
        allow_br_import = mode in ["both", "program_or_branch", "branch_only"]
        
        prog_err_msg = ""
        if not allow_prog_import:
            prog_err_msg = f"Import failed: This degree's cohort mode ('{mode}') does not support Programs."
            
        br_err_msg = ""
        if not allow_br_import:
            br_err_msg = f"Import failed: This degree's cohort mode ('{mode}') does not support Branches."
        # --- END ADDED ---
        
        # --- ENHANCED IMPORT/EXPORT UI ---
        with st.expander("📥 Import / Export Programs & Branches"):
            st.markdown(f"""
            ### Import / Export
            Import Programs or Branches for the **{deg.title} ({degree_sel})** degree.
            """)
            
            # =================================================================
            # ADD THESE GLOBAL CONTROLS AT THE TOP
            # =================================================================
            
            st.markdown("### 🛠️ Import Controls")
            
            col1, col2 = st.columns(2)
            with col1:
                dry_run_mode = st.checkbox(
                    "🔍 Dry-Run Mode",
                    value=False,
                    help="Simulate import without saving to database. Shows what WOULD be imported."
                )
            with col2:
                debug_mode = st.checkbox(
                    "🐛 Debug Mode", 
                    value=False,
                    help="Show detailed step-by-step information about the import process."
                )
            
            if dry_run_mode:
                st.warning("⚠️ **DRY-RUN MODE ENABLED**: No changes will be saved to the database. This is a simulation only.")
            
            if debug_mode:
                st.info("ℹ️ **DEBUG MODE ENABLED**: Detailed information will be displayed for each row processed.")
            
            st.markdown("---")
            
            # =================================================================
            # IMPORT TABS
            # =================================================================
            
            if active_tab in ["Programs", "Branches"]:
                im_tab1, im_tab2 = st.tabs(["Import Programs", "Import Branches"])
                
                with im_tab1:
                    # --- Program Import ---
                    if not allow_prog_import:
                        st.warning(prog_err_msg)
                    
                    df_prog_template = pd.DataFrame(columns=PROGRAM_IMPORT_COLS)
                    st.download_button(
                        label="📄 Download Program Template (CSV)",
                        data=_df_to_csv(df_prog_template),
                        file_name=f"{degree_sel}_programs_template.csv",
                        mime="text/csv",
                        key="dload_prog_template",
                        disabled=not allow_prog_import
                    )
                    
                    prog_file = st.file_uploader(
                        "Upload Program CSV", 
                        type=["csv"], 
                        key="prog_uploader", 
                        disabled=not CAN_EDIT or not allow_prog_import,
                        help="Upload a CSV file with columns: " + ", ".join(PROGRAM_IMPORT_COLS)
                    )
                    
                    # Show preview of uploaded file
                    if prog_file:
                        with st.expander("👀 Preview Uploaded File", expanded=False):
                            try:
                                df_preview = pd.read_csv(prog_file, dtype=str).fillna("")
                                st.write(f"**Rows:** {len(df_preview)}")
                                st.write(f"**Columns:** {list(df_preview.columns)}")
                                st.dataframe(df_preview.head(10), use_container_width=True)
                                # Reset file pointer for actual import
                                prog_file.seek(0)
                            except Exception as e:
                                st.error(f"Error reading CSV: {e}")
                    
                    button_label = "🔍 Preview Import (Dry-Run)" if dry_run_mode else "📥 Import Programs"
                    
                    if st.button(button_label, key="import_prog_btn", disabled=not CAN_EDIT or not prog_file or not allow_prog_import):
                        c_count = u_count = 0
                        errors = []
                        
                        try:
                            df_import = pd.read_csv(prog_file, dtype=str).fillna("")
                            
                            # Import in transaction block
                            with engine.begin() as conn:
                                c_count, u_count, errors = import_programs(
                                    conn, df_import, degree_sel, actor, engine,
                                    dry_run=dry_run_mode,
                                    debug=debug_mode
                                )
                            # Transaction commits HERE when exiting with block
                            
                            # Show results AFTER transaction
                            if dry_run_mode:
                                st.success(f"🔍 DRY-RUN: Would create {c_count}, would update {u_count}")
                                st.info("No changes saved (dry-run mode)")
                            else:
                                if c_count > 0 or u_count > 0:
                                    st.success(f"✅ Import complete: {c_count} created, {u_count} updated")
                                    
                                    # Verify
                                    with engine.begin() as v:
                                        cnt = v.execute(sa_text("SELECT COUNT(*) FROM programs WHERE degree_code=:d"), 
                                                       {"d": degree_sel}).fetchone()[0]
                                        st.info(f"✅ Verified: {cnt} programs in database for {degree_sel}")
                                    
                                    st.cache_data.clear()
                                    st.rerun()
                                else:
                                    st.info("Import complete: No changes (data identical)")
                            
                            if errors:
                                st.error(f"Errors: {len(errors)}")
                                for e in errors:
                                    st.error(f"• {e}")
                                    
                        except Exception as e:
                            st.error(f"Import failed: {e}")
                            import traceback
                            st.code(traceback.format_exc())
                            
                with im_tab2:
                    # --- Branch Import ---
                    if not allow_br_import:
                        st.warning(br_err_msg)
                    
                    df_br_template = pd.DataFrame(columns=BRANCH_IMPORT_COLS)
                    st.download_button(
                        label="📄 Download Branch Template (CSV)",
                        data=_df_to_csv(df_br_template),
                        file_name=f"{degree_sel}_branches_template.csv",
                        mime="text/csv",
                        key="dload_br_template",
                        disabled=not allow_br_import
                    )
                    
                    branch_file = st.file_uploader(
                        "Upload Branch CSV", 
                        type=["csv"], 
                        key="branch_uploader", 
                        disabled=not CAN_EDIT or not allow_br_import,
                        help="Upload a CSV file with columns: " + ", ".join(BRANCH_IMPORT_COLS)
                    )
                    
                    # Show preview of uploaded file
                    if branch_file:
                        with st.expander("👀 Preview Uploaded File", expanded=False):
                            try:
                                df_preview = pd.read_csv(branch_file, dtype=str).fillna("")
                                st.write(f"**Rows:** {len(df_preview)}")
                                st.write(f"**Columns:** {list(df_preview.columns)}")
                                st.dataframe(df_preview.head(10), use_container_width=True)
                                # Reset file pointer for actual import
                                branch_file.seek(0)
                            except Exception as e:
                                st.error(f"Error reading CSV: {e}")
                    
                    button_label = "🔍 Preview Import (Dry-Run)" if dry_run_mode else "📥 Import Branches"
                    
                    if st.button(button_label, key="import_br_btn", disabled=not CAN_EDIT or not branch_file or not allow_br_import):
                        c_count = u_count = 0
                        errors = []
                        
                        try:
                            df_import = pd.read_csv(branch_file, dtype=str).fillna("")
                            
                            # Import in transaction block
                            with engine.begin() as conn:
                                c_count, u_count, errors = import_branches(
                                    conn, df_import, degree_sel, actor, BR_HAS_PID, engine,
                                    dry_run=dry_run_mode,
                                    debug=debug_mode
                                )
                            # Transaction commits HERE when exiting with block
                            
                            # Show results AFTER transaction
                            if dry_run_mode:
                                st.success(f"🔍 DRY-RUN: Would create {c_count}, would update {u_count}")
                                st.info("No changes saved (dry-run mode)")
                            else:
                                if c_count > 0 or u_count > 0:
                                    st.success(f"✅ Import complete: {c_count} created, {u_count} updated")
                                    
                                    # Verify
                                    with engine.begin() as v:
                                        if BR_HAS_PID:
                                            cnt = v.execute(sa_text("""
                                                SELECT COUNT(*) FROM branches b
                                                JOIN programs p ON b.program_id = p.id
                                                WHERE p.degree_code = :d
                                            """), {"d": degree_sel}).fetchone()[0]
                                        else:
                                            cnt = v.execute(sa_text("""
                                                SELECT COUNT(*) FROM branches WHERE degree_code = :d
                                            """), {"d": degree_sel}).fetchone()[0]
                                        st.info(f"✅ Verified: {cnt} branches in database for {degree_sel}")
                                    
                                    st.cache_data.clear()
                                    st.rerun()
                                else:
                                    st.info("Import complete: No changes (data identical)")
                            
                            if errors:
                                st.error(f"Errors: {len(errors)}")
                                for e in errors:
                                    st.error(f"• {e}")
                                    
                        except Exception as e:
                            st.error(f"Import failed: {e}")
                            import traceback
                            st.code(traceback.format_exc())            
            # =================================================================
            # CURRICULUM GROUPS IMPORT
            # =================================================================
            elif active_tab == "Curriculum Groups":
                im_tab1, im_tab2 = st.tabs(["Import Curriculum Groups", "Import Group Links"])
                
                with im_tab1:
                    # --- Curriculum Group Import ---
                    df_cg_template = pd.DataFrame(columns=CG_IMPORT_COLS)
                    st.download_button(
                        label="📄 Download Curriculum Groups Template (CSV)",
                        data=_df_to_csv(df_cg_template),
                        file_name=f"{degree_sel}_curriculum_groups_template.csv",
                        mime="text/csv",
                        key="dload_cg_template",
                        disabled=not SHOW_CG
                    )
                    
                    cg_file = st.file_uploader(
                        "Upload Curriculum Groups CSV", 
                        type=["csv"], 
                        key="cg_uploader", 
                        disabled=not CAN_EDIT or not SHOW_CG,
                        help="Upload a CSV file with columns: " + ", ".join(CG_IMPORT_COLS)
                    )
                    
                    # Show preview of uploaded file
                    if cg_file:
                        with st.expander("👀 Preview Uploaded File", expanded=False):
                            try:
                                df_preview = pd.read_csv(cg_file, dtype=str).fillna("")
                                st.write(f"**Rows:** {len(df_preview)}")
                                st.write(f"**Columns:** {list(df_preview.columns)}")
                                st.dataframe(df_preview.head(10), use_container_width=True)
                                # Reset file pointer for actual import
                                cg_file.seek(0)
                            except Exception as e:
                                st.error(f"Error reading CSV: {e}")
                    
                    button_label = "🔍 Preview Import (Dry-Run)" if dry_run_mode else "📥 Import Curriculum Groups"
                    
                    if st.button(button_label, key="import_cg_btn", disabled=not CAN_EDIT or not cg_file or not SHOW_CG):
                        c_count = u_count = 0
                        errors = []
                        
                        try:
                            df_import = pd.read_csv(cg_file, dtype=str).fillna("")
                            
                            # Import in transaction block
                            with engine.begin() as conn:
                                c_count, u_count, errors = import_cgs(
                                    conn, df_import, degree_sel, actor, SHOW_CG, engine,
                                    dry_run=dry_run_mode,
                                    debug=debug_mode
                                )
                            # Transaction commits HERE when exiting with block
                            
                            # Show results AFTER transaction
                            if dry_run_mode:
                                st.success(f"🔍 DRY-RUN: Would create {c_count}, would update {u_count}")
                                st.info("No changes saved (dry-run mode)")
                            else:
                                if c_count > 0 or u_count > 0:
                                    st.success(f"✅ Import complete: {c_count} created, {u_count} updated")
                                    
                                    # Verify
                                    with engine.begin() as v:
                                        cnt = v.execute(sa_text("""
                                            SELECT COUNT(*) FROM curriculum_groups WHERE degree_code = :d
                                        """), {"d": degree_sel}).fetchone()[0]
                                        st.info(f"✅ Verified: {cnt} curriculum groups in database for {degree_sel}")
                                    
                                    st.cache_data.clear()
                                    st.rerun()
                                else:
                                    st.info("Import complete: No changes (data identical)")
                            
                            if errors:
                                st.error(f"Errors: {len(errors)}")
                                for e in errors:
                                    st.error(f"• {e}")
                                    
                        except Exception as e:
                            st.error(f"Import failed: {e}")
                            import traceback
                            st.code(traceback.format_exc())
                with im_tab2:
                    # --- Curriculum Group Links Import ---
                    df_cgl_template = pd.DataFrame(columns=CGL_IMPORT_COLS)
                    st.download_button(
                        label="📄 Download Group Links Template (CSV)",
                        data=_df_to_csv(df_cgl_template),
                        file_name=f"{degree_sel}_curriculum_group_links_template.csv",
                        mime="text/csv",
                        key="dload_cgl_template",
                        disabled=not SHOW_CG
                    )
                    
                    cgl_file = st.file_uploader(
                        "Upload Group Links CSV", 
                        type=["csv"], 
                        key="cgl_uploader", 
                        disabled=not CAN_EDIT or not SHOW_CG,
                        help="Upload a CSV file with columns: " + ", ".join(CGL_IMPORT_COLS)
                    )
                    
                    # Show preview of uploaded file
                    if cgl_file:
                        with st.expander("👀 Preview Uploaded File", expanded=False):
                            try:
                                df_preview = pd.read_csv(cgl_file, dtype=str).fillna("")
                                st.write(f"**Rows:** {len(df_preview)}")
                                st.write(f"**Columns:** {list(df_preview.columns)}")
                                st.dataframe(df_preview.head(10), use_container_width=True)
                                # Reset file pointer for actual import
                                cgl_file.seek(0)
                            except Exception as e:
                                st.error(f"Error reading CSV: {e}")
                    
                    button_label = "🔍 Preview Import (Dry-Run)" if dry_run_mode else "📥 Import Group Links"
                    
                    if st.button(button_label, key="import_cgl_btn", disabled=not CAN_EDIT or not cgl_file or not SHOW_CG):
                        c_count = u_count = 0
                        errors = []
                        
                        try:
                            df_import = pd.read_csv(cgl_file, dtype=str).fillna("")
                            group_codes = df_cg["group_code"].tolist() if not df_cg.empty else []
                            program_codes = dfp["program_code"].tolist() if not dfp.empty else []
                            branch_codes = dfb_all["branch_code"].tolist() if not dfb_all.empty else []
                            
                            # Import in transaction block
                            with engine.begin() as conn:
                                c_count, u_count, errors = import_cg_links(
                                    conn, df_import, degree_sel, actor, SHOW_CG, 
                                    group_codes, program_codes, branch_codes, engine,
                                    dry_run=dry_run_mode,
                                    debug=debug_mode
                                )
                            # Transaction commits HERE when exiting with block
                            
                            # Show results AFTER transaction
                            if dry_run_mode:
                                st.success(f"🔍 DRY-RUN: Would create {c_count} links")
                                st.info("No changes saved (dry-run mode)")
                            else:
                                if c_count > 0:
                                    st.success(f"✅ Import complete: {c_count} links created")
                                    
                                    # Verify
                                    with engine.begin() as v:
                                        cnt = v.execute(sa_text("""
                                            SELECT COUNT(*) FROM curriculum_group_links WHERE degree_code = :d
                                        """), {"d": degree_sel}).fetchone()[0]
                                        st.info(f"✅ Verified: {cnt} curriculum group links in database for {degree_sel}")
                                    
                                    # Manual refresh button instead of auto-rerun
                                    if st.button("🔄 Refresh to see changes", key="refresh_cgl"):
                                        st.cache_data.clear()
                                        st.rerun()
                                else:
                                    st.info("Import complete: No new links created (all links already exist)")
                            
                            if errors:
                                st.error(f"Errors: {len(errors)}")
                                for e in errors:
                                    st.error(f"• {e}")
                                    
                        except Exception as e:
                            st.error(f"Import failed: {e}")
                            import traceback
                            st.code(traceback.format_exc())
            # =================================================================
            # EXPORT SECTION
            # =================================================================
            
            st.markdown("---")
            st.subheader("Export")
            st.info(f"Download all Programs or Branches currently associated with the **{deg.title} ({degree_sel})** degree.")
            
            if active_tab in ["Programs", "Branches"]:
                exp_col1, exp_col2 = st.columns(2)
                with exp_col1:
                    # Export Programs
                    if not dfp.empty and allow_prog_import:
                        export_dfp = dfp[PROGRAM_IMPORT_COLS] if all(col in dfp.columns for col in PROGRAM_IMPORT_COLS) else dfp
                        st.download_button(
                            label=f"📥 Export {len(export_dfp)} Programs (CSV)",
                            data=_df_to_csv(export_dfp),
                            file_name=f"{degree_sel}_programs_export.csv",
                            mime="text/csv",
                            key="export_prog_btn"
                        )
                    else:
                        st.caption("No programs to export.")
                
                with exp_col2:
                    # Export Branches
                    if not dfb_all.empty and allow_br_import:
                        export_dfb = dfb_all[BRANCH_IMPORT_COLS] if all(col in dfb_all.columns for col in BRANCH_IMPORT_COLS) else dfb_all
                        st.download_button(
                            label=f"📥 Export {len(export_dfb)} Branches (CSV)",
                            data=_df_to_csv(export_dfb),
                            file_name=f"{degree_sel}_branches_export.csv",
                            mime="text/csv",
                            key="export_br_btn"
                        )
                    else:
                        st.caption("No branches to export.")
            
            elif active_tab == "Curriculum Groups":
                exp_col1, exp_col2 = st.columns(2)
                with exp_col1:
                    # Export Curriculum Groups
                    if not df_cg.empty and SHOW_CG:
                        export_dfcg = df_cg[CG_IMPORT_COLS] if all(col in df_cg.columns for col in CG_IMPORT_COLS) else df_cg
                        st.download_button(
                            label=f"📥 Export {len(export_dfcg)} Curriculum Groups (CSV)",
                            data=_df_to_csv(export_dfcg),
                            file_name=f"{degree_sel}_curriculum_groups_export.csv",
                            mime="text/csv",
                            key="export_cg_btn"
                        )
                    else:
                        st.caption("No curriculum groups to export.")
                
                with exp_col2:
                    # Export Curriculum Group Links
                    if not df_cgl.empty and SHOW_CG:
                        export_dfcgl = df_cgl[CGL_IMPORT_COLS] if all(col in df_cgl.columns for col in CGL_IMPORT_COLS) else df_cgl
                        st.download_button(
                            label=f"📥 Export {len(export_dfcgl)} Group Links (CSV)",
                            data=_df_to_csv(export_dfcgl),
                            file_name=f"{degree_sel}_curriculum_group_links_export.csv",
                            mime="text/csv",
                            key="export_cgl_btn"
                        )
                    else:
                        st.caption("No curriculum group links to export.")

        st.markdown("---")
        # --- END ENHANCED IMPORT/EXPORT ---
    
    # ───────────────────────── Programs Tab ─────────────────────────
    
    if active_tab == "Programs":
        st.subheader("Programs (per degree)")
        st.markdown("**Existing Programs**")
        st.dataframe(dfp, use_container_width=True, hide_index=True)
        
        # Approval table
        if not df_approvals.empty:
            program_ids = dfp["id"].astype(str).tolist() if "id" in dfp.columns else []
            prog_approvals = df_approvals[
                (df_approvals["object_type"] == "program") &
                (df_approvals["object_id"].isin(program_ids)) &
                (df_approvals["status"].isin(["pending", "under_review"]))
            ]
            
            if not prog_approvals.empty:
                st.markdown("---")
                st.markdown("#### Program Approval Status")
                st.dataframe(prog_approvals, use_container_width=True, hide_index=True)
        
        if not CAN_EDIT:
            st.info("You don't have permissions to create or edit Programs.")
        else:
            st.markdown("### Create Program")
            with st.form(key="prog_create_form"):
                c1, c2 = st.columns(2)
                with c1:
                    pc = st.text_input("Program code").strip()
                    pn = st.text_input("Program name").strip()
                    pactive = st.checkbox("Active", value=True)
                    psort = st.number_input("Sort order", 1, 10000, 100, step=1)
                with c2:
                    plogo = st.text_input("Logo file name (optional)")
                    pdesc = st.text_area("Description", "")
                
                submitted = st.form_submit_button("Create Program", disabled=not CAN_EDIT)
                
                # --- CORRECTED LOGIC TO PREVENT TRANSACTION ROLLBACK ---
                if submitted:
                    st.session_state.prog_create_error = None # Clear old error
                    success = False # Flag to control rerun
                    
                    if not pc or not pn:
                        st.error("Program code and name are required.")
                    else:
                        try:
                            with engine.begin() as conn: # <-- FIXED: Use 'engine'
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
                                success = True # Set flag *after* all DB work is done
                        except IntegrityError:
                            st.error(f"Error: A program with the code '{pc}' already exists.")
                        except Exception as ex:
                            st.session_state.prog_create_error = ex # Save error
                            import traceback
                            print("--- ERROR: FAILED TO CREATE PROGRAM ---")
                            traceback.print_exc()
                            print("------------------------------------------")
                    
                    if success:
                        st.cache_data.clear()
                        st.rerun() # Rerun *outside* the transaction block

            # --- ADDED FOR ERROR CATCHING ---
            if st.session_state.prog_create_error:
                st.error("An error occurred during creation. See details below:")
                st.exception(st.session_state.prog_create_error)
                st.session_state.prog_create_error = None # Clear error
            # --- END ADDED ---
            
            st.markdown("---")
            st.markdown("### Edit / Delete Program")
            prog_codes = dfp["program_code"].tolist() if "program_code" in dfp.columns else []
            sel_pc = st.selectbox("Select program_code", [""] + prog_codes, key="prog_edit_pick")
            
            if sel_pc:
                with engine.begin() as conn: # <-- FIXED: Use 'engine'
                    prow = conn.execute(sa_text("""
                        SELECT id, program_code, program_name, degree_code, active, sort_order, logo_file_name, description
                          FROM programs
                         WHERE degree_code=:d AND lower(program_code)=lower(:pc)
                         LIMIT 1
                    """), {"d": degree_sel, "pc": sel_pc}).fetchone()
                
                if prow:
                    with st.form(key=f"prog_edit_form_{sel_pc}"):
                        e1, e2 = st.columns(2)
                        with e1:
                            editable_name = st.text_input("Program name", prow.program_name or "", key=f"prog_edit_name_{sel_pc}")
                            editable_active = st.checkbox("Active", value=bool(prow.active), key=f"prog_edit_active_{sel_pc}")
                            editable_so = st.number_input("Sort order", 1, 10000, int(prow.sort_order), step=1, key=f"prog_edit_sort_{sel_pc}")
                        with e2:
                            editable_logo = st.text_input("Logo file name (optional)", prow.logo_file_name or "", key=f"prog_edit_logo_{sel_pc}")
                            editable_desc = st.text_area("Description", prow.description or "", key=f"prog_edit_desc_{sel_pc}")
                        
                        save_submitted = st.form_submit_button("Save changes", disabled=not CAN_EDIT)
                        
                        # --- CORRECTED LOGIC TO PREVENT TRANSACTION ROLLBACK ---
                        if save_submitted:
                            success = False
                            try:
                                with engine.begin() as conn: # <-- FIXED: Use 'engine'
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
                                    success = True
                            except Exception as ex:
                                st.error(str(ex))
                            
                            if success:
                                st.cache_data.clear()
                                st.rerun()
                    
                    # --- UPDATED: "Request Delete" button with FIXED PAYLOAD ---
                    if st.button("Request Delete", disabled=not CAN_EDIT, key=f"prog_delete_req_{sel_pc}"):
                        try:
                            with engine.begin() as conn: 
                                success, error = _request_deletion(
                                    conn,
                                    object_type="program",
                                    object_id=prow.id,
                                    actor=actor,
                                    audit_function=_audit_program,
                                    audit_row=dict(prow._mapping),
                                    reason_note="Program delete (requires approval)"
                                )
                                
                                if not success:
                                    raise error # Re-raise to be caught by the UI

                            # --- Success case (outside the transaction) ---
                            st.success("Delete request submitted.")
                            st.cache_data.clear()
                            st.rerun()
                        except Exception as ex:
                            st.error(str(ex))
    
    # ───────────────────────── Branches Tab ─────────────────────────
    
    if active_tab == "Branches":
        st.subheader("Branches")
        
        with engine.begin() as conn: # <-- FIXED: Use 'engine'
            dfp2 = _programs_df(engine, degree_sel) # <-- FIXED: Pass 'engine'
        
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
            if filter_pc:
                with engine.begin() as conn: # <-- FIXED: Use 'engine'
                    filter_pid = _program_id_by_code(conn, degree_sel, filter_pc)
            
            dfb = _branches_df(engine, degree_sel, program_id=filter_pid) # <-- FIXED: Pass 'engine'
            
            st.markdown("**Existing Branches**")
            st.dataframe(dfb, use_container_width=True, hide_index=True)
            
            # Approval table
            if not df_approvals.empty:
                branch_ids = dfb_all["id"].astype(str).tolist() if "id" in dfb_all.columns else []
                branch_approvals = df_approvals[
                    (df_approvals["object_type"] == "branch") &
                    (df_approvals["object_id"].isin(branch_ids)) &
                    (df_approvals["status"].isin(["pending", "under_review"]))
                ]
                
                if not branch_approvals.empty:
                    st.markdown("---")
                    st.markdown("#### Branch Approval Status")
                    st.dataframe(branch_approvals, use_container_width=True, hide_index=True)
            
            if not CAN_EDIT:
                st.info("You don't have permissions to create or edit Branches.")
            else:
                st.markdown("### Create Branch")
                with st.form(key="branch_create_form"):
                    c1, c2 = st.columns(2)
                    with c1:
                        parent_pc = ""
                        if mode == 'both' or (mode == 'program_or_branch' and not dfp2.empty) or not supports_degree_level_branches:
                            parent_pc = st.selectbox(
                                "Parent program_code",
                                options=([""] + prog_pick_codes)
                            )
                        
                        bc = st.text_input("Branch code").strip()
                        bn = st.text_input("Branch name").strip()
                        bactive = st.checkbox("Active", value=True)
                        bsort = st.number_input("Sort order", 1, 10000, 100, step=1)
                    with c2:
                        blogo = st.text_input("Logo file name (optional)")
                        bdesc = st.text_area("Description", "")
                    
                    submitted = st.form_submit_button("Create Branch", disabled=not CAN_EDIT)
                    parent_required = (mode == 'both') or (not supports_degree_level_branches)
                    
                    # --- CORRECTED LOGIC TO PREVENT TRANSACTION ROLLBACK ---
                    if submitted:
                        st.session_state.branch_create_error = None # Clear old error
                        success = False # Flag to control rerun
                        
                        if parent_required and not parent_pc:
                            st.error("Select a parent program.")
                        elif not bc or not bn:
                            st.error("Branch code and name are required.")
                        else:
                            try:
                                with engine.begin() as conn: # <-- FIXED: Use 'engine'
                                    pid = _program_id_by_code(conn, degree_sel, parent_pc) if parent_pc else None
                                    if parent_pc and not pid:
                                        st.error("Parent program not found."); raise RuntimeError("parent program missing")
                                    
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
                                        _audit_branch(conn, "create", actor, {
                                            "degree_code": degree_sel, "branch_code": bc, "branch_name": bn,
                                            "active": 1 if bactive else 0, "sort_order": int(bsort),
                                            "logo_file_name": (blogo or None), "description": (bdesc or None)
                                        })
                                    else:
                                        raise ValueError("Schema requires branches to be attached to a Program.")
                                    
                                    st.success("Branch created.")
                                    success = True # Set flag *after* all DB work is done
                            except IntegrityError:
                                st.error(f"Error: A branch with the code '{bc}' already exists.")
                            except Exception as ex:
                                st.session_state.branch_create_error = ex # Save error
                                import traceback
                                print("--- ERROR: FAILED TO CREATE BRANCH ---")
                                traceback.print_exc()
                                print("------------------------------------------")
                        
                        if success:
                            st.cache_data.clear()
                            st.rerun() # Rerun *outside* the transaction block
                
                # --- ADDED FOR ERROR CATCHING ---
                if st.session_state.branch_create_error:
                    st.error("An error occurred during creation. See details below:")
                    st.exception(st.session_state.branch_create_error)
                    st.session_state.branch_create_error = None # Clear error
                # --- END ADDED ---

                st.markdown("---")
                st.markdown("### Edit / Delete Branch")
                br_codes = dfb["branch_code"].tolist() if "branch_code" in dfb.columns else []
                sel_bc = st.selectbox("Select branch_code", [""] + br_codes, key="branch_edit_pick")
                
                if sel_bc:
                    with engine.begin() as conn: # <-- FIXED: Use 'engine'
                        params = {"deg": degree_sel, "bc": sel_bc}
                        
                        if BR_HAS_PID and BR_HAS_DEG:
                            sql = """
                                SELECT b.id, b.branch_code, b.branch_name, b.active, b.sort_order, b.logo_file_name, b.description,
                                       p.program_code, p.degree_code, b.program_id
                                  FROM branches b
                                  LEFT JOIN programs p ON p.id=b.program_id
                                 WHERE (p.degree_code=:deg OR b.degree_code=:deg) AND lower(b.branch_code)=lower(:bc)
                                 LIMIT 1
                            """
                        elif BR_HAS_PID:
                            sql = """
                                SELECT b.id, b.branch_code, b.branch_name, b.active, b.sort_order, b.logo_file_name, b.description,
                                       p.program_code, p.degree_code, b.program_id
                                  FROM branches b
                                  LEFT JOIN programs p ON p.id=b.program_id
                                 WHERE p.degree_code=:deg AND lower(b.branch_code)=lower(:bc)
                                 LIMIT 1
                            """
                        elif BR_HAS_DEG:
                            sql = """
                                SELECT id, branch_code, branch_name, active, sort_order, logo_file_name, description,
                                       degree_code, NULL as program_code, NULL as program_id
                                  FROM branches
                                 WHERE degree_code=:deg AND lower(branch_code)=lower(:bc)
                                 LIMIT 1
                            """
                        
                        brow = conn.execute(sa_text(sql), params).fetchone()
                    
                    if brow:
                        with st.form(key=f"branch_edit_form_{sel_bc}"):
                            e1, e2 = st.columns(2)
                            with e1:
                                editable_name = st.text_input("Branch name", brow.branch_name or "", key=f"branch_edit_name_{sel_bc}")
                                editable_active = st.checkbox("Active", value=bool(brow.active), key=f"branch_edit_active_{sel_bc}")
                                editable_so = st.number_input("Sort order", 1, 10000, int(brow.sort_order), step=1, key=f"branch_edit_sort_{sel_bc}")
                            with e2:
                                editable_logo = st.text_input("Logo file name (optional)", brow.logo_file_name or "", key=f"branch_edit_logo_{sel_bc}")
                                editable_desc = st.text_area("Description", brow.description or "", key=f"branch_edit_desc_{sel_bc}")
                            
                            save_submitted = st.form_submit_button("Save changes", disabled=not CAN_EDIT)
                            
                            # --- CORRECTED LOGIC TO PREVENT TRANSACTION ROLLBACK ---
                            if save_submitted:
                                success = False
                                try:
                                    with engine.begin() as conn: # <-- FIXED: Use 'engine'
                                        conn.execute(sa_text("""
                                            UPDATE branches
                                               SET branch_name=:bn, active=:act, sort_order=:so, logo_file_name=:logo, description=:desc,
                                                   updated_at=CURRENT_TIMESTAMP
                                             WHERE id=:id
                                        """), {
                                            "bn": (editable_name or None), "act": 1 if editable_active else 0, "so": int(editable_so),
                                            "logo": (editable_logo or None), "desc": (editable_desc or None), "id": int(brow.id)
                                        })
                                        
                                        audit_row = {
                                            "program_id": brow.program_id, "degree_code": brow.degree_code,
                                            "branch_code": brow.branch_code, "branch_name": editable_name,
                                            "active": 1 if editable_active else 0, "sort_order": int(editable_so),
                                            "logo_file_name": (editable_logo or None), "description": (editable_desc or None)
                                        }
                                        _audit_branch(conn, "edit", actor, audit_row)
                                        
                                        st.success("Saved.")
                                        success = True
                                except Exception as ex:
                                    st.error(str(ex))
                                
                                if success:
                                    st.cache_data.clear()
                                    st.rerun()
                        
                        # --- UPDATED: "Request Delete" button with FIXED PAYLOAD ---
                        if st.button("Request Delete", disabled=not CAN_EDIT, key=f"branch_delete_req_{sel_bc}"):
                            try:
                                with engine.begin() as conn: 
                                    success, error = _request_deletion(
                                        conn,
                                        object_type="branch",
                                        object_id=brow.id,
                                        actor=actor,
                                        audit_function=_audit_branch,
                                        audit_row=dict(brow._mapping),
                                        reason_note="Branch delete (requires approval)"
                                    )
                                    
                                    if not success:
                                        raise error # Re-raise to be caught by the UI
                                
                                # --- Success case (outside the transaction) ---
                                st.success("Delete request submitted.")
                                st.cache_data.clear()
                                st.rerun()
                            except Exception as ex:
                                st.error(str(ex))
    
    # ───────────────────── Curriculum Groups Tab ─────────────────────
    
    if active_tab == "Curriculum Groups":
        st.subheader("Curriculum Groups")
        st.markdown("**Existing Groups**")
        st.dataframe(df_cg, use_container_width=True, hide_index=True)
        
        # Approval status for curriculum groups
        if not df_approvals.empty:
            group_ids = df_cg["id"].astype(str).tolist() if "id" in df_cg.columns else []
            cg_approvals = df_approvals[
                (df_approvals["object_type"] == "curriculum_group") &
                (df_approvals["object_id"].isin(group_ids)) &
                (df_approvals["status"].isin(["pending", "under_review"]))
            ]
            
            if not cg_approvals.empty:
                st.markdown("#### Group Approval Status")
                st.dataframe(cg_approvals, use_container_width=True, hide_index=True)
        
        st.markdown("**Existing Links**")
        st.dataframe(df_cgl, use_container_width=True, hide_index=True)
        
        # Delete Curriculum Group Link
        if CAN_EDIT and not df_cgl.empty:
            st.markdown("### Delete Link")
            link_options_map = {}
            for _, row in df_cgl.iterrows():
                if row['program_code'] and row['branch_code']:
                    label = f"Link ID {row['id']} (Complex Link)"
                elif row['program_code']:
                    label = f"Group '{row['group_code']}' → Program '{row['program_code']}' (ID: {row['id']})"
                elif row['branch_code']:
                    label = f"Group '{row['group_code']}' → Branch '{row['branch_code']}' (ID: {row['id']})"
                else:
                    label = f"Group '{row['group_code']}' → Degree '{degree_sel}' (ID: {row['id']})"
                link_options_map[label] = row['id']
            
            link_to_delete_label = st.selectbox(
                "Select a link to delete",
                options=[""] + list(link_options_map.keys()),
                key="cg_link_delete_pick"
            )
            
            if st.button("Delete Selected Link", disabled=(not link_to_delete_label or not CAN_EDIT)):
                try:
                    link_id_to_delete = link_options_map[link_to_delete_label]
                    link_row_details = df_cgl[df_cgl['id'] == link_id_to_delete].to_dict('records')[0]
                    with engine.begin() as conn: # <-- FIXED: Use 'engine'
                        conn.execute(sa_text("DELETE FROM curriculum_group_links WHERE id = :id"), {"id": link_id_to_delete})
                        _audit_curriculum_group_link(conn, "delete", actor, link_row_details, note="Link deleted")
                    st.success(f"Successfully deleted link: {link_to_delete_label}")
                    st.cache_data.clear()
                    st.rerun()
                except Exception as ex:
                    st.error(f"Could not delete link: {ex}")
        
        if not CAN_EDIT:
            st.info("You don't have permissions to create or edit Curriculum Groups.")
        else:
            st.markdown("---")
            st.markdown("### Create Curriculum Group")
            with st.form(key="cg_create_form"):
                c1, c2 = st.columns(2)
                with c1:
                    gc = st.text_input("Group code").strip()
                    gn = st.text_input("Group name").strip()
                    gkind = st.selectbox("Group Kind", ["pseudo", "cohort"])
                with c2:
                    gactive = st.checkbox("Active", value=True)
                    gsort = st.number_input("Sort order", 1, 10000, 100, step=1)
                    gdesc = st.text_area("Description", "")
                
                submitted = st.form_submit_button("Create Group", disabled=not CAN_EDIT)
                
                # --- CORRECTED LOGIC TO PREVENT TRANSACTION ROLLBACK ---
                if submitted:
                    st.session_state.cg_create_error = None # Clear old error
                    success = False # Flag to control rerun
                    
                    if not gc or not gn:
                        st.error("Group code and name are required.")
                    else:
                        try:
                            with engine.begin() as conn: # <-- FIXED: Use 'engine'
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
                                success = True # Set flag *after* all DB work is done
                        except IntegrityError:
                            st.error(f"Error: A group with the code '{gc}' already exists for this degree.")
                        except Exception as ex:
                            st.session_state.cg_create_error = ex # Save error
                            import traceback
                            print("--- ERROR: FAILED TO CREATE CURRICULUM GROUP ---")
                            traceback.print_exc()
                            print("------------------------------------------")
                    
                    if success:
                        st.cache_data.clear()
                        st.rerun() # Rerun *outside* the transaction block
            
            # --- ADDED FOR ERROR CATCHING ---
            # This block goes *after* the `with st.form(...)` block
            if st.session_state.cg_create_error:
                st.error("An error occurred during creation. See details below:")
                st.exception(st.session_state.cg_create_error)
                # Clear the error so it doesn't show again
                st.session_state.cg_create_error = None
            # --- END ADDED ---
            
            st.markdown("---")
            st.markdown("### Edit / Delete Group")
            group_codes = df_cg["group_code"].tolist() if "group_code" in df_cg.columns else []
            sel_gc = st.selectbox("Select group_code", [""] + group_codes, key="cg_edit_pick")
            
            if sel_gc:
                with engine.begin() as conn: # <-- FIXED: Use 'engine'
                    grow = conn.execute(sa_text("""
                        SELECT id, group_code, group_name, kind, active, sort_order, description
                          FROM curriculum_groups
                         WHERE degree_code=:d AND lower(group_code)=lower(:gc)
                         LIMIT 1
                    """), {"d": degree_sel, "gc": sel_gc}).fetchone()
                
                if grow:
                    with st.form(key=f"cg_edit_form_{sel_gc}"):
                        e1, e2 = st.columns(2)
                        with e1:
                            editable_name = st.text_input("Group name", grow.group_name or "", key=f"cg_edit_name_{sel_gc}")
                            editable_kind = st.selectbox("Group Kind", ["pseudo", "cohort"], index=["pseudo", "cohort"].index(grow.kind), key=f"cg_edit_kind_{sel_gc}")
                        with e2:
                            editable_active = st.checkbox("Active", value=bool(grow.active), key=f"cg_edit_active_{sel_gc}")
                            editable_so = st.number_input("Sort order", 1, 10000, int(grow.sort_order), step=1, key=f"cg_edit_sort_{sel_gc}")
                            editable_desc = st.text_area("Description", grow.description or "", key=f"cg_edit_desc_{sel_gc}")
                        
                        save_submitted = st.form_submit_button("Save changes", disabled=not CAN_EDIT)
                        
                        # --- CORRECTED LOGIC TO PREVENT TRANSACTION ROLLBACK ---
                        if save_submitted:
                            success = False
                            try:
                                with engine.begin() as conn: # <-- FIXED: Use 'engine'
                                    conn.execute(sa_text("""
                                        UPDATE curriculum_groups
                                           SET group_name=:gn, kind=:kind, active=:act, sort_order=:so, description=:desc,
                                               updated_at=CURRENT_TIMESTAMP
                                         WHERE id=:id
                                    """), {
                                        "gn": (editable_name or None),
                                        "kind": editable_kind,
                                        "act": 1 if editable_active else 0,
                                        "so": int(editable_so),
                                        "desc": (editable_desc or None),
                                        "id": int(grow.id)
                                    })
                                    
                                    _audit_curriculum_group(conn, "edit", actor, {
                                        "degree_code": degree_sel, "group_code": grow.group_code,
                                        "group_name": (editable_name or None), "kind": editable_kind,
                                        "active": 1 if editable_active else 0, "sort_order": int(editable_so),
                                        "description": (editable_desc or None)
                                    })
                                    
                                    st.success("Saved.")
                                    success = True
                            except Exception as ex:
                                st.error(str(ex))
                            
                            if success:
                                st.cache_data.clear()
                                st.rerun()
                    
                    # --- UPDATED: "Request Delete Group" button with FIXED PAYLOAD ---
                    if st.button("Request Delete Group", disabled=not CAN_EDIT, key=f"cg_delete_req_{sel_gc}"):
                        try:
                            with engine.begin() as conn: 
                                success, error = _request_deletion(
                                    conn,
                                    object_type="curriculum_group",
                                    object_id=grow.id,
                                    actor=actor,
                                    audit_function=_audit_curriculum_group,
                                    audit_row=dict(grow._mapping),
                                    reason_note="Curriculum Group delete (requires approval)"
                                )
                                
                                if not success:
                                    raise error # Re-raise to be caught by the UI
                            
                            # --- Success case (outside the transaction) ---
                            st.success("Delete request submitted.")
                            st.cache_data.clear()
                            st.rerun()
                            
                        except Exception as ex:
                            st.error(str(ex))
            
            st.markdown("---")
            
            can_link_degree = bool(deg.cg_degree)
            can_link_program = bool(deg.cg_program) and not dfp.empty
            can_link_branch = bool(deg.cg_branch) and not dfb_all.empty
            
            if can_link_degree or can_link_program or can_link_branch:
                st.markdown("### Link Group to Degree/Program/Branch")
                
                # Get all defined groups
                all_groups = df_cg["group_code"].tolist() if not df_cg.empty else []
                
                # Get all possible targets
                all_targets = []
                if can_link_degree:
                    all_targets.append({"type": "Degree", "code": degree_sel, "pc": None, "bc": None})
                if can_link_program:
                    prog_codes = dfp["program_code"].tolist() if not dfp.empty else []
                    all_targets.extend([{"type": "Program", "code": pc, "pc": pc, "bc": None} for pc in prog_codes])
                if can_link_branch:
                    branch_codes = dfb_all["branch_code"].tolist() if not dfb_all.empty else []
                    all_targets.extend([{"type": "Branch", "code": bc, "pc": None, "bc": bc} for bc in branch_codes])
                
                # Get all existing links as a set of tuples for easy lookup
                existing_links_set = set()
                if not df_cgl.empty:
                    for _, row in df_cgl.iterrows():
                        gc = row['group_code']
                        pc = row['program_code']
                        bc = row['branch_code']
                        pc_key = pc if pd.notna(pc) else None
                        bc_key = bc if pd.notna(bc) else None
                        existing_links_set.add((gc, pc_key, bc_key))
                
                # Create the list of *available new links*
                available_links_map = {}
                if all_groups and all_targets:
                    for group_code in all_groups:
                        for target in all_targets:
                            target_pc = target["pc"]
                            target_bc = target["bc"]
                            link_tuple = (group_code, target_pc, target_bc)
                            
                            if link_tuple not in existing_links_set:
                                label = f"Group '{group_code}' → {target['type']} '{target['code']}'"
                                payload = {
                                    "group_code": group_code,
                                    "program_code": target_pc,
                                    "branch_code": target_bc,
                                    "label": label
                                }
                                available_links_map[label] = payload
                
                # Show the form OR the "all done" message
                if not all_groups:
                    st.info("Linking is available, but no Curriculum Groups exist yet. Please create one first.")
                elif not all_targets:
                    st.info("Linking is enabled, but no Degrees, Programs, or Branches are available to link to.")
                elif not available_links_map:
                    st.info("All possible group links have been created.")
                else:
                    with st.form(key="cg_link_form_new"):
                        sel_link_label = st.selectbox(
                            "Select new link to create",
                            options=[""] + list(available_links_map.keys())
                        )
                        
                        link_submitted = st.form_submit_button("Link Group", disabled=(not CAN_EDIT))
                        
                        # --- CORRECTED LOGIC TO PREVENT TRANSACTION ROLLBACK ---
                        if link_submitted:
                            success = False
                            if not sel_link_label:
                                st.error("You must select a link to create.")
                            else:
                                try:
                                    link_payload_data = available_links_map[sel_link_label]
                                    sel_group = link_payload_data["group_code"]
                                    prog_code_to_link = link_payload_data["program_code"]
                                    branch_code_to_link = link_payload_data["branch_code"]
                                    
                                    with engine.begin() as conn: # <-- FIXED: Use 'engine'
                                        group_id_row = conn.execute(sa_text(
                                            "SELECT id FROM curriculum_groups WHERE degree_code=:d AND group_code=:gc"
                                        ), {"d": degree_sel, "gc": sel_group}).fetchone()
                                        
                                        if not group_id_row:
                                            st.error(f"Selected group '{sel_group}' not found."); raise RuntimeError("Group missing")
                                        
                                        link_insert_payload = {
                                            "gid": group_id_row.id,
                                            "deg": degree_sel,
                                            "pc": prog_code_to_link,
                                            "bc": branch_code_to_link
                                        }
                                        
                                        conn.execute(sa_text("""
                                            INSERT INTO curriculum_group_links(group_id, degree_code, program_code, branch_code)
                                            VALUES(:gid, :deg, :pc, :bc)
                                        """), link_insert_payload)
                                        
                                        audit_link_payload = {
                                            "group_id": group_id_row.id,
                                            "degree_code": degree_sel,
                                            "program_code": prog_code_to_link,
                                            "branch_code": branch_code_to_link
                                        }
                                        
                                        _audit_curriculum_group_link(conn, "create", actor, audit_link_payload, note="Link created")
                                        
                                        st.success(f"Successfully created link: {sel_link_label}")
                                        success = True
                                except Exception as ex:
                                    st.error(f"Failed to create link. Details: {ex}")
                            
                            if success:
                                st.cache_data.clear()
                                st.rerun()
            else:
                st.info("Linking is not available. Enable curriculum groups at the Degree, Program, or Branch level on the Degrees page.")
    
    if active_tab == "View":
        st.info("This degree's cohort mode does not allow Programs or Branches.")
    
    st.markdown("---")
    #render_footer_global()

try:
    render()
except Exception as e:
    # This is a fallback error handler in case the migrations *still* fail
    # (e.g., if the degrees table doesn't exist at all and _fetch_degree fails)
    import traceback
    st.error(f"An unexpected error occurred on this page: {e}")
    st.warning("If you just created a new database, please visit the 'Degrees' page *first* to initialize the application schema.")
    with st.expander("Show Error Details"):
        st.code(traceback.format_exc())
