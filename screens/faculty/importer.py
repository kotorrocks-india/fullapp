# app/screens/faculty/importer.py
# -------------------------------------------------------------------
# All Import/Export functions and UI sections
# NO TABLE CREATION - all schema is handled by schema installers
# -------------------------------------------------------------------
from __future__ import annotations
from typing import List, Tuple, Dict, Any, Optional, Set
from dataclasses import dataclass, field
import pandas as pd
import streamlit as st
from sqlalchemy import text as sa_text
from sqlalchemy.engine import Engine, Connection
from collections import defaultdict
import logging

# Import helpers from other modules
from screens.faculty.utils import _safe_int_convert, _handle_error
from screens.faculty.db import (
    _get_custom_profile_fields, _get_custom_field_mapping, _save_custom_field_value,
    _active_degrees, _designation_catalog, _designation_enabled, _get_all_custom_field_data,
    _get_all_positions,              # for positions import validation
    _generate_faculty_username,      # NEW: username generator
    _initial_faculty_password_from_name,  # NEW: initial password generator
)

# Setup logger
log = logging.getLogger(__name__)

# ------------------------------------------------------------------
# NEW: Helpers for Stateful Combined Import
# ------------------------------------------------------------------

@dataclass
class AffiliationCheckResult:
    """Holds the data from pre-checking affiliations."""
    unmatched_cgs: Set[str] = field(default_factory=set)
    existing_cgs: List[str] = field(default_factory=list)
    unmatched_programs: Set[str] = field(default_factory=set)
    existing_programs: List[str] = field(default_factory=list)
    unmatched_branches: Set[str] = field(default_factory=set)
    existing_branches: List[str] = field(default_factory=list)
    ignored_rows: int = 0


def _get_existing_affiliations(engine: Engine, degree_code: str) -> Dict[str, List[str]]:
    """
    Fetches existing CGs, Programs, and Branches for a specific degree.
    """
    with engine.connect() as conn:
        cg_res = conn.execute(
            sa_text("SELECT group_code FROM curriculum_groups WHERE degree_code = :degree"),
            {"degree": degree_code}
        ).fetchall()
        
        prog_res = conn.execute(
            sa_text("SELECT program_code FROM programs WHERE degree_code = :degree"),
            {"degree": degree_code}
        ).fetchall()
        
        branch_res = conn.execute(
            sa_text("SELECT branch_code FROM branches WHERE degree_code = :degree"),
            {"degree": degree_code}
        ).fetchall()

    return {
        "cgs": [r[0] for r in cg_res],
        "programs": [r[0] for r in prog_res],
        "branches": [r[0] for r in branch_res],
    }


def _pre_check_affiliations(df: pd.DataFrame, engine: Engine, degree_code: str) -> Tuple[AffiliationCheckResult, pd.DataFrame]:
    """
    Compares the CSV data against the database for a single degree.
    """
    # 1. Filter the DataFrame for the selected degree
    # Ensure the degree code from session state is also stripped
    degree_code_clean = degree_code.strip()

    # Ensure all relevant columns are treated as strings and stripped
    for col in ['degree_code', 'group_code', 'program_code', 'branch_code']:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().replace('nan', '')

    # Use the cleaned degree code for comparison
    df_filtered = df[df['degree_code'].str.lower() == degree_code_clean.lower()].copy()
    
    ignored_rows = len(df) - len(df_filtered)
    
    if df_filtered.empty:
        raise ValueError(f"No rows found in the CSV for the selected degree '{degree_code_clean}'.")

    # 2. Get unique codes from the *filtered* CSV
    csv_cgs = set(df_filtered['group_code'].dropna().unique()) - {''}
    csv_programs = set(df_filtered['program_code'].dropna().unique()) - {''}
    csv_branches = set(df_filtered['branch_code'].dropna().unique()) - {''}

    # 3. Get existing codes from the DB
    existing_data = _get_existing_affiliations(engine, degree_code_clean) # Use cleaned degree
    db_cgs = set(existing_data['cgs'])
    db_programs = set(existing_data['programs'])
    db_branches = set(existing_data['branches'])

    # 4. Find the mismatches (codes in CSV that are not in DB)
    result = AffiliationCheckResult(
        unmatched_cgs=csv_cgs - db_cgs,
        existing_cgs=sorted(list(db_cgs)),
        unmatched_programs=csv_programs - db_programs,
        existing_programs=sorted(list(db_programs)),
        unmatched_branches=csv_branches - db_branches,
        existing_branches=sorted(list(db_branches)),
        ignored_rows=ignored_rows
    )
    # Return both the result and the dataframe separately
    return result, df_filtered


def _create_new_affiliation(
    engine: Engine, 
    degree_code: str, 
    aff_type: str, 
    code: str, 
    name: Optional[str] = None,
    conn_for_transaction: Optional[Connection] = None
):
    """
    Creates a new Program, Branch, or CG in the database.
    This version MATCHES THE SCHEMA FILES (for new database).
    """
    if name is None or name.strip() == '':
        name = code
    
    log.info(f"Creating new {aff_type} '{code}' for degree '{degree_code}'")

    # This new inner function contains the database logic
    def _do_insert(conn: Connection):
        if aff_type == "cg":
            # --- CORRECT (matches programs_branches_schema.py) ---
            # Includes 'is_active' and 'ON CONFLICT'
            table, code_col, name_col = "curriculum_groups", "group_code", "group_name"
            conn.execute(
                sa_text(f"""
                INSERT INTO {table} (degree_code, {code_col}, {name_col}, is_active)
                VALUES (:degree, :code, :name, 1)
                ON CONFLICT (degree_code, {code_col}) DO NOTHING
                """),
                {"degree": degree_code, "code": code, "name": name}
            )
            # --- END ---
        elif aff_type == "program":
            # --- CORRECT (matches programs_branches_schema.py) ---
            table, code_col, name_col = "programs", "program_code", "program_name"
            conn.execute(
                sa_text(f"""
                INSERT INTO {table} (degree_code, {code_col}, {name_col})
                VALUES (:degree, :code, :name)
                ON CONFLICT (degree_code, {code_col}) DO NOTHING
                """),
                {"degree": degree_code, "code": code, "name": name}
            )
            # --- END ---
        elif aff_type == "branch":
            # --- FIX FOR branches ---
            # The schema requires (degree_code, program_code, branch_code)
            # for its PRIMARY KEY. This function doesn't have program_code.
            # We must use a simple INSERT. The pre-check logic will prevent duplicates.
            table, code_col, name_col = "branches", "branch_code", "branch_name"
            conn.execute(
                sa_text(f"""
                INSERT INTO {table} (degree_code, {code_col}, {name_col})
                VALUES (:degree, :code, :name)
                """),
                {"degree": degree_code, "code": code, "name": name}
            )
            # --- END OF FIX ---
        else:
            raise ValueError(f"Unknown affiliation type: {aff_type}")

    # This logic block is correct
    if conn_for_transaction:
        # Use the passed connection (for Dry Run)
        _do_insert(conn_for_transaction)
    else:
        # Create a new auto-committing connection (for Execute)
        with engine.begin() as conn:
            _do_insert(conn)


def _apply_creations_from_mappings(
    engine: Engine, 
    degree_code: str, 
    mappings: Dict[str, Dict[str, str]],
    conn_for_transaction: Optional[Connection] = None
) -> None:
    """
    Iterate over user's mapping choices and create new entities.
    Passes conn_for_transaction to the creation function.
    """
    for aff_type, type_mappings in mappings.items():
        for code, action in type_mappings.items():
            if action == "[CREATE_NEW]":
                _create_new_affiliation(
                    engine,
                    degree_code, 
                    aff_type, 
                    code, 
                    name=code, 
                    conn_for_transaction=conn_for_transaction
                )
                log.info(f"Successfully created new {aff_type} '{code}'")


def _build_translation_map(
    mappings: Dict[str, Dict[str, str]]
) -> Dict[str, Dict[str, str]]:
    """
    Converts the raw UI mappings into a clean translation map
    for the import function.
    
    Input: {'cg': {'TECH': '[CREATE_NEW]', 'DES': 'CG_A', 'ALD': '[IGNORE]'}}
    Output: {'cg': {'TECH': 'TECH', 'DES': 'CG_A', 'ALD': '[IGNORE]'}}
    """
    translation_map = {}
    for aff_type, type_mappings in mappings.items():
        translation_map[aff_type] = {}
        for code, action in type_mappings.items():
            if action == "[CREATE_NEW]":
                translation_map[aff_type][code] = code  # Map to itself (it now exists)
            else:
                translation_map[aff_type][code] = action  # Map to 'CG_A' or '[IGNORE]'
    return translation_map


def _reset_import_state():
    """Resets the session state for the import wizard."""
    st.session_state.import_step = 'initial'
    st.session_state.import_mappings = {}
    st.session_state.import_validation_data = None
    st.session_state.import_df = None
    # --- FIX: Do not reset the degree ---
    # st.session_state.import_degree = None 
    log.debug("Resetting import state.")


# ----------------------------- Shared helpers -----------------------------

def _is_academic_admin(conn, email: str) -> bool:
    """Check if an email belongs to an academic admin (managed in User Roles)"""
    try:
        result = conn.execute(sa_text("""
            SELECT 1 FROM academic_admins aa
            JOIN users u ON aa.user_id = u.id
            WHERE lower(u.email) = lower(:e) AND u.active = 1
        """), {"e": email}).fetchone()
        return bool(result)
    except Exception:
        return False

def _pa_schema_info(conn) -> dict:
    """
    Introspect position_assignments schema so we can handle variants:
    - Either 'assignee_email' or legacy 'faculty_email'
    - 'is_active' vs legacy 'active'
    - Optional columns (credit_relief, start_date, end_date, notes)
    """
    cols = conn.execute(sa_text("PRAGMA table_info(position_assignments)")).fetchall()
    names = {c[1] for c in cols}
    return {
        "has_assignee_email": "assignee_email" in names,
        "has_faculty_email": "faculty_email" in names,  # legacy
        "has_is_active": "is_active" in names,
        "has_active": "active" in names,                # legacy
        "has_degree": "degree_code" in names,
        "has_program": "program_code" in names,
        "has_branch": "branch_code" in names,
        "has_group": "group_code" in names,
        "has_start": "start_date" in names,
        "has_end": "end_date" in names,
        "has_credit": "credit_relief" in names,
        "has_notes": "notes" in names,
    }

def _csv_get_email(row: pd.Series) -> str:
    """Accept both assignee_email and faculty_email in positions CSVs."""
    return (row.get("assignee_email") or row.get("faculty_email") or "").strip().lower()

# ---------- NEW: Ensure username + initial credentials for a faculty email ----------

def _ensure_username_and_initial_creds(conn, email: str, full_name: str) -> None:
    """
    Creates a username in faculty_profiles (if missing), and an entry in
    faculty_initial_credentials with a plaintext password (if missing or consumed).
    Also flips first_login_pending=1 and password_export_available=1 so
    Export Credentials can list the row.
    """
    # Get profile id, name, username
    prof = conn.execute(sa_text(
        "SELECT id, name, COALESCE(username,'') FROM faculty_profiles WHERE lower(email)=lower(:e)"
    ), {"e": email}).fetchone()
    if not prof:
        return
    pid, name_db, username = int(prof[0]), (full_name or prof[1] or ""), (prof[2] or "")
    if not username:
        # Generate a unique username from the name
        username = _generate_faculty_username(conn, name_db)
        conn.execute(sa_text(
            "UPDATE faculty_profiles SET username=:u, updated_at=CURRENT_TIMESTAMP WHERE id=:id"
        ), {"u": username, "id": pid})

    # Build an initial password from name (uses a deterministic pattern)
    initial_pw = _initial_faculty_password_from_name(name_db, "0000")

    # If there is already a row for this profile and it's not consumed, keep it.
    cred = conn.execute(sa_text(
        "SELECT id, consumed FROM faculty_initial_credentials WHERE faculty_profile_id=:pid"
    ), {"pid": pid}).fetchone()

    if cred:
        if int(cred[1]) != 0:
            # Re-issue a fresh credential for re-export
            conn.execute(sa_text("""
                UPDATE faculty_initial_credentials
                   SET username=:u, plaintext=:p, consumed=0, created_at=CURRENT_TIMESTAMP
                 WHERE faculty_profile_id=:pid
            """), {"u": username, "p": initial_pw, "pid": pid})
    else:
        conn.execute(sa_text("""
            INSERT INTO faculty_initial_credentials(faculty_profile_id, username, plaintext, consumed)
            VALUES(:pid, :u, :p, 0)
        """), {"pid": pid, "u": username, "p": initial_pw})

    # Make available for export
    conn.execute(sa_text("""
        UPDATE faculty_profiles
           SET first_login_pending=1,
               password_export_available=1,
               updated_at=CURRENT_TIMESTAMP
         WHERE id=:pid
    """), {"pid": pid})

# ----------------------------- Profiles -----------------------------

@st.cache_data
def _export_profiles_template():
    """Generate profiles template CSV"""
    columns = [
        "name", "email", "phone", "employee_id",
        "status", "first_login_pending"
    ]
    sample_data = pd.DataFrame(columns=columns)
    return sample_data.to_csv(index=False)

def _import_profiles_with_validation(engine: Engine, df: pd.DataFrame, dry_run: bool = False) -> Tuple[List[Dict[str, Any]], int]:
    """
    Import profiles with validation.
    SKIPS academic admins (managed in User Roles).
    Also ensures username + initial credentials are generated for each profile.
    """
    df.columns = [col.lower().strip().replace(' ', '_') for col in df.columns]

    errors: List[Dict[str, Any]] = []
    success_count = 0
    skipped_admins: List[str] = []

    conn = engine.connect()
    trans = conn.begin()
    should_close = True

    try:
        with engine.begin() as meta_conn:
            custom_fields = _get_custom_profile_fields(meta_conn)
            active_fields = {f['field_name']: f for f in custom_fields if f['is_active']}
            field_mapping = _get_custom_field_mapping(meta_conn)

        custom_columns = []
        for col in df.columns:
            if col.startswith('custom_'):
                custom_columns.append(col)
            elif col.lower() in field_mapping:
                custom_columns.append(col)

        for idx, row in df.iterrows():
            try:
                if not row.get('name') or not row.get('email'):
                    errors.append({'row': idx + 2, 'email': row.get('email', 'unknown'), 'error': "Missing required fields: name and email"})
                    continue

                # Skip academic admins
                if _is_academic_admin(conn, row['email']):
                    skipped_admins.append(row['email'])
                    continue

                # Upsert profile
                conn.execute(sa_text("""
                    INSERT INTO faculty_profiles(name, email, phone, employee_id, status, first_login_pending, password_export_available)
                    VALUES(:n, :e, :p, :emp, :s, COALESCE(:flp,1), 1)
                    ON CONFLICT(email) DO UPDATE SET
                      name=excluded.name,
                      phone=excluded.phone,
                      employee_id=excluded.employee_id,
                      status=excluded.status,
                      first_login_pending=COALESCE(excluded.first_login_pending, faculty_profiles.first_login_pending),
                      password_export_available=1,
                      updated_at=CURRENT_TIMESTAMP
                """), {
                    "n": row['name'],
                    "e": row['email'].lower(),
                    "p": row.get('phone'),
                    "emp": row.get('employee_id'),
                    "s": row.get('status', 'active'),
                    "flp": _safe_int_convert(row.get('first_login_pending'), 1)
                })

                # Custom fields
                for col in custom_columns:
                    if col in row and pd.notna(row[col]) and row[col] != '':
                        field_name = col[7:] if col.startswith('custom_') else field_mapping.get(col.lower())
                        if field_name and field_name in active_fields:
                            _save_custom_field_value(conn, row['email'].lower(), field_name, str(row[col]))

                # NEW: Ensure username + initial credentials exist
                _ensure_username_and_initial_creds(conn, row['email'], row['name'])

                success_count += 1

            except Exception as e:
                errors.append({'row': idx + 2, 'email': row.get('email', 'unknown'), 'error': str(e)})

        if dry_run:
            trans.rollback()
        else:
            trans.commit()
    except Exception:
        if trans:
            trans.rollback()
        raise
    finally:
        if should_close:
            conn.close()

    if skipped_admins and not dry_run:
        st.info(f"ℹ️ Skipped {len(skipped_admins)} academic admin(s): {', '.join(skipped_admins)}\n(Academic admins are managed in User Roles)")

    return errors, success_count

# ----------------------------- Affiliations -----------------------------

@st.cache_data
def _export_affiliations_template():
    """Generate affiliations template CSV"""
    columns = [
        "email", "degree_code", "program_code", "branch_code", "group_code",
        "designation", "type", "allowed_credit_override", "active"
    ]
    sample_data = pd.DataFrame(columns=columns)
    return sample_data.to_csv(index=False)

def _import_affiliations_with_validation(engine: Engine, df: pd.DataFrame, degree: str, dry_run: bool = False) -> Tuple[List[Dict[str, Any]], int]:
    """
    Import affiliations with validation.
    SKIPS academic admins (managed in User Roles).
    """
    df.columns = [col.lower().strip().replace(' ', '_') for col in df.columns]

    errors: List[Dict[str, Any]] = []
    success_count = 0
    skipped_admins: List[str] = []

    conn = engine.connect()
    trans = conn.begin()
    should_close = True

    try:
        for idx, row in df.iterrows():
            try:
                if not row.get('email') or not row.get('designation'):
                    errors.append({'row': idx + 2, 'email': row.get('email', 'unknown'), 'error': "Missing required fields: email and designation"})
                    continue

                affiliation_type = row.get('type', 'core').lower()
                if affiliation_type not in ['core', 'visiting']:
                    errors.append({'row': idx + 2, 'email': row['email'], 'error': f"Invalid type '{row.get('type')}'. Must be 'core' or 'visiting'."})
                    continue

                # Skip academic admins
                if _is_academic_admin(conn, row['email']):
                    skipped_admins.append(row['email'])
                    continue

                # ensure designation is enabled for this degree
                with engine.begin() as chk:
                    if not _designation_enabled(chk, degree, row['designation']):
                        errors.append({'row': idx + 2, 'email': row['email'], 'error': f"Designation '{row['designation']}' not enabled for degree {degree}"})
                        continue

                prog = row.get('program_code')
                br = row.get('branch_code')
                grp = row.get('group_code')

                prog = None if pd.isna(prog) or not str(prog).strip() else prog
                br   = None if pd.isna(br)   or not str(br).strip()   else br
                grp  = None if pd.isna(grp)  or not str(grp).strip()  else grp

                if br and not prog:
                    errors.append({'row': idx + 2, 'email': row['email'], 'error': "program_code is required when branch_code is specified"})
                    continue

                existing = conn.execute(sa_text("""
                    SELECT 1 FROM faculty_affiliations
                    WHERE email = :e AND degree_code = :d
                      AND COALESCE(program_code, '') = COALESCE(:p, '')
                      AND COALESCE(branch_code,  '') = COALESCE(:b, '')
                      AND COALESCE(group_code,   '') = COALESCE(:g, '')
                """), {"e": row['email'].lower(), "d": degree, "p": prog, "b": br, "g": grp}).fetchone()

                if existing:
                    conn.execute(sa_text("""
                        UPDATE faculty_affiliations
                           SET designation = :des,
                               type = :t,
                               allowed_credit_override = :o,
                               active = :a,
                               updated_at = CURRENT_TIMESTAMP
                         WHERE email = :e AND degree_code = :d
                           AND COALESCE(program_code, '') = COALESCE(:p, '')
                           AND COALESCE(branch_code,  '') = COALESCE(:b, '')
                           AND COALESCE(group_code,   '') = COALESCE(:g, '')
                    """), {
                        "e": row['email'].lower(),
                        "d": degree,
                        "p": prog,
                        "b": br,
                        "g": grp,
                        "des": row['designation'],
                        "t": affiliation_type,
                        "o": _safe_int_convert(row.get('allowed_credit_override'), 0),
                        "a": _safe_int_convert(row.get('active'), 1)
                    })
                else:
                    conn.execute(sa_text("""
                        INSERT INTO faculty_affiliations
                          (email, degree_code, program_code, branch_code, group_code,
                           designation, type, allowed_credit_override, active)
                        VALUES(:e, :d, :p, :b, :g, :des, :t, :o, :a)
                    """), {
                        "e": row['email'].lower(),
                        "d": degree,
                        "p": prog,
                        "b": br,
                        "g": grp,
                        "des": row['designation'],
                        "t": affiliation_type,
                        "o": _safe_int_convert(row.get('allowed_credit_override'), 0),
                        "a": _safe_int_convert(row.get('active'), 1)
                    })

                success_count += 1

            except Exception as e:
                errors.append({'row': idx + 2, 'email': row.get('email', 'unknown'), 'error': str(e)})

        if dry_run:
            trans.rollback()
        else:
            trans.commit()
    except Exception:
        if trans:
            trans.rollback()
        raise
    finally:
        if should_close:
            conn.close()

    if skipped_admins and not dry_run:
        st.info(f"ℹ️ Skipped {len(skipped_admins)} academic admin(s): {', '.join(skipped_admins)}\n(Academic admin affiliations are auto-managed)")

    return errors, success_count

# ----------------------------- Administrative Positions -----------------------------

@st.cache_data
def _export_positions_template() -> str:
    """Generate administrative positions template CSV."""
    columns = [
        "assignee_email", "position_code", "degree_code",
        "program_code", "branch_code", "group_code",
        "start_date", "end_date", "credit_relief", "notes", "is_active"
    ]
    sample = pd.DataFrame(columns=columns)
    return sample.to_csv(index=False)

@st.cache_data
def _prepare_positions_export_data(_engine: Engine) -> pd.DataFrame:
    """Prepare DataFrame for exporting all administrative position assignments (schema-aware)."""
    with _engine.begin() as conn:
        info = _pa_schema_info(conn)

        email_sel = "COALESCE(pa.assignee_email,'')" if info["has_assignee_email"] else (
                    "COALESCE(pa.faculty_email,'')" if info["has_faculty_email"] else "''")
        act_sel   = "COALESCE(pa.is_active,1)" if info["has_is_active"] else (
                    "COALESCE(pa.active,1)" if info["has_active"] else "1")

        deg_sel = "COALESCE(pa.degree_code,'')"   if info["has_degree"]  else "''"
        prg_sel = "COALESCE(pa.program_code,'')"  if info["has_program"] else "''"
        br_sel  = "COALESCE(pa.branch_code,'')"   if info["has_branch"]  else "''"
        grp_sel = "COALESCE(pa.group_code,'')"    if info["has_group"]   else "''"
        st_sel  = "COALESCE(pa.start_date,'')"    if info["has_start"]   else "''"
        en_sel  = "COALESCE(pa.end_date,'')"      if info["has_end"]     else "''"
        cr_sel  = "COALESCE(pa.credit_relief,0)"  if info["has_credit"]  else "0"
        nt_sel  = "COALESCE(pa.notes,'')"         if info["has_notes"]   else "''"

        sql = f"""
            SELECT
                {email_sel} AS assignee_email,
                pa.position_code,
                {deg_sel}  AS degree_code,
                {prg_sel}  AS program_code,
                {br_sel}   AS branch_code,
                {grp_sel}  AS group_code,
                {st_sel}   AS start_date,
                {en_sel}   AS end_date,
                {cr_sel}   AS credit_relief,
                {nt_sel}   AS notes,
                {act_sel}  AS is_active
            FROM position_assignments pa
            ORDER BY 1, pa.position_code
        """
        rows = conn.execute(sa_text(sql)).fetchall()

    return pd.DataFrame(rows, columns=[
        "Assignee Email", "Position Code", "Degree Code",
        "Program Code", "Branch Code", "Group Code",
        "Start Date", "End Date", "Credit Relief", "Notes", "Is Active"
    ])

def _import_positions_with_validation(engine: Engine, df: pd.DataFrame, dry_run: bool = False) -> Tuple[List[Dict[str, Any]], int]:
    """
    Import administrative position assignments with validation.
    - Validates position_code against active positions.
    - Allows institution-wide positions (empty degree/program/branch/group).
    - Enforces: branch_code requires program_code.
    - Schema-aware: works with assignee_email (current) or legacy faculty_email; is_active or active.
    """
    df.columns = [c.lower().strip().replace(' ', '_') for c in df.columns]

    errors: List[Dict[str, Any]] = []
    success_count = 0

    conn = engine.connect()
    trans = conn.begin()
    should_close = True

    try:
        info = _pa_schema_info(conn)

        # Validate position codes once
        with engine.begin() as chk:
            pos_rows = _get_all_positions(chk) or []
            valid_pos = {p["position_code"] for p in pos_rows if p.get("is_active", 1)}

        for i, row in df.iterrows():
            try:
                email = _csv_get_email(row)
                pcode = (row.get("position_code") or "").strip()
                if not email or not pcode:
                    errors.append({"row": i + 2, "email": email or "unknown", "error": "assignee_email (or faculty_email) and position_code are required"})
                    continue
                if pcode not in valid_pos:
                    errors.append({"row": i + 2, "email": email, "error": f"Unknown or inactive position_code '{pcode}'"})
                    continue

                deg  = (row.get("degree_code")  or "").strip() or None
                prog = (row.get("program_code") or "").strip() or None
                br   = (row.get("branch_code")  or "").strip() or None
                grp  = (row.get("group_code")   or "").strip() or None

                if br and not prog:
                    errors.append({"row": i + 2, "email": email, "error": "program_code is required when branch_code is specified"})
                    continue

                start_date = (row.get("start_date") or "").strip() or None
                end_date   = (row.get("end_date")   or "").strip() or None
                credit_relief = _safe_int_convert(row.get("credit_relief"), 0)
                notes = (row.get("notes") or "").strip() or None

                csv_active_val = row.get("is_active", row.get("active", 1))
                is_active_val = _safe_int_convert(csv_active_val, 1)

                # Upsert by email + scope + start_date
                if info["has_assignee_email"]:
                    key_bind = {"e": email, "p": pcode, "d": deg, "g1": prog, "b": br, "g2": grp, "sd": start_date or ""}
                    sel_sql = """
                        SELECT id FROM position_assignments pa
                        WHERE lower(pa.assignee_email)=lower(:e) AND pa.position_code=:p
                          AND COALESCE(pa.degree_code,'')  = COALESCE(:d,'')
                          AND COALESCE(pa.program_code,'') = COALESCE(:g1,'')
                          AND COALESCE(pa.branch_code,'')  = COALESCE(:b,'')
                          AND COALESCE(pa.group_code,'')   = COALESCE(:g2,'')
                          AND COALESCE(pa.start_date,'')   = COALESCE(:sd,'')
                    """
                    insert_email_col = "assignee_email"
                elif info["has_faculty_email"]:
                    key_bind = {"e": email, "p": pcode, "d": deg, "g1": prog, "b": br, "g2": grp, "sd": start_date or ""}
                    sel_sql = """
                        SELECT id FROM position_assignments pa
                        WHERE lower(pa.faculty_email)=lower(:e) AND pa.position_code=:p
                          AND COALESCE(pa.degree_code,'')  = COALESCE(:d,'')
                          AND COALESCE(pa.program_code,'') = COALESCE(:g1,'')
                          AND COALESCE(pa.branch_code,'')  = COALESCE(:b,'')
                          AND COALESCE(pa.group_code,'')   = COALESCE(:g2,'')
                          AND COALESCE(pa.start_date,'')   = COALESCE(:sd,'')
                    """
                    insert_email_col = "faculty_email"
                else:
                    errors.append({"row": i + 2, "email": email, "error": "position_assignments table has neither assignee_email nor faculty_email"})
                    continue

                existing = conn.execute(sa_text(sel_sql), key_bind).fetchone()

                update_sets = ["updated_at=CURRENT_TIMESTAMP"]
                update_vals: Dict[str, Any] = {}
                cols = [insert_email_col, "position_code", "degree_code", "program_code", "branch_code", "group_code", "start_date"]
                vals = {"e": email, "p": pcode, "d": deg, "g1": prog, "b": br, "g2": grp, "sd": start_date}

                # Optional cols
                if end_date is not None:
                    update_sets.append("end_date=:ed")
                    update_vals["ed"] = end_date
                    cols.append("end_date"); vals["ed"] = end_date

                # credit_relief / notes
                pa_cols = conn.execute(sa_text("PRAGMA table_info(position_assignments)")).fetchall()
                names = {c[1] for c in pa_cols}
                if "credit_relief" in names:
                    update_sets.append("credit_relief=:cr"); update_vals["cr"] = credit_relief
                    cols.append("credit_relief"); vals["cr"] = credit_relief
                if "notes" in names:
                    update_sets.append("notes=:n"); update_vals["n"] = notes
                    cols.append("notes"); vals["n"] = notes

                # active flag
                if "is_active" in names:
                    update_sets.append("is_active=:ia"); update_vals["ia"] = is_active_val
                    cols.append("is_active"); vals["ia"] = is_active_val
                elif "active" in names:
                    update_sets.append("active=:ia"); update_vals["ia"] = is_active_val
                    cols.append("active"); vals["ia"] = is_active_val

                if existing:
                    up_sql = f"UPDATE position_assignments SET {', '.join(update_sets)} WHERE id=:id"
                    conn.execute(sa_text(up_sql), {"id": int(existing[0]), **update_vals})
                else:
                    cols_sql = ", ".join(cols)
                    vals_sql = ", ".join(f":{k}" for k in vals.keys())
                    ins_sql = f"INSERT INTO position_assignments({cols_sql}) VALUES({vals_sql})"
                    conn.execute(sa_text(ins_sql), vals)

                success_count += 1

            except Exception as row_ex:
                errors.append({"row": i + 2, "email": _csv_get_email(row) or "unknown", "error": str(row_ex)})

        if dry_run:
            trans.rollback()
        else:
            trans.commit()

    except Exception:
        if trans:
            trans.rollback()
        raise
    finally:
        if should_close:
            conn.close()

    return errors, success_count

# ----------------------------- Combined (Profiles + Affiliations) with STATEFUL IMPORT -----------------------------

@st.cache_data
def _export_combined_template(_engine: Engine) -> str:
    """Generate combined template CSV with profiles and affiliations"""
    with _engine.begin() as conn:
        custom_fields = _get_custom_profile_fields(conn)
        active_fields = [f for f in custom_fields if f['is_active']]
        degrees = _active_degrees(conn)

    columns = [
        "name", "email", "phone", "employee_id",
        "status", "first_login_pending",
        "degree_code", "program_code", "branch_code", "group_code",
        "designation", "type", "allowed_credit_override", "active"
    ]

    for field in active_fields:
        columns.append(f"custom_{field['field_name']}")

    sample_data = pd.DataFrame(columns=columns)

    if degrees:
        sample_row = {col: "" for col in columns}
        sample_row["degree_code"] = degrees[0]
        sample_data = pd.concat([sample_data, pd.DataFrame([sample_row])], ignore_index=True)

    return sample_data.to_csv(index=False)


def _import_combined_with_validation(
    engine: Engine, 
    df: pd.DataFrame, 
    dry_run: bool,
    mappings: Optional[Dict[str, Dict[str, str]]] = None,
    conn_for_transaction: Optional[Connection] = None
) -> Tuple[List[Dict[str, Any]], int, List[Dict[str, Any]]]: # <-- NEW: ADDED SKIPPED LIST
    """
    Import combined profiles and affiliations with validation AND MAPPINGS.
    SKIPS academic admins (managed in User Roles).
    Also ensures username + initial credentials are generated for each profile.
    
    If conn_for_transaction is provided, it uses it for the import.
    
    RETURNS: (errors, success_count, skipped_rows)
    """
    df.columns = [col.lower().strip().replace(' ', '_') for col in df.columns]

    errors: List[Dict[str, Any]] = []
    skipped_rows: List[Dict[str, Any]] = [] # <-- NEW: INITIALIZE SKIPPED LIST
    success_count = 0
    skipped_admins: List[str] = []

    translation_map = _build_translation_map(mappings) if mappings else {}

    # Metadata fetch (this is safe, it creates and closes its own connection)
    with engine.begin() as meta_conn:
        custom_fields = _get_custom_profile_fields(meta_conn)
        active_fields = {f['field_name']: f for f in custom_fields if f['is_active']}
        field_mapping = _get_custom_field_mapping(meta_conn)
        all_degrees = _active_degrees(meta_conn)

    # --- NEW TRANSACTION LOGIC ---
    if conn_for_transaction:
        conn = conn_for_transaction
        trans = conn.begin_nested()
        should_close = False
    else:
        conn = engine.connect()
        trans = conn.begin()
        should_close = True
    # --- END NEW LOGIC ---

    try:
        custom_columns = []
        for col in df.columns:
            if col.startswith('custom_'):
                custom_columns.append(col)
            elif col.lower() in field_mapping:
                custom_columns.append(col)

        for idx, row in df.iterrows():
            try:
                if not row.get('name') or not row.get('email'):
                    errors.append({'row': idx + 2, 'email': row.get('email', 'unknown'), 'error': "Missing required fields: name and email"})
                    continue

                if _is_academic_admin(conn, row['email']):
                    skipped_admins.append(row['email'])
                    continue

                degree_code = row.get('degree_code')

                if degree_code:
                    if degree_code not in all_degrees:
                        errors.append({'row': idx + 2, 'email': row['email'], 'error': f"Degree '{degree_code}' not found."})
                        continue

                    if row.get('designation'):
                        if not _designation_enabled(conn, degree_code, row['designation']):
                            errors.append({'row': idx + 2, 'email': row['email'], 'error': f"Designation '{row['designation']}' not enabled for degree {degree_code}"})
                            continue

                # (Profile Upsert logic is unchanged)
                conn.execute(sa_text("""
                    INSERT INTO faculty_profiles(name, email, phone, employee_id, status, first_login_pending, password_export_available)
                    VALUES(:n, :e, :p, :emp, :s, COALESCE(:flp,1), 1)
                    ON CONFLICT(email) DO UPDATE SET
                      name=excluded.name, phone=excluded.phone, employee_id=excluded.employee_id, status=excluded.status,
                      first_login_pending=COALESCE(excluded.first_login_pending, faculty_profiles.first_login_pending),
                      password_export_available=1, updated_at=CURRENT_TIMESTAMP
                """), {
                    "n": row['name'], "e": row['email'].lower(), "p": row.get('phone'), "emp": row.get('employee_id'),
                    "s": row.get('status', 'active'), "flp": _safe_int_convert(row.get('first_login_pending'), 1)
                })

                # (Custom fields logic is unchanged)
                for col in custom_columns:
                    if col in row and pd.notna(row[col]) and row[col] != '':
                        field_name = col[7:] if col.startswith('custom_') else field_mapping.get(col.lower())
                        if field_name and field_name in active_fields:
                            _save_custom_field_value(conn, row['email'].lower(), field_name, str(row[col]))

                # (Credentials logic is unchanged)
                _ensure_username_and_initial_creds(conn, row['email'], row['name'])

                # Process affiliation if degree and designation provided
                if degree_code and row.get('designation'):
                    affiliation_type = row.get('type', 'core').lower()
                    if affiliation_type not in ['core', 'visiting']:
                        errors.append({'row': idx + 2, 'email': row['email'], 'error': f"Invalid type '{row.get('type')}'. Must be 'core' or 'visiting'."})
                        continue

                    group_code = str(row.get('group_code', '')).strip()
                    program_code = str(row.get('program_code', '')).strip()
                    branch_code = str(row.get('branch_code', '')).strip()

                    if translation_map:
                        mapped_group = translation_map.get('cg', {}).get(group_code, group_code)
                        mapped_program = translation_map.get('program', {}).get(program_code, program_code)
                        mapped_branch = translation_map.get('branch', {}).get(branch_code, branch_code)

                        # --- NEW: TRACK SKIPPED ROWS ---
                        if mapped_group == "[IGNORE]" or mapped_program == "[IGNORE]" or mapped_branch == "[IGNORE]":
                            log.debug(f"Row {idx + 2}: Ignoring affiliation due to user mapping.")
                            skipped_rows.append({
                                "row": idx + 2, 
                                "email": row.get('email', 'unknown'), 
                                "reason": "Ignored by user mapping rule."
                            })
                            continue # Skip the rest of the affiliation logic
                        # --- END NEW LOGIC ---

                        prog = mapped_program if mapped_program else None
                        br = mapped_branch if mapped_branch else None
                        grp = mapped_group if mapped_group else None
                    else:
                        prog = program_code if program_code else None
                        br = branch_code if branch_code else None
                        grp = group_code if group_code else None

                    if br and not prog:
                        errors.append({'row': idx + 2, 'email': row['email'], 'error': "program_code is required when branch_code is specified"})
                        continue

                    try:
                        # (Affiliation upsert logic is unchanged)
                        existing = conn.execute(sa_text("""
                            SELECT 1 FROM faculty_affiliations
                            WHERE email = :e AND degree_code = :d
                              AND COALESCE(program_code, '') = COALESCE(:p, '')
                              AND COALESCE(branch_code,  '') = COALESCE(:b, '')
                              AND COALESCE(group_code,   '') = COALESCE(:g, '')
                        """), {"e": row['email'].lower(), "d": degree_code, "p": prog, "b": br, "g": grp}).fetchone()

                        if existing:
                            conn.execute(sa_text("""
                                UPDATE faculty_affiliations
                                   SET designation = :des, type = :t, allowed_credit_override = :o, active = :a, updated_at = CURRENT_TIMESTAMP
                                 WHERE email = :e AND degree_code = :d
                                   AND COALESCE(program_code, '') = COALESCE(:p, '')
                                   AND COALESCE(branch_code,  '') = COALESCE(:b, '')
                                   AND COALESCE(group_code,   '') = COALESCE(:g, '')
                            """), {
                                "e": row['email'].lower(), "d": degree_code, "p": prog, "b": br, "g": grp, "des": row['designation'],
                                "t": affiliation_type, "o": _safe_int_convert(row.get('allowed_credit_override'), 0), "a": _safe_int_convert(row.get('active'), 1)
                            })
                        else:
                            conn.execute(sa_text("""
                                INSERT INTO faculty_affiliations
                                  (email, degree_code, program_code, branch_code, group_code,
                                   designation, type, allowed_credit_override, active)
                                VALUES(:e, :d, :p, :b, :g, :des, :t, :o, :a)
                            """), {
                                "e": row['email'].lower(), "d": degree_code, "p": prog, "b": br, "g": grp, "des": row['designation'],
                                "t": affiliation_type, "o": _safe_int_convert(row.get('allowed_credit_override'), 0), "a": _safe_int_convert(row.get('active'), 1)
                            })
                    except Exception as aff_error:
                        errors.append({'row': idx + 2, 'email': row['email'], 'error': f"Affiliation error: {str(aff_error)}"})
                        continue

                success_count += 1

            except Exception as e:
                errors.append({'row': idx + 2, 'email': row.get('email', 'unknown'), 'error': str(e)})

        if dry_run:
            trans.rollback()
        else:
            trans.commit()
    except Exception:
        if trans:
            trans.rollback()
        raise
    finally:
        if should_close:
            conn.close()

    if skipped_admins and not dry_run:
        st.info(f"ℹ️ Skipped {len(skipped_admins)} academic admin(s): {', '.join(skipped_admins)}\n(Academic admins are managed in User Roles)")

    return errors, success_count, skipped_rows # <-- NEW: RETURN SKIPPED LIST


# ----------------------------- Export helpers (profiles/affiliations) -----------------------------

@st.cache_data
def _prepare_profiles_export_data(_engine: Engine) -> pd.DataFrame:
    """Helper to generate the DataFrame for profiles export."""
    with _engine.begin() as conn:
        rows = conn.execute(sa_text("""
            SELECT name, email, phone, employee_id, status, first_login_pending
            FROM faculty_profiles ORDER BY name
        """)).fetchall()
        df = pd.DataFrame(rows, columns=["Name", "Email", "Phone", "Employee ID", "Status", "First Login Pending"])

        custom_fields = _get_custom_profile_fields(conn)
        active_fields = [f for f in custom_fields if f['is_active']]
        custom_data_map = _get_all_custom_field_data(conn)

        for field in active_fields:
            field_name = field['field_name']
            df[field['display_name']] = df['Email'].apply(
                lambda email: custom_data_map.get(email.lower(), {}).get(field_name, '')
            )
    return df

@st.cache_data
def _prepare_affiliations_export_data(_engine: Engine, degree: str) -> pd.DataFrame:
    """Helper to generate the DataFrame for affiliations export."""
    with _engine.begin() as conn:
        rows = conn.execute(sa_text("""
            SELECT email, degree_code, program_code, branch_code, group_code,
                   designation, type, allowed_credit_override, active
            FROM faculty_affiliations
            WHERE lower(degree_code)=lower(:d) ORDER BY email
        """), {"d": degree}).fetchall()
    df = pd.DataFrame(rows, columns=[
        "Email", "Degree Code", "Program Code", "Branch Code", "Group Code",
        "Designation", "Type", "Allowed Credit Override", "Active"
    ])
    return df

@st.cache_data
def _prepare_combined_export_data(_engine: Engine) -> pd.DataFrame:
    """Helper to generate the DataFrame for combined export."""
    with _engine.begin() as conn:
        profile_rows = conn.execute(sa_text("""
            SELECT name, email, phone, employee_id, status, first_login_pending
            FROM faculty_profiles ORDER BY name
        """)).fetchall()

        custom_fields = _get_custom_profile_fields(conn)
        active_fields = [f for f in custom_fields if f['is_active']]
        custom_data_map = _get_all_custom_field_data(conn)

        affiliation_rows = conn.execute(sa_text("""
            SELECT email, degree_code, program_code, branch_code, group_code,
                   designation, type, allowed_credit_override, active
            FROM faculty_affiliations ORDER BY email, degree_code
        """)).fetchall()

        profiles_dict: Dict[str, Dict[str, Any]] = {}
        for row in profile_rows:
            email = row[1].lower()
            profiles_dict[email] = {
                "name": row[0],
                "email": row[1],
                "phone": row[2],
                "employee_id": row[3],
                "status": row[4],
                "first_login_pending": row[5]
            }
            user_custom_data = custom_data_map.get(email, {})
            for field in active_fields:
                profiles_dict[email][field['display_name']] = user_custom_data.get(field['field_name'], '')

        affiliation_map: Dict[str, List[Any]] = defaultdict(list)
        for aff_row in affiliation_rows:
            affiliation_map[aff_row[0].lower()].append(aff_row)

        combined_data: List[Dict[str, Any]] = []

        for email, profile_data in profiles_dict.items():
            if email in affiliation_map:
                for aff_row in affiliation_map[email]:
                    combined_row = profile_data.copy()
                    combined_row.update({
                        "degree_code": aff_row[1],
                        "program_code": aff_row[2] or "",
                        "branch_code": aff_row[3] or "",
                        "group_code": aff_row[4] or "",
                        "designation": aff_row[5],
                        "type": aff_row[6],
                        "allowed_credit_override": aff_row[7],
                        "active": aff_row[8]
                    })
                    combined_data.append(combined_row)
            else:
                combined_data.append(profile_data.copy())

        columns = [
            "name", "email", "phone", "employee_id", "status", "first_login_pending",
            "degree_code", "program_code", "branch_code", "group_code",
            "designation", "type", "allowed_credit_override", "active"
        ]

        df = pd.DataFrame(combined_data)

        final_columns = columns.copy()
        for field in active_fields:
            display_name = field['display_name']
            field_name = f"custom_{field['field_name']}"

            if display_name not in final_columns:
                final_columns.append(display_name)
                df[display_name] = df['email'].map(lambda e: profiles_dict.get(e.lower(), {}).get(display_name, ''))

            if field_name not in final_columns:
                final_columns.append(field_name)
                if display_name in df.columns:
                    df[field_name] = df[display_name]
                else:
                    df[field_name] = ''

        for col in final_columns:
            if col not in df.columns:
                df[col] = ''

        df = df.reindex(columns=final_columns)
        return df


# ----------------------------- UI Sections -----------------------------

def _add_import_export_section(engine: Engine, entity_type: str, degree: str):
    """Add import/export UI section for profiles or affiliations"""

    if entity_type == "profiles":
        st.divider()
        st.subheader("Profiles Import/Export")

        st.info("""
        ℹ️ **Note:** Academic admins are managed in User Roles and will be skipped during import.
        """)

        col1, col2 = st.columns(2)

        with col1:
            st.markdown("**Export**")
            template_csv = _export_profiles_template()
            st.download_button("Download Profiles Template", template_csv, "faculty_profiles_template.csv", "text/csv")

            try:
                export_df = _prepare_profiles_export_data(engine)
                export_csv = export_df.to_csv(index=False)
                st.download_button("Export All Profiles", export_csv, "faculty_profiles_export.csv", "text/csv")
            except Exception as e:
                st.error(f"Failed to prepare export data: {str(e)}")

        with col2:
            st.markdown("**Import**")
            uploaded_file = st.file_uploader("Upload Profiles CSV", type=["csv"], key="upload_profiles")
            if uploaded_file:
                if st.button("Dry Run Import", key="dry_run_profiles"):
                    try:
                        uploaded_file.seek(0)
                        df = pd.read_csv(uploaded_file)
                        errors, success_count = _import_profiles_with_validation(engine, df, dry_run=True)
                        if errors:
                            st.warning(f"Dry run: {success_count} would succeed, {len(errors)} would fail")
                            st.dataframe(pd.DataFrame(errors)[['row', 'email', 'error']])
                        else:
                            st.success(f"✅ Dry run: All {success_count} records would import successfully (no changes made)")
                    except Exception as e:
                        _handle_error(e, "Dry run failed.")
                    finally:
                        uploaded_file.seek(0)

                if st.button("Execute Import", key="execute_profiles", type="primary"):
                    try:
                        uploaded_file.seek(0)
                        df = pd.read_csv(uploaded_file)
                        errors, success_count = _import_profiles_with_validation(engine, df, dry_run=False)
                        if errors:
                            st.error(f"Import completed with {len(errors)} errors")
                            st.download_button("Download Import Errors", pd.DataFrame(errors).to_csv(index=False), "profiles_import_errors.csv")
                        else:
                            st.success(f"✅ Successfully imported {success_count} profiles")
                            st.cache_data.clear() # <-- CACHE CLEAR
                            st.rerun()
                    except Exception as e:
                        _handle_error(e, "Import failed.")

    elif entity_type == "affiliations":
        st.divider()
        st.subheader("Affiliations Import/Export")

        st.info("""
        ℹ️ **Note:** Academic admin affiliations are auto-managed and will be skipped during import.
        """)

        col1, col2 = st.columns(2)

        with col1:
            st.markdown("**Export**")
            template_csv = _export_affiliations_template()
            st.download_button("Download Affiliations Template", template_csv, "faculty_affiliations_template.csv", "text/csv")

            try:
                export_df = _prepare_affiliations_export_data(engine, degree)
                export_csv = export_df.to_csv(index=False)
                st.download_button("Export Current Affiliations", export_csv, f"faculty_affiliations_{degree}_export.csv", "text/csv")
            except Exception as e:
                st.error(f"Failed to prepare export data: {e}")

        with col2:
            st.markdown("**Import**")

            uploaded_file = st.file_uploader(
                "Upload Affiliations CSV",
                type=["csv"],
                key="upload_affiliations",
                help=f"This will import affiliations for the currently selected degree ({degree}). Any 'degree_code' column in the CSV will be ignored."
            )

            if uploaded_file:
                if st.button("Dry Run Import", key="dry_run_affiliations"):
                    try:
                        uploaded_file.seek(0)
                        df = pd.read_csv(uploaded_file)
                        errors, success_count = _import_affiliations_with_validation(engine, df, degree, dry_run=True)
                        if errors:
                            st.warning(f"Dry run: {success_count} would succeed, {len(errors)} would fail")
                            st.dataframe(pd.DataFrame(errors)[['row', 'email', 'error']])
                        else:
                            st.success(f"✅ Dry run: All {success_count} records would import successfully (no changes made)")
                    except Exception as e:
                        _handle_error(e, "Dry run failed.")
                    finally:
                        uploaded_file.seek(0)

                if st.button("Execute Import", key="execute_affiliations", type="primary"):
                    try:
                        uploaded_file.seek(0)
                        df = pd.read_csv(uploaded_file)
                        errors, success_count = _import_affiliations_with_validation(engine, df, degree, dry_run=False)
                        if errors:
                            st.error(f"Import completed with {len(errors)} errors")
                            st.download_button("Download Import Errors", pd.DataFrame(errors).to_csv(index=False), "affiliations_import_errors.csv")
                        else:
                            st.success(f"✅ Successfully imported {success_count} affiliations")
                            st.cache_data.clear() # <-- CACHE CLEAR
                            st.rerun()
                    except Exception as e:
                        _handle_error(e, "Import failed.")


def _add_positions_import_export_section(engine: Engine):
    """Add Administrative Positions Import/Export UI section."""
    st.divider()
    st.subheader("🎓 Administrative Positions – Import/Export")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("**Export**")
        tpl = _export_positions_template()
        st.download_button("Download Positions Template", tpl, "admin_positions_template.csv", "text/csv")

        try:
            df = _prepare_positions_export_data(engine)
            st.download_button(
                "Export All Position Assignments",
                df.to_csv(index=False),
                "admin_positions_export.csv",
                "text/csv"
            )
        except Exception as e:
            st.error(f"Failed to prepare positions export: {e}")

    with col2:
        st.markdown("**Import**")
        up = st.file_uploader(
            "Upload Administrative Positions CSV",
            type=["csv"],
            key="upload_positions",
            help="Columns: assignee_email (or faculty_email), position_code, degree_code, program_code, branch_code, group_code, start_date, end_date, credit_relief, notes, is_active"
        )
        if up:
            if st.button("Dry Run Import", key="dry_run_positions"):
                try:
                    up.seek(0)
                    df = pd.read_csv(up)
                    errors, success = _import_positions_with_validation(engine, df, dry_run=True)
                    if errors:
                        st.warning(f"Dry run: {success} would succeed, {len(errors)} would fail")
                        st.dataframe(pd.DataFrame(errors)[['row', 'email', 'error']], use_container_width=True)
                    else:
                        st.success(f"✅ Dry run: All {success} records would import successfully (no changes made)")
                except Exception as e:
                    _handle_error(e, "Dry run failed.")
                finally:
                    up.seek(0)

            if st.button("Execute Import", key="execute_positions", type="primary"):
                try:
                    up.seek(0)
                    df = pd.read_csv(up)
                    errors, success = _import_positions_with_validation(engine, df, dry_run=False)
                    if errors:
                        st.error(f"Import completed with {len(errors)} errors out of {len(df)} rows")
                        st.download_button(
                            "Download Import Errors",
                            pd.DataFrame(errors).to_csv(index=False),
                            "positions_import_errors.csv",
                            "text/csv"
                        )
                    else:
                        st.success(f"✅ Successfully imported {success} position assignment(s)")
                        st.cache_data.clear() # <-- CACHE CLEAR
                        st.rerun()
                except Exception as e:
                    _handle_error(e, "Import failed.")


def _add_combined_import_export_section(engine: Engine):
    """Add combined import/export UI section with STATEFUL multi-step validation"""

    st.divider()
    st.subheader("🔥📤 Combined Faculty Import/Export")

    st.info("""
    ℹ️ **Note:** Academic admins (Principal, Director, etc.) are managed in **User Roles**
    and automatically synced. They will be skipped during import.
    """)

    # --- 1. State Initialization ---
    if 'import_step' not in st.session_state:
        _reset_import_state()

    # --- 2. Select Degree (Required for validation) ---
    with engine.begin() as conn:
        degrees = _active_degrees(conn)

    if not degrees:
        st.error("❌ No degrees available. Please create degrees first.")
        return

    # Persistent degree selector
    if st.session_state.get('import_degree'):
        selected_degree = st.session_state.import_degree
        st.info(f"📋 **Selected Degree:** `{selected_degree}`")
    else:
        selected_degree_raw = st.selectbox("Select Degree for Import", options=degrees, key="degree_selector_combined")
        if st.button("Confirm Degree Selection", type="primary"):
            st.session_state.import_degree = selected_degree_raw.strip() # <-- WHITESPACE FIX
            st.rerun()
        st.warning("⚠️ Please confirm your degree selection to proceed with import.")
        return

    # --- 3. Export Section (Always available) ---
    with st.expander("📥 Export Data"):
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("**Download Templates**")
            template_csv = _export_combined_template(engine)
            st.download_button(
                label="Download Combined Template",
                data=template_csv,
                file_name="faculty_combined_template.csv",
                mime="text/csv",
                key="download_combined_template_direct"
            )

        with col2:
            st.markdown("**Export Existing Data**")
            try:
                export_df = _prepare_combined_export_data(engine)
                export_csv = export_df.to_csv(index=False)
                st.download_button(
                    label="Export All Faculty Data",
                    data=export_csv,
                    file_name="faculty_combined_export.csv",
                    mime="text/csv",
                    key="download_combined_export_direct"
                )
            except Exception as e:
                st.error(f"Failed to prepare export data: {e}")

    # --- 4. File Uploader (Persistent at top) ---
    st.markdown("### 📤 Import Faculty Data")
    
    up = st.file_uploader("Upload Combined CSV", type="csv", key="combined_uploader")
    
    # --- 5. Cancel Button (Always available after 'initial') ---
    if st.session_state.import_step != 'initial':
        if st.button("🔄 Cancel Import & Start Over", key="cancel_import"):
            _reset_import_state()
            st.rerun()
        st.divider()

    # --- 6. State: Initial (Validation) ---
    if st.session_state.import_step == 'initial':
        st.markdown("#### Step 1: Validate File")
        
        if not up:
            st.warning("Please upload a CSV file to begin.")
            if 'import_df' in st.session_state:
                _reset_import_state()
            return

        if st.button("🔍 Validate File", type="primary"):
            try:
                up.seek(0)
                df = pd.read_csv(up)
                
                with st.spinner("Validating CSV against database..."):
                    # --- STATE LOSS FIX ---
                    validation_data, filtered_df = _pre_check_affiliations(df, engine, selected_degree)
                
                st.session_state.import_validation_data = validation_data
                st.session_state.import_df = filtered_df
                # --- END STATE LOSS FIX ---
                st.session_state.import_mappings = {
                    "cg": {}, "program": {}, "branch": {}
                }
                
                if validation_data.ignored_rows > 0:
                    # --- STATE LOSS FIX ---
                    st.info(f"✅ Found {len(filtered_df)} rows for degree '{selected_degree}'. "
                            f"{validation_data.ignored_rows} rows for other degrees will be ignored.")
                    # --- END STATE LOSS FIX ---

                # --- Decide next step ---
                if validation_data.unmatched_cgs:
                    st.session_state.import_step = 'map_cgs'
                elif validation_data.unmatched_programs:
                    st.session_state.import_step = 'map_programs'
                elif validation_data.unmatched_branches:
                    st.session_state.import_step = 'map_branches'
                else:
                    st.session_state.import_step = 'ready_to_import'
                
                st.rerun()

            except Exception as e:
                _handle_error(e, "Validation Failed")
                _reset_import_state()

    # --- 7. State: Map Curriculum Groups ---
    elif st.session_state.import_step == 'map_cgs':
        st.markdown("#### Step 2: Map Curriculum Groups (CGs)")
        st.warning("⚠️ Your file contains CGs that are not in the database for this degree.")
        
        data: AffiliationCheckResult = st.session_state.import_validation_data
        options = ["[Select Action]"] + data.existing_cgs + ["[-- Create New CG --]", "[-- Ignore These Rows --]"]
        
        valid = True
        col1, col2 = st.columns([1, 2])
        col1.markdown("**CSV Value**")
        col2.markdown("**Action**")

        for code in sorted(list(data.unmatched_cgs)):
            col1.write(f"`{code}`")  # <-- DISPLAY CSV VALUE
            key = f"map_cg_{code}"
            
            # --- VALUEERROR FIX ---
            stored_action = st.session_state.import_mappings['cg'].get(code, "[Select Action]")
            if stored_action == "[CREATE_NEW]":
                default_display_value = "[-- Create New CG --]"
            elif stored_action == "[IGNORE]":
                default_display_value = "[-- Ignore These Rows --]"
            else:
                default_display_value = stored_action

            try:
                default_index = options.index(default_display_value)
            except ValueError:
                default_index = 0
            
            choice = col2.selectbox(
                f"map_{code}", 
                options=options, 
                key=key,
                index=default_index,
                label_visibility="collapsed"
            )
            # --- END VALUEERROR FIX ---

            if choice == "[-- Create New CG --]":
                st.session_state.import_mappings['cg'][code] = "[CREATE_NEW]"
            elif choice == "[-- Ignore These Rows --]":
                st.session_state.import_mappings['cg'][code] = "[IGNORE]"
            else:
                st.session_state.import_mappings['cg'][code] = choice
            
            if choice == "[Select Action]":
                valid = False

        if st.button("➡️ Next", type="primary"):
            if not valid:
                st.error("❌ Please select an action for every item.")
            else:
                if data.unmatched_programs:
                    st.session_state.import_step = 'map_programs'
                elif data.unmatched_branches:
                    st.session_state.import_step = 'map_branches'
                else:
                    st.session_state.import_step = 'ready_to_import'
                st.rerun()

    # --- 8. State: Map Programs ---
    elif st.session_state.import_step == 'map_programs':
        st.markdown("#### Step 3: Map Programs")
        
        data: AffiliationCheckResult = st.session_state.import_validation_data
        selected_degree = st.session_state.get('import_degree', 'this degree')
        
        if not data.existing_programs:
            st.warning(
                f"⚠️ **Context Warning:** The selected degree (`{selected_degree}`) does not "
                "currently have any programs defined in the database."
                "\n\nYour CSV file contains program codes. It is recommended to select "
                "**'[-- Ignore These Rows --]'** for all items below unless you are "
                "certain you want to add new programs to this degree."
            )
        else:
            st.warning("⚠️ Your file contains Programs that are not in the database for this degree.")
            
        options = ["[Select Action]"] + data.existing_programs + ["[-- Create New Program --]", "[-- Ignore These Rows --]"]
        
        valid = True
        col1, col2 = st.columns([1, 2])
        col1.markdown("**CSV Value**")
        col2.markdown("**Action**")
        
        for code in sorted(list(data.unmatched_programs)):
            col1.write(f"`{code}`")  # <-- DISPLAY CSV VALUE
            key = f"map_program_{code}"
            
            # --- VALUEERROR FIX ---
            stored_action = st.session_state.import_mappings['program'].get(code, "[Select Action]")
            if stored_action == "[CREATE_NEW]":
                default_display_value = "[-- Create New Program --]"
            elif stored_action == "[IGNORE]":
                default_display_value = "[-- Ignore These Rows --]"
            else:
                default_display_value = stored_action

            try:
                default_index = options.index(default_display_value)
            except ValueError:
                default_index = 0 
            
            choice = col2.selectbox(
                f"map_{code}", 
                options=options, 
                key=key,
                index=default_index,
                label_visibility="collapsed"
            )
            # --- END VALUEERROR FIX ---
            
            if choice == "[-- Create New Program --]":
                st.session_state.import_mappings['program'][code] = "[CREATE_NEW]"
            elif choice == "[-- Ignore These Rows --]":
                st.session_state.import_mappings['program'][code] = "[IGNORE]"
            else:
                st.session_state.import_mappings['program'][code] = choice
            
            if choice == "[Select Action]":
                valid = False

        if st.button("➡️ Next", type="primary"):
            if not valid:
                st.error("❌ Please select an action for every item.")
            else:
                if data.unmatched_branches:
                    st.session_state.import_step = 'map_branches'
                else:
                    st.session_state.import_step = 'ready_to_import'
                st.rerun()

    # --- 9. State: Map Branches ---
    elif st.session_state.import_step == 'map_branches':
        st.markdown("#### Step 4: Map Branches")
        st.warning("⚠️ Your file contains Branches that are not in the database for this degree.")
        
        data: AffiliationCheckResult = st.session_state.import_validation_data
        options = ["[Select Action]"] + data.existing_branches + ["[-- Create New Branch --]", "[-- Ignore These Rows --]"]
        
        valid = True
        col1, col2 = st.columns([1, 2])
        col1.markdown("**CSV Value**")
        col2.markdown("**Action**")
        
        for code in sorted(list(data.unmatched_branches)):
            col1.write(f"`{code}`")  # <-- DISPLAY CSV VALUE
            key = f"map_branch_{code}"
            
            # --- VALUEERROR FIX ---
            stored_action = st.session_state.import_mappings['branch'].get(code, "[Select Action]")
            if stored_action == "[CREATE_NEW]":
                default_display_value = "[-- Create New Branch --]"
            elif stored_action == "[IGNORE]":
                default_display_value = "[-- Ignore These Rows --]"
            else:
                default_display_value = stored_action

            try:
                default_index = options.index(default_display_value)
            except ValueError:
                default_index = 0 
            
            choice = col2.selectbox(
                f"map_{code}", 
                options=options, 
                key=key,
                index=default_index,
                label_visibility="collapsed"
            )
            # --- END VALUEERROR FIX ---
            
            if choice == "[-- Create New Branch --]":
                st.session_state.import_mappings['branch'][code] = "[CREATE_NEW]"
            elif choice == "[-- Ignore These Rows --]":
                st.session_state.import_mappings['branch'][code] = "[IGNORE]"
            else:
                st.session_state.import_mappings['branch'][code] = choice
            
            if choice == "[Select Action]":
                valid = False

        if st.button("➡️ Next", type="primary"):
            if not valid:
                st.error("❌ Please select an action for every item.")
            else:
                st.session_state.import_step = 'ready_to_import'
                st.rerun()

    # --- 10. State: Ready to Import (Final Step) ---
    elif st.session_state.import_step == 'ready_to_import':
        st.markdown("#### Step 5: Review and Import")
        st.success("✅ All data is validated and mapped. Ready to import.")

        if 'import_df' not in st.session_state or st.session_state.import_df is None:
            st.error("❌ Session data was lost (e.g., page was fully reloaded). Please cancel and start over.")
            return

        with st.expander("🔍 Show Final Mappings"):
            st.json(st.session_state.import_mappings)
        
        df_to_import = st.session_state.import_df
        mappings = st.session_state.import_mappings
        
        col1, col2 = st.columns(2)
        
        with col1:
            if st.button("🧪 Dry Run", key="dry_run_combined"):
                conn = engine.connect()
                trans = conn.begin()
                try:
                    with st.spinner("Executing Dry Run... (no changes will be made)"):
                        
                        _apply_creations_from_mappings(
                            engine, selected_degree, mappings, 
                            conn_for_transaction=conn
                        )
                        
                        # --- NEW: UNPACK SKIPPED ---
                        errors, success, skipped = _import_combined_with_validation(
                            engine, df_to_import, dry_run=True, mappings=mappings, 
                            conn_for_transaction=conn
                        )
                            
                    if errors:
                        st.warning(f"⚠️ Dry run: {success} would succeed, {len(errors)} would fail")
                        st.dataframe(pd.DataFrame(errors)[['row', 'email', 'error']], use_container_width=True)
                    else:
                        st.success(f"✅ Dry run: All {success} records would import successfully")
                    
                    # --- NEW: DISPLAY SKIPPED ---
                    if skipped:
                        st.info(f"ℹ️ {len(skipped)} rows would be skipped due to your 'Ignore' mapping rules.")
                    
                except Exception as e:
                    log.error(f"Dry run failed with exception: {e}", exc_info=True)
                    st.error(f"Dry run failed. See details below:")
                    st.exception(e)
                finally:
                    trans.rollback()
                    conn.close()
                    log.info("Dry Run: All changes rolled back.")
                        
        with col2:
            if st.button("🚀 Execute Import", key="execute_combined", type="primary"):
                try:
                    with st.spinner("Executing Import..."):
                        
                        _apply_creations_from_mappings(engine, selected_degree, mappings)
                        
                        # --- NEW: UNPACK SKIPPED ---
                        errors, success, skipped = _import_combined_with_validation(
                            engine, df_to_import, dry_run=False, mappings=mappings
                        )
                    
                    if errors:
                        st.error(f"❌ Import completed with {len(errors)} errors out of {len(df_to_import)} rows")
                        st.download_button(
                            "Download Import Errors",
                            pd.DataFrame(errors).to_csv(index=False),
                            "combined_import_errors.csv",
                            "text/csv"
                        )
                    else:
                        st.success(f"✅ Successfully imported {success} record(s)")
                    
                    # --- NEW: DISPLAY SKIPPED ---
                    if skipped:
                        st.info(f"ℹ️ {len(skipped)} rows were skipped due to your 'Ignore' mapping rules.")
                    
                    # --- NEW: CACHE CLEAR ---
                    st.cache_data.clear()
                    log.info("Cleared all st.cache_data after successful import.")
                    
                    _reset_import_state()
                    st.rerun()

                except Exception as e:
                    _handle_error(e, "Import failed.")
